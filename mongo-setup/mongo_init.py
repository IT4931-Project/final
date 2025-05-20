#!/usr/bin/env python3
"""
MongoDB Sharded Cluster Setup and Optimization Script
This script replaces the original setup.sh and optimization.sh bash scripts
with a more maintainable Python implementation.
"""

import os
import sys
import time
import logging
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("mongo-setup")

# Configuration
MONGO_ROOT_USERNAME = os.environ.get('MONGO_ROOT_USERNAME', 'root')
MONGO_ROOT_PASSWORD = os.environ.get('MONGO_ROOT_PASSWORD', 'example')
MAX_RETRY_ATTEMPTS = 30
RETRY_INTERVAL_SECONDS = 2

# MongoDB connection URLs
CONFIG_SERVER_URL = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@mongo-config1:27017/?authSource=admin"
SHARD1_URL = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@mongo-shard1:27017/?authSource=admin"
SHARD2_URL = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@mongo-shard2:27017/?authSource=admin"
ROUTER_URL = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@mongo-router:27017/?authSource=admin"

def connect_with_retry(mongo_url, max_attempts=MAX_RETRY_ATTEMPTS, interval=RETRY_INTERVAL_SECONDS):
    """Connect to MongoDB with retry logic"""
    attempts = 0
    host_details = mongo_url.split('@')[1].split('/')[0]
    
    while attempts < max_attempts:
        try:
            logger.info(f"Attempting to connect to {host_details} (attempt {attempts+1}/{max_attempts})")
            client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
            # Verify connection is working
            client.admin.command('ping')
            logger.info(f"Successfully connected to {host_details}")
            return client
        except Exception as e:
            attempts += 1
            logger.warning(f"Failed to connect to {host_details}: {str(e)}")
            if attempts >= max_attempts:
                logger.error(f"Could not connect to {host_details} after {max_attempts} attempts")
                raise
            time.sleep(interval)

def init_config_server_replica_set():
    """Initialize the config server replica set"""
    logger.info("=== CONFIGURING CONFIG SERVER REPLICA SET ===")
    
    try:
        client = connect_with_retry(CONFIG_SERVER_URL)
        admin_db = client.admin
        
        # Check if replica set already initialized
        try:
            status = admin_db.command('replSetGetStatus')
            logger.info("Config server replica set already initialized")
        except errors.OperationFailure as e:
            if "no replset config has been received" in str(e) or "not running with --replSet" in str(e):
                logger.info("Initializing config server replica set...")
                config = {
                    "_id": "configrs",
                    "configsvr": True,
                    "members": [
                        {"_id": 0, "host": "mongo-config1:27017"},
                        {"_id": 1, "host": "mongo-config2:27017"},
                        {"_id": 2, "host": "mongo-config3:27017"}
                    ]
                }
                result = admin_db.command("replSetInitiate", config)
                logger.info(f"Config server replica set initialization result: {result}")
            else:
                raise
        
        # Wait for primary election
        logger.info("Waiting for config server replica set to stabilize...")
        primary_ready = False
        attempts = 0
        max_attempts = 30
        
        while not primary_ready and attempts < max_attempts:
            try:
                status = admin_db.command('replSetGetStatus')
                primary_member = next((m for m in status["members"] if m["state"] == 1), None)
                
                if primary_member:
                    logger.info(f"Config server replica set has a primary: {primary_member['name']}")
                    primary_ready = True
                else:
                    logger.info("No primary elected yet in config server replica set. Waiting...")
            except Exception as e:
                logger.warning(f"Error checking replica set status: {str(e)}")
            
            if not primary_ready:
                attempts += 1
                time.sleep(2)
        
        if not primary_ready:
            raise Exception("Config server replica set failed to elect a primary after multiple attempts")
            
        client.close()
    except Exception as e:
        logger.error(f"Error initializing config server replica set: {str(e)}")
        raise

def init_shard_replica_set(shard_number, shard_url):
    """Initialize a shard replica set"""
    logger.info(f"=== CONFIGURING SHARD{shard_number} REPLICA SET ===")
    
    try:
        client = connect_with_retry(shard_url)
        admin_db = client.admin
        
        # Check if replica set already initialized
        try:
            status = admin_db.command('replSetGetStatus')
            logger.info(f"Shard{shard_number} replica set already initialized")
        except errors.OperationFailure as e:
            if "no replset config has been received" in str(e) or "not running with --replSet" in str(e):
                logger.info(f"Initializing shard{shard_number} replica set...")
                config = {
                    "_id": f"shard{shard_number}rs",
                    "members": [
                        {"_id": 0, "host": f"mongo-shard{shard_number}:27017"}
                    ]
                }
                result = admin_db.command("replSetInitiate", config)
                logger.info(f"Shard{shard_number} replica set initialization result: {result}")
            else:
                raise
        
        client.close()
    except Exception as e:
        logger.error(f"Error initializing shard{shard_number} replica set: {str(e)}")
        raise

def add_shards_to_cluster():
    """Add shards to the MongoDB cluster"""
    logger.info("=== CONFIGURING MONGOS ROUTER AND ADDING SHARDS ===")
    
    try:
        # Wait for shard replica sets to be ready
        logger.info("Waiting for shard replica sets to be ready...")
        time.sleep(10)
        
        client = connect_with_retry(ROUTER_URL)
        admin_db = client.admin
        
        # Check existing shards
        existing_shards = admin_db.command("listShards")
        existing_shard_names = [s["_id"] for s in existing_shards.get("shards", [])]
        
        # Add shard1 if not already added
        if "shard1rs" not in existing_shard_names:
            logger.info("Adding shard1 to the cluster...")
            result = admin_db.command("addShard", "shard1rs/mongo-shard1:27017")
            logger.info(f"Add shard1 result: {result}")
        else:
            logger.info("Shard1 already added to the cluster")
        
        # Add shard2 if not already added
        if "shard2rs" not in existing_shard_names:
            logger.info("Adding shard2 to the cluster...")
            result = admin_db.command("addShard", "shard2rs/mongo-shard2:27017")
            logger.info(f"Add shard2 result: {result}")
        else:
            logger.info("Shard2 already added to the cluster")
        
        # Display shard status
        shards = admin_db.command("listShards")
        logger.info(f"Current shard status: {shards}")
        
        client.close()
    except Exception as e:
        logger.error(f"Error adding shards to cluster: {str(e)}")
        raise

def configure_sharding():
    """Configure database and collections for sharding"""
    logger.info("=== CONFIGURING DATABASE AND COLLECTIONS FOR SHARDING ===")
    
    try:
        client = connect_with_retry(ROUTER_URL)
        admin_db = client.admin
        
        # Enable sharding for the finance_data database
        try:
            result = admin_db.command("enableSharding", "finance_data")
            logger.info(f"Enable sharding for finance_data result: {result}")
        except errors.OperationFailure as e:
            if "already enabled" in str(e):
                logger.info("Sharding already enabled for finance_data database")
            else:
                raise
        
        # Get finance_data database
        finance_db = client.finance_data
        
        # Setup collections for common stock symbols
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
        
        # Setup company_info collection
        setup_collection(
            client=client,
            db=finance_db,
            coll_name="company_info",
            shard_key={"ticker": 1}
        )
        
        # Setup predictions collection
        setup_collection(
            client=client,
            db=finance_db,
            coll_name="predictions",
            shard_key={"symbol": 1, "prediction_date": 1}
        )
        
        # Setup collections for each symbol
        for symbol in symbols:
            # Setup stock data collection
            stock_coll_name = f"stock_{symbol}"
            setup_collection(
                client=client,
                db=finance_db,
                coll_name=stock_coll_name,
                shard_key={"ticker": 1, "date": 1},
                indexes=[
                    {"date": 1},
                    {"ticker": 1, "date": 1}
                ]
            )
            
            # Setup stock actions collection
            actions_coll_name = f"stock_{symbol}_actions"
            setup_collection(
                client=client,
                db=finance_db,
                coll_name=actions_coll_name,
                shard_key={"ticker": 1, "date": 1},
                indexes=[
                    {"date": 1},
                    {"ticker": 1, "date": 1}
                ]
            )
        
        client.close()
    except Exception as e:
        logger.error(f"Error configuring sharding: {str(e)}")
        raise

def setup_collection(client, db, coll_name, shard_key, indexes=None):
    """Setup a collection for sharding with indexes"""
    try:
        # Create collection if it doesn't exist
        try:
            db.create_collection(coll_name)
            logger.info(f"Created collection {coll_name}")
        except errors.CollectionInvalid as e:
            if "already exists" in str(e):
                logger.info(f"Collection {coll_name} already exists")
            else:
                raise
        
        # Create indexes
        if indexes:
            for index in indexes:
                db[coll_name].create_index(index)
                logger.info(f"Created index {index} on collection {coll_name}")
        
        # Create index for shard key
        key_index = list(shard_key.keys())[0]
        db[coll_name].create_index(key_index)
        logger.info(f"Created index {key_index} on collection {coll_name}")
        
        # Shard the collection
        try:
            result = client.admin.command(
                "shardCollection",
                f"finance_data.{coll_name}",
                key=shard_key
            )
            logger.info(f"Shard collection result for {coll_name}: {result}")
        except errors.OperationFailure as e:
            if "already sharded" in str(e):
                logger.info(f"Collection {coll_name} already sharded")
            else:
                raise
    except Exception as e:
        logger.error(f"Error setting up collection {coll_name}: {str(e)}")
        raise

def optimize_mongodb():
    """Optimize MongoDB indexes and configuration"""
    logger.info("=== OPTIMIZING MONGODB INDEXES AND CONFIGURATION ===")
    
    try:
        client = connect_with_retry(ROUTER_URL)
        
        # Configure chunk size for better distribution
        config_db = client.config
        chunk_size_result = config_db.settings.update_one(
            {"_id": "chunksize"},
            {"$set": {"value": 32}},
            upsert=True
        )
        logger.info(f"Set optimal chunk size to 32MB: {chunk_size_result.acknowledged}")
        
        finance_db = client.finance_data
        
        # Optimize company_info collection
        finance_db.company_info.create_index([("sector", 1)])
        finance_db.company_info.create_index([("industry", 1)])
        finance_db.company_info.create_index([("marketCap", -1)])
        finance_db.company_info.create_index([("ticker", 1), ("sector", 1)])
        logger.info("Created indexes for company_info collection")
        
        # Optimize stock collections
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
        for symbol in symbols:
            optimize_stock_collection(finance_db, symbol)
        
        # Optimize predictions collection
        finance_db.predictions.create_index([("target_date", 1), ("predicted_price", 1)])
        finance_db.predictions.create_index([("prediction_date", 1), ("accuracy_percent", -1)])
        finance_db.predictions.create_index([("symbol", 1), ("accuracy_percent", -1)])
        finance_db.predictions.create_index([
            ("symbol", 1), 
            ("target_date", 1), 
            ("predicted_price", 1), 
            ("actual_price", 1)
        ])
        
        # Create a time-series view collection for time-based prediction analysis
        try:
            finance_db.command({
                "create": "prediction_accuracy_timeseries",
                "viewOn": "predictions",
                "pipeline": [
                    {"$match": {"actual_price": {"$ne": None}}},
                    {"$project": {
                        "symbol": 1,
                        "prediction_date": 1,
                        "target_date": 1,
                        "accuracy_percent": {
                            "$multiply": [
                                100,
                                {"$subtract": [
                                    1,
                                    {"$abs": {"$divide": [
                                        {"$subtract": ["$predicted_price", "$actual_price"]},
                                        "$actual_price"
                                    ]}}
                                ]}
                            ]
                        }
                    }}
                ]
            })
            logger.info("Created prediction_accuracy_timeseries view for analytics")
        except errors.OperationFailure as e:
            if "already exists" in str(e):
                logger.info("prediction_accuracy_timeseries view already exists")
            else:
                raise
        
        # Add wildcard index for flexible queries
        try:
            finance_db.predictions.create_index([("symbol.$**", 1)])
            logger.info("Created wildcard index for flexible symbol queries")
        except Exception as e:
            logger.warning(f"Error creating wildcard index: {str(e)}")
        
        # Create analytics_summary collection if it doesn't exist
        try:
            finance_db.create_collection("analytics_summary")
            finance_db.analytics_summary.create_index([("date", 1)], unique=True)
            finance_db.analytics_summary.create_index([("most_accurate_model", 1)])
            logger.info("Created analytics_summary collection for model performance tracking")
        except errors.CollectionInvalid as e:
            if "already exists" in str(e):
                logger.info("analytics_summary collection already exists")
            else:
                raise
        
        client.close()
    except Exception as e:
        logger.error(f"Error during optimization: {str(e)}")
        raise

def optimize_stock_collection(db, symbol):
    """Optimize a specific stock collection with technical indicators"""
    coll_name = f"stock_{symbol}"
    
    # Check if collection exists
    if coll_name in db.list_collection_names():
        # Add indexes for time-based queries with specific fields
        # For technical indicators
        db[coll_name].create_index([("date", 1), ("close", 1)])
        db[coll_name].create_index([("date", 1), ("volume", 1)])
        
        # For technical indicator queries
        db[coll_name].create_index([("date", 1), ("sma_20", 1)])
        db[coll_name].create_index([("date", 1), ("rsi", 1)])
        db[coll_name].create_index([("date", 1), ("macd", 1)])
        
        # For volatility analysis
        db[coll_name].create_index([("date", 1), ("bb_upper", 1), ("bb_lower", 1)])
        
        # For correlation queries
        db[coll_name].create_index([("date", 1), ("ticker", 1), ("close", 1), ("volume", 1)])
        
        # Compound indexes for technical analysis
        db[coll_name].create_index([("rsi", 1), ("date", -1)])
        db[coll_name].create_index([("macd", 1), ("date", -1)])
        
        logger.info(f"Optimized {coll_name} with technical indicator indexes")
    
    # Optimize actions collection
    actions_coll_name = f"stock_{symbol}_actions"
    if actions_coll_name in db.list_collection_names():
        # Add specialized indexes for dividend information
        db[actions_coll_name].create_index([("date", 1), ("dividends", 1)])
        db[actions_coll_name].create_index([("dividends", -1)])  # For finding highest dividends
        logger.info(f"Optimized {actions_coll_name} for dividend analysis")

def display_final_status():
    """Display final shard status"""
    logger.info("=== FINAL SHARD STATUS ===")
    
    try:
        client = connect_with_retry(ROUTER_URL)
        admin_db = client.admin
        
        # Get final shard status
        final_status = admin_db.command("listShards")
        logger.info(f"Final shard status: {final_status}")
        
        # Get sharding status
        server_status = admin_db.command("serverStatus")
        if "sharding" in server_status:
            logger.info(f"Sharding status: {server_status['sharding']}")
        
        client.close()
    except Exception as e:
        logger.error(f"Error getting final status: {str(e)}")

def main():
    """Main function to set up MongoDB sharded cluster"""
    try:
        logger.info("Starting MongoDB Sharded Cluster Setup and Optimization")
        
        # Wait for MongoDB services to start
        logger.info("Waiting for MongoDB services to be available...")
        time.sleep(30)
        
        # Step 1: Initialize config server replica set
        init_config_server_replica_set()
        
        # Step 2: Initialize shard replica sets
        init_shard_replica_set(1, SHARD1_URL)
        init_shard_replica_set(2, SHARD2_URL)
        
        # Step 3: Add shards to the cluster
        add_shards_to_cluster()
        
        # Step 4: Configure database and collections for sharding
        configure_sharding()
        
        # Step 5: Optimize MongoDB
        optimize_mongodb()
        
        # Step 6: Display final status
        display_final_status()
        
        logger.info("=== MONGODB SHARDED CLUSTER SETUP COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"MongoDB setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
