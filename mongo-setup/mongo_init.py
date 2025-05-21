#!/usr/bin/env python3
"""
MongoDB Single Instance Setup Script
This script replaces the original sharded cluster setup with a simpler single instance MongoDB setup
to minimize potential errors.
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

# MongoDB connection URL - now using single instance instead of sharded cluster
MONGO_URL = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@mongodb:27017/?authSource=admin"

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
            time.sleep(interval)
            
        if attempts >= max_attempts:
            logger.error(f"Could not connect to {host_details} after {max_attempts} attempts")
            raise

def setup_database():
    """Setup finance_data database and collections"""
    logger.info("=== SETTING UP FINANCE_DATA DATABASE AND COLLECTIONS ===")
    
    try:
        client = connect_with_retry(MONGO_URL)
        
        # Create finance_data database
        finance_db = client.finance_data
        
        # Setup collections for common stock symbols
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
        
        # Setup company_info collection
        setup_collection(
            db=finance_db,
            coll_name="company_info",
            indexes=[{"ticker": 1}]
        )
        
        # Setup predictions collection
        setup_collection(
            db=finance_db,
            coll_name="predictions",
            indexes=[
                {"symbol": 1, "prediction_date": 1},
                {"target_date": 1, "predicted_price": 1},
                {"prediction_date": 1, "accuracy_percent": -1},
                {"symbol": 1, "accuracy_percent": -1}
            ]
        )
        
        # Setup collections for each symbol
        for symbol in symbols:
            # Setup stock data collection
            stock_coll_name = f"stock_{symbol}"
            setup_collection(
                db=finance_db,
                coll_name=stock_coll_name,
                indexes=[
                    {"date": 1},
                    {"ticker": 1, "date": 1},
                    {"date": 1, "close": 1},
                    {"date": 1, "volume": 1}
                ]
            )
            
            # Setup stock actions collection
            actions_coll_name = f"stock_{symbol}_actions"
            setup_collection(
                db=finance_db,
                coll_name=actions_coll_name,
                indexes=[
                    {"date": 1},
                    {"ticker": 1, "date": 1},
                    {"date": 1, "dividends": 1},
                    {"dividends": -1}
                ]
            )
        
        # Create analytics_summary collection
        setup_collection(
            db=finance_db,
            coll_name="analytics_summary",
            indexes=[
                {"date": 1},
                {"most_accurate_model": 1}
            ]
        )
        
        # Create prediction_accuracy_timeseries view
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
        
        client.close()
        logger.info("=== DATABASE SETUP COMPLETED SUCCESSFULLY ===")
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        raise

def setup_collection(db, coll_name, indexes=None):
    """Setup a collection with indexes"""
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
    except Exception as e:
        logger.error(f"Error setting up collection {coll_name}: {str(e)}")
        raise

def main():
    """Main function to set up MongoDB database"""
    try:
        logger.info("Starting MongoDB Single Instance Setup")
        
        # Wait for MongoDB service to start
        logger.info("Waiting for MongoDB service to be available...")
        time.sleep(30)  # Wait time for MongoDB startup
        
        # Verify DNS resolution for the MongoDB host
        import socket
        try:
            logger.info("Resolving hostname mongodb...")
            ip_address = socket.gethostbyname("mongodb")
            logger.info(f"Successfully resolved mongodb to {ip_address}")
        except socket.gaierror as e:
            logger.warning(f"Could not resolve mongodb: {str(e)}")
        
        # Setup database and collections
        setup_database()
        
        logger.info("=== MONGODB SETUP COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"MongoDB setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
