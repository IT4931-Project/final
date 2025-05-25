#!/usr/bin/env python3
"""
infer job

load lstm, read latest 30 days of data, predict next 7 days,
store in elasticsearch for visualization in kibana
"""

import os
import sys
import time
import logging
import datetime
import glob
import json
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from keras.models import load_model
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
import pymongo

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/inference.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("inference")

# Load environment variables
# Explicitly load .env from /app/.env and override existing env vars
# This requires .env to be copied to /app/.env in the Dockerfile
load_dotenv(dotenv_path='/app/.env', override=True)

# Configuration
PROCESSED_DATA_LOCAL_BACKUP_PATH = os.getenv('PROCESSED_DATA_LOCAL_BACKUP_PATH', '/app/data/processed_backup') # For fallback
MODEL_PATH = os.getenv('MODEL_PATH', '/app/data/models') # Local path for models
USE_HDFS = False # HDFS is removed

# MongoDB Configuration
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123') # Updated fallback
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin') # Updated fallback

# Elasticsearch Cluster Configuration
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch-master')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
ELASTICSEARCH_USERNAME = os.getenv('ELASTICSEARCH_USERNAME', 'elastic')
ELASTICSEARCH_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD', 'devpassword123') # Updated fallback for ES
USE_ELASTICSEARCH = os.getenv('USE_ELASTICSEARCH', 'true').lower() == 'true'

# Spark Configuration for distributed processing
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', 2))  # Default to 2 if not set

# Inference parameters
# These are typically fixed based on the model, but can be env vars if needed
SEQUENCE_LENGTH = int(os.getenv('SEQUENCE_LENGTH', 30))
FUTURE_DAYS = int(os.getenv('FUTURE_DAYS', 7))

# Stock symbols to make predictions for
SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')


def create_spark_session():
    """
    Create and configure Spark session for distributed inference
    
    Returns:
        pyspark.sql.SparkSession: Configured Spark session or None if failed
    """
    try:
        from pyspark.sql import SparkSession
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Model Inference") \
            .master(SPARK_MASTER) \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.instances", "1") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()
        
        logger.info("Created Spark session successfully for distributed inference")
        return spark
        
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        logger.warning("Will continue without Spark. HDFS functionality may be limited.")
        return None

def connect_to_elasticsearch():
    """
    Connect to Elasticsearch cluster
    
    Returns:
        Elasticsearch: Elasticsearch client or None if connection fails
    """
    if not USE_ELASTICSEARCH:
        logger.info("Elasticsearch is disabled, using MongoDB for predictions storage")
        return None
    
    try:
        # Connect to Elasticsearch cluster
        es = Elasticsearch(
            [f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"],
            basic_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
        )
        
        # Check connection
        if es.ping():
            logger.info("Connected to Elasticsearch cluster")
            
            # Create index for predictions if it doesn't exist
            # Using more optimized settings for distributed search
            if not es.indices.exists(index='stock_predictions'):
                es.indices.create(index='stock_predictions', body={
                    'settings': {
                        'number_of_shards': 1,  # Simplified for single ES node
                        'number_of_replicas': 0, # Simplified for single ES node
                        'refresh_interval': '1s'
                    },
                    'mappings': {
                        'properties': {
                            'symbol': {'type': 'keyword'},
                            'prediction_date': {'type': 'date'},
                            'target_date': {'type': 'date'},
                            'predicted_price': {'type': 'float'},
                            'actual_price': {'type': 'float'},
                            'timestamp': {'type': 'date'},
                            'distributed_info': {
                                'properties': {
                                    'processing_node': {'type': 'keyword'},
                                    'model_version': {'type': 'keyword'}
                                }
                            }
                        }
                    }
                })
                logger.info("Created 'stock_predictions' index with optimized sharding")
            
            return es
        else:
            logger.warning("Could not connect to Elasticsearch cluster")
            return None
    
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch: {str(e)}")
        return None


def connect_to_mongodb():
    """
    Connect to MongoDB
    
    Returns:
        pymongo.database.Database: MongoDB database connection
    """
    try:
        client = pymongo.MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            authSource=MONGO_AUTH_SOURCE, # Add authSource
            serverSelectionTimeoutMS=5000,  # 5 second timeout
            readPreference='primary'  # Use primary for single node
        )
        
        # Test the connection
        client.server_info()
        logger.info(f"Connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
        
        # Get database
        db = client[MONGO_DATABASE]
        
        # Create predictions collection if it doesn't exist (no sharding needed for single node)
        if 'predictions' not in db.list_collection_names():
            db.create_collection('predictions')
            logger.info("Created 'predictions' collection in MongoDB")
        
        return db
    
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return None

def load_latest_model_files(symbol, spark=None): # spark parameter is no longer used for HDFS
    """
    Load the latest model and associated files for a symbol from local storage.
    HDFS loading has been removed.
    
    Args:
        symbol (str): Stock symbol
        spark (SparkSession, optional): No longer used. Kept for signature compatibility if needed elsewhere.
    
    Returns:
        tuple: (model, scaler_data, metadata) or (None, None, None) if not found
    """
    logger.info(f"Attempting to load model files for {symbol} from local storage.")
    try:
        model_dir = os.path.join(MODEL_PATH, symbol)
        if not os.path.exists(model_dir):
            logger.error(f"No model directory found for {symbol} at {model_dir}")
            return None, None, None
        
        # Get the latest model version from latest.txt
        latest_path = os.path.join(model_dir, "latest.txt")
        if os.path.exists(latest_path):
            with open(latest_path, 'r') as f:
                timestamp = f.read().strip()
        else:
            # If latest.txt doesn't exist, find the latest model file by sorting
            model_files = glob.glob(os.path.join(model_dir, "model_*.h5"))
            if not model_files:
                logger.error(f"No model files (model_*.h5) found for {symbol} in {model_dir}")
                return None, None, None
            
            timestamps = [os.path.basename(f).replace("model_", "").replace(".h5", "") for f in model_files]
            timestamp = sorted(timestamps, reverse=True)[0] # Get the most recent
        
        logger.info(f"Using local model version {timestamp} for {symbol}")
        
        model_file_path = os.path.join(model_dir, f"model_{timestamp}.h5")
        scaler_file_path = os.path.join(model_dir, f"scaler_{timestamp}.npz")
        metadata_file_path = os.path.join(model_dir, f"metadata_{timestamp}.json")

        if not os.path.exists(model_file_path):
            logger.error(f"Model file not found: {model_file_path}")
            return None, None, None
        model = load_model(model_file_path)
        logger.info(f"Loaded model from local path: {model_file_path}")
        
        if not os.path.exists(scaler_file_path):
            logger.error(f"Scaler file not found: {scaler_file_path}")
            # Depending on strictness, you might return or allow continuation without scaler
            return None, None, None
        scaler_data = np.load(scaler_file_path)
        logger.info(f"Loaded scaler from local path: {scaler_file_path}")
        
        metadata = {}
        if os.path.exists(metadata_file_path):
            with open(metadata_file_path, 'r') as f:
                metadata = json.load(f)
            logger.info(f"Loaded metadata from local path: {metadata_file_path}")
        else:
            logger.warning(f"Metadata file not found: {metadata_file_path}. Proceeding without metadata.")
        
        return model, scaler_data, metadata
    
    except Exception as e:
        logger.error(f"Error loading model files for {symbol} from local storage: {str(e)}")
        return None, None, None

# HDFS related functions are removed:
# - load_model_from_hdfs
# - get_data_from_hdfs

def get_recent_data_from_mongodb(mongo_db, symbol, days=30):
    """
    Get recent data for the symbol from the 'processed_{symbol}' collection in MongoDB.
    """
    if mongo_db is None:
        logger.warning(f"MongoDB connection not available for fetching processed data for {symbol}.")
        return None
    try:
        collection_name = f"processed_{symbol}"
        if collection_name not in mongo_db.list_collection_names():
            logger.warning(f"Processed data collection '{collection_name}' not found in MongoDB for {symbol}.")
            return None

        # Fetch the most recent 'days' records, sorted by date
        # Assuming 'date' field is a datetime object or a string that can be sorted chronologically.
        # If 'date' is string, ensure it's in YYYY-MM-DD format for correct sorting.
        # For robust date handling, convert to datetime if it's string.
        # For simplicity here, we assume 'date' can be sorted.
        cursor = mongo_db[collection_name].find().sort("date", pymongo.DESCENDING).limit(days)
        data_list = list(cursor)

        if not data_list:
            logger.info(f"No processed data found in MongoDB collection '{collection_name}' for {symbol}.")
            return None

        df = pd.DataFrame(data_list)
        # Ensure 'date' column is datetime
        if 'date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=True) # Ensure ascending order for sequence
        
        # Drop MongoDB's _id if it exists
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])

        logger.info(f"Loaded {len(df)} recent records for {symbol} from MongoDB collection '{collection_name}'.")
        return df
    except Exception as e:
        logger.error(f"Error loading recent data for {symbol} from MongoDB: {str(e)}")
        return None

def get_recent_data_from_local_backup(symbol, days=30):
    """
    Get recent data for the symbol from local Parquet backup.
    """
    try:
        symbol_backup_path = os.path.join(PROCESSED_DATA_LOCAL_BACKUP_PATH, symbol)
        if not os.path.exists(symbol_backup_path):
            logger.warning(f"No local processed data backup directory found for {symbol} at {symbol_backup_path}")
            return None
        
        # Find the most recent backup_{timestamp} directory
        backup_dirs = [d for d in os.listdir(symbol_backup_path) if os.path.isdir(os.path.join(symbol_backup_path, d)) and d.startswith("backup_")]
        if not backup_dirs:
            logger.warning(f"No backup directories found in {symbol_backup_path}")
            return None
        
        backup_dirs.sort(reverse=True) # Get the latest timestamped backup
        latest_backup_dir_path = os.path.join(symbol_backup_path, backup_dirs[0])
        
        # Parquet files are directly inside this timestamped directory
        parquet_files = glob.glob(os.path.join(latest_backup_dir_path, "*.parquet"))
        if not parquet_files: # Should be at least one part-xxxxx.parquet file
             # Check for _SUCCESS file as an indicator if no direct parquet files found (less ideal)
            success_file = os.path.join(latest_backup_dir_path, "_SUCCESS")
            if os.path.exists(success_file): # Spark wrote output here
                 df = pd.read_parquet(latest_backup_dir_path) # Read the directory
            else:
                logger.warning(f"No Parquet files or _SUCCESS file found in {latest_backup_dir_path} for {symbol}")
                return None
        else: # Read the first part file, or the whole dir if read_parquet handles it
            df = pd.read_parquet(latest_backup_dir_path)


        logger.info(f"Loading processed data from local backup: {latest_backup_dir_path}")
        
        # Ensure 'date' column is datetime
        if 'date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        df = df.sort_values('date', ascending=True)
        if len(df) > days:
            df = df.tail(days) # Get the most recent 'days'
        
        logger.info(f"Loaded {len(df)} recent records for {symbol} from local backup.")
        return df
        
    except Exception as e:
        logger.error(f"Error loading recent data for {symbol} from local backup: {str(e)}")
        return None

def get_recent_data(mongo_db, symbol, days=30, spark=None): # spark parameter is no longer used for HDFS
    """
    Get recent data for the symbol.
    Prioritizes MongoDB, then falls back to local Parquet backup.
    HDFS loading has been removed.
    
    Args:
        mongo_db (pymongo.database.Database): MongoDB connection
        symbol (str): Stock symbol
        days (int): Number of recent days to retrieve
        spark (SparkSession, optional): No longer used.
        
    Returns:
        pd.DataFrame: Recent data or None if not available
    """
    logger.info(f"Fetching recent data for {symbol}...")
    
    # 1. Try MongoDB first
    df = get_recent_data_from_mongodb(mongo_db, symbol, days)
    if df is not None and not df.empty:
        return df
    
    logger.warning(f"Could not fetch recent data for {symbol} from MongoDB. Falling back to local backup.")
    
    # 2. Fallback to local Parquet backup
    df = get_recent_data_from_local_backup(symbol, days)
    if df is not None and not df.empty:
        return df

    logger.error(f"Failed to load recent data for {symbol} from all available sources.")
    return None

def prepare_data_for_prediction(df, scaler_data, metadata):
    """
    Prepare data for model prediction
    
    Args:
        df (pd.DataFrame): Recent data
        scaler_data (dict): Scaler data from npz file
        metadata (dict): Model metadata
        
    Returns:
        np.ndarray: Prepared data for model input
    """
    try:
        if df is None or len(df) < SEQUENCE_LENGTH:
            logger.error(f"Insufficient data for prediction (need at least {SEQUENCE_LENGTH} days)")
            return None
        
        # Reconstruct the scaler for inverse transform
        from sklearn.preprocessing import MinMaxScaler
        scaler = MinMaxScaler()
        scaler.scale_ = scaler_data['scale']
        scaler.min_ = scaler_data['min']
        scaler.data_min_ = scaler_data['data_min']
        scaler.data_max_ = scaler_data['data_max']
        scaler.data_range_ = scaler_data['data_range']
        scaler.feature_range = tuple(scaler_data['feature_range'])
        
        # Extract features
        try:
            features_used = metadata.get('features_used', 
                                         ['open', 'high', 'low', 'close', 'volume', 
                                          'sma_5', 'sma_20', 'rsi', 'macd', 
                                          'bb_upper', 'bb_lower', 'obv'])
            features = df[features_used].values
        except KeyError as e:
            logger.warning(f"Some features in metadata not found in data: {e}")
            # Fallback to using standard features
            features = df[['open', 'high', 'low', 'close', 'volume', 
                          'sma_5', 'sma_20', 'rsi', 'macd', 
                          'bb_upper', 'bb_lower', 'obv']].values
        
        # Scale the features
        scaled_features = scaler.transform(features)
        
        # Create sequence for LSTM (most recent SEQUENCE_LENGTH days)
        input_sequence = scaled_features[-SEQUENCE_LENGTH:]
        
        # Reshape for LSTM [samples, time steps, features]
        X = np.reshape(input_sequence, (1, input_sequence.shape[0], input_sequence.shape[1]))
        
        return X, scaler, df
        
    except Exception as e:
        logger.error(f"Error preparing data for prediction: {str(e)}")
        return None, None, None


def make_predictions(model, input_data, scaler, recent_data, metadata, symbol):
    """
    Make predictions using the loaded model
    
    Args:
        model (keras.Model): Loaded model
        input_data (np.ndarray): Prepared input data
        scaler (MinMaxScaler): Feature scaler
        recent_data (pd.DataFrame): Recent data
        metadata (dict): Model metadata
        symbol (str): Stock symbol
        
    Returns:
        pd.DataFrame: DataFrame with predictions
    """
    try:
        # Get target column index
        target_column = metadata.get('target_column', 'close')
        target_idx = metadata.get('target_column_idx', list(recent_data.columns).index(target_column))
        
        # Make prediction (output shape is [1, FUTURE_DAYS])
        logger.info(f"Making predictions for {symbol}")
        predicted_scaled = model.predict(input_data)
        
        # Create dummy array for inverse transformation
        dummy = np.zeros((FUTURE_DAYS, scaler.scale_.shape[0]))
        
        # Place the predictions in the dummy array at the target index
        for i in range(FUTURE_DAYS):
            dummy[i, target_idx] = predicted_scaled[0, i]
        
        # Inverse transform to get the actual predicted values
        predicted_prices = scaler.inverse_transform(dummy)[:, target_idx]
        
        # Get the last date in the data
        last_date = recent_data['date'].iloc[-1]
        
        # Generate dates for the predictions
        prediction_dates = [last_date + datetime.timedelta(days=i+1) for i in range(FUTURE_DAYS)]
        
        # Create prediction DataFrame
        predictions_df = pd.DataFrame({
            'symbol': symbol,
            'prediction_date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'target_date': [d.strftime('%Y-%m-%d') for d in prediction_dates],
            'predicted_price': predicted_prices,
            'actual_price': np.nan  # To be filled later when actual data becomes available
        })
        
        logger.info(f"Generated {len(predictions_df)} predictions for {symbol}")
        return predictions_df
        
    except Exception as e:
        logger.error(f"Error making predictions: {str(e)}")
        return None


def store_predictions_elasticsearch(es, predictions_df):
    """
    Store predictions in Elasticsearch
    
    Args:
        es (Elasticsearch): Elasticsearch client
        predictions_df (pd.DataFrame): Predictions data
        
    Returns:
        bool: True if successful, False otherwise
    """
    if es is None or predictions_df is None or predictions_df.empty:
        return False
    
    try:
        # Prepare actions for bulk insert
        actions = []
        for _, row in predictions_df.iterrows():
            action = {
                "_index": "stock_predictions",
                "_source": {
                    "symbol": row['symbol'],
                    "prediction_date": row['prediction_date'],
                    "target_date": row['target_date'],
                    "predicted_price": float(row['predicted_price']),
                    "actual_price": None if np.isnan(row['actual_price']) else float(row['actual_price']),
                    "timestamp": datetime.datetime.now().isoformat()
                }
            }
            actions.append(action)
        
        # Bulk insert
        if actions:
            helpers.bulk(es, actions)
            logger.info(f"Stored {len(actions)} predictions in Elasticsearch")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error storing predictions in Elasticsearch: {str(e)}")
        return False


def store_predictions_mongodb(db, predictions_df, hostname=None):
    """
    Store predictions in MongoDB sharded cluster
    
    Args:
        db (pymongo.database.Database): MongoDB connection
        predictions_df (pd.DataFrame): Predictions data
        hostname (str, optional): Hostname of the processing node
        
    Returns:
        bool: True if successful, False otherwise
    """
    if db is None or predictions_df is None or predictions_df.empty:
        return False
    
    try:
        # Convert DataFrame to list of dictionaries
        records = predictions_df.to_dict('records')
        
        # Add additional metadata for distributed processing
        for record in records:
            # Add timestamp
            record['timestamp'] = datetime.datetime.now()
            if 'actual_price' in record and np.isnan(record['actual_price']):
                record['actual_price'] = None
                
            # Add distributed processing information
            record['distributed_info'] = {
                'processing_node': hostname or 'unknown',
                'process_id': os.getpid(),
                'process_time': datetime.datetime.now().isoformat()
            }
        
        # Insert to predictions collection using bulk write with ordered=False for better performance
        # in a sharded environment
        result = db.predictions.insert_many(records, ordered=False)
        
        logger.info(f"Stored {len(result.inserted_ids)} predictions in MongoDB sharded cluster")
        return True
        
    except Exception as e:
        logger.error(f"Error storing predictions in MongoDB: {str(e)}")
        return False


def process_symbol(es_client, mongo_db, symbol, spark=None):
    """
    Process a single symbol for predictions with distributed resources
    
    Args:
        es_client (Elasticsearch): Elasticsearch client
        mongo_db (pymongo.database.Database): MongoDB connection
        symbol (str): Stock symbol
        spark (SparkSession, optional): Spark session for HDFS access
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get hostname for distributed tracking
        hostname = socket.gethostname()
        logger.info(f"Processing symbol {symbol} for predictions on node {hostname}")
        
        # Load model files from local storage (HDFS part removed)
        model, scaler_data, metadata = load_latest_model_files(symbol) # spark arg removed from call
        if model is None:
            logger.error(f"Failed to load model for {symbol}")
            return False
        
        # Get recent data (prioritizes MongoDB, then local backup)
        recent_data = get_recent_data(mongo_db, symbol, days=SEQUENCE_LENGTH) # spark arg removed
        if recent_data is None or recent_data.empty:
            logger.error(f"Failed to get recent data for {symbol}")
            return False
        
        # Prepare data for prediction
        input_data, scaler, recent_data = prepare_data_for_prediction(recent_data, scaler_data, metadata)
        if input_data is None:
            logger.error(f"Failed to prepare input data for {symbol}")
            return False
        
        # Make predictions
        predictions_df = make_predictions(model, input_data, scaler, recent_data, metadata, symbol)
        if predictions_df is None:
            logger.error(f"Failed to make predictions for {symbol}")
            return False
        
        # Add distributed processing information to predictions
        predictions_df['processing_node'] = hostname
        predictions_df['distributed'] = USE_HDFS
        
        # Store predictions in Elasticsearch if available
        if es_client is not None:
            store_predictions_elasticsearch(es_client, predictions_df)
        
        # Always store in MongoDB as a backup
        store_predictions_mongodb(mongo_db, predictions_df, hostname)
        
        logger.info(f"Successfully processed predictions for {symbol} on node {hostname}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing predictions for {symbol}: {str(e)}")
        return False


def process_symbols_parallel(es_client, mongo_db, spark):
    """
    Process symbols in parallel using Spark
    
    Args:
        es_client (Elasticsearch): Elasticsearch client
        mongo_db (pymongo.database.Database): MongoDB connection
        spark (SparkSession): Spark session
        
    Returns:
        int: Number of successfully processed symbols
    """
    logger.info("Processing symbols in parallel using Spark")
    
    # Create an RDD from the list of symbols
    symbols_rdd = spark.sparkContext.parallelize(SYMBOLS, NUM_PARTITIONS)
    
    # Define a function to process each symbol
    def process_symbol_wrapper(symbol):
        symbol = symbol.strip()
        try:
            # Cannot pass es_client and mongo_db directly to executors
            # Need to create new connections within the executor
            local_es = connect_to_elasticsearch()
            local_mongo = connect_to_mongodb()
            
            # Get a local SparkSession in the executor
            from pyspark.sql import SparkSession
            local_spark = SparkSession.builder.getOrCreate()
            
            # Process the symbol
            success = process_symbol(local_es, local_mongo, symbol, local_spark)
            return (symbol, success)
        except Exception as e:
            logger.error(f"Error processing symbol {symbol} in parallel: {str(e)}")
            return (symbol, False)
    
    # Process symbols in parallel
    results = symbols_rdd.map(process_symbol_wrapper).collect()
    
    # Count successes
    success_count = sum(1 for _, success in results if success)
    
    # Log results for each symbol
    for symbol, success in results:
        logger.info(f"Symbol {symbol}: {'Success' if success else 'Failed'}")
    
    return success_count

def main():
    """Main function that runs the distributed inference job"""
    logger.info("Starting Distributed Model Inference Job")
    
    # Create Spark session for distributed processing and HDFS access
    spark = create_spark_session()
    
    # Connect to Elasticsearch
    es_client = connect_to_elasticsearch()
    
    # Connect to MongoDB
    mongo_db = connect_to_mongodb()
    if mongo_db is None:
        logger.error("Could not connect to MongoDB, exiting")
        return
    
    # Set up TensorFlow for better GPU utilization if available
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        logger.info(f"Found {len(gpus)} GPUs")
        for gpu in gpus:
            try:
                tf.config.experimental.set_memory_growth(gpu, True)
            except RuntimeError as e:
                logger.warning(f"Error setting memory growth: {e}")
    else:
        logger.info("No GPUs found, using CPU")
    
    # Process symbols based on whether we have Spark available for distributed processing
    if spark is not None:
        # Process symbols in parallel using Spark
        success_count = process_symbols_parallel(es_client, mongo_db, spark)
        logger.info(f"Distributed inference job completed. Successfully processed {success_count}/{len(SYMBOLS)} symbols using Spark parallelism")
    else:
        # Fall back to sequential processing
        logger.warning("Spark session not available, falling back to sequential processing")
        success_count = 0
        for symbol in SYMBOLS:
            symbol = symbol.strip()
            if process_symbol(es_client, mongo_db, symbol):
                success_count += 1
        logger.info(f"Sequential inference job completed. Successfully processed {success_count}/{len(SYMBOLS)} symbols")
    
    # Clean up
    if spark is not None:
        spark.stop()


if __name__ == "__main__":
    main()
