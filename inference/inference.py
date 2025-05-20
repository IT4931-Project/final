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
load_dotenv()

# Configuration
PROCESSED_DATA_PATH = os.getenv('PROCESSED_DATA_PATH', '/app/data/processed')
MODEL_PATH = os.getenv('MODEL_PATH', '/app/data/models')

# HDFS Configuration
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode')
HDFS_NAMENODE_PORT = os.getenv('HDFS_NAMENODE_PORT', '8020')
HDFS_PROCESSED_PATH = os.getenv('HDFS_PROCESSED_PATH', '/user/finance/processed')
HDFS_MODEL_PATH = os.getenv('HDFS_MODEL_PATH', '/user/finance/models')
USE_HDFS = os.getenv('USE_HDFS', 'true').lower() == 'true'

# MongoDB Sharded Cluster Configuration
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo-router')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'password')

# Elasticsearch Cluster Configuration
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch-master')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
ELASTICSEARCH_USERNAME = os.getenv('ELASTICSEARCH_USERNAME', 'elastic')
ELASTICSEARCH_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD', 'changeme')
USE_ELASTICSEARCH = os.getenv('USE_ELASTICSEARCH', 'true').lower() == 'true'

# Spark Configuration for distributed processing
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '12'))  # Default to 3 workers * 4 cores

# Inference parameters
SEQUENCE_LENGTH = 30  # Number of days to look back
FUTURE_DAYS = 7       # Number of days to predict

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
            .appName("Distributed Model Inference") \
            .master(SPARK_MASTER) \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}") \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.hadoop.dfs.replication", "3") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
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
                        'number_of_shards': 3,
                        'number_of_replicas': 2,
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
    Connect to MongoDB sharded cluster
    
    Returns:
        pymongo.database.Database: MongoDB database connection
    """
    try:
        client = pymongo.MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            serverSelectionTimeoutMS=5000,  # 5 second timeout
            readPreference='secondaryPreferred'  # Use secondary nodes for reads by default for better performance
        )
        
        # Test the connection
        client.server_info()
        logger.info(f"Connected to MongoDB sharded cluster at {MONGO_HOST}:{MONGO_PORT}")
        
        # Get database
        db = client[MONGO_DATABASE]
        
        # Create predictions collection if it doesn't exist
        if 'predictions' not in db.list_collection_names():
            # Create a sharded collection with a good shard key for predictions
            db.command('create', 'predictions')
            db.command('shardCollection', f'{MONGO_DATABASE}.predictions', 
                       key={'symbol': 1, 'prediction_date': 1})
            logger.info("Created sharded 'predictions' collection in MongoDB")
        
        return db
    
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return None


def load_latest_model_files(symbol, spark=None):
    """
    Load the latest model and associated files for a symbol from HDFS or local storage
    
    Args:
        symbol (str): Stock symbol
        spark (SparkSession, optional): Spark session for HDFS access
    
    Returns:
        tuple: (model, scaler_data, metadata) or (None, None, None) if not found
    """
    if USE_HDFS and spark is not None:
        # Try to load from HDFS first
        try:
            return load_model_from_hdfs(symbol, spark)
        except Exception as e:
            logger.error(f"Error loading model from HDFS: {str(e)}")
            logger.warning(f"Falling back to local model for {symbol}")
            
    # Fall back to local filesystem if HDFS fails or is disabled
    try:
        model_dir = os.path.join(MODEL_PATH, symbol)
        if not os.path.exists(model_dir):
            logger.error(f"No model directory found for {symbol}")
            return None, None, None
        
        # Get the latest model version from latest.txt
        latest_path = os.path.join(model_dir, "latest.txt")
        if os.path.exists(latest_path):
            with open(latest_path, 'r') as f:
                timestamp = f.read().strip()
        else:
            # If latest.txt doesn't exist, find the latest model file
            model_files = glob.glob(os.path.join(model_dir, "model_*.h5"))
            if not model_files:
                logger.error(f"No model files found for {symbol}")
                return None, None, None
            
            # Extract timestamps from filenames and get the latest
            timestamps = [os.path.basename(f).replace("model_", "").replace(".h5", "") for f in model_files]
            timestamp = sorted(timestamps)[-1]
        
        logger.info(f"Using local model version {timestamp} for {symbol}")
        
        # Load the model
        model_path = os.path.join(model_dir, f"model_{timestamp}.h5")
        if not os.path.exists(model_path):
            logger.error(f"Model file not found: {model_path}")
            return None, None, None
        
        model = load_model(model_path)
        logger.info(f"Loaded model from local path: {model_path}")
        
        # Load the scaler
        scaler_path = os.path.join(model_dir, f"scaler_{timestamp}.npz")
        if not os.path.exists(scaler_path):
            logger.error(f"Scaler file not found: {scaler_path}")
            return None, None, None
        
        scaler_data = np.load(scaler_path)
        logger.info(f"Loaded scaler from local path: {scaler_path}")
        
        # Load metadata
        metadata_path = os.path.join(model_dir, f"metadata_{timestamp}.json")
        if not os.path.exists(metadata_path):
            logger.warning(f"Metadata file not found: {metadata_path}")
            metadata = {}
        else:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            logger.info(f"Loaded metadata from local path: {metadata_path}")
        
        return model, scaler_data, metadata
    
    except Exception as e:
        logger.error(f"Error loading model files for {symbol}: {str(e)}")
        return None, None, None

def load_model_from_hdfs(symbol, spark):
    """
    Load model files from HDFS
    
    Args:
        symbol (str): Stock symbol
        spark (SparkSession): Spark session for HDFS access
    
    Returns:
        tuple: (model, scaler_data, metadata) or (None, None, None) if not found
    """
    try:
        # Get Hadoop filesystem
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        
        # HDFS path for this symbol's models
        hdfs_symbol_dir = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_MODEL_PATH}/{symbol}"
        hdfs_dir_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_symbol_dir)
        
        if not fs.exists(hdfs_dir_path):
            logger.error(f"HDFS directory not found: {hdfs_symbol_dir}")
            return None, None, None
        
        # Get the latest model version from latest.txt in HDFS
        hdfs_latest_path = spark._jvm.org.apache.hadoop.fs.Path(f"{hdfs_symbol_dir}/latest.txt")
        if fs.exists(hdfs_latest_path):
            # Create a BufferedReader to read the file content
            input_stream = fs.open(hdfs_latest_path)
            reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(input_stream))
            timestamp = reader.readLine().strip()
            input_stream.close()
        else:
            # If latest.txt doesn't exist, find the latest model file in HDFS
            file_statuses = fs.listStatus(hdfs_dir_path)
            if file_statuses is None or len(file_statuses) == 0:
                logger.error(f"No files found in HDFS path: {hdfs_symbol_dir}")
                return None, None, None
                
            # Look for model files and sort by timestamp in the name
            model_files = []
            for status in file_statuses:
                path = status.getPath().toString()
                if "model_" in path and path.endswith(".h5"):
                    model_files.append(path)
                    
            if not model_files:
                logger.error(f"No model files found in HDFS for {symbol}")
                return None, None, None
                
            # Sort model files by timestamp in filename (descending)
            model_files.sort(reverse=True)
            
            # Extract timestamp from the latest model file path
            timestamp = model_files[0].split("model_")[1].split(".h5")[0]
        
        logger.info(f"Using HDFS model version {timestamp} for {symbol}")
        
        # Temporary local paths for model files
        tmp_dir = "/tmp/model_files"
        os.makedirs(tmp_dir, exist_ok=True)
        local_model_path = os.path.join(tmp_dir, f"model_{timestamp}.h5")
        local_scaler_path = os.path.join(tmp_dir, f"scaler_{timestamp}.npz")
        local_metadata_path = os.path.join(tmp_dir, f"metadata_{timestamp}.json")
        
        # Download model file from HDFS
        hdfs_model_path = spark._jvm.org.apache.hadoop.fs.Path(f"{hdfs_symbol_dir}/model_{timestamp}.h5")
        if not fs.exists(hdfs_model_path):
            logger.error(f"Model file not found in HDFS: {hdfs_model_path}")
            return None, None, None
            
        output_stream = fs.open(hdfs_model_path)
        java_input_stream = output_stream.getWrappedInputStream()
        
        # Convert Java InputStream to Python bytes
        java_bytes = spark._jvm.org.apache.commons.io.IOUtils.toByteArray(java_input_stream)
        py_bytes = bytes(java_bytes)
        
        # Write to local file
        with open(local_model_path, 'wb') as f:
            f.write(py_bytes)
            
        output_stream.close()
        logger.info(f"Downloaded model from HDFS to local path: {local_model_path}")
        
        # Download scaler file from HDFS
        hdfs_scaler_path = spark._jvm.org.apache.hadoop.fs.Path(f"{hdfs_symbol_dir}/scaler_{timestamp}.npz")
        if not fs.exists(hdfs_scaler_path):
            logger.error(f"Scaler file not found in HDFS: {hdfs_scaler_path}")
            return None, None, None
            
        output_stream = fs.open(hdfs_scaler_path)
        java_input_stream = output_stream.getWrappedInputStream()
        java_bytes = spark._jvm.org.apache.commons.io.IOUtils.toByteArray(java_input_stream)
        py_bytes = bytes(java_bytes)
        
        with open(local_scaler_path, 'wb') as f:
            f.write(py_bytes)
            
        output_stream.close()
        logger.info(f"Downloaded scaler from HDFS to local path: {local_scaler_path}")
        
        # Download metadata file from HDFS
        hdfs_metadata_path = spark._jvm.org.apache.hadoop.fs.Path(f"{hdfs_symbol_dir}/metadata_{timestamp}.json")
        metadata = {}
        if fs.exists(hdfs_metadata_path):
            output_stream = fs.open(hdfs_metadata_path)
            java_input_stream = output_stream.getWrappedInputStream()
            java_bytes = spark._jvm.org.apache.commons.io.IOUtils.toByteArray(java_input_stream)
            py_bytes = bytes(java_bytes)
            
            with open(local_metadata_path, 'wb') as f:
                f.write(py_bytes)
                
            output_stream.close()
            
            # Read the metadata
            with open(local_metadata_path, 'r') as f:
                metadata = json.load(f)
                
            logger.info(f"Downloaded metadata from HDFS to local path: {local_metadata_path}")
        else:
            logger.warning(f"Metadata file not found in HDFS: {hdfs_metadata_path}")
        
        # Load the model and scaler from local files
        model = load_model(local_model_path)
        scaler_data = np.load(local_scaler_path)
        
        return model, scaler_data, metadata
    
    except Exception as e:
        logger.error(f"Error loading model files for {symbol}: {str(e)}")
        return None, None, None


def get_recent_data(symbol, days=30, spark=None):
    """
    Get recent data for the symbol from processed data in HDFS or local storage
    
    Args:
        symbol (str): Stock symbol
        days (int): Number of recent days to retrieve
        spark (SparkSession, optional): Spark session for HDFS access
        
    Returns:
        pd.DataFrame: Recent data or None if not available
    """
    if USE_HDFS and spark is not None:
        # Try to load from HDFS first
        try:
            return get_data_from_hdfs(symbol, days, spark)
        except Exception as e:
            logger.error(f"Error loading data from HDFS: {str(e)}")
            logger.warning(f"Falling back to local storage for {symbol}")
    
    # Fall back to local filesystem if HDFS fails or is disabled
    try:
        symbol_path = os.path.join(PROCESSED_DATA_PATH, symbol)
        if not os.path.exists(symbol_path):
            logger.error(f"No processed data directory found for {symbol}")
            return None
        
        # Find the most recent parquet file
        parquet_files = glob.glob(os.path.join(symbol_path, "*.parquet"))
        if not parquet_files:
            logger.error(f"No Parquet files found for {symbol}")
            return None
        
        # Sort by modification time (newest first)
        parquet_files.sort(key=os.path.getmtime, reverse=True)
        latest_file = parquet_files[0]
        
        logger.info(f"Loading processed data from local path: {latest_file}")
        
        # Read the parquet file
        df = pd.read_parquet(latest_file)
        
        # Sort by date and get the most recent days
        df = df.sort_values('date')
        if len(df) > days:
            df = df.tail(days)
        
        logger.info(f"Loaded {len(df)} recent records for {symbol} from local storage")
        return df
        
    except Exception as e:
        logger.error(f"Error loading recent data for {symbol} from local storage: {str(e)}")
        return None

def get_data_from_hdfs(symbol, days, spark):
    """
    Get recent data for the symbol from HDFS
    
    Args:
        symbol (str): Stock symbol
        days (int): Number of recent days to retrieve
        spark (SparkSession): Spark session for HDFS access
        
    Returns:
        pd.DataFrame: Recent data or None if not available
    """
    try:
        # HDFS path for this symbol's processed data
        hdfs_symbol_dir = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_PROCESSED_PATH}/{symbol}"
        
        # Get Hadoop filesystem
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        hdfs_dir_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_symbol_dir)
        
        if not fs.exists(hdfs_dir_path):
            logger.error(f"HDFS directory not found: {hdfs_symbol_dir}")
            return None
        
        # Get list of parquet files in the directory
        file_statuses = fs.listStatus(hdfs_dir_path)
        if file_statuses is None or len(file_statuses) == 0:
            logger.error(f"No files found in HDFS path: {hdfs_symbol_dir}")
            return None
            
        # Look for parquet files and sort by timestamp in the name
        parquet_files = []
        for status in file_statuses:
            path = status.getPath().toString()
            if "processed_" in path and path.endswith(".parquet"):
                parquet_files.append(path)
                
        if not parquet_files:
            logger.error(f"No parquet files found in HDFS for {symbol}")
            return None
            
        # Sort parquet files by timestamp in filename (descending)
        parquet_files.sort(reverse=True)
        latest_file = parquet_files[0]
        
        logger.info(f"Loading processed data from HDFS: {latest_file}")
        
        # Read parquet file using Spark and convert to pandas
        spark_df = spark.read.parquet(latest_file)
        
        # Convert to pandas for local processing
        df = spark_df.toPandas()
        
        # Sort by date and get the most recent days
        if 'date' in df.columns:
            df = df.sort_values('date')
            if len(df) > days:
                df = df.tail(days)
        
        logger.info(f"Loaded {len(df)} recent records for {symbol} from HDFS")
        return df
        
    except Exception as e:
        logger.error(f"Error loading recent data for {symbol}: {str(e)}")
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
        
        # Load model files from HDFS or local storage
        model, scaler_data, metadata = load_latest_model_files(symbol, spark)
        if model is None:
            logger.error(f"Failed to load model for {symbol}")
            return False
        
        # Get recent data from HDFS or local storage
        recent_data = get_recent_data(symbol, days=SEQUENCE_LENGTH, spark=spark)
        if recent_data is None:
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
