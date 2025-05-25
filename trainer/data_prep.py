#!/usr/bin/env python3
"""
data prep

- down data
- add technical indicators
- scale features
"""

import os
import glob
import logging
import datetime
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, avg, stddev, lag, abs, when, sum as spark_sum, lead
# No longer using Spark ML MinMaxScaler here, will use sklearn if needed on Pandas df.
# from pyspark.ml.feature import VectorAssembler, MinMaxScaler
# from pyspark.ml.functions import vector_to_array
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler as SklearnMinMaxScaler # For scaling target if needed
# from bigdl.orca.data import SparkXShards # No longer using Orca

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/trainer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_prep")

# Tải biến môi trường
# Explicitly load .env from /app/.env and override existing env vars
# This requires .env to be copied to /app/.env in the Dockerfile
load_dotenv(dotenv_path='/app/.env', override=True)

# Cấu hình
PROCESSED_DATA_LOCAL_BACKUP_PATH = os.getenv('PROCESSED_DATA_LOCAL_BACKUP_PATH', '/app/data/processed_backup')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123') # Updated fallback
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin') # Updated fallback
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_CORES_PER_WORKER = int(os.getenv('NUM_CORES_PER_WORKER', 2)) # Default to 2 if not set
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', NUM_CORES_PER_WORKER)) # Uses the (potentially defaulted) NUM_CORES_PER_WORKER
SEQUENCE_LENGTH = int(os.getenv('SEQUENCE_LENGTH', 30))
FUTURE_DAYS = int(os.getenv('FUTURE_DAYS', 7))
TRAIN_TEST_SPLIT = float(os.getenv('TRAIN_TEST_SPLIT', 0.8))
SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')

def create_spark_session():
    """
    Tạo và cấu hình phiên Spark cho xử lý phân tán
    
    Trả về:
        pyspark.sql.SparkSession: Phiên Spark đã được cấu hình
    """
    try:
        # Tạo phiên Spark
        spark = SparkSession.builder \
            .appName("Distributed Stock Price Prediction") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.mongodb.input.uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}") \
            .config("spark.sql.parquet.int96AsTimestamp", "true") \
            .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.executor.instances", "1") \
            .config("spark.executor.cores", str(NUM_CORES_PER_WORKER)) \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.memory.fraction", "0.7") \
            .config("spark.memory.storageFraction", "0.3") \
            .getOrCreate()
        
        # Thiết lập mức log
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Created distributed Spark session successfully")
        return spark
        
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise

def load_processed_data_from_mongo(spark, symbol):
    """
    Tải dữ liệu đã xử lý từ MongoDB collection 'processed_{symbol}'.
    """
    try:
        collection_name = f"processed_{symbol}"
        logger.info(f"Attempting to load processed data for {symbol} from MongoDB collection: {collection_name}")
        
        df = spark.read \
            .format("mongo") \
            .option("database", MONGO_DATABASE) \
            .option("collection", collection_name) \
            .option("readPreference", "primary") \
            .load()

        if df.rdd.isEmpty():
            logger.warning(f"No data found in MongoDB collection '{collection_name}' for {symbol}.")
            return None
        
        # Ensure 'date' column is present and sort by it
        if "date" not in df.columns:
            logger.error(f"'date' column not found in MongoDB data for {symbol}.")
            return None # Or handle as per requirements

        df = df.orderBy(col("date").asc())
        
        # Repartition for further processing
        df = df.repartition(NUM_PARTITIONS)
        
        logger.info(f"Loaded {df.count()} records from MongoDB for {symbol} distributed across {NUM_PARTITIONS} partitions")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from MongoDB for {symbol}: {str(e)}")
        return None

def load_data_from_local_backup(spark, symbol):
    """
    Tải dữ liệu từ backup local Parquet.
    """
    try:
        logger.info(f"Attempting to load data for {symbol} from local Parquet backup path: {PROCESSED_DATA_LOCAL_BACKUP_PATH}")
        
        symbol_backup_path = os.path.join(PROCESSED_DATA_LOCAL_BACKUP_PATH, symbol)
        if not os.path.exists(symbol_backup_path):
            logger.warning(f"No local processed data backup directory found for {symbol} at {symbol_backup_path}")
            return None
        
        # Find the most recent backup_{timestamp} directory
        backup_dirs = [d for d in os.listdir(symbol_backup_path) if os.path.isdir(os.path.join(symbol_backup_path, d)) and d.startswith("backup_")]
        if not backup_dirs:
            logger.warning(f"No backup directories (backup_YYYYMMDD_HHMMSS) found in {symbol_backup_path}")
            return None
        
        backup_dirs.sort(reverse=True) # Get the latest timestamped backup
        latest_backup_dir_path = os.path.join(symbol_backup_path, backup_dirs[0])
        
        logger.info(f"Loading processed data from latest local backup directory: {latest_backup_dir_path}")
        
        # Spark reads the directory containing Parquet part-files
        df = spark.read.parquet(latest_backup_dir_path)

        if df.rdd.isEmpty():
            logger.warning(f"No data found in local Parquet backup at {latest_backup_dir_path} for {symbol}.")
            return None

        if "date" not in df.columns:
            logger.error(f"'date' column not found in local Parquet backup for {symbol}.")
            return None
            
        df = df.orderBy(col("date").asc())
        df = df.repartition(NUM_PARTITIONS)
        logger.info(f"Loaded {df.count()} records for {symbol} from local Parquet backup.")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from local Parquet backup for {symbol}: {str(e)}")
        return None

def load_data_for_symbol(spark, symbol):
    """
    Tải dữ liệu cho một mã cổ phiếu.
    Ưu tiên MongoDB, sau đó fallback về local Parquet backup.
    """
    logger.info(f"Loading data for symbol: {symbol}")
    
    # 1. Thử tải từ MongoDB
    df = load_processed_data_from_mongo(spark, symbol)
    if df is not None and not df.rdd.isEmpty():
        return df
    
    logger.warning(f"Failed to load data for {symbol} from MongoDB. Attempting local Parquet backup.")
    
    # 2. Fallback về local Parquet backup
    df = load_data_from_local_backup(spark, symbol)
    if df is not None and not df.rdd.isEmpty():
        return df
        
    logger.error(f"Failed to load data for {symbol} from all sources.")
    return None

def add_technical_indicators(spark_df, date_col="date", window_size=3):
    """
    Thêm các chỉ báo kỹ thuật vào DataFrame
    
    Hàm này thêm các chỉ báo kỹ thuật giống như trong train.py:
    - Simple Moving Average (SMA)
    - Standard Deviation (STD)
    - Bollinger Bands (BB_upper, BB_lower)
    - Average True Range (ATR)
    - On-Balance Volume (OBV)
    
    Tham số:
        spark_df: Spark DataFrame
        date_col: Tên cột ngày
        window_size: Kích thước cửa sổ cho các chỉ báo
        
    Trả về:
        Spark DataFrame với các chỉ báo kỹ thuật
    """
    logger.info(f"Adding technical indicators with window size {window_size}")
    
    window_spec = Window.orderBy(date_col).rowsBetween(-window_size + 1, 0)
    window_spec_cumsum = Window.orderBy(date_col).rowsBetween(Window.unboundedPreceding, 0)

    # SMA trung bình trong cửa sổ
    spark_df = spark_df.withColumn(f"SMA_{window_size}", avg("close").over(window_spec))

    # STD trong cửa sổ
    spark_df = spark_df.withColumn(f"STD_{window_size}", stddev("close").over(window_spec))

    # Bollinger Bands giới hạn trên và giới hạn dưới
    spark_df = spark_df.withColumn("BB_upper", col(f"SMA_{window_size}") + 2 * col(f"STD_{window_size}"))
    spark_df = spark_df.withColumn("BB_lower", col(f"SMA_{window_size}") - 2 * col(f"STD_{window_size}"))

    # True Range (TR) - biến động giá tối đa giữa các ngày
    spark_df = spark_df.withColumn("prev_close", lag("close").over(Window.orderBy(date_col)))
    spark_df = spark_df.withColumn("high_low", col("high") - col("low"))
    spark_df = spark_df.withColumn("high_close_prev", abs(col("high") - col("prev_close")))
    spark_df = spark_df.withColumn("low_close_prev", abs(col("low") - col("prev_close")))
    spark_df = spark_df.withColumn("TR",
        when((col("high_low") >= col("high_close_prev")) & (col("high_low") >= col("low_close_prev")), col("high_low"))
        .when((col("high_close_prev") >= col("high_low")) & (col("high_close_prev") >= col("low_close_prev")), col("high_close_prev"))
        .otherwise(col("low_close_prev"))
    )

    # ATR - Average True Range
    spark_df = spark_df.withColumn(f"ATR_{window_size}", avg("TR").over(window_spec))

    # OBV - On-Balance Volume
    spark_df = spark_df.withColumn("direction",
        when(col("close") > col("prev_close"), 1)
        .when(col("close") < col("prev_close"), -1)
        .otherwise(0)
    )
    spark_df = spark_df.withColumn("volume_dir", col("direction") * col("volume"))
    spark_df = spark_df.withColumn("OBV", spark_sum("volume_dir").over(window_spec_cumsum))

    # Loại bỏ các cột tạm thời
    spark_df = spark_df.drop("prev_close", "high_low", "high_close_prev", "low_close_prev", "TR", "direction", "volume_dir")

    logger.info("Technical indicators added successfully")
    return spark_df

# def scale_features(spark_df, feature_cols): # This Spark ML scaling is no longer primary
#     ... (previous implementation) ...
# We will use the 'scaled_features' array from ETL or apply sklearn scaling on Pandas DF.

def create_ml_features_target(df_pandas, feature_col_name='scaled_features', target_col_name='close', seq_length=SEQUENCE_LENGTH, future_days=FUTURE_DAYS):
    """
    Prepares features (X) and target (y) for traditional ML models.
    Each sample in X is a flattened sequence of past 'feature_col_name' values.
    The target y is the 'target_col_name' 'future_days' ahead.

    Args:
        df_pandas (pd.DataFrame): Input DataFrame with a column containing feature arrays (e.g., 'scaled_features')
                                 and a column for the target (e.g., 'close'). Must be sorted by date.
        feature_col_name (str): Name of the column containing the array of features for each day.
        target_col_name (str): Name of the column to use for the target variable.
        seq_length (int): Number of past days' features to use for each input sample.
        future_days (int): How many days into the future to predict the target.

    Returns:
        Tuple of (np.ndarray, np.ndarray, Optional[SklearnMinMaxScaler]): X_flat, y, target_scaler
        X_flat: 2D array where each row is a flattened sequence of features.
        y: 1D array of target values.
        target_scaler: Scaler used for the target variable (if scaled), None otherwise.
    """
    logger.info(f"Creating ML features: using '{feature_col_name}' for input, '{target_col_name}' for target, sequence length {seq_length}, predicting {future_days} days ahead.")

    if feature_col_name not in df_pandas.columns:
        logger.error(f"Feature column '{feature_col_name}' not found in DataFrame.")
        return np.array([]), np.array([]), None
    if target_col_name not in df_pandas.columns:
        logger.error(f"Target column '{target_col_name}' not found in DataFrame.")
        return np.array([]), np.array([]), None

    # Ensure 'date' column exists for future target calculation if needed, though not directly used for X,y creation from pre-windowed data
    if 'date' not in df_pandas.columns:
        logger.warning("'date' column not found. Target shifting relies on row order.")

    # Convert feature column (expected to be list/array of features) to a 2D numpy array
    # This assumes 'scaled_features' from ETL is already an array of numbers.
    # If it's a list of lists, np.array() handles it. If it's a column of pd.Series, .tolist() then np.array().
    try:
        # Check if elements are already lists/arrays
        if isinstance(df_pandas[feature_col_name].iloc[0], (list, np.ndarray)):
            features_matrix = np.array(df_pandas[feature_col_name].tolist())
        else:
            # This case should ideally not happen if 'scaled_features' is an array from ETL
            logger.error(f"'{feature_col_name}' is not in the expected array/list format. Each element should be a list/array of features.")
            return np.array([]), np.array([]), None
    except Exception as e:
        logger.error(f"Error processing feature column '{feature_col_name}': {e}")
        return np.array([]), np.array([]), None

    # Target variable: price 'future_days' from the *end* of the sequence
    # df_pandas[target_col_name] gives the actual prices. We might want to scale this too.
    # For simplicity, let's predict the actual close price first.
    # We shift the target column to align: y_i corresponds to X_i (features up to day t) predicting price at day t + future_days
    df_pandas['y_target'] = df_pandas[target_col_name].shift(-future_days)
    
    X_list = []
    y_list = []

    # Iterate to create sequences for X and corresponding y
    # We need `seq_length` days for features, and the target is `future_days` after the *last* day of the sequence.
    # So, the last possible `i` is `len(df_pandas) - seq_length - future_days`.
    # However, since 'y_target' is already shifted, the last `i` for which `y_target` is not NaN
    # and we have enough preceding data for X is `len(df_pandas) - seq_length`.
    # The loop should go up to `len(df_pandas) - seq_length`.
    # The target `y_target.iloc[i + seq_length -1]` corresponds to features ending at `i + seq_length -1`.

    for i in range(len(features_matrix) - seq_length + 1):
        # Window for features: from i to i + seq_length -1
        sequence_features = features_matrix[i : i + seq_length]
        
        # Target: y_target at the end of the input sequence window
        # This y_target is already the price 'future_days' ahead of that point.
        current_target_y = df_pandas['y_target'].iloc[i + seq_length - 1]

        if pd.isna(current_target_y): # Skip if target is NaN (due to shift at the end of series)
            continue

        X_list.append(sequence_features.flatten()) # Flatten the sequence of feature vectors
        y_list.append(current_target_y)

    if not X_list: # If no sequences could be formed
        logger.warning("No sequences could be formed with the given parameters.")
        return np.array([]), np.array([]), None

    X_flat = np.array(X_list)
    y_array = np.array(y_list)
    
    # Optional: Scale the target variable (y)
    target_scaler = SklearnMinMaxScaler(feature_range=(0, 1))
    y_scaled = target_scaler.fit_transform(y_array.reshape(-1, 1)).flatten()
    
    logger.info(f"Created {len(X_flat)} flat feature samples for ML. X_flat shape: {X_flat.shape}, y_scaled shape: {y_scaled.shape}")
    return X_flat, y_scaled, target_scaler


def prepare_data_for_training(symbol, train_ratio=TRAIN_TEST_SPLIT):
    """
    Prepares data for traditional Machine Learning model training.
    """
    try:
        spark = create_spark_session()
        df_spark = load_data_for_symbol(spark, symbol) # Loads 'processed_<symbol>'

        if df_spark is None or df_spark.rdd.isEmpty():
            logger.error(f"Failed to load processed data for {symbol} or data is empty.")
            spark.stop()
            return None, None, None, None, 0, None # X_train, X_val, y_train, y_val, num_features, target_scaler

        # Ensure data is sorted by date before converting to Pandas
        # The 'scaled_features' column should already be an array from ETL
        # Required columns: 'date', 'scaled_features' (array of features), 'close' (for target)
        required_cols = ["date", "scaled_features", "close"]
        if not all(col_name in df_spark.columns for col_name in required_cols):
            logger.error(f"Missing one or more required columns ({required_cols}) in DataFrame for symbol {symbol}. Available: {df_spark.columns}")
            spark.stop()
            return None, None, None, None, 0, None

        df_spark = df_spark.orderBy("date")
        df_pandas = df_spark.select(required_cols).toPandas()
        spark.stop() # Stop Spark session after data is in Pandas

        if df_pandas.empty:
            logger.error(f"Pandas DataFrame is empty after loading for {symbol}.")
            return None, None, None, None, 0, None

        # Create flattened features (X) and target (y)
        # Using 'scaled_features' from ETL as input features, and 'close' as the base for target
        X_flat, y_scaled, target_scaler = create_ml_features_target(
            df_pandas,
            feature_col_name='scaled_features',
            target_col_name='close',
            seq_length=SEQUENCE_LENGTH,
            future_days=FUTURE_DAYS
        )

        if X_flat.shape[0] == 0:
            logger.error(f"No training samples generated for {symbol}.")
            return None, None, None, None, 0, None
            
        num_features = X_flat.shape[1] # Number of features in the flattened vector

        # Split data
        X_train, X_val, y_train, y_val = train_test_split(
            X_flat, y_scaled, test_size=1 - train_ratio, shuffle=False # Time series data, so no shuffle
        )
        
        logger.info(f"Data for {symbol} prepared: X_train shape {X_train.shape}, y_train shape {y_train.shape}, X_val shape {X_val.shape}, y_val shape {y_val.shape}")
        return X_train, X_val, y_train, y_val, num_features, target_scaler

    except Exception as e:
        logger.error(f"Error preparing data for ML training for symbol {symbol}: {e}", exc_info=True)
        # Ensure Spark context is stopped in case of error
        try:
            if 'spark' in locals() and spark._sc._jsc is not None : # Check if spark session exists and is active
                spark.stop()
        except Exception as e_spark_stop:
            logger.error(f"Error stopping spark session during exception handling: {e_spark_stop}")
        return None, None, None, None, 0, None
