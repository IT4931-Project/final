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
from pyspark.sql.functions import col, avg, stddev, lag, abs, when, sum as spark_sum
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.functions import vector_to_array
from sklearn.model_selection import train_test_split
from bigdl.orca.data import SparkXShards

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
load_dotenv()

# Cấu hình
PROCESSED_DATA_PATH = os.getenv('PROCESSED_DATA_PATH', '/app/data/processed')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode')
HDFS_NAMENODE_PORT = os.getenv('HDFS_NAMENODE_PORT', '8020')
HDFS_PROCESSED_PATH = os.getenv('HDFS_PROCESSED_PATH', '/user/finance/processed')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '12'))  # Default to 3 workers * 4 cores
SEQUENCE_LENGTH = int(os.getenv('SEQUENCE_LENGTH', '30'))
FUTURE_DAYS = int(os.getenv('FUTURE_DAYS', '7'))
TRAIN_TEST_SPLIT = float(os.getenv('TRAIN_TEST_SPLIT', '0.8'))
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
            .config("spark.sql.parquet.int96AsTimestamp", "true") \
            .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}") \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.executor.instances", "3") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.hadoop.dfs.replication", "3") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
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

def load_data_from_hdfs(spark, symbol):
    """
    Tải dữ liệu đã xử lý từ HDFS
    
    Tham số:
        spark (pyspark.sql.SparkSession): Phiên Spark
        symbol (str): Mã cổ phiếu cần tải
        
    Trả về:
        pyspark.sql.DataFrame: Dữ liệu đã tải
    """
    try:
        logger.info(f"Loading data for {symbol} from HDFS")
        
        # Đường dẫn HDFS
        hdfs_base_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_PROCESSED_PATH}/{symbol}"
        
        # Kiểm tra đường dẫn HDFS tồn tại
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_base_path)
            
            if not hadoop_fs.exists(hdfs_path):
                logger.error(f"HDFS path does not exist: {hdfs_base_path}")
                
                # Thử load từ local backup nếu không tìm thấy trong HDFS
                logger.info(f"Trying to load from local backup")
                return load_data_from_local_backup(spark, symbol)
        except Exception as e:
            logger.error(f"Error checking HDFS path: {str(e)}")
            return load_data_from_local_backup(spark, symbol)
            
        # Lấy danh sách các thư mục con trong HDFS
        # (Mỗi thư mục con là một parquet file)
        file_statuses = hadoop_fs.listStatus(hdfs_path)
        if file_statuses is None or len(file_statuses) == 0:
            logger.error(f"No files found in HDFS path: {hdfs_base_path}")
            return load_data_from_local_backup(spark, symbol)
            
        # Sắp xếp theo thời gian tạo (mới nhất trước)
        # Đường dẫn thường có dạng .../processed_20250510.parquet
        file_paths = []
        for status in file_statuses:
            path = status.getPath().toString()
            if "parquet" in path:
                file_paths.append(path)
                
        if not file_paths:
            logger.error(f"No Parquet files found in HDFS for {symbol}")
            return load_data_from_local_backup(spark, symbol)
            
        # Sắp xếp theo thời gian trong tên file (giả sử tên có format processed_YYYYMMDD.parquet)
        file_paths.sort(reverse=True)
        latest_file = file_paths[0]
        
        logger.info(f"Loading processed data from HDFS: {latest_file}")
        
        # Đọc file parquet từ HDFS
        df = spark.read.parquet(latest_file)
        
        # Repartition để phân tán xử lý
        df = df.repartition(NUM_PARTITIONS)
        
        logger.info(f"Loaded {df.count()} records from HDFS for {symbol} distributed across {NUM_PARTITIONS} partitions")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from HDFS: {str(e)}")
        return load_data_from_local_backup(spark, symbol)

def load_data_from_local_backup(spark, symbol):
    """
    Tải dữ liệu từ backup local nếu HDFS không khả dụng
    
    Tham số:
        spark (pyspark.sql.SparkSession): Phiên Spark
        symbol (str): Mã cổ phiếu cần tải
        
    Trả về:
        pyspark.sql.DataFrame: Dữ liệu đã tải
    """
    try:
        logger.info(f"Loading data for {symbol} from local backup")
        
        # Đường dẫn đến dữ liệu đã xử lý cho mã cổ phiếu này
        symbol_path = os.path.join(PROCESSED_DATA_PATH, symbol)
        
        if not os.path.exists(symbol_path):
            logger.error(f"No processed data directory found for {symbol}")
            return None
        
        # Tìm file parquet mới nhất
        parquet_files = glob.glob(os.path.join(symbol_path, "*.parquet"))
        if not parquet_files:
            logger.error(f"No Parquet files found for {symbol}")
            return None
        
        # Sắp xếp theo thời gian chỉnh sửa (mới nhất trước)
        parquet_files.sort(key=os.path.getmtime, reverse=True)
        latest_file = parquet_files[0]
        
        logger.info(f"Loading processed data from local backup: {latest_file}")
        
        # Đọc file parquet
        df = spark.read.parquet(latest_file)
        logger.info(f"Loaded {df.count()} records for {symbol} from local backup")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from Parquet: {str(e)}")
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

def scale_features(spark_df, feature_cols):
    """
    Chuẩn hóa các đặc trưng sử dụng MinMaxScaler
    
    Tham số:
        spark_df: Spark DataFrame
        feature_cols: Danh sách tên các cột đặc trưng
        
    Trả về:
        Tuple của (DataFrame đã chuẩn hóa, mô hình scaler)
    """
    logger.info("Scaling features")
    
    # Đảm bảo các cột đặc trưng hợp lệ
    valid_cols = [col_name for col_name in feature_cols if col_name in spark_df.columns]
    logger.info(f"Using features: {valid_cols}")
    
    # Gộp các đặc trưng thành một vector
    assembler = VectorAssembler(inputCols=valid_cols, outputCol="features_vec")
    df_vec = assembler.transform(spark_df)
    
    # Chuẩn hóa các đặc trưng
    scaler = MinMaxScaler(inputCol="features_vec", outputCol="scaled_features")
    df_clean = df_vec.dropna()
    scaler_model = scaler.fit(df_clean)
    df_scaled = scaler_model.transform(df_clean)
    
    # Chuyển đổi vector thành mảng để dễ sử dụng
    df_array = df_scaled.withColumn("features_array", vector_to_array("scaled_features"))
    
    # Sắp xếp theo ngày
    if "date" in df_array.columns:
        df_array = df_array.orderBy("date")
    elif "year" in df_array.columns and "month" in df_array.columns:
        df_array = df_array.orderBy(["year", "month"])
    
    logger.info(f"Scaled {df_array.count()} rows of data")
    return df_array, scaler_model

def prepare_sequences(df_pandas, seq_length=SEQUENCE_LENGTH, future_days=FUTURE_DAYS):
    """
    Chuẩn bị các chuỗi cho việc huấn luyện mô hình LSTM
    
    Tham số:
        df_pandas: Pandas DataFrame với các đặc trưng đã chuẩn hóa
        seq_length: Chiều dài chuỗi (khoảng thời gian nhìn lại)
        future_days: Số ngày trong tương lai cần dự đoán
        
    Trả về:
        Tuple của các mảng (X, y)
    """
    logger.info(f"Preparing sequences with length {seq_length} and predicting {future_days} days ahead")
    
    # Trích xuất các đặc trưng đã chuẩn hóa
    features = np.array(df_pandas["features_array"].tolist())
    
    # Tạo các chuỗi
    X = []
    y = []
    
    for i in range(len(df_pandas) - seq_length - future_days + 1):
        # Chuỗi đầu vào
        X.append(features[i:i+seq_length])
        
        # Chuỗi mục tiêu (giá đóng cửa của future_days ngày tiếp theo)
        # Chúng ta cần trích xuất giá từ vector đặc trưng
        # Giả định giá đóng cửa ở vị trí 3 trong vector đặc trưng
        close_idx = 3  # Typically the 4th value in OHLCV data
        future_prices = [features[i+seq_length+j][close_idx] for j in range(future_days)]
        y.append(future_prices)
    
    X = np.array(X)
    y = np.array(y)
    
    # Reshape y để phù hợp với định dạng đầu ra của mô hình
    y = np.reshape(y, (y.shape[0], y.shape[1], 1))
    
    logger.info(f"Created {len(X)} sequences")
    return X, y

def prepare_data_for_training(symbol, train_ratio=TRAIN_TEST_SPLIT):
    """
    Chuẩn bị dữ liệu cho việc huấn luyện phân tán
    
    Chức năng của hàm này:
    1. Tạo phiên Spark phân tán
    2. Tải dữ liệu cho mã cổ phiếu từ HDFS
    3. Thêm các chỉ báo kỹ thuật
    4. Chuẩn hóa các đặc trưng
    5. Chuẩn bị các chuỗi cho LSTM
    6. Chia dữ liệu thành tập huấn luyện và tập kiểm định
    7. Tạo SparkXShards cho BigDL Orca phân tán
    
    Tham số:
        symbol: Mã cổ phiếu
        train_ratio: Tỷ lệ chia tập huấn luyện/kiểm thử
        
    Trả về:
        Tuple của (train_shards, val_shards, feature_count, scaler)
    """
    try:
        # Tạo phiên Spark phân tán
        spark = create_spark_session()
        
        # Tải dữ liệu từ HDFS
        df = load_data_from_hdfs(spark, symbol)
        if df is None:
            logger.error(f"Failed to load data for {symbol}")
            return None, None, 0, None
        
        # Cache dataframe để tăng tốc xử lý
        df.cache()
        
        # Thêm các chỉ báo kỹ thuật
        df = add_technical_indicators(df)
        
        # Định nghĩa các cột đặc trưng
        feature_cols = [
            "open", "high", "low", "close", "volume",
            f"SMA_3", f"STD_3", "BB_upper", "BB_lower", f"ATR_3", "OBV"
        ]
        
        # Thêm bất kỳ chỉ báo kinh tế bổ sung nào nếu có sẵn
        economic_indicators = ["unemployment_rate", "gold_price", "fed_funds_rate"]
        for indicator in economic_indicators:
            if indicator in df.columns:
                feature_cols.append(indicator)
        
        # Chuẩn hóa các đặc trưng
        df_scaled, scaler_model = scale_features(df, feature_cols)
        
        # Release memory của df gốc
        df.unpersist()
        
        # Cache DataFrame để tăng tốc độ xử lý
        df_scaled.cache()
        
        # Chuyển đổi sang Pandas để chuẩn bị chuỗi
        # Lưu ý: có thể dùng Spark ML để làm việc này trong phiên bản distributed hoàn toàn
        # nhưng hiện tại BigDL Orca cần input là numpy arrays
        df_pandas = df_scaled.toPandas()
        
        # Release memory của Spark DataFrame
        df_scaled.unpersist()
        
        # Chuẩn bị các chuỗi
        X, y = prepare_sequences(df_pandas)
        
        # Lấy số lượng đặc trưng
        feature_count = X.shape[2]
        
        # Chia thành tập huấn luyện và tập kiểm định
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=1-train_ratio, shuffle=False)
        
        logger.info(f"Split data into {len(X_train)} training and {len(X_val)} validation samples")
        
        # Tạo SparkXShards cho BigDL Orca phân tán training
        # Chú ý: Đặt num_partitions để phân tán dữ liệu đều trên các worker
        train_shards = SparkXShards.from_numpy((X_train, y_train), num_partitions=NUM_PARTITIONS)
        val_shards = SparkXShards.from_numpy((X_val, y_val), num_partitions=max(1, NUM_PARTITIONS // 4))
        
        logger.info(f"Created distributed data shards: {train_shards.num_partitions()} training partitions and {val_shards.num_partitions()} validation partitions")
        return train_shards, val_shards, feature_count, scaler_model
    
    except Exception as e:
        logger.error(f"Error preparing data: {str(e)}")
        return None, None, 0, None
