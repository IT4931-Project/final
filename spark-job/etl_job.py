#!/usr/bin/env python3
"""
ETL job

read from mongo, spark process, write to parquet

process:
- clean, standardize
- feature engineering
- organize data
"""

import os
import sys
import time
import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, lag, avg, stddev, sum as spark_sum
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, TimestampType
from pyspark.ml.feature import StandardScaler, VectorAssembler
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/etl_job.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_job")

# Load environment variables
load_dotenv()

# Configuration
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo-router')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'password')
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', '/app/data/raw')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode')
HDFS_NAMENODE_PORT = os.getenv('HDFS_NAMENODE_PORT', '8020')
HDFS_PROCESSED_PATH = os.getenv('HDFS_PROCESSED_PATH', '/user/finance/processed')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '12'))  # Default to 3 workers * 4 cores

# Stock symbols to process (should match what the crawler collects)
SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')

def create_spark_session():
    """
    Create and configure the Spark session for distributed processing
    
    Returns:
        pyspark.sql.SparkSession: Configured Spark session
    """
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Financial Data ETL") \
            .master(SPARK_MASTER) \
            .config("spark.mongodb.input.uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}") \
            .config("spark.mongodb.output.uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.hadoop:hadoop-aws:3.2.0") \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}") \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.executor.instances", "3") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.hadoop.dfs.replication", "3") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Created distributed Spark session successfully")
        return spark
        
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        sys.exit(1)

def load_data_from_mongodb(spark, symbol):
    """
    Load raw financial data from MongoDB sharded cluster
    
    Args:
        spark (pyspark.sql.SparkSession): Spark session
        symbol (str): Stock symbol to load
        
    Returns:
        pyspark.sql.DataFrame: Raw stock data
    """
    try:
        logger.info(f"Loading data for {symbol} from MongoDB sharded cluster")
        
        # Load from MongoDB with specific read preference to ensure we're using the sharded configuration
        df = spark.read \
            .format("mongo") \
            .option("database", MONGO_DATABASE) \
            .option("collection", f"stock_{symbol}") \
            .option("readPreference", "secondaryPreferred") \
            .option("partitioner", "MongoShardedPartitioner") \
            .option("partitionerOptions.shardkey", '{"ticker": 1, "date": 1}') \
            .load()
        
        # Repartition to distribute the data processing across the cluster
        df = df.repartition(NUM_PARTITIONS)
        
        logger.info(f"Loaded {df.count()} records for {symbol} distributed across {NUM_PARTITIONS} partitions")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from MongoDB: {str(e)}")
        
        # Try to load from CSV backup if MongoDB fails
        try:
            logger.info(f"Trying to load data from CSV backup for {symbol}")
            
            # Find the most recent CSV file for this symbol
            csv_files = [f for f in os.listdir(RAW_DATA_PATH) 
                        if f.startswith(symbol) and f.endswith('.csv')]
            
            if not csv_files:
                logger.error(f"No CSV backup found for {symbol}")
                return None
            
            # Sort by timestamp in filename (assuming format: SYMBOL_TIMESTAMP.csv)
            latest_file = sorted(csv_files)[-1]
            file_path = os.path.join(RAW_DATA_PATH, latest_file)
            
            # Read CSV file
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            logger.info(f"Loaded {df.count()} records for {symbol} from CSV backup")
            return df
            
        except Exception as nested_e:
            logger.error(f"Error loading data from CSV backup: {str(nested_e)}")
            return None

def clean_and_prepare_data(df, symbol):
    """
    Clean and prepare raw financial data
    
    Args:
        df (pyspark.sql.DataFrame): Raw stock data
        symbol (str): Stock symbol
        
    Returns:
        pyspark.sql.DataFrame: Cleaned data
    """
    if df is None or df.count() == 0:
        logger.warning(f"No data to clean for {symbol}")
        return None
    
    try:
        logger.info(f"Cleaning and preparing data for {symbol}")
        
        # Convert string dates to proper date type
        df = df.withColumn("date", col("date").cast("date"))
        
        # Ensure numeric columns are double type
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col_name in numeric_cols:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        # Handle missing values
        for col_name in numeric_cols:
            df = df.filter(col(col_name).isNotNull())
        
        # Sort by date (ascending)
        df = df.orderBy("date")
        
        # Add symbol column if not present
        if "symbol" not in df.columns:
            df = df.withColumn("symbol", lit(symbol))
        
        # Add a row_id column for easier identification
        df = df.withColumn("row_id", expr("uuid()"))
        
        logger.info(f"Data cleaning completed for {symbol}")
        return df
        
    except Exception as e:
        logger.error(f"Error cleaning data for {symbol}: {str(e)}")
        return None

def calculate_technical_indicators(df):
    """
    Calculate technical indicators for stock data
    
    Args:
        df (pyspark.sql.DataFrame): Cleaned stock data
        
    Returns:
        pyspark.sql.DataFrame: Data with technical indicators
    """
    if df is None or df.count() == 0:
        return None
    
    try:
        logger.info("Calculating technical indicators")
        
        # Define windows for different lookback periods
        window_5d = Window.orderBy("date").rowsBetween(-4, 0)
        window_20d = Window.orderBy("date").rowsBetween(-19, 0)
        window_50d = Window.orderBy("date").rowsBetween(-49, 0)
        window_200d = Window.orderBy("date").rowsBetween(-199, 0)
        
        # 1. Simple Moving Averages (SMA)
        df = df.withColumn("sma_5", avg("close").over(window_5d))
        df = df.withColumn("sma_20", avg("close").over(window_20d))
        df = df.withColumn("sma_50", avg("close").over(window_50d))
        df = df.withColumn("sma_200", avg("close").over(window_200d))
        
        # 2. Moving Average Convergence/Divergence (MACD) - simplified version
        df = df.withColumn("ema_12", 
                        avg("close").over(Window.orderBy("date").rowsBetween(-11, 0)))
        df = df.withColumn("ema_26", 
                        avg("close").over(Window.orderBy("date").rowsBetween(-25, 0)))
        df = df.withColumn("macd", col("ema_12") - col("ema_26"))
        df = df.withColumn("signal_line", 
                        avg("macd").over(Window.orderBy("date").rowsBetween(-8, 0)))
        df = df.withColumn("macd_histogram", col("macd") - col("signal_line"))
        
        # 3. Bollinger Bands (BB)
        df = df.withColumn("bb_middle", avg("close").over(window_20d))
        df = df.withColumn("bb_stddev", stddev("close").over(window_20d))
        df = df.withColumn("bb_upper", col("bb_middle") + (col("bb_stddev") * lit(2)))
        df = df.withColumn("bb_lower", col("bb_middle") - (col("bb_stddev") * lit(2)))
        
        # 4. Relative Strength Index (RSI) - simplified calculation
        window_prev = Window.orderBy("date").rowsBetween(-1, -1)
        df = df.withColumn("prev_close", lag("close", 1).over(Window.orderBy("date")))
        df = df.withColumn("price_change", col("close") - col("prev_close"))
        df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
        df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        window_14d = Window.orderBy("date").rowsBetween(-13, 0)
        df = df.withColumn("avg_gain", avg("gain").over(window_14d))
        df = df.withColumn("avg_loss", avg("loss").over(window_14d))
        df = df.withColumn("rs", when(col("avg_loss") != 0, col("avg_gain") / col("avg_loss")).otherwise(lit(100)))
        df = df.withColumn("rsi", lit(100) - (lit(100) / (lit(1) + col("rs"))))
        
        # 5. On-Balance Volume (OBV)
        df = df.withColumn("volume_sign", 
                        when(col("price_change") > 0, col("volume"))
                        .when(col("price_change") < 0, -col("volume"))
                        .otherwise(0))
        
        # We need to calculate running sum, which is tricky in Spark
        # Using a window function with growing window
        growing_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        df = df.withColumn("obv", spark_sum("volume_sign").over(growing_window))
        
        # 6. Price momentum indicators (percent changes)
        df = df.withColumn("day_change_pct", (col("close") - col("prev_close")) / col("prev_close") * 100)
        
        # Week change percent - close vs 5 days ago close
        df = df.withColumn("prev_5d_close", lag("close", 5).over(Window.orderBy("date")))
        df = df.withColumn("week_change_pct", 
                        when(col("prev_5d_close").isNotNull(), 
                            (col("close") - col("prev_5d_close")) / col("prev_5d_close") * 100)
                        .otherwise(lit(0)))
        
        # Month change percent - close vs 20 days ago close
        df = df.withColumn("prev_20d_close", lag("close", 20).over(Window.orderBy("date")))
        df = df.withColumn("month_change_pct", 
                        when(col("prev_20d_close").isNotNull(), 
                            (col("close") - col("prev_20d_close")) / col("prev_20d_close") * 100)
                        .otherwise(lit(0)))
        
        logger.info("Technical indicators calculated")
        return df
        
    except Exception as e:
        logger.error(f"Error calculating technical indicators: {str(e)}")
        return df  # Return original DataFrame without indicators

def standardize_features(df):
    """
    Standardize numeric features to have zero mean and unit variance
    
    Args:
        df (pyspark.sql.DataFrame): Data with technical indicators
        
    Returns:
        pyspark.sql.DataFrame: Data with standardized features
    """
    if df is None or df.count() == 0:
        return None
    
    try:
        logger.info("Standardizing features")
        
        # Identify numeric feature columns
        feature_cols = [
            "open", "high", "low", "close", "volume",
            "sma_5", "sma_20", "sma_50", "sma_200",
            "macd", "signal_line", "macd_histogram",
            "bb_middle", "bb_upper", "bb_lower", "bb_stddev",
            "rsi", "obv",
            "day_change_pct", "week_change_pct", "month_change_pct"
        ]
        
        # Filter only columns that exist in the DataFrame
        feature_cols = [col_name for col_name in feature_cols if col_name in df.columns]
        
        # Assemble features into a vector
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip"
        )
        df_assembled = assembler.transform(df)
        
        # Standardize the features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        logger.info("Features standardized")
        return df_scaled
        
    except Exception as e:
        logger.error(f"Error standardizing features: {str(e)}")
        return df  # Return original DataFrame without standardization

def write_to_hdfs(df, symbol):
    """
    Write processed data to HDFS as Parquet files
    
    Args:
        df (pyspark.sql.DataFrame): Processed data
        symbol (str): Stock symbol
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None or df.count() == 0:
        logger.warning(f"No data to write for {symbol}")
        return False
    
    try:
        # Generate timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        
        # HDFS path
        hdfs_output_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_PROCESSED_PATH}/{symbol}/processed_{timestamp}.parquet"
        
        # Write DataFrame as Parquet
        logger.info(f"Writing processed data for {symbol} to HDFS: {hdfs_output_path}")
        
        # Drop unnecessary columns to save space
        columns_to_drop = ["prev_close", "ema_12", "ema_26", "price_change", "gain", "loss", 
                          "avg_gain", "avg_loss", "rs", "volume_sign"]
        
        for col_name in columns_to_drop:
            if col_name in df.columns:
                df = df.drop(col_name)
        
        # Repartition for optimal storage in HDFS
        # Parquet files should be between 128MB to 1GB for optimal performance in HDFS
        # Let's aim for ~256MB per partition
        optimized_partitions = max(1, min(NUM_PARTITIONS, int(df.count() / 1000000) + 1))
        df = df.repartition(optimized_partitions)
        
        # Write the data to HDFS with compression
        df.write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .partitionBy("symbol") \
          .parquet(hdfs_output_path)
        
        # Create a backup in local filesystem
        local_output_dir = os.path.join(RAW_DATA_PATH, symbol)
        local_output_path = os.path.join(local_output_dir, f"processed_{timestamp}_backup.parquet")
        os.makedirs(local_output_dir, exist_ok=True)
        
        # Write a single partition backup locally
        df.coalesce(1).write.mode("overwrite").parquet(local_output_path)
        
        logger.info(f"Successfully wrote {df.count()} records to HDFS {hdfs_output_path} with {optimized_partitions} partitions")
        return True
        
    except Exception as e:
        logger.error(f"Error writing data to Parquet: {str(e)}")
        return False

def process_symbol(spark, symbol):
    """
    Process a single stock symbol using distributed computing
    
    Args:
        spark (pyspark.sql.SparkSession): Spark session
        symbol (str): Stock symbol to process
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Processing symbol {symbol} using distributed computing")
        
        # Load raw data
        raw_df = load_data_from_mongodb(spark, symbol)
        if raw_df is None:
            return False
        
        # Cache the dataframe to speed up processing
        # This distributes the data across the cluster memory
        raw_df.cache()
        
        # Clean and prepare data
        clean_df = clean_and_prepare_data(raw_df, symbol)
        if clean_df is None:
            return False
        
        # Release memory from raw_df as we don't need it anymore
        raw_df.unpersist()
        
        # Cache the clean dataframe
        clean_df.cache()
        
        # Calculate technical indicators
        tech_df = calculate_technical_indicators(clean_df)
        if tech_df is None:
            return False
        
        # Release memory from clean_df
        clean_df.unpersist()
        
        # Cache the technical dataframe
        tech_df.cache()
        
        # Standardize features
        std_df = standardize_features(tech_df)
        if std_df is None:
            return False
        
        # Release memory from tech_df
        tech_df.unpersist()
        
        # Write to HDFS
        success = write_to_hdfs(std_df, symbol)
        
        return success
        
    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {str(e)}")
        return False

def main():
    """Main function that runs the ETL job using distributed processing"""
    logger.info("Starting Distributed Financial Data ETL Job")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Check HDFS connection
    try:
        # Get the Hadoop filesystem
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        
        # Create base directory if it doesn't exist
        hdfs_base_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_PROCESSED_PATH}"
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_base_path)
        
        if not hadoop_fs.exists(hdfs_path):
            hadoop_fs.mkdirs(hdfs_path)
            logger.info(f"Created HDFS directory: {hdfs_base_path}")
        else:
            logger.info(f"HDFS directory exists: {hdfs_base_path}")
    except Exception as e:
        logger.error(f"Error checking/creating HDFS directory: {str(e)}")
        logger.warning("Continuing with processing, but HDFS writes may fail")
    
    # Process symbols in parallel
    # Convert to RDD for parallel processing
    symbols_rdd = spark.sparkContext.parallelize(SYMBOLS, NUM_PARTITIONS)
    
    # Map each symbol to its processing result
    results = symbols_rdd.map(lambda symbol: (symbol.strip(), process_symbol(spark, symbol.strip()))).collect()
    
    # Count successes
    success_count = sum(1 for _, success in results if success)
    
    # Log results for each symbol
    for symbol, success in results:
        logger.info(f"Symbol {symbol}: {'Success' if success else 'Failed'}")
    
    # Log summary
    logger.info(f"Distributed ETL job completed. Successfully processed {success_count}/{len(SYMBOLS)} symbols")
    
    # Stop Spark session
    spark.stop()
    logger.info("Distributed Spark session stopped")

if __name__ == "__main__":
    main()
