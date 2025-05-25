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
from pyspark.sql.types import DoubleType, StringType, TimestampType, ArrayType
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import pymongo # For listing collections
import re # For regex matching collection names

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
# Explicitly load .env from /app/.env and override existing env vars
# This requires .env to be copied to /app/.env in the Dockerfile
load_dotenv(dotenv_path='/app/.env', override=True)

# Configuration
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123') # Updated fallback
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin') # Updated fallback
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', '/app/data/raw')
PROCESSED_DATA_LOCAL_BACKUP_PATH = os.getenv('PROCESSED_DATA_LOCAL_BACKUP_PATH', '/app/data/processed_backup')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
# Default NUM_PARTITIONS to a reasonable value if not set, e.g., 4
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '4'))

# Elasticsearch Configuration
ES_NODES = os.getenv('ES_NODES', 'elasticsearch-master') # Service name from docker-compose
ES_PORT = os.getenv('ES_PORT', '9200')
ES_INDEX_PREFIX = os.getenv('ES_INDEX_PREFIX', 'processed_stock_data') # Prefix for ES indices

# Stock symbols to process (this will be overridden by dynamic discovery)
# SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')

def get_symbols_from_mongodb_collections():
    """
    Connects to MongoDB and lists collections matching the 'stock_<SYMBOL>' pattern
    to dynamically discover symbols to process.
    """
    symbols = []
    try:
        logger.info("Attempting to connect to MongoDB to discover stock symbols...")
        mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}"
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DATABASE]
        
        # Test connection
        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB: {MONGO_HOST} for symbol discovery.")

        collection_names = db.list_collection_names()
        logger.info(f"Found collections: {collection_names}")

        # Regex to match 'stock_SYMBOL' and capture SYMBOL part
        # Assumes symbols are uppercase letters and do not contain underscores.
        # Adjust regex if symbol naming convention is different.
        pattern = re.compile(r"^stock_([A-Z.]+)$")
        
        for name in collection_names:
            match = pattern.match(name)
            if match:
                symbol = match.group(1)
                symbols.append(symbol)
        
        if symbols:
            logger.info(f"Dynamically discovered symbols from MongoDB: {symbols}")
        else:
            logger.warning("No 'stock_<SYMBOL>' collections found in MongoDB. ETL will have no symbols to process dynamically.")
            # Fallback to environment variable if no dynamic symbols found, or keep it empty
            env_symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA')
            symbols = [s.strip() for s in env_symbols_str.split(',') if s.strip()]
            logger.info(f"Falling back to STOCK_SYMBOLS from env/default: {symbols}")

        client.close()
    except Exception as e:
        logger.error(f"Error discovering symbols from MongoDB: {e}")
        logger.warning("Falling back to default symbols due to MongoDB connection/discovery error.")
        # Fallback to environment variable or a hardcoded default list in case of error
        env_symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA')
        symbols = [s.strip() for s in env_symbols_str.split(',') if s.strip()]
        logger.info(f"Using fallback symbols: {symbols}")
    
    return list(set(symbols)) # Return unique symbols

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
            .config("spark.mongodb.input.uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}") \
            .config("spark.mongodb.output.uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.3.3") \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.es.nodes", ES_NODES) \
            .config("spark.es.port", ES_PORT) \
            .config("spark.es.nodes.wan.only", "true") \
            .config("spark.es.resource", f"{ES_INDEX_PREFIX}_{{symbol}}") \
            .config("spark.es.mapping.id", "row_id") \
            .config("spark.es.write.operation", "upsert") \
            .config("spark.executor.instances", "1") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
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
            .option("readPreference", "primary") \
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
        
        # Define base window partitioned by symbol and ordered by date
        # This ensures calculations are per symbol if multiple symbols were ever in the DataFrame
        # and silences the "No Partition Defined" warning.
        base_window = Window.partitionBy("symbol").orderBy("date")

        # Define windows for different lookback periods based on the base window
        window_5d = base_window.rowsBetween(-4, 0)
        window_20d = base_window.rowsBetween(-19, 0)
        window_50d = base_window.rowsBetween(-49, 0)
        window_200d = base_window.rowsBetween(-199, 0)
        
        # 1. Simple Moving Averages (SMA)
        df = df.withColumn("sma_5", avg("close").over(window_5d))
        df = df.withColumn("sma_20", avg("close").over(window_20d))
        df = df.withColumn("sma_50", avg("close").over(window_50d))
        df = df.withColumn("sma_200", avg("close").over(window_200d))
        
        # 2. Moving Average Convergence/Divergence (MACD) - simplified version
        df = df.withColumn("ema_12",
                        avg("close").over(base_window.rowsBetween(-11, 0)))
        df = df.withColumn("ema_26",
                        avg("close").over(base_window.rowsBetween(-25, 0)))
        df = df.withColumn("macd", col("ema_12") - col("ema_26"))
        df = df.withColumn("signal_line",
                        avg("macd").over(base_window.rowsBetween(-8, 0)))
        df = df.withColumn("macd_histogram", col("macd") - col("signal_line"))
        
        # 3. Bollinger Bands (BB)
        df = df.withColumn("bb_middle", avg("close").over(window_20d))
        df = df.withColumn("bb_stddev", stddev("close").over(window_20d))
        df = df.withColumn("bb_upper", col("bb_middle") + (col("bb_stddev") * lit(2)))
        df = df.withColumn("bb_lower", col("bb_middle") - (col("bb_stddev") * lit(2)))
        
        # 4. Relative Strength Index (RSI) - simplified calculation
        # lag function needs a window just ordered by date within the partition
        df = df.withColumn("prev_close", lag("close", 1).over(base_window)) # lag uses the base_window
        df = df.withColumn("price_change", col("close") - col("prev_close"))
        df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
        df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        window_14d_rsi = base_window.rowsBetween(-13, 0) # RSI specific window
        df = df.withColumn("avg_gain", avg("gain").over(window_14d_rsi))
        df = df.withColumn("avg_loss", avg("loss").over(window_14d_rsi))
        df = df.withColumn("rs", when(col("avg_loss") != 0, col("avg_gain") / col("avg_loss")).otherwise(lit(100)))
        df = df.withColumn("rsi", lit(100) - (lit(100) / (lit(1) + col("rs"))))
        
        # 5. On-Balance Volume (OBV)
        df = df.withColumn("volume_sign", 
                        when(col("price_change") > 0, col("volume"))
                        .when(col("price_change") < 0, -col("volume"))
                        .otherwise(0))
        
        # We need to calculate running sum, which is tricky in Spark
        # Using a window function with growing window, partitioned by symbol
        growing_window_partitioned = base_window.rowsBetween(Window.unboundedPreceding, 0)
        df = df.withColumn("obv", spark_sum("volume_sign").over(growing_window_partitioned))
        
        # 6. Price momentum indicators (percent changes)
        # prev_close is already calculated using base_window
        df = df.withColumn("day_change_pct", (col("close") - col("prev_close")) / col("prev_close") * 100)
        
        # Week change percent - close vs 5 days ago close
        df = df.withColumn("prev_5d_close", lag("close", 5).over(base_window))
        df = df.withColumn("week_change_pct",
                        when(col("prev_5d_close").isNotNull(),
                            (col("close") - col("prev_5d_close")) / col("prev_5d_close") * 100)
                        .otherwise(lit(0)))
        
        # Month change percent - close vs 20 days ago close
        df = df.withColumn("prev_20d_close", lag("close", 20).over(base_window))
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

def write_processed_data_to_mongo_and_local_backup(df, symbol):
    """
    Write processed data to MongoDB and create a local Parquet backup.
    
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
        processed_collection_name = f"processed_{symbol}"

        logger.info(f"Writing processed data for {symbol} to MongoDB collection: {processed_collection_name}")
        
        df_to_write = df # Start with the input DataFrame

        # Check for 'scaled_features' column first
        if "scaled_features" in df_to_write.columns:
            logger.info("Processing 'scaled_features' column.")
            # Assume it's VectorUDT if it exists from standardize_features and convert
            df_to_write = df_to_write.withColumn("scaled_features_temp_array", vector_to_array(col("scaled_features"))) \
                                     .drop("scaled_features") \
                                     .withColumnRenamed("scaled_features_temp_array", "scaled_features")
            logger.info("'scaled_features' column converted to array.")
            
            # If 'scaled_features' was processed, drop the raw 'features' column if it also exists
            if "features" in df_to_write.columns:
                logger.info("Dropping 'features' column as 'scaled_features' has been processed.")
                df_to_write = df_to_write.drop("features")
        
        # If 'scaled_features' was not present, check for 'features' column
        elif "features" in df_to_write.columns:
            logger.info("Processing 'features' column (scaled_features not found).")
            # Assume it's VectorUDT if it exists and convert
            df_to_write = df_to_write.withColumn("features_temp_array", vector_to_array(col("features"))) \
                                     .drop("features") \
                                     .withColumnRenamed("features_temp_array", "features")
            logger.info("'features' column converted to array.")
        else:
            logger.info("Neither 'scaled_features' nor 'features' vector columns found to convert.")

        logger.info(f"Schema of DataFrame being written to MongoDB for {symbol} (after potential conversions):")
        df_to_write.printSchema()

        # Write the data to MongoDB
        df_to_write.write \
          .format("mongo") \
          .mode("overwrite") \
          .option("database", MONGO_DATABASE) \
          .option("collection", processed_collection_name) \
          .save()
        
        logger.info(f"Successfully wrote {df_to_write.count()} records to MongoDB collection {processed_collection_name}")

        # Create a backup in local filesystem (Parquet)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        local_backup_dir_path = os.path.join(PROCESSED_DATA_LOCAL_BACKUP_PATH, symbol, f"backup_{timestamp}")
        os.makedirs(local_backup_dir_path, exist_ok=True)
        
        df_to_write.coalesce(1).write.mode("overwrite").parquet(local_backup_dir_path)
        logger.info(f"Successfully created local Parquet backup at {local_backup_dir_path}")

        # Write to Elasticsearch
        try:
            logger.info(f"Writing processed data for {symbol} to Elasticsearch index: {ES_INDEX_PREFIX}_{symbol}")
            # The 'es.resource' in SparkSession is already configured with the symbol placeholder.
            # Spark will replace {symbol} with the actual symbol value from the DataFrame column.
            # However, the connector might not automatically pick up the 'symbol' column for dynamic index naming
            # if it's not explicitly part of the es.resource string during the write operation itself.
            # A common practice is to set the resource dynamically per write if needed,
            # or ensure the global config is sufficient.
            # For simplicity, we rely on the global config and assume 'symbol' column exists.
            
            # Ensure the DataFrame has the 'symbol' column for dynamic index name resolution by the connector
            # if it's not already present (though it should be from clean_and_prepare_data)
            if "symbol" not in df_to_write.columns:
                df_with_symbol_for_es = df_to_write.withColumn("symbol", lit(symbol))
            else:
                df_with_symbol_for_es = df_to_write

            df_with_symbol_for_es.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", f"{ES_INDEX_PREFIX}_{symbol.lower()}") \
                .mode("overwrite") \
                .save()

            logger.info(f"Successfully wrote {df_with_symbol_for_es.count()} records to Elasticsearch index {ES_INDEX_PREFIX}_{symbol}")

        except Exception as es_ex:
            logger.error(f"Error writing data to Elasticsearch for {symbol}: {str(es_ex)}")
            # Optionally, decide if this error should make the overall function fail.
            # For now, we log the error and let the function return based on Mongo/Parquet success.
            # To make ES write failure critical, you could re-raise or return False here.

        return True
        
    except Exception as e:
        logger.error(f"Error writing processed data for {symbol}: {str(e)}")
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
        
        # Write to MongoDB and local backup
        success = write_processed_data_to_mongo_and_local_backup(std_df, symbol)
        
        return success
        
    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {str(e)}")
        return False

def main():
    """Main function that runs the ETL job using distributed processing"""
    logger.info("Starting Distributed Financial Data ETL Job")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Dynamically discover symbols from MongoDB collections
    discovered_symbols = get_symbols_from_mongodb_collections()

    if not discovered_symbols:
        logger.error("No symbols to process (neither discovered dynamically nor from fallback). Exiting ETL job.")
        spark.stop()
        logger.info("Distributed Spark session stopped.")
        return

    logger.info(f"ETL job will process the following symbols: {discovered_symbols}")
    
    results = []
    for symbol in discovered_symbols:
        # Symbol from discovery should already be stripped and valid
        logger.info(f"Initiating processing for symbol: {symbol}")
        success = process_symbol(spark, symbol)
        results.append((symbol, success))
    
    # Count successes
    success_count = sum(1 for _, success in results if success)
    
    # Log results for each symbol
    for symbol, success in results:
        logger.info(f"Symbol {symbol}: {'Success' if success else 'Failed'}")
    
    # Log summary
    logger.info(f"Distributed ETL job completed. Successfully processed {success_count}/{len(discovered_symbols)} symbols")
    
    # Stop Spark session
    spark.stop()
    logger.info("Distributed Spark session stopped")

if __name__ == "__main__":
    main()
