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
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, lag, avg, stddev, sum as spark_sum, regexp_extract, to_date, concat_ws, date_format, isnan
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, TimestampType, ArrayType, DateType, FloatType
from dotenv import load_dotenv
import pymongo

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

load_dotenv(dotenv_path='/app/.env', override=True)

# Configuration
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123') # Updated fallback
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin') # Updated fallback
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', '/app/data/raw')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '4'))

# Elasticsearch Configuration
ES_NODES = os.getenv('ES_NODES', 'elasticsearch-master') # Service name from docker-compose
ES_PORT = os.getenv('ES_PORT', '9200')
ES_INDEX_PREFIX = os.getenv('ES_INDEX_PREFIX', 'processed_stock_data') # Prefix for ES indices

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
            env_symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA')
            symbols = [s.strip() for s in env_symbols_str.split(',') if s.strip()]
            logger.info(f"Falling back to STOCK_SYMBOLS from env/default: {symbols}")

        client.close()
    except Exception as e:
        logger.error(f"Error discovering symbols from MongoDB: {e}")
        logger.warning("Falling back to default symbols due to MongoDB connection/discovery error.")
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
        
        # Check if the dataframe has the required columns
        required_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in dataframe: {missing_cols}")
        
        # Print the schema to help with debugging
        
        if "date" in df.columns:
            # Log a sample of date values for debugging

            df = df.withColumn(
                "trading_date",
                when(
                    col("date").rlike("\\d{4}-\\d{2}-\\d{2}"),
                    regexp_extract(col("date"), "(\\d{4}-\\d{2}-\\d{2})", 1)
                ).otherwise(None)
            )
            
            # Attempt to convert the extracted date to a proper date type
            df = df.withColumn("trading_date", to_date(col("trading_date"), "yyyy-MM-dd"))
            
            # Check if trading_date conversion was successful
            null_dates = df.filter(col("trading_date").isNull()).count()
            logger.info(f"Records with null trading_date after extraction: {null_dates}")
            
            # If trading_date extraction failed for most records, try using the timestamp as a fallback
            if null_dates > df.count() * 0.5 and "timestamp" in df.columns:
                logger.warning("Date extraction failed for most records. Using timestamp as fallback.")
                df = df.withColumn("trading_date", to_date(col("timestamp")))
        
        # Ensure numeric columns are double type
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        # Handle missing values
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.filter(col(col_name).isNotNull())
        
        # Sort by trading_date or date (ascending)
        if "trading_date" in df.columns:
            df = df.orderBy("trading_date")
        elif "date" in df.columns:
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
        
        if "trading_date" in df.columns:
            base_window = Window.partitionBy("symbol").orderBy("trading_date")
        else:
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
        
        # this, my man, is tricky as funk
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

        # Ensure all technical indicator columns exist and have the correct type (DoubleType)
        # If an indicator failed to calculate, it will be filled with nulls of DoubleType.
        indicator_cols = [
            "sma_5", "sma_20", "sma_50", "sma_200",
            "ema_12", "ema_26", "macd", "signal_line", "macd_histogram",
            "bb_middle", "bb_stddev", "bb_upper", "bb_lower",
            "rsi", "avg_gain", "avg_loss", "rs",
            "obv", "volume_sign",
            "day_change_pct", "prev_5d_close", "week_change_pct",
            "prev_20d_close", "month_change_pct"
        ]

        for col_name in indicator_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            else:
                # If an indicator column is missing, add it as null of DoubleType
                logger.warning(f"Technical indicator column '{col_name}' was missing for symbol {df.select('symbol').first()[0] if 'symbol' in df.columns else 'UNKNOWN'}. Adding as null.")
                df = df.withColumn(col_name, lit(None).cast(DoubleType()))
        
        logger.info(f"Ensured consistent types for technical indicators for symbol {df.select('symbol').first()[0] if 'symbol' in df.columns else 'UNKNOWN'}")
        return df
        
    except Exception as e:
        logger.error(f"Error calculating technical indicators: {str(e)}")
        # This part might need refinement based on where the exception occurs
        try:
            logger.warning(f"Attempting to return DataFrame with null indicators due to error for symbol {df.select('symbol').first()[0] if 'symbol' in df.columns else 'UNKNOWN'}")
            indicator_cols = [
                "sma_5", "sma_20", "sma_50", "sma_200",
                "ema_12", "ema_26", "macd", "signal_line", "macd_histogram",
                "bb_middle", "bb_stddev", "bb_upper", "bb_lower",
                "rsi", "avg_gain", "avg_loss", "rs",
                "obv", "volume_sign",
                "day_change_pct", "prev_5d_close", "week_change_pct",
                "prev_20d_close", "month_change_pct"
            ]
            
            current_symbol = "UNKNOWN_SYMBOL_ON_ERROR"
            if "symbol" in df.columns:
                # Attempt to get the symbol if the DataFrame is not empty
                if df.count() > 0:
                    current_symbol = df.select("symbol").first()[0]

            for col_name in indicator_cols:
                if col_name not in df.columns: # Add if missing
                    df = df.withColumn(col_name, lit(None).cast(DoubleType()))
                else: # Cast if present
                    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            logger.info(f"Returning DataFrame with null/typed indicators for symbol {current_symbol} after error.")
        except Exception as inner_e:
            logger.error(f"Further error when trying to create null indicators for symbol {current_symbol}: {inner_e}")
            # Ideally, the job for this symbol should fail more gracefully or be skipped.
        return df


def write_processed_data_to_mongo_and_elasticsearch(df, symbol):
    """
    Write processed data to MongoDB and Elasticsearch.
    
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
        
        df_to_write = df 

        # check if trading_date column exists
        if "trading_date" not in df_to_write.columns and "date" in df_to_write.columns:
            # If trading_date doesn't exist but date does, use date as trading_date
            df_to_write = df_to_write.withColumn("trading_date", col("date"))
        
        # Write the data to MongoDB using a composite key of symbol+trading_date if available
        if "trading_date" in df_to_write.columns:
            logger.info(f"Using composite key of symbol+trading_date for MongoDB write")
            # Create a composite key column
            df_to_write = df_to_write.withColumn("symbol_date_key", 
                                              concat_ws("_", col("symbol"), 
                                                       date_format(col("trading_date"), "yyyy-MM-dd")))
            
            # Write using this key for MongoDB
            df_to_write.write \
              .format("mongo") \
              .mode("overwrite") \
              .option("database", MONGO_DATABASE) \
              .option("collection", processed_collection_name) \
              .save()
        else:
            # Fallback to the original overwrite method if no trading_date is available
            logger.warning(f"No trading_date column available, using simple overwrite by symbol")
            df_to_write.write \
              .format("mongo") \
              .mode("overwrite") \
              .option("database", MONGO_DATABASE) \
              .option("collection", processed_collection_name) \
              .save()
        
        logger.info(f"Successfully wrote {df_to_write.count()} records to MongoDB collection {processed_collection_name}")


        # Write to Elasticsearch
        try:
            logger.info(f"Writing processed data for {symbol} to Elasticsearch index: {ES_INDEX_PREFIX}_{symbol}")
            if "symbol" not in df_to_write.columns:
                df_with_symbol_for_es = df_to_write.withColumn("symbol", lit(symbol))
            else:
                df_with_symbol_for_es = df_to_write

            # Xóa trường '_id' của MongoDB trước khi ghi vào Elasticsearch
            # vì ES sử dụng trường _id riêng của nó và 'row_id' sẽ được ánh xạ vào đó.
            if "_id" in df_with_symbol_for_es.columns:
                df_for_es = df_with_symbol_for_es.drop("_id")
                logger.info("Dropped MongoDB '_id' column before writing to Elasticsearch.")
            else:
                df_for_es = df_with_symbol_for_es
            
            # Add a unique document ID using both symbol and trading_date if available
            if "trading_date" in df_for_es.columns:
                df_for_es = df_for_es.withColumn("es_id",
                                               concat_ws("_", col("symbol"),
                                                        date_format(col("trading_date"), "yyyy-MM-dd")))

            # === BEGIN NaN to Null CONVERSION FOR ES ===
            logger.info(f"Converting NaN to null for numeric fields before writing to ES for symbol: {symbol}")
            # Định nghĩa danh sách các cột có thể chứa giá trị NaN và cần chuyển thành null trước khi ghi vào Elasticsearch
            # Đây chủ yếu là các cột số được sinh ra từ các phép tính.
            numeric_cols_for_nan_conversion = [
                "open", "high", "low", "close", "volume",
                "sma_5", "sma_20", "sma_50", "sma_200",
                "ema_12", "ema_26", "macd", "signal_line", "macd_histogram",
                "bb_middle", "bb_stddev", "bb_upper", "bb_lower",
                "prev_close", # Used in RSI and other calculations
                "price_change", "gain", "loss", # Intermediate RSI parts
                "avg_gain", "avg_loss", "rs", "rsi",
                "volume_sign", "obv", # Intermediate OBV parts
                "day_change_pct",
                "prev_5d_close", "week_change_pct",
                "prev_20d_close", "month_change_pct"
            ]
            
            df_for_es_final = df_for_es # Start with the DataFrame prepared so far
            for field_name in numeric_cols_for_nan_conversion:
                if field_name in df_for_es_final.columns:
                    # Check if the column is actually numeric or could be a string 'NaN'
                    current_type = df_for_es_final.schema[field_name].dataType
                    if isinstance(current_type, (DoubleType, FloatType)): # Add FloatType for completeness
                        df_for_es_final = df_for_es_final.withColumn(field_name,
                                                                     when(isnan(col(field_name)), lit(None).cast(DoubleType()))
                                                                     .otherwise(col(field_name)))
                    elif isinstance(current_type, StringType):
                        # If it's a string, check for 'NaN' string and convert, then cast others to Double
                        df_for_es_final = df_for_es_final.withColumn(field_name,
                                                                     when(col(field_name) == "NaN", lit(None).cast(DoubleType()))
                                                                     .otherwise(col(field_name).cast(DoubleType()))) # Attempt cast
                    # Hiện tại, giả định rằng các chỉ báo chủ yếu là DoubleType hoặc đã trở thành chuỗi 'NaN'
            
            logger.info(f"Completed NaN to null conversion for symbol: {symbol}")
            # === END NaN to Null CONVERSION FOR ES ===
            
            df_for_es_final.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", f"{ES_INDEX_PREFIX}_{symbol.lower()}") \
                .option("es.mapping.id", "es_id" if "es_id" in df_for_es_final.columns else "row_id") \
                .option("es.spark.dataframe.write.null", "true") \
                .mode("overwrite") \
                .save()

            logger.info(f"Successfully wrote {df_for_es_final.count()} records to Elasticsearch index {ES_INDEX_PREFIX}_{symbol.lower()}")

        except Exception as es_ex:
            logger.error(f"Error writing data to Elasticsearch for {symbol}: {str(es_ex)}")
            # cũng có thể re-raise nếu cần thiết

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
        
        # Write to MongoDB and local backup
        # Pass tech_df directly as std_df is no longer created
        success = write_processed_data_to_mongo_and_elasticsearch(tech_df, symbol)
        
        # Unpersist tech_df after writing
        tech_df.unpersist()

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
