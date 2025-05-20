#!/usr/bin/env python3
"""
1. fetch data
2. process && align data
3. save data
"""

import os
import sys
import time
import logging
import datetime
import requests
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, last, first, lag, when
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, TimestampType, DateType, StructType, StructField
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/economic_indicators.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("economic_indicators")

# Load environment variables
load_dotenv()

# Configuration
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode')
HDFS_NAMENODE_PORT = os.getenv('HDFS_NAMENODE_PORT', '8020')
HDFS_PROCESSED_PATH = os.getenv('HDFS_PROCESSED_PATH', '/user/finance/processed')
HDFS_ECONOMIC_PATH = os.getenv('HDFS_ECONOMIC_PATH', '/user/finance/economic')
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', '/app/data/raw')
ECONOMIC_DATA_PATH = os.getenv('ECONOMIC_DATA_PATH', '/app/data/economic')
FRED_API_KEY = os.getenv('FRED_API_KEY', '')
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '12'))

# List of economic indicators to fetch
ECONOMIC_INDICATORS = [
    # Interest rates
    {"id": "DFF", "name": "fed_funds_rate", "source": "fred", "frequency": "daily"},
    {"id": "FEDFUNDS", "name": "effective_fed_funds_rate", "source": "fred", "frequency": "monthly"},
    {"id": "TB3MS", "name": "treasury_3m", "source": "fred", "frequency": "monthly"},
    {"id": "GS10", "name": "treasury_10y", "source": "fred", "frequency": "monthly"},
    
    # Inflation and prices
    {"id": "CPIAUCSL", "name": "cpi", "source": "fred", "frequency": "monthly"},
    {"id": "PPIACO", "name": "ppi", "source": "fred", "frequency": "monthly"},
    {"id": "GOLDAMGBD228NLBM", "name": "gold_price", "source": "fred", "frequency": "daily"},
    
    # Labor market
    {"id": "UNRATE", "name": "unemployment_rate", "source": "fred", "frequency": "monthly"},
    {"id": "PAYEMS", "name": "nonfarm_payrolls", "source": "fred", "frequency": "monthly"},
    
    # Economic activity
    {"id": "GDPC1", "name": "real_gdp", "source": "fred", "frequency": "quarterly"},
    {"id": "INDPRO", "name": "industrial_production", "source": "fred", "frequency": "monthly"},
    {"id": "RSAFS", "name": "retail_sales", "source": "fred", "frequency": "monthly"},
    
    # Market sentiment
    {"id": "VIXCLS", "name": "vix", "source": "fred", "frequency": "daily"},
]

def create_spark_session():
    """
    Create and configure Spark session for distributed processing
    
    Returns:
        pyspark.sql.SparkSession: Configured Spark session
    """
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Economic Indicators ETL") \
            .master(SPARK_MASTER) \
            .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
            .config("spark.default.parallelism", NUM_PARTITIONS) \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}") \
            .config("spark.executor.instances", "3") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Created Spark session successfully")
        return spark
        
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        sys.exit(1)

def fetch_fred_data(indicator_id, start_date, end_date):
    """
    Fetch economic data from Federal Reserve Economic Data (FRED)
    
    Args:
        indicator_id (str): FRED series ID
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: DataFrame with date and value columns
    """
    if not FRED_API_KEY:
        logger.error("FRED API key not provided. Cannot fetch real data.")
        sys.exit(1)
    
    try:
        logger.info(f"Fetching {indicator_id} data from FRED")
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        params = {
            "series_id": indicator_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
            "frequency": "d",  # Get daily observations where available
            "units": "lin"     # Linear units (not percent change)
        }
        
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            logger.error(f"Error fetching {indicator_id}: {response.status_code}, {response.text}")
            return None
        
        data = response.json()
        observations = data.get('observations', [])
        
        if not observations:
            logger.warning(f"No data returned for {indicator_id}")
            return None
        
        # Create DataFrame from observations
        df = pd.DataFrame(observations)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Keep only date and value columns
        df = df[['date', 'value']].dropna()
        
        logger.info(f"Fetched {len(df)} observations for {indicator_id}")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching {indicator_id} from FRED: {str(e)}")
        return None


def fetch_all_economic_indicators(start_date, end_date):
    """
    Fetch all economic indicators
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        dict: Dictionary of indicator DataFrames
    """
    indicators = {}
    
    for indicator in ECONOMIC_INDICATORS:
        if indicator["source"] == "fred":
            df = fetch_fred_data(indicator["id"], start_date, end_date)
            indicators[indicator["name"]] = df
            
            # Add slight delay to avoid rate limiting
            time.sleep(0.5)
    
    return indicators

def save_indicators_to_parquet(spark, indicators, hdfs_path):
    """
    Save economic indicators to Parquet files
    
    Args:
        spark (SparkSession): Spark session
        indicators (dict): Dictionary of indicator DataFrames
        hdfs_path (str): HDFS path to save files
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create base directory if it doesn't exist
        hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        hdfs_base_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        
        if not hadoop_fs.exists(hdfs_base_path):
            hadoop_fs.mkdirs(hdfs_base_path)
            logger.info(f"Created HDFS directory: {hdfs_path}")
        
        # Save each indicator
        for name, df in indicators.items():
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(df)
            
            # Save to HDFS
            indicator_path = f"{hdfs_path}/{name}.parquet"
            spark_df.write.mode("overwrite").parquet(indicator_path)
            
            logger.info(f"Saved {name} to {indicator_path}")
            
            # Save local backup
            os.makedirs(ECONOMIC_DATA_PATH, exist_ok=True)
            local_path = os.path.join(ECONOMIC_DATA_PATH, f"{name}.parquet")
            df.to_parquet(local_path)
            logger.info(f"Saved local backup to {local_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error saving indicators to Parquet: {str(e)}")
        return False

def align_economic_data_with_stock_data(spark, symbol):
    """
    Align economic indicators with stock data
    
    Args:
        spark (SparkSession): Spark session
        symbol (str): Stock symbol
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with aligned data or None if error
    """
    try:
        logger.info(f"Aligning economic data with stock data for {symbol}")
        
        # Load stock data
        stock_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_PROCESSED_PATH}/{symbol}"
        stock_df = spark.read.parquet(stock_path)
        
        # Get date range
        min_date = stock_df.agg({"date": "min"}).collect()[0][0]
        max_date = stock_df.agg({"date": "max"}).collect()[0][0]
        
        logger.info(f"Date range for {symbol}: {min_date} to {max_date}")
        
        # Load economic indicators
        economic_dfs = {}
        for indicator in ECONOMIC_INDICATORS:
            name = indicator["name"]
            indicator_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_ECONOMIC_PATH}/{name}.parquet"
            
            try:
                indicator_df = spark.read.parquet(indicator_path)
                indicator_df = indicator_df.withColumnRenamed("value", name)
                economic_dfs[name] = indicator_df
            except Exception as e:
                logger.warning(f"Could not load {name}: {str(e)}")
        
        if not economic_dfs:
            logger.warning("No economic indicators loaded")
            return stock_df
        
        # Join all economic indicators with stock data
        result_df = stock_df
        
        for name, indicator_df in economic_dfs.items():
            # Join with stock data using date
            # Use left join to keep all stock dates
            result_df = result_df.join(indicator_df, on="date", how="left")
            
            # Fill forward missing values (use last known value)
            window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
            result_df = result_df.withColumn(
                name, 
                last(name, True).over(window_spec)
            )
        
        logger.info(f"Successfully aligned economic data with stock data for {symbol}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error aligning economic data with stock data: {str(e)}")
        return stock_df  # Return original stock data if error

def write_aligned_data(df, symbol):
    """
    Write aligned data back to HDFS
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame with aligned data
        symbol (str): Stock symbol
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Generate timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        
        # HDFS path
        hdfs_output_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_PROCESSED_PATH}/{symbol}/processed_with_econ_{timestamp}.parquet"
        
        # Write DataFrame as Parquet
        logger.info(f"Writing aligned data for {symbol} to HDFS: {hdfs_output_path}")
        
        # Repartition for optimal storage
        optimized_partitions = max(1, min(NUM_PARTITIONS, int(df.count() / 500000) + 1))
        df = df.repartition(optimized_partitions)
        
        df.write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(hdfs_output_path)
        
        logger.info(f"Successfully wrote aligned data to HDFS: {hdfs_output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing aligned data to HDFS: {str(e)}")
        return False

def perform_data_quality_checks(df, symbol):
    """
    Perform data quality checks on stock and economic data
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame with stock and economic data
        symbol (str): Stock symbol
        
    Returns:
        tuple: (passed_all_checks, results_dict)
    """
    logger.info(f"Performing data quality checks for {symbol}")
    
    checks = {}
    passed_all = True
    
    # Check 1: Missing values
    try:
        total_rows = df.count()
        checks["total_rows"] = total_rows
        
        # Check missing values in key columns
        key_columns = ["open", "high", "low", "close", "volume", "date"]
        missing_counts = {}
        
        for col_name in key_columns:
            missing_count = df.filter(col(col_name).isNull()).count()
            missing_pct = (missing_count / total_rows) * 100 if total_rows > 0 else 0
            missing_counts[col_name] = {"count": missing_count, "percentage": missing_pct}
            
            # Flag if missing percentage is high
            if missing_pct > 5:
                logger.warning(f"High missing values in {col_name}: {missing_pct:.2f}%")
                passed_all = False
        
        checks["missing_values"] = missing_counts
        
        # Check 2: Data integrity - verify high >= low and other relationships
        invalid_high_low = df.filter(col("high") < col("low")).count()
        if invalid_high_low > 0:
            logger.warning(f"Found {invalid_high_low} records where high < low")
            passed_all = False
        
        invalid_close = df.filter(
            (col("close") > col("high")) | (col("close") < col("low"))
        ).count()
        if invalid_close > 0:
            logger.warning(f"Found {invalid_close} records where close is outside high-low range")
            passed_all = False
        
        checks["data_integrity"] = {
            "invalid_high_low": invalid_high_low,
            "invalid_close": invalid_close
        }
        
        # Check 3: Extreme values (potential outliers)
        outliers = {}
        
        # Check for price outliers
        price_stats = df.select(
            lit(symbol).alias("symbol"),
            "date",
            "close",
            "high",
            "low"
        ).describe().collect()
        
        # Convert statistics to dictionary
        stats_dict = {row["summary"]: row for row in price_stats}
        
        if "mean" in stats_dict and "stddev" in stats_dict:
            mean_close = float(stats_dict["mean"]["close"])
            stddev_close = float(stats_dict["stddev"]["close"])
            
            # Flag potential outliers (more than 3 standard deviations from mean)
            price_outliers = df.filter(
                (col("close") > mean_close + 3 * stddev_close) | 
                (col("close") < mean_close - 3 * stddev_close)
            ).count()
            
            outliers["price_outliers"] = price_outliers
            
            if price_outliers > 0:
                logger.warning(f"Found {price_outliers} potential price outliers")
                # Don't fail the check for outliers, just warn
        
        # Check for volume outliers
        volume_stats = df.select("volume").describe().collect()
        stats_dict = {row["summary"]: row for row in volume_stats}
        
        if "mean" in stats_dict and "stddev" in stats_dict:
            mean_volume = float(stats_dict["mean"]["volume"])
            stddev_volume = float(stats_dict["stddev"]["volume"])
            
            volume_outliers = df.filter(
                col("volume") > mean_volume + 5 * stddev_volume
            ).count()
            
            outliers["volume_outliers"] = volume_outliers
            
            if volume_outliers > 0:
                logger.warning(f"Found {volume_outliers} potential volume outliers")
                # Don't fail the check for outliers, just warn
        
        checks["outliers"] = outliers
        
        # Check 4: Duplicates
        duplicate_count = df.count() - df.dropDuplicates(["date"]).count()
        checks["duplicates"] = duplicate_count
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate date records")
            passed_all = False
        
        # Check 5: Date continuity (gaps in trading days)
        date_df = df.select("date").orderBy("date")
        date_df = date_df.withColumn("next_date", lead("date", 1).over(Window.orderBy("date")))
        date_df = date_df.withColumn("days_diff", datediff("next_date", "date"))
        
        # Find gaps larger than 3 days (excludes weekends)
        gaps = date_df.filter(col("days_diff") > 3).count()
        checks["date_gaps"] = gaps
        
        if gaps > 0:
            logger.warning(f"Found {gaps} gaps in date sequence (> 3 days)")
            # Don't fail for gaps, just warn
        
        # Log results
        logger.info(f"Data quality check results for {symbol}: {passed_all}")
        return passed_all, checks
        
    except Exception as e:
        logger.error(f"Error performing data quality checks: {str(e)}")
        return False, {"error": str(e)}

def main():
    """Main function for economic indicators ETL job"""
    logger.info("Starting Economic Indicators ETL Job")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Calculate date range (last 5 years)
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365*5)).strftime('%Y-%m-%d')
        
        # Fetch all economic indicators
        logger.info(f"Fetching economic indicators from {start_date} to {end_date}")
        indicators = fetch_all_economic_indicators(start_date, end_date)
        
        # Save to HDFS
        hdfs_econ_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_NAMENODE_PORT}{HDFS_ECONOMIC_PATH}"
        save_indicators_to_parquet(spark, indicators, hdfs_econ_path)
        
        # Get stock symbols
        symbols = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')
        
        # Process each symbol
        success_count = 0
        quality_check_results = {}
        
        for symbol in symbols:
            symbol = symbol.strip()
            
            # Align economic data with stock data
            aligned_df = align_economic_data_with_stock_data(spark, symbol)
            
            if aligned_df is not None:
                # Perform data quality checks
                passed_checks, check_results = perform_data_quality_checks(aligned_df, symbol)
                quality_check_results[symbol] = check_results
                
                # Write aligned data back to HDFS
                if passed_checks and write_aligned_data(aligned_df, symbol):
                    success_count += 1
        
        logger.info(f"Economic Indicators ETL job completed. Successfully processed {success_count}/{len(symbols)} symbols")
        
        # Save quality check results
        quality_check_path = os.path.join(RAW_DATA_PATH, "data_quality_checks.json")
        import json
        with open(quality_check_path, 'w') as f:
            json.dump(quality_check_results, f)
        
        logger.info(f"Saved data quality check results to {quality_check_path}")
        
    except Exception as e:
        logger.error(f"Error in Economic Indicators ETL job: {str(e)}")
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
