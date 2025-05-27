#!/usr/bin/env python3
"""
Crawler

Crawl -> Kafka

"""

import os
import time
import json
import logging
import datetime
import pandas as pd
import pytz
from confluent_kafka import Producer
import csv
import uuid
import concurrent.futures
from tqdm import tqdm
import re
import random

from fetch_utils import (
    fetch_stock_history,
    fetch_stock_actions,
    fetch_stock_info,
    fetch_stock_financials,
    load_stock_symbols
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("crawler")

# config kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
KAFKA_TOPIC_OHLCV = os.getenv('KAFKA_TOPIC', 'stock_ohlcv')
KAFKA_TOPIC_ACTIONS = os.getenv('KAFKA_TOPIC_ACTIONS', 'stock_actions')
KAFKA_TOPIC_INFO = os.getenv('KAFKA_TOPIC_INFO', 'stock_info')

# intervals
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '86400'))  # Default: 24 hours

# Parallelization settings
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '10'))  # Default: 10 parallel workers

# Retry settings
BACKOFF_MAX_TRIES = int(os.getenv('BACKOFF_MAX_TRIES', '5'))  # Maximum number of retry attempts
MIN_RETRY_WAIT = float(os.getenv('MIN_RETRY_WAIT', '1.0'))    # Initial wait time before retry (seconds)
MAX_RETRY_WAIT = float(os.getenv('MAX_RETRY_WAIT', '60.0'))   # Maximum wait time before retry (seconds)

def delivery_report(err, msg):
    """callback cho producer để check message status"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_kafka_producer():
    """create kafka producer"""
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    }
    return Producer(producer_conf)

def save_to_csv(data, symbol, data_type='history'):
    """
    Lưu dữ liệu vào file CSV để backup trong thư mục logs
    
    Tham số:
        data: Dữ liệu cần lưu (DataFrame hoặc dict)
        symbol (str): Mã cổ phiếu
        data_type (str): Loại dữ liệu (history, actions, info)
    """
    try:
        # Sử dụng thư mục logs để lưu backup data
        backup_dir = os.path.join('/app/logs', 'data_backup', symbol)
        os.makedirs(backup_dir, exist_ok=True)
        
        # Tạo tên file với timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        filename = os.path.join(backup_dir, f"{symbol}_{data_type}_{timestamp}")
        
        # Lưu dựa trên loại dữ liệu
        if isinstance(data, pd.DataFrame):
            data.to_csv(f"{filename}.csv", index=False)
        elif isinstance(data, dict):
            with open(f"{filename}.json", 'w') as json_file:
                json.dump(data, json_file, default=str)
        
        logger.info(f"Saved {data_type} data to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving {data_type} data to CSV/JSON: {e}")
def format_date(date_value):
    """
    Standardize date formatting from various formats to YYYY-MM-DD string
    
    Args:
        date_value: Date in various formats (pd.Timestamp, string, etc.)
    
    Returns:
        str: Formatted date string in YYYY-MM-DD format
    """
    try:
        if date_value is None:
            return None
            
        # Handle pandas Series or DataFrame
        if isinstance(date_value, pd.Series):
            # Extract first value if it's a Series
            if not date_value.empty:
                date_obj = date_value.iloc[0]
            else:
                return None
        # Handle direct pandas Timestamp
        elif isinstance(date_value, pd.Timestamp):
            date_obj = date_value
        # Handle string with embedded date pattern (from pandas Series string representation)
        elif isinstance(date_value, str):
            # Try to extract date pattern YYYY-MM-DD from string
            date_pattern = re.search(r'(\d{4}-\d{2}-\d{2})', date_value)
            if date_pattern:
                return date_pattern.group(1)
            # If no pattern found, try to parse as date
            try:
                date_obj = pd.to_datetime(date_value)
            except:
                return date_value
        else:
            # Try to convert to pandas timestamp
            try:
                date_obj = pd.to_datetime(date_value)
            except:
                return str(date_value)
        
        # Format as YYYY-MM-DD
        return date_obj.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Error formatting date {date_value}: {e}")
        # Return string representation as fallback
        return str(date_value)

def process_historical_data(symbol, producer):
    """data processing cho dữ liệu lịch sử"""
    logger.info(f"Processing historical data for {symbol}")
    
    
    df = fetch_stock_history(symbol, period="30d")
    if df is None or df.empty:
        return False
    
    # Lưu vào CSV để backup
    save_to_csv(df, symbol, 'history')
    
    # Gửi từng dòng đến Kafka
    count = 0
    for index, row in df.iterrows():
        # Tạo một bản ghi với các trường cần thiết
        try:
            # Use the format_date utility function
            date_str = None
            
            # Try to get date from 'Date' column first
            if 'Date' in row:
                date_str = format_date(row['Date'])
            
            # If no date from column, use index
            if not date_str:
                date_str = format_date(index)
            
            # Fallback to current date if all else fails
            if not date_str:
                date_str = datetime.date.today().strftime('%Y-%m-%d')
                
            record = {
                'ticker': symbol,
                'date': date_str,
                'open': float(row['Open'].iloc[0]) if hasattr(row['Open'], 'iloc') else float(row['Open']),
                'high': float(row['High'].iloc[0]) if hasattr(row['High'], 'iloc') else float(row['High']),
                'low': float(row['Low'].iloc[0]) if hasattr(row['Low'], 'iloc') else float(row['Low']),
                'close': float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close']),
                'volume': int(row['Volume'].iloc[0]) if hasattr(row['Volume'], 'iloc') else int(row['Volume']),
                'timestamp': datetime.datetime.now().isoformat()  # Add collection timestamp
            }
            
            # Chuyển đổi thành JSON và gửi đến Kafka
            json_value = json.dumps(record)
            producer.produce(
                topic=KAFKA_TOPIC_OHLCV,
                key=symbol,
                value=json_value,
                callback=delivery_report
            )
            count += 1
        except Exception as e:
            logger.error(f"Error processing row for {symbol}: {e}")
            continue
    
    producer.flush()
    logger.info(f"Sent {count} historical data records for {symbol}")
    return True

def process_actions_data(symbol, producer):
    """Xử lý và gửi dữ liệu hành động (cổ tức/chia tách) cho một mã cổ phiếu"""
    logger.info(f"Processing actions data for {symbol}")
    
    df = fetch_stock_actions(symbol)
    if df is None or df.empty:
        logger.info(f"No actions data available for {symbol}")
        return False
    
    # Lưu vào CSV để backup
    save_to_csv(df, symbol, 'actions')
    
    # Gửi từng dòng đến Kafka
    count = 0
    for index, row in df.iterrows():
        try:
            # Use the format_date utility function
            date_str = None
            
            # Try to get date from 'date' column first
            if 'date' in row:
                date_str = format_date(row['date'])
            
            # If no date from column, use index
            if not date_str:
                date_str = format_date(index)
            
            # Fallback to current date if all else fails
            if not date_str:
                date_str = datetime.date.today().strftime('%Y-%m-%d')
                
            # Tạo một bản ghi với các trường cần thiết
            dividends = 0.0
            stock_splits = 0.0
            
            if 'Dividends' in row:
                if hasattr(row['Dividends'], 'iloc'):
                    dividends = float(row['Dividends'].iloc[0])
                else:
                    dividends = float(row['Dividends'])
                    
            if 'Stock Splits' in row:
                if hasattr(row['Stock Splits'], 'iloc'):
                    stock_splits = float(row['Stock Splits'].iloc[0])
                else:
                    stock_splits = float(row['Stock Splits'])
                
            record = {
                'ticker': symbol,
                'date': date_str,
                'dividends': dividends,
                'stock_splits': stock_splits,
                'timestamp': datetime.datetime.now().isoformat()  # Add collection timestamp
            }
            
            # Chuyển đổi thành JSON và gửi đến Kafka
            json_value = json.dumps(record)
            producer.produce(
                topic=KAFKA_TOPIC_ACTIONS,
                key=symbol,
                value=json_value,
                callback=delivery_report
            )
            count += 1
        except Exception as e:
            logger.error(f"Error processing actions row for {symbol}: {e}")
            continue
    
    producer.flush()
    logger.info(f"Sent {count} actions records for {symbol}")
    return True

def process_company_info(symbol, producer):
    """Xử lý và gửi dữ liệu thông tin công ty cho một mã cổ phiếu"""
    logger.info(f"Processing company info for {symbol}")
    
    info = fetch_stock_info(symbol)
    if info is None:
        logger.info(f"No company info available for {symbol}")
        return False
    
    # Lưu vào JSON để backup
    save_to_csv(info, symbol, 'info')
    
    # Add timestamp to info
    info['timestamp'] = datetime.datetime.now().isoformat()
    
    # Chuyển đổi thành JSON và gửi đến Kafka
    json_value = json.dumps(info, default=str)
    producer.produce(
        topic=KAFKA_TOPIC_INFO,
        key=symbol,
        value=json_value,
        callback=delivery_report
    )
    
    producer.flush()
    logger.info(f"Sent company info for {symbol}")
    return True

def process_symbol(symbol, producer):
    """Xử lý tất cả dữ liệu cho một mã cổ phiếu"""
    logger.info(f"Processing all data for {symbol}")
    
    success = True
    try:
        # Dữ liệu giá lịch sử
        if not process_historical_data(symbol, producer):
            success = False
            
        # Dữ liệu hành động (cổ tức, chia tách)
        if not process_actions_data(symbol, producer):
            # Điều này có thể không có sẵn cho tất cả các cổ phiếu, nên không coi là thất bại
            logger.warning(f"No actions data for {symbol}")
        
        # Thông tin công ty
        if not process_company_info(symbol, producer):
            # Điều này có thể không có sẵn cho tất cả các cổ phiếu, nên không coi là thất bại
            logger.warning(f"No company info for {symbol}")
    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {e}")
        success = False
    
    return success

def process_symbol_with_retry(symbol, producer):
    """Process a symbol with simple retry for API rate limits"""
    max_tries = BACKOFF_MAX_TRIES
    retry_wait = MIN_RETRY_WAIT
    
    for attempt in range(max_tries):
        try:
            return process_symbol(symbol, producer)
        except Exception as e:
            if attempt < max_tries - 1:  # Don't sleep on the last attempt
                logger.warning(f"Attempt {attempt+1}/{max_tries} failed for {symbol}: {str(e)}. Retrying in {retry_wait:.2f}s")
                time.sleep(retry_wait)
                # Increase wait time for next attempt, but don't exceed max
                retry_wait = min(retry_wait * 2, MAX_RETRY_WAIT)
            else:
                logger.error(f"All {max_tries} attempts failed for {symbol}: {str(e)}")
                raise

def process_symbols_parallel(symbols, producer):
    """Process multiple symbols in parallel using a thread pool"""
    logger.info(f"Processing {len(symbols)} symbols in parallel with {MAX_WORKERS} workers")
    
    successful_symbols = 0
    failed_symbols = 0
    
    # Shuffle symbols to distribute API load more evenly
    shuffled_symbols = list(symbols)
    random.shuffle(shuffled_symbols)

    # Use ThreadPoolExecutor for parallelization
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks to the executor with retry mechanism
        future_to_symbol = {
            executor.submit(process_symbol_with_retry, symbol.strip(), producer): symbol.strip()
            for symbol in shuffled_symbols if symbol.strip()
        }
        
        # Process results as they complete
        for future in tqdm(concurrent.futures.as_completed(future_to_symbol), total=len(future_to_symbol), desc="Processing Symbols"):
            symbol = future_to_symbol[future]
            try:
                success = future.result()
                if success:
                    logger.info(f"Successfully processed {symbol}")
                    successful_symbols += 1
                else:
                    logger.warning(f"Failed to process {symbol}")
                    failed_symbols += 1
            except Exception as e:
                logger.error(f"Exception while processing {symbol}: {e}")
                failed_symbols += 1
    
    logger.info(f"Parallel processing complete. Success: {successful_symbols}, Failed: {failed_symbols}")
    return successful_symbols > 0

def main():
    logger.info("Starting Financial Data Crawler with Kafka streaming and parallel processing")
    
    # Tạo Kafka producer
    producer = create_kafka_producer()
    
    while True:
        try:
            start_time = datetime.datetime.now()
            logger.info(f"Starting data collection cycle at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Tải các mã cổ phiếu từ Google Drive
            symbols = load_stock_symbols()
            
            # Process symbols in parallel
            success = process_symbols_parallel(symbols, producer)
            
            end_time = datetime.datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            logger.info(f"Completed data collection cycle in {processing_time:.2f} seconds at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Calculate time until next cycle
            wait_time = max(1, FETCH_INTERVAL - processing_time)
            logger.info(f"Waiting {wait_time:.2f} seconds before next cycle...")
            
            # Wait until next cycle (with sanity check)
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            # Add a delay before retrying
            time.sleep(60)

if __name__ == "__main__":
    main()
