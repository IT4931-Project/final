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
from dotenv import load_dotenv
from confluent_kafka import Producer
import csv

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

# load env
load_dotenv()

# config kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
KAFKA_TOPIC_OHLCV = os.getenv('KAFKA_TOPIC', 'stock_ohlcv')
KAFKA_TOPIC_ACTIONS = os.getenv('KAFKA_TOPIC_ACTIONS', 'stock_actions')
KAFKA_TOPIC_INFO = os.getenv('KAFKA_TOPIC_INFO', 'stock_info')

# intervals
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '86400'))  # Default: 24 hours

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
    for _, row in df.iterrows():
        # Tạo một bản ghi với các trường cần thiết
        try:
            # Try to format the date, handle different possible types
            if hasattr(row['Date'], 'strftime'):
                date_str = row['Date'].strftime('%Y-%m-%d')
            else:
                date_str = str(row['Date'])
                
            record = {
                'ticker': symbol,
                'date': date_str,
                'open': float(row['Open'].iloc[0]) if hasattr(row['Open'], 'iloc') else float(row['Open']),
                'high': float(row['High'].iloc[0]) if hasattr(row['High'], 'iloc') else float(row['High']),
                'low': float(row['Low'].iloc[0]) if hasattr(row['Low'], 'iloc') else float(row['Low']),
                'close': float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close']),
                'volume': int(row['Volume'].iloc[0]) if hasattr(row['Volume'], 'iloc') else int(row['Volume'])
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
    for _, row in df.iterrows():
        try:
            # Try to format the date, handle different possible types
            if hasattr(row['date'], 'strftime'):
                date_str = row['date'].strftime('%Y-%m-%d')
            else:
                date_str = str(row['date'])
                
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
                'stock_splits': stock_splits
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

def main():
    logger.info("Starting Financial Data Crawler with Kafka streaming")
    
    # Tạo Kafka producer
    producer = create_kafka_producer()
    
    while True:
        try:
            logger.info(f"Starting data collection cycle at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Tải các mã cổ phiếu từ Google Drive
            symbols = load_stock_symbols()
            
            # Xử lý từng mã cổ phiếu
            for symbol in symbols:
                symbol = symbol.strip()
                success = process_symbol(symbol, producer)
                if not success:
                    logger.warning(f"Failed to process symbol {symbol}")
                
                # Thêm độ trễ nhỏ giữa các mã cổ phiếu để tránh giới hạn tốc độ
                time.sleep(2)
            
            logger.info(f"Completed data collection cycle at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Waiting {FETCH_INTERVAL} seconds before next cycle...")
            
            # Chờ đến chu kỳ tiếp theo
            time.sleep(FETCH_INTERVAL)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            # Add a delay before retrying
            time.sleep(60)

if __name__ == "__main__":
    main()
