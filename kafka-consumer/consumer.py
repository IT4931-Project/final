#!/usr/bin/env python3
"""
Kafka Consumer cho Dữ Liệu Cổ Phiếu

Script này consume data cổ phiếu từ Kafka và save trong Mongo.
"""

import os
import json
import logging
import datetime
import pandas as pd
import pymongo
import pytz
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/kafka_consumer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("kafka_consumer")

# Tải biến môi trường
load_dotenv()

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
KAFKA_TOPIC_OHLCV = os.getenv('KAFKA_TOPIC', 'stock_ohlcv')
KAFKA_TOPIC_ACTIONS = os.getenv('KAFKA_TOPIC_ACTIONS', 'stock_actions')
KAFKA_TOPIC_INFO = os.getenv('KAFKA_TOPIC_INFO', 'stock_info')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'stock_consumer_group')
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# Cấu hình MongoDB
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'password')

def connect_to_mongodb():
    """
    Kết nối đến cơ sở dữ liệu MongoDB
    
    Trả về:
        pymongo.database.Database: Kết nối cơ sở dữ liệu MongoDB
    """
    try:
        client = pymongo.MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            serverSelectionTimeoutMS=5000  # 5 second timeout
        )
        
        # Kiểm tra kết nối
        client.server_info()
        logger.info(f"Connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
        
        # Lấy cơ sở dữ liệu
        db = client[MONGO_DATABASE]
        return db
    
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return None

def create_kafka_consumer():
    """Tạo và trả về một thể hiện của Kafka consumer"""
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET
    }
    consumer = Consumer(consumer_conf)
    
    # Đăng ký nhiều topic
    topics = [KAFKA_TOPIC_OHLCV, KAFKA_TOPIC_ACTIONS, KAFKA_TOPIC_INFO]
    consumer.subscribe(topics)
    
    logger.info(f"Kafka consumer subscribed to topics: {', '.join(topics)}")
    return consumer

def save_ohlcv_to_mongodb(db, data, ticker):
    """
    Lưu bản ghi dữ liệu giá vào MongoDB
    
    Tham số:
        db (pymongo.database.Database): Kết nối cơ sở dữ liệu MongoDB
        data (dict): Bản ghi dữ liệu cần lưu
        ticker (str): Mã cổ phiếu
    
    Trả về:
        bool: True nếu thành công, False nếu không thành công
    """
    if db is None:
        logger.error("MongoDB connection not available")
        return False
    
    try:
        # Lấy collection cho mã cổ phiếu này
        collection = db[f'stock_{ticker}']
        
        # Thêm trường timestamp
        data['timestamp'] = datetime.datetime.now(pytz.UTC)
        
        # Chèn document
        result = collection.insert_one(data)
        logger.debug(f"Saved price record with ID {result.inserted_id} to collection stock_{ticker}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving price data to MongoDB: {str(e)}")
        return False

def save_actions_to_mongodb(db, data, ticker):
    """
    Lưu bản ghi dữ liệu hành động vào MongoDB
    
    Tham số:
        db (pymongo.database.Database): Kết nối cơ sở dữ liệu MongoDB
        data (dict): Bản ghi dữ liệu cần lưu
        ticker (str): Mã cổ phiếu
    
    Trả về:
        bool: True nếu thành công, False nếu không thành công
    """
    if db is None:
        logger.error("MongoDB connection not available")
        return False
    
    try:
        # Lấy collection cho hành động của mã cổ phiếu này
        collection = db[f'stock_{ticker}_actions']
        
        # Thêm trường timestamp
        data['timestamp'] = datetime.datetime.now(pytz.UTC)
        
        # Chèn document
        result = collection.insert_one(data)
        logger.debug(f"Saved actions record with ID {result.inserted_id} to collection stock_{ticker}_actions")
        return True
    
    except Exception as e:
        logger.error(f"Error saving actions data to MongoDB: {str(e)}")
        return False

def save_info_to_mongodb(db, data, ticker):
    """
    Lưu bản ghi thông tin công ty vào MongoDB
    
    Tham số:
        db (pymongo.database.Database): Kết nối cơ sở dữ liệu MongoDB
        data (dict): Bản ghi dữ liệu cần lưu
        ticker (str): Mã cổ phiếu
    
    Trả về:
        bool: True nếu thành công, False nếu không thành công
    """
    if db is None:
        logger.error("MongoDB connection not available")
        return False
    
    try:
        # Lấy collection cho thông tin công ty
        collection = db['company_info']
        
        # Thêm trường timestamp
        data['timestamp'] = datetime.datetime.now(pytz.UTC)
        
        # Sử dụng mã cổ phiếu làm định danh duy nhất và cập nhật nếu tồn tại
        query = {'ticker': ticker}
        update = {'$set': data}
        result = collection.update_one(query, update, upsert=True)
        
        if result.upserted_id:
            logger.debug(f"Inserted company info for {ticker} with ID {result.upserted_id}")
        else:
            logger.debug(f"Updated company info for {ticker}, matched {result.matched_count} documents")
        
        return True
    
    except Exception as e:
        logger.error(f"Error saving company info to MongoDB: {str(e)}")
        return False

def process_message(db, msg, topic_name):
    """
    Xử lý một tin nhắn từ Kafka
    
    Tham số:
        db (pymongo.database.Database): Kết nối cơ sở dữ liệu MongoDB
        msg: Đối tượng tin nhắn Kafka
        topic_name (str): Topic mà tin nhắn đến từ đó
    
    Trả về:
        bool: True nếu thành công, False nếu không thành công
    """
    try:
        # Phân tích dữ liệu JSON
        value = msg.value().decode('utf-8') if msg.value() else None
        data = json.loads(value)
        ticker = data.get('ticker')
        
        if not ticker:
            logger.warning(f"Message missing ticker: {data}")
            return False
        
        # Xử lý dựa trên topic
        if topic_name == KAFKA_TOPIC_OHLCV:
            return save_ohlcv_to_mongodb(db, data, ticker)
        elif topic_name == KAFKA_TOPIC_ACTIONS:
            return save_actions_to_mongodb(db, data, ticker)
        elif topic_name == KAFKA_TOPIC_INFO:
            return save_info_to_mongodb(db, data, ticker)
        else:
            logger.warning(f"Unknown topic: {topic_name}")
            return False
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from message: {e}")
        logger.debug(f"Raw message value: {value}")
        return False
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False

def main():
    """Hàm chính để chạy Kafka consumer"""
    logger.info("Starting Kafka Consumer for Stock Data")
    
    # Kết nối đến MongoDB
    db = connect_to_mongodb()
    if db is None:
        logger.error("Failed to connect to MongoDB. Exiting.")
        return
    
    # Tạo Kafka consumer
    consumer = create_kafka_consumer()
    
    # Theo dõi thống kê
    message_count = 0
    success_count = 0
    topic_counts = {
        KAFKA_TOPIC_OHLCV: 0,
        KAFKA_TOPIC_ACTIONS: 0,
        KAFKA_TOPIC_INFO: 0
    }
    last_report_time = datetime.datetime.now()
    
    try:
        while True:
            # Lấy tin nhắn
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            
            message_count += 1
            topic_name = msg.topic()
            
            # Xử lý tin nhắn dựa trên topic của nó
            if process_message(db, msg, topic_name):
                success_count += 1
                topic_counts[topic_name] = topic_counts.get(topic_name, 0) + 1
            
            # Ghi log tiến trình sau mỗi 100 tin nhắn hoặc 30 giây
            current_time = datetime.datetime.now()
            if message_count % 100 == 0 or (current_time - last_report_time).seconds >= 30:
                logger.info(f"Processed {message_count} messages ({success_count} successful)")
                logger.info(f"Breakdown by topic: OHLCV={topic_counts[KAFKA_TOPIC_OHLCV]}, " +
                            f"Actions={topic_counts[KAFKA_TOPIC_ACTIONS]}, " +
                            f"Info={topic_counts[KAFKA_TOPIC_INFO]}")
                last_report_time = current_time
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    finally:
        # Đóng consumer
        consumer.close()
        logger.info(f"Consumer closed. Processed {message_count} messages ({success_count} successful)")
        logger.info(f"Final breakdown by topic: OHLCV={topic_counts[KAFKA_TOPIC_OHLCV]}, " +
                    f"Actions={topic_counts[KAFKA_TOPIC_ACTIONS]}, " +
                    f"Info={topic_counts[KAFKA_TOPIC_INFO]}")

if __name__ == "__main__":
    main()
