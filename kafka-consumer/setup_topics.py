#!/usr/bin/env python3
"""
Script setup Topic Kafka

script create topic cho Kafka
run script một lần trước khi khởi động nếu các topic chưa tồn tại.
"""

import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("kafka_setup")

# Tải biến môi trường
load_dotenv()

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
KAFKA_TOPIC_OHLCV = os.getenv('KAFKA_TOPIC', 'stock_ohlcv')
KAFKA_TOPIC_ACTIONS = os.getenv('KAFKA_TOPIC_ACTIONS', 'stock_actions')
KAFKA_TOPIC_INFO = os.getenv('KAFKA_TOPIC_INFO', 'stock_info')

def create_topics():
    """Tạo các topic Kafka nếu chúng chưa tồn tại"""
    
    # Tạo admin client
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    # Lấy các topic đã tồn tại
    existing_topics = admin_client.list_topics().topics
    logger.info(f"Existing topics: {', '.join(existing_topics.keys())}")
    
    # Định nghĩa các topic mới cần tạo
    topics_to_create = []
    
    # Topic OHLCV
    if KAFKA_TOPIC_OHLCV not in existing_topics:
        topics_to_create.append(NewTopic(
            KAFKA_TOPIC_OHLCV,
            num_partitions=3,  # Nhiều partition để có thể mở rộng
            replication_factor=3,  # Đặt bằng 3 để tận dụng cả 3 broker 
            config={
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # Giữ lại dữ liệu 7 ngày
                'cleanup.policy': 'delete',
                'min.insync.replicas': '2',  # Yêu cầu ít nhất 2 replica báo nhận write
                'unclean.leader.election.enable': 'false'  # Ngăn chặn bầu leader không đồng bộ
            }
        ))
        logger.info(f"Will create topic {KAFKA_TOPIC_OHLCV} with 3x replication")
    
    # Topic Actions
    if KAFKA_TOPIC_ACTIONS not in existing_topics:
        topics_to_create.append(NewTopic(
            KAFKA_TOPIC_ACTIONS,
            num_partitions=2,
            replication_factor=3,  # Tăng từ 1 lên 3
            config={
                'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # Giữ lại dữ liệu 30 ngày
                'cleanup.policy': 'delete',
                'min.insync.replicas': '2',
                'unclean.leader.election.enable': 'false'
            }
        ))
        logger.info(f"Will create topic {KAFKA_TOPIC_ACTIONS} with 3x replication")
    
    # Topic Info
    if KAFKA_TOPIC_INFO not in existing_topics:
        topics_to_create.append(NewTopic(
            KAFKA_TOPIC_INFO,
            num_partitions=1,  # Dự kiến khối lượng thấp hơn
            replication_factor=3,  # Tăng từ 1 lên 3
            config={
                'retention.ms': str(90 * 24 * 60 * 60 * 1000),  # Giữ lại dữ liệu 90 ngày
                'cleanup.policy': 'delete',
                'min.insync.replicas': '2',
                'unclean.leader.election.enable': 'false'
            }
        ))
        logger.info(f"Will create topic {KAFKA_TOPIC_INFO} with 3x replication")
    
    # Tạo các topic
    if topics_to_create:
        futures = admin_client.create_topics(topics_to_create)
        
        # Chờ đợi việc tạo topic hoàn tất
        for topic, future in futures.items():
            try:
                future.result()  # Chờ đợi hoàn thành
                logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
    else:
        logger.info("All required topics already exist")

if __name__ == "__main__":
    logger.info("Starting Kafka topic setup")
    create_topics()
    logger.info("Kafka topic setup completed")
