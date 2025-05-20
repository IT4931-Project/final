#!/usr/bin/env python3
"""
test script

TODO: remove
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import tensorflow as tf

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_integration")

def test_imports():
    """Kiểm tra xem tất cả các module cần thiết có thể được import không"""
    try:
        # Các import này sẽ hoạt động nếu việc tích hợp thành công
        from trainer.model import SelfAttention, build_model, model_creator
        from trainer.data_prep import add_technical_indicators, prepare_data_for_training
        
        logger.info("✅ All required modules imported successfully")
        return True
    except ImportError as e:
        logger.error(f"❌ Import error: {str(e)}")
        return False

def test_model_creation():
    """Kiểm tra xem mô hình có thể được tạo với kiến trúc đúng không"""
    try:
        from trainer.model import build_model
        
        # Tạo mô hình kiểm thử
        model = build_model(input_timesteps=10, output_timesteps=5, features=3, lstm_units=8, heads=2)
        
        # Xác minh kiến trúc mô hình
        expected_input_shape = (None, 10, 3)  # (batch, timesteps, features)
        expected_output_shape = (None, 5, 1)  # (batch, future_days, 1)
        
        actual_input_shape = model.input_shape
        actual_output_shape = model.output_shape
        
        if actual_input_shape != expected_input_shape:
            logger.error(f"❌ Model input shape mismatch: expected {expected_input_shape}, got {actual_input_shape}")
            return False
            
        if actual_output_shape != expected_output_shape:
            logger.error(f"❌ Model output shape mismatch: expected {expected_output_shape}, got {actual_output_shape}")
            return False
        
        logger.info("✅ Model created with correct architecture")
        return True
    except Exception as e:
        logger.error(f"❌ Model creation error: {str(e)}")
        return False

def test_spark_session():
    """Kiểm tra xem một phiên Spark có thể được tạo không"""
    try:
        # Tạo một phiên Spark cục bộ tối thiểu
        spark = SparkSession.builder \
            .appName("TestIntegration") \
            .master("local[1]") \
            .getOrCreate()
            
        # Kiểm tra xem phiên có hoạt động không
        if spark.sparkContext.isActive:
            logger.info("✅ Spark session created successfully")
            spark.stop()
            return True
        else:
            logger.error("❌ Spark session created but not active")
            return False
    except Exception as e:
        logger.error(f"❌ Spark session creation error: {str(e)}")
        return False

def test_technical_indicators():
    """Kiểm tra xem các chỉ báo kỹ thuật có thể được tính toán không"""
    try:
        from trainer.data_prep import add_technical_indicators
        
        # Tạo một phiên Spark tối thiểu
        spark = SparkSession.builder \
            .appName("TestTechnicalIndicators") \
            .master("local[1]") \
            .getOrCreate()
        
        # Tạo dữ liệu kiểm thử tối thiểu
        data = [
            (pd.Timestamp('2023-01-01'), 100.0, 102.0, 99.0, 101.0, 1000),
            (pd.Timestamp('2023-01-02'), 101.0, 103.0, 100.0, 102.0, 1200),
            (pd.Timestamp('2023-01-03'), 102.0, 104.0, 101.0, 103.0, 1300),
            (pd.Timestamp('2023-01-04'), 103.0, 105.0, 102.0, 104.0, 1100),
            (pd.Timestamp('2023-01-05'), 104.0, 106.0, 103.0, 105.0, 1400),
        ]
        
        columns = ["date", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, columns)
        
        # Thêm các chỉ báo kỹ thuật
        df_with_indicators = add_technical_indicators(df)
        
        # Kiểm tra xem các chỉ báo đã được thêm chưa
        expected_columns = columns + [f"SMA_3", f"STD_3", "BB_upper", "BB_lower", f"ATR_3", "OBV"]
        for col in expected_columns:
            if col not in df_with_indicators.columns:
                logger.error(f"❌ Expected column {col} not found in result")
                spark.stop()
                return False
        
        logger.info("✅ Technical indicators calculated successfully")
        spark.stop()
        return True
    except Exception as e:
        logger.error(f"❌ Technical indicators calculation error: {str(e)}")
        try:
            spark.stop()
        except:
            pass
        return False

def run_all_tests():
    """Chạy tất cả các kiểm thử tích hợp"""
    tests = [
        ("Import Test", test_imports),
        ("Model Creation Test", test_model_creation),
        ("Spark Session Test", test_spark_session),
        ("Technical Indicators Test", test_technical_indicators)
    ]
    
    success_count = 0
    
    logger.info("Starting Integration Tests")
    logger.info("-----------------------")
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning: {test_name}")
        if test_func():
            success_count += 1
        logger.info("-----------------------")
    
    logger.info(f"\nTest Results: {success_count}/{len(tests)} tests passed")
    
    if success_count == len(tests):
        logger.info("✅ All integration tests passed!")
        return True
    else:
        logger.warning(f"⚠️ {len(tests) - success_count} tests failed")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
