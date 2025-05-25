#!/usr/bin/env python3
"""
trainer

- architecture: Bidirectional LSTM + Self-Attention
- data_prep: Chuẩn bị dữ liệu cho huấn luyện
- model: Tạo mô hình
- test_integration: check integration
"""

import os
import sys
import time
import logging
import datetime
import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, TensorBoard
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from bigdl.orca import init_orca_context, stop_orca_context
from bigdl.orca.learn.tf.estimator import Estimator
import horovod.tensorflow.keras as hvd
from tensorflow.distribute import MultiWorkerMirroredStrategy, MirroredStrategy
import socket

# Import our custom modules
from trainer.model import model_creator
from trainer.data_prep import prepare_data_for_training

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/trainer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("trainer")

# Tải biến môi trường
# Explicitly load .env from /app/.env and override existing env vars
# This requires .env to be copied to /app/.env in the Dockerfile
load_dotenv(dotenv_path='/app/.env', override=True)

# Cấu hình Spark
MODEL_PATH = os.getenv('MODEL_PATH', '/app/data/models') # Local path for models
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
NUM_WORKERS = int(os.getenv('NUM_WORKERS', 1))  # Number of spark workers
NUM_CORES_PER_WORKER = int(os.getenv('NUM_CORES_PER_WORKER', 2)) # Cores for the worker
WORKER_MEMORY = os.getenv('WORKER_MEMORY', '2g') # Memory for the worker
NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', NUM_CORES_PER_WORKER)) # Partitions for the worker
USE_HOROVOD = os.getenv('USE_HOROVOD', 'false').lower() == 'true'
USE_MULTI_WORKER = os.getenv('USE_MULTI_WORKER', 'true').lower() == 'true'
LOG_DIR = os.getenv('LOG_DIR', '/app/logs/tensorboard')

# Tham số huấn luyện
SEQUENCE_LENGTH = int(os.getenv('SEQUENCE_LENGTH', 30))
FUTURE_DAYS = int(os.getenv('FUTURE_DAYS', 7))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 64))
GLOBAL_BATCH_SIZE = BATCH_SIZE * NUM_WORKERS  # For distributed training
EPOCHS = int(os.getenv('EPOCHS', 50))
PATIENCE = int(os.getenv('PATIENCE', 10))
TRAIN_TEST_SPLIT = float(os.getenv('TRAIN_TEST_SPLIT', 0.8))
LEARNING_RATE = float(os.getenv('LEARNING_RATE', 0.001))
HIDDEN_UNITS = int(os.getenv('HIDDEN_UNITS', 64))
DROPOUT_RATE = float(os.getenv('DROPOUT_RATE', 0.1))
ATTENTION_HEADS = int(os.getenv('ATTENTION_HEADS', 4))

# Mã cổ phiếu để huấn luyện mô hình
SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')

def evaluate_model(estimator, val_shards, feature_idx=3, symbol="UNKNOWN"):
    """
    Đánh giá hiệu suất mô hình
    
    Tham số:
        estimator (Estimator): BigDL Orca estimator đã huấn luyện
        val_shards (SparkXShards): Dữ liệu kiểm định dạng SparkXShards
        feature_idx (int): Chỉ số của đặc trưng mục tiêu (giá đóng cửa)
        symbol (str): Mã cổ phiếu
        
    Trả về:
        dict: Các chỉ số đánh giá
    """
    logger.info(f"Evaluating model for {symbol}")
    
    # Lấy dữ liệu kiểm định
    val_x, val_y = val_shards.get_data()
    
    # Thực hiện dự đoán
    y_pred = estimator.predict(val_x)
    
    # Trích xuất dự đoán của ngày cuối cùng để đơn giản hóa
    y_pred_last_day = np.array([pred[:, -1, 0] for pred in y_pred])
    y_true_last_day = np.array([y[:, -1, 0] for y in val_y])
    
    # Tính toán các chỉ số trên dữ liệu đã chuẩn hóa (chúng ta không có quyền truy cập trực tiếp vào scaler ở đây)
    mse = mean_squared_error(y_true_last_day, y_pred_last_day)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_true_last_day, y_pred_last_day)
    
    # Đối với R², chúng ta cần đảm bảo dữ liệu không phải là một hằng số
    if np.var(y_true_last_day) > 0:
        r2 = r2_score(y_true_last_day, y_pred_last_day)
    else:
        r2 = 0.0
    
    # Tính MAPE nếu không có giá trị 0 trong dữ liệu thực
    if np.all(y_true_last_day != 0):
        mape = np.mean(np.abs((y_true_last_day - y_pred_last_day) / y_true_last_day)) * 100
    else:
        mape = np.nan
    
    logger.info(f"Evaluation metrics for {symbol}:")
    logger.info(f"  MSE: {mse:.4f}")
    logger.info(f"  RMSE: {rmse:.4f}")
    logger.info(f"  MAE: {mae:.4f}")
    logger.info(f"  R²: {r2:.4f}")
    if not np.isnan(mape):
        logger.info(f"  MAPE: {mape:.2f}%")
    else:
        logger.info("  MAPE: N/A (contains zero values)")
    
    return {
        'mse': float(mse),
        'rmse': float(rmse),
        'mae': float(mae),
        'r2': float(r2),
        'mape': float(mape) if not np.isnan(mape) else None
    }

def save_model_files(model, scaler, metrics, metadata, symbol, spark=None):
    """
    Lưu các file mô hình và metadata vào cả HDFS và local filesystem
    
    Tham số:
        model (keras.Model): Mô hình đã huấn luyện
        scaler: Bộ chuẩn hóa đặc trưng
        metrics (dict): Các chỉ số đánh giá
        metadata (dict): Metadata bổ sung
        symbol (str): Mã cổ phiếu
        spark (SparkSession, optional): Phiên Spark để tương tác với HDFS
        
    Trả về:
        bool: True nếu thành công, False nếu không
    """
    try:
        # Rank/Worker ID - trong trường hợp huấn luyện phân tán
        # Chỉ worker 0 lưu model
        is_main_worker = True
        if USE_HOROVOD:
            is_main_worker = hvd.rank() == 0
        
        if not is_main_worker:
            logger.info(f"Worker {hvd.rank()} skipping model saving (only worker 0 saves model)")
            return True
        
        # Tạo thư mục lưu mô hình local
        model_dir = os.path.join(MODEL_PATH, symbol)
        os.makedirs(model_dir, exist_ok=True)
        
        # Tạo timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        hostname = socket.gethostname()
        
        # 1. Lưu mô hình hoàn chỉnh với trọng số vào local filesystem
        model_path = os.path.join(model_dir, f"model_{timestamp}.h5")
        model.save(model_path)
        logger.info(f"Saved model to local path: {model_path}")
        
        # 2. Lưu scaler cho việc chuyển đổi ngược trong tương lai
        if hasattr(scaler, 'scale_'):
            scaler_path = os.path.join(model_dir, f"scaler_{timestamp}.npz")
            np.savez(scaler_path,
                    scale=scaler.scale_,
                    min=scaler.min_,
                    data_min=scaler.data_min_,
                    data_max=scaler.data_max_,
                    data_range=scaler.data_range_,
                    feature_range=scaler.feature_range)
            logger.info(f"Saved scaler to local path: {scaler_path}")
        else:
            logger.warning("Scaler doesn't have expected attributes, saving just the model")
        
        # 3. Lưu các chỉ số và metadata
        metadata.update({
            "metrics": metrics,
            "created_at": datetime.datetime.now().isoformat(),
            "hostname": hostname,
            "distributed_training": {
                "use_horovod": USE_HOROVOD,
                "use_multi_worker": USE_MULTI_WORKER,
                "num_workers": NUM_WORKERS,
                "cores_per_worker": NUM_CORES_PER_WORKER
            },
            "params": {
                "sequence_length": SEQUENCE_LENGTH,
                "future_days": FUTURE_DAYS,
                "batch_size": GLOBAL_BATCH_SIZE,
                "epochs": EPOCHS,
                "learning_rate": LEARNING_RATE,
                "hidden_units": HIDDEN_UNITS,
                "attention_heads": ATTENTION_HEADS,
                "dropout_rate": DROPOUT_RATE
            }
        })
        
        metadata_path = os.path.join(model_dir, f"metadata_{timestamp}.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata to local path: {metadata_path}")
        
        # 4. Tạo file latest.txt trỏ đến phiên bản mô hình hiện tại
        latest_path = os.path.join(model_dir, "latest.txt")
        with open(latest_path, 'w') as f:
            f.write(timestamp)
        logger.info(f"Updated local latest.txt to point to {timestamp}")
        
        # HDFS saving is removed for simplification. Models are saved locally.
        logger.info("Model, scaler, and metadata saved locally. HDFS saving is skipped.")
        return True
    
    except Exception as e:
        logger.error(f"Error saving model files: {str(e)}")
        return False

def init_distributed_strategy():
    """
    Initialize TensorFlow distributed training strategy
    
    Returns:
        Strategy object and a boolean indicating whether this is the main worker
    """
    if USE_HOROVOD:
        # Initialize Horovod
        logger.info("Initializing Horovod distributed training")
        hvd.init()
        
        # Pin GPU to be used to process local rank (one GPU per process)
        gpus = tf.config.experimental.list_physical_devices('GPU')
        if gpus:
            for gpu in gpus:
                tf.config.experimental.set_memory_growth(gpu, True)
            if len(gpus) > 1:
                tf.config.experimental.set_visible_devices(gpus[hvd.local_rank()], 'GPU')
        
        # Adjust learning rate based on number of workers
        global LEARNING_RATE
        LEARNING_RATE = LEARNING_RATE * hvd.size()
        
        # Check if this is the main worker
        is_main_worker = hvd.rank() == 0
        logger.info(f"Horovod initialized: rank={hvd.rank()}, size={hvd.size()}, local_rank={hvd.local_rank()}")
        
        return None, is_main_worker  # No need for a strategy object with Horovod
    
    elif USE_MULTI_WORKER:
        # Use TensorFlow's MultiWorkerMirroredStrategy
        logger.info("Initializing MultiWorkerMirroredStrategy distributed training")
        try:
            strategy = MultiWorkerMirroredStrategy()
            # Detect if this is the main worker (usually worker with task_id=0)
            tf_config = json.loads(os.environ.get('TF_CONFIG', '{}'))
            task_type = tf_config.get('task', {}).get('type', 'worker')
            task_id = tf_config.get('task', {}).get('index', 0)
            is_main_worker = (task_type == 'worker' and task_id == 0)
            logger.info(f"MultiWorkerMirroredStrategy initialized: task_type={task_type}, task_id={task_id}")
            return strategy, is_main_worker
        except Exception as e:
            logger.warning(f"Failed to initialize MultiWorkerMirroredStrategy: {e}")
            logger.warning("Falling back to MirroredStrategy for single-worker multi-GPU training")
            strategy = MirroredStrategy()
            return strategy, True  # Single worker is always the main worker
    else:
        # Use MirroredStrategy for local multi-GPU training
        logger.info("Initializing MirroredStrategy for single-worker multi-GPU training")
        strategy = MirroredStrategy()
        return strategy, True  # Single worker is always the main worker

def train_model_for_symbol(symbol, spark=None):
    """
    Huấn luyện mô hình cho một mã cổ phiếu sử dụng huấn luyện phân tán
    
    Tham số:
        symbol (str): Mã cổ phiếu để huấn luyện
        spark (SparkSession, optional): Phiên Spark để tương tác với HDFS
        
    Trả về:
        bool: True nếu thành công, False nếu không
    """
    try:
        logger.info(f"Starting distributed model training for {symbol}")

        # Initialize distributed training
        strategy, is_main_worker = init_distributed_strategy()
        
        # Chuẩn bị dữ liệu để huấn luyện sử dụng module data_prep
        train_shards, val_shards, feature_count, scaler_model = prepare_data_for_training(symbol)
        
        if train_shards is None or val_shards is None:
            logger.error(f"Failed to prepare data for {symbol}")
            return False
        
        logger.info(f"Data prepared with {feature_count} features")
        
        # Cấu hình tham số mô hình
        model_config = {
            "features": feature_count,
            "lstm_units": HIDDEN_UNITS,
            "heads": ATTENTION_HEADS
        }
        
        try:
            # Thiết lập log directory cho TensorBoard
            log_dir = os.path.join(LOG_DIR, symbol, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
            os.makedirs(log_dir, exist_ok=True)
            
            # Khởi tạo Orca context
            logger.info("Initializing Orca context with distributed settings")
            init_orca_context(
                cluster_mode="spark", 
                cores=NUM_CORES_PER_WORKER, # Cores per worker node
                memory=WORKER_MEMORY,       # Memory per worker node
                num_nodes=NUM_WORKERS,      # Number of worker nodes (should be 1 for simplified setup)
                driver_cores=2,             # Cores for the driver
                driver_memory="2g"          # Memory for the driver
            )
            
            # Tạo Orca Estimator với model creator và distributed configs
            logger.info("Creating Orca Estimator with distributed configuration")
            extra_config = {}
            
            if USE_HOROVOD:
                # Horovod specific configurations
                extra_config.update({
                    "horovod": True,
                    "learning_rate": LEARNING_RATE,
                    "horovod_size": hvd.size(),
                    "horovod_rank": hvd.rank()
                })
                
            estimator = Estimator.from_keras(
                model_creator=model_creator, 
                config={**model_config, **extra_config},
                optimizer="adam"
            )
            
            # Thiết lập callbacks
            checkpoint_path = os.path.join(MODEL_PATH, f"{symbol}_best_model.h5")
            os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
            
            callbacks = [
                EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True),
                ModelCheckpoint(filepath=checkpoint_path, monitor='val_loss', save_best_only=True),
                TensorBoard(log_dir=log_dir, histogram_freq=1, profile_batch='500,520')
            ]
            
            # Add Horovod specific callbacks if using Horovod
            if USE_HOROVOD:
                callbacks.append(hvd.callbacks.BroadcastGlobalVariablesCallback(0))
                callbacks.append(hvd.callbacks.MetricAverageCallback())
                # Only save checkpoints on worker 0
                if hvd.rank() == 0:
                    callbacks.append(ModelCheckpoint(filepath=checkpoint_path, monitor='val_loss', save_best_only=True))
                else:
                    # Remove ModelCheckpoint from other workers
                    callbacks = [cb for cb in callbacks if not isinstance(cb, ModelCheckpoint)]
            
            # Huấn luyện mô hình
            logger.info(f"Starting distributed training for {symbol} with batch size {GLOBAL_BATCH_SIZE}")
            estimator.fit(
                data=train_shards,
                epochs=EPOCHS,
                batch_size=GLOBAL_BATCH_SIZE if USE_HOROVOD else BATCH_SIZE,
                validation_data=val_shards,
                callbacks=callbacks
            )
            
            # Đánh giá mô hình
            metrics = evaluate_model(estimator, val_shards, feature_idx=3, symbol=symbol)
            
            # Lấy mô hình đã huấn luyện
            model = estimator.get_model()
            
            # Lưu các file mô hình và metadata
            metadata = {
                "symbol": symbol,
                "feature_count": feature_count,
                "model_type": "Distributed_Bidirectional_LSTM_SelfAttention",
                "target_column": "close",
                "target_column_idx": 3,  # Assuming 'close' is the 4th column (index 3)
                "distributed_training": {
                    "mode": "horovod" if USE_HOROVOD else "tf_distributed",
                    "workers": NUM_WORKERS if USE_MULTI_WORKER else 1,
                    "global_batch_size": GLOBAL_BATCH_SIZE
                }
            }
            
            # Save model - only the main worker in distributed training
            if is_main_worker:
                save_model_files(model, scaler_model, metrics, metadata, symbol, None) # Pass None for spark
            
            # Dừng Orca context
            stop_orca_context()
            
            logger.info(f"Successfully completed distributed model training for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error during training: {str(e)}")
            
            # Thử dọn dẹp Orca context nếu nó đã được khởi tạo
            try:
                stop_orca_context()
            except:
                pass
                
            return False
        
    except Exception as e:
        logger.error(f"Error setting up training for {symbol}: {str(e)}")
        return False

def main():
    """Hàm chính chạy quá trình huấn luyện mô hình phân tán"""
    logger.info("Starting Distributed Model Training Job with Bidirectional LSTM + Self-Attention Architecture")
    
    # Cấu hình tăng trưởng bộ nhớ TensorFlow để tránh lỗi hết bộ nhớ (OOM)
    physical_devices = tf.config.list_physical_devices('GPU')
    if physical_devices:
        logger.info(f"Found {len(physical_devices)} GPUs")
        for device in physical_devices:
            tf.config.experimental.set_memory_growth(device, True)
    else:
        logger.info("No GPUs found, using CPU")
    
    # Tạo thư mục mô hình nếu nó chưa tồn tại
    os.makedirs(MODEL_PATH, exist_ok=True)
    
    # Tạo thư mục log cho TensorBoard
    os.makedirs(LOG_DIR, exist_ok=True)
    
    spark = None # No HDFS, so no need for a separate Spark session here for HDFS ops
    
    # Determine if we're using distributed training
    if USE_HOROVOD:
        # Initialize Horovod before training
        hvd.init()
        
        # Only worker 0 logs the full list of symbols
        if hvd.rank() == 0:
            logger.info(f"Training models for symbols: {', '.join(SYMBOLS)}")
        
        # In Horovod mode, distribute symbols across workers
        worker_symbols = []
        worker_id = hvd.rank()
        num_workers = hvd.size()
        
        for i, symbol in enumerate(SYMBOLS):
            if i % num_workers == worker_id:
                worker_symbols.append(symbol.strip())
        
        logger.info(f"Worker {worker_id}/{num_workers} will train symbols: {', '.join(worker_symbols)}")
        
        # Each worker trains its subset of symbols
        success_count = 0
        for symbol in worker_symbols:
            if train_model_for_symbol(symbol, spark):
                success_count += 1
        
        logger.info(f"Worker {worker_id} training completed. Successfully trained {success_count}/{len(worker_symbols)} symbols")
        
        # Aggregated results can only be shown by worker 0
        if hvd.rank() == 0:
            logger.info(f"Distributed training job completed across {num_workers} workers")
    else:
        # Standard mode - process all symbols
        logger.info(f"Training models for symbols: {', '.join(SYMBOLS)}")
        
        # Train models in sequence
        success_count = 0
        for symbol in SYMBOLS:
            symbol = symbol.strip()
            if train_model_for_symbol(symbol, spark):
                success_count += 1
        
        logger.info(f"Training job completed. Successfully trained models for {success_count}/{len(SYMBOLS)} symbols")
    
    # Clean up
    # if spark is not None: # spark is None now
    #     spark.stop()

if __name__ == "__main__":
    main()
