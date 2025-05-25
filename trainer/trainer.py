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
# import tensorflow as tf # No longer using TensorFlow directly here
# from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, TensorBoard # No longer using Keras callbacks
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
# from bigdl.orca import init_orca_context, stop_orca_context # No longer using Orca
# from bigdl.orca.learn.tf.estimator import Estimator # No longer using Orca Estimator
# import horovod.tensorflow.keras as hvd # No longer using Horovod
# from tensorflow.distribute import MultiWorkerMirroredStrategy, MirroredStrategy # No longer using TF distributed strategies
import socket
import joblib # For saving scikit-learn models

# Import our custom modules
from trainer.model import create_random_forest_regressor # Changed model import
from trainer.data_prep import prepare_data_for_training # This will return X_train, X_val, y_train, y_val, num_features, target_scaler

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

# Cấu hình Trainer
MODEL_PATH = os.getenv('MODEL_PATH', '/app/data/models') # Local path for models
# SPARK_MASTER, NUM_WORKERS, etc. are not strictly needed for local sklearn training per symbol
# LOG_DIR = os.getenv('LOG_DIR', '/app/logs/tensorboard') # TensorBoard not used for RandomForest

# Tham số huấn luyện (can be used to pass to model creator if needed, or model.py handles its own env vars)
SEQUENCE_LENGTH = int(os.getenv('SEQUENCE_LENGTH', 30)) # Used by data_prep
FUTURE_DAYS = int(os.getenv('FUTURE_DAYS', 7)) # Used by data_prep
TRAIN_TEST_SPLIT = float(os.getenv('TRAIN_TEST_SPLIT', 0.8)) # Used by data_prep

# RF Hyperparameters from env (model.py also has defaults)
RF_N_ESTIMATORS = int(os.getenv('RF_N_ESTIMATORS', 100))
RF_MAX_DEPTH_STR = os.getenv('RF_MAX_DEPTH', 'None')
RF_MAX_DEPTH = None if RF_MAX_DEPTH_STR == 'None' else int(RF_MAX_DEPTH_STR)
RF_MIN_SAMPLES_SPLIT = int(os.getenv('RF_MIN_SAMPLES_SPLIT', 2))
RF_MIN_SAMPLES_LEAF = int(os.getenv('RF_MIN_SAMPLES_LEAF', 1))
RF_RANDOM_STATE = int(os.getenv('RF_RANDOM_STATE', 42))


# Mã cổ phiếu để huấn luyện mô hình
# For dynamic processing, this could be fetched from processed data in MongoDB
# For now, let's keep it simple and use the environment variable.
# Alternatively, trainer could list `processed_<SYMBOL>` collections similar to ETL.
SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')

def evaluate_sklearn_model(model, X_val, y_val, target_scaler=None, symbol="UNKNOWN"):
    """
    Evaluate a scikit-learn regression model.
    y_val and predictions are expected to be scaled.
    If target_scaler is provided, metrics can also be reported in original scale.
    """
    logger.info(f"Evaluating scikit-learn model for {symbol}")
    y_pred_scaled = model.predict(X_val)

    mse_scaled = mean_squared_error(y_val, y_pred_scaled)
    rmse_scaled = np.sqrt(mse_scaled)
    mae_scaled = mean_absolute_error(y_val, y_pred_scaled)
    r2_scaled = r2_score(y_val, y_pred_scaled)

    logger.info(f"Evaluation metrics for {symbol} (scaled target):")
    logger.info(f"  MSE (scaled): {mse_scaled:.4f}")
    logger.info(f"  RMSE (scaled): {rmse_scaled:.4f}")
    logger.info(f"  MAE (scaled): {mae_scaled:.4f}")
    logger.info(f"  R² (scaled): {r2_scaled:.4f}")

    metrics = {
        'mse_scaled': float(mse_scaled),
        'rmse_scaled': float(rmse_scaled),
        'mae_scaled': float(mae_scaled),
        'r2_scaled': float(r2_scaled)
    }

    if target_scaler:
        y_val_orig = target_scaler.inverse_transform(y_val.reshape(-1, 1)).flatten()
        y_pred_orig = target_scaler.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()

        mse_orig = mean_squared_error(y_val_orig, y_pred_orig)
        rmse_orig = np.sqrt(mse_orig)
        mae_orig = mean_absolute_error(y_val_orig, y_pred_orig)
        r2_orig = r2_score(y_val_orig, y_pred_orig)
        
        if np.all(y_val_orig != 0):
            mape_orig = np.mean(np.abs((y_val_orig - y_pred_orig) / y_val_orig)) * 100
        else:
            mape_orig = np.nan

        logger.info(f"Evaluation metrics for {symbol} (original scale):")
        logger.info(f"  MSE (original): {mse_orig:.4f}")
        logger.info(f"  RMSE (original): {rmse_orig:.4f}")
        logger.info(f"  MAE (original): {mae_orig:.4f}")
        logger.info(f"  R² (original): {r2_orig:.4f}")
        if not np.isnan(mape_orig):
            logger.info(f"  MAPE (original): {mape_orig:.2f}%")

        metrics.update({
            'mse_original': float(mse_orig),
            'rmse_original': float(rmse_orig),
            'mae_original': float(mae_orig),
            'r2_original': float(r2_orig),
            'mape_original': float(mape_orig) if not np.isnan(mape_orig) else None
        })
    return metrics

def save_sklearn_model_files(model, target_scaler, metrics, model_params, num_features, symbol):
    """
    Save scikit-learn model, target_scaler, and metadata.
    """
    try:
        model_dir = os.path.join(MODEL_PATH, symbol)
        os.makedirs(model_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        hostname = socket.gethostname()
        
        # 1. Save scikit-learn model using joblib
        model_filename = f"model_rf_{timestamp}.joblib"
        model_filepath = os.path.join(model_dir, model_filename)
        joblib.dump(model, model_filepath)
        logger.info(f"Saved RandomForest model to local path: {model_filepath}")
        
        # 2. Save target_scaler
        if target_scaler:
            scaler_filename = f"target_scaler_{timestamp}.joblib"
            scaler_filepath = os.path.join(model_dir, scaler_filename)
            joblib.dump(target_scaler, scaler_filepath)
            logger.info(f"Saved target_scaler to local path: {scaler_filepath}")
        else:
            logger.warning("No target_scaler provided to save.")
            
        # 3. Save metrics and metadata
        full_metadata = {
            "symbol": symbol,
            "model_type": "RandomForestRegressor",
            "num_input_features": num_features, # From flattened sequence
            "sequence_length_input": SEQUENCE_LENGTH, # How many past days' features are flattened
            "future_days_predicted": FUTURE_DAYS,
            "metrics": metrics,
            "model_params": model_params,
            "created_at": datetime.datetime.now().isoformat(),
            "hostname": hostname,
            "training_params": { # Add relevant training params
                 "train_test_split": TRAIN_TEST_SPLIT
            }
        }
        
        metadata_filename = f"metadata_rf_{timestamp}.json"
        metadata_filepath = os.path.join(model_dir, metadata_filename)
        with open(metadata_filepath, 'w') as f:
            json.dump(full_metadata, f, indent=2, default=lambda o: '<not serializable>')
        logger.info(f"Saved metadata to local path: {metadata_filepath}")
        
        # 4. Update latest.txt
        latest_path = os.path.join(model_dir, "latest_rf.txt") # Differentiate from potential DL models
        with open(latest_path, 'w') as f:
            f.write(timestamp)
        logger.info(f"Updated local latest_rf.txt to point to {timestamp}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error saving scikit-learn model files for {symbol}: {e}", exc_info=True)
        return False

# Removed init_distributed_strategy as it's not needed for sklearn per-symbol training

def train_model_for_symbol(symbol): # Removed spark argument
    """
    Train a RandomForestRegressor model for a single stock symbol.
    """
    try:
        logger.info(f"Starting RandomForest model training for {symbol}")

        # Prepare data: X_train, X_val, y_train, y_val are NumPy arrays
        # num_features is the number of features in the flattened X
        # target_scaler is the sklearn scaler used for y
        X_train, X_val, y_train, y_val, num_features, target_scaler = prepare_data_for_training(
            symbol,
            train_ratio=TRAIN_TEST_SPLIT
        )
        
        if X_train is None or X_val is None or y_train is None or y_val is None or num_features == 0:
            logger.error(f"Failed to prepare data for {symbol}, or no features generated. Skipping training.")
            return False
        
        logger.info(f"Data prepared for {symbol}: {num_features} input features after flattening sequence of length {SEQUENCE_LENGTH}.")
        
        # Define model hyperparameters (can be loaded from env or passed as config)
        model_params = {
            'n_estimators': RF_N_ESTIMATORS,
            'max_depth': RF_MAX_DEPTH,
            'min_samples_split': RF_MIN_SAMPLES_SPLIT,
            'min_samples_leaf': RF_MIN_SAMPLES_LEAF,
            'random_state': RF_RANDOM_STATE,
            'n_jobs': -1 # Use all cores
        }
        
        model = create_random_forest_regressor(model_params)
        
        logger.info(f"Training RandomForestRegressor for {symbol}...")
        model.fit(X_train, y_train) # y_train is already scaled
        logger.info(f"Training completed for {symbol}.")
        
        # Evaluate model
        metrics = evaluate_sklearn_model(model, X_val, y_val, target_scaler, symbol=symbol)
        
        # Save model, scaler, and metadata
        save_sklearn_model_files(model, target_scaler, metrics, model_params, num_features, symbol)
            
        logger.info(f"Successfully completed RandomForest model training for {symbol}")
        return True
            
    except Exception as e:
        logger.error(f"Error during RandomForest training for symbol {symbol}: {e}", exc_info=True)
        return False

def main():
    logger.info("Starting Machine Learning Model Training Job (RandomForestRegressor)")
    
    # GPU check is not relevant for scikit-learn RandomForest
    logger.info("Using CPU for scikit-learn model training.")
    
    os.makedirs(MODEL_PATH, exist_ok=True)
    # LOG_DIR for TensorBoard is not used. Logging goes to trainer.log / stdout.

    # No distributed setup like Horovod or TF_CONFIG needed for per-symbol sklearn training
    
    logger.info(f"Training models for symbols: {', '.join(SYMBOLS)}")
    
    success_count = 0
    for symbol_to_train in SYMBOLS:
        symbol_to_train = symbol_to_train.strip()
        if not symbol_to_train:
            continue
        logger.info(f"===== Processing symbol: {symbol_to_train} =====")
        if train_model_for_symbol(symbol_to_train):
            success_count += 1
    
    logger.info(f"Training job completed. Successfully trained models for {success_count}/{len(SYMBOLS)} symbols using RandomForestRegressor.")

if __name__ == "__main__":
    main()
