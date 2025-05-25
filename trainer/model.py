#!/usr/bin/env python3
"""
Model definition for traditional Machine Learning.
We will use RandomForestRegressor from scikit-learn.
"""

import os
import logging
from sklearn.ensemble import RandomForestRegressor
from dotenv import load_dotenv

# Cấu hình logging (can be simplified or use trainer's logger if preferred)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/trainer_model.log"), # Separate log for model creation
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_model")

# Tải biến môi trường (primarily for model hyperparameters if any)
# Explicitly load .env from /app/.env and override existing env vars
# This requires .env to be copied to /app/.env in the Dockerfile
load_dotenv(dotenv_path='/app/.env', override=True)

# Model Hyperparameters (can be loaded from env or set as defaults)
N_ESTIMATORS = int(os.getenv('RF_N_ESTIMATORS', 100))
MAX_DEPTH = os.getenv('RF_MAX_DEPTH', None) # None means nodes are expanded until all leaves are pure or contain less than min_samples_split samples
if MAX_DEPTH is not None:
    try:
        MAX_DEPTH = int(MAX_DEPTH)
    except ValueError:
        logger.warning(f"Invalid RF_MAX_DEPTH value '{MAX_DEPTH}', using None.")
        MAX_DEPTH = None
MIN_SAMPLES_SPLIT = int(os.getenv('RF_MIN_SAMPLES_SPLIT', 2))
MIN_SAMPLES_LEAF = int(os.getenv('RF_MIN_SAMPLES_LEAF', 1))
RANDOM_STATE = int(os.getenv('RF_RANDOM_STATE', 42)) # For reproducibility

def create_random_forest_regressor(config=None):
    """
    Creates and returns an un-trained RandomForestRegressor model.
    Hyperparameters can be passed via config dict or taken from environment variables/defaults.

    Args:
        config (dict, optional): Dictionary with model hyperparameters.
                                 Keys like 'n_estimators', 'max_depth', etc.

    Returns:
        sklearn.ensemble.RandomForestRegressor: An instance of the model.
    """
    if config is None:
        config = {}

    n_estimators = config.get('n_estimators', N_ESTIMATORS)
    max_depth = config.get('max_depth', MAX_DEPTH)
    min_samples_split = config.get('min_samples_split', MIN_SAMPLES_SPLIT)
    min_samples_leaf = config.get('min_samples_leaf', MIN_SAMPLES_LEAF)
    random_state = config.get('random_state', RANDOM_STATE)
    
    # Ensure max_depth is an int or None
    if max_depth is not None:
        try:
            max_depth = int(max_depth)
        except ValueError:
            logger.warning(f"Config provided invalid max_depth '{max_depth}', using module default {MAX_DEPTH}.")
            max_depth = MAX_DEPTH


    logger.info(f"Creating RandomForestRegressor with parameters: "
                f"n_estimators={n_estimators}, max_depth={max_depth}, "
                f"min_samples_split={min_samples_split}, min_samples_leaf={min_samples_leaf}, "
                f"random_state={random_state}")

    model = RandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        min_samples_split=min_samples_split,
        min_samples_leaf=min_samples_leaf,
        random_state=random_state,
        n_jobs=-1  # Use all available cores for training
    )
    return model

# Example of how it might be called (though trainer.py will handle actual creation)
if __name__ == "__main__":
    # This is just for testing this script directly if needed
    logger.info("Testing RandomForestRegressor creation.")
    # Example config
    sample_config = {
        'n_estimators': 150,
        'max_depth': 10,
        'random_state': 123
    }
    rf_model = create_random_forest_regressor(sample_config)
    logger.info(f"Successfully created model: {rf_model}")

    rf_model_default = create_random_forest_regressor()
    logger.info(f"Successfully created model with default params: {rf_model_default}")
