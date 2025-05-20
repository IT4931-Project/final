#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Apply environment variables to spark-env.sh
# This allows environment variables from docker-compose to override defaults
source $SPARK_HOME/conf/spark-env.sh

# Create necessary directories
mkdir -p /app/logs/spark-events
mkdir -p /app/data/raw
mkdir -p /app/data/processed

# Log startup information
echo "Starting Spark in $SPARK_MODE mode"
echo "SPARK_HOME: $SPARK_HOME"
echo "SPARK_MASTER_URL: $SPARK_MASTER_URL"
echo "SPARK_WORKER_CORES: $SPARK_WORKER_CORES"
echo "SPARK_WORKER_MEMORY: $SPARK_WORKER_MEMORY"

# Start Spark based on mode
case "$SPARK_MODE" in
  "master")
    # Start Master
    echo "Starting Spark Master..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master
    ;;
  "worker")
    # Wait for master to be available
    echo "Waiting for Spark Master to be available..."
    while ! nc -z spark-master 7077; do
      sleep 1
    done
    
    # Start Worker
    echo "Starting Spark Worker..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
    ;;
  *)
    echo "Unknown Spark mode: $SPARK_MODE"
    echo "Use 'master' or 'worker'"
    exit 1
    ;;
esac