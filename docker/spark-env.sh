#!/usr/bin/env bash

# Environment variables for Spark

# Set Spark home
export SPARK_HOME=/opt/spark

# Set Java home
export JAVA_HOME=/usr/local/openjdk-11

# Set Python settings
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Network settings
export SPARK_LOCAL_IP=$(hostname -i)
export SPARK_PUBLIC_DNS=$(hostname -f)

# Memory settings
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-2g}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-1g}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-1g}

# Log directory
export SPARK_LOG_DIR=/app/logs
mkdir -p $SPARK_LOG_DIR

# Event log directory
export SPARK_EVENTLOG_DIR=/app/logs/spark-events
mkdir -p $SPARK_EVENTLOG_DIR

# Master settings
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-spark-master}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8080}

# Worker settings
export SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-8081}