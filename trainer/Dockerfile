FROM python:3.9.18-slim

# Install system dependencies (removed openjdk, wget, git as they are likely not needed for sklearn)
# If any Python package in requirements.txt needs build tools, add 'build-essential'
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME not needed if not running a local Spark JVM context for the trainer itself

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY ./trainer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the .env file from the project root (which is now the build context)
# to /app/.env in the container.
COPY .env /app/.env

# Copy source code from the trainer subdirectory
COPY ./trainer/ /app/

# Create directories
RUN mkdir -p /app/data/processed /app/data/models /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Run the trainer on container start
CMD ["python", "trainer.py"]
