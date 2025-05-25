FROM python:3.9-slim

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY ./spark-job/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the .env file from the project root (which is now the build context)
# to /app/.env in the container.
COPY .env /app/.env

# Copy source code from the spark-job subdirectory
COPY ./spark-job/ /app/

# Create directories
RUN mkdir -p /app/data/raw /app/data/processed /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Run the ETL job on container start
CMD ["python", "etl_job.py"]
