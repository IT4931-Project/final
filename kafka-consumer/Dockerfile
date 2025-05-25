FROM python:3.9-slim

WORKDIR /app

# Install essential packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    librdkafka-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY ./kafka-consumer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the .env file from the project root (which is now the build context)
# to /app/.env in the container.
COPY .env /app/.env

# Copy the application code from the kafka-consumer subdirectory
COPY ./kafka-consumer/ /app/

# Create necessary directories
RUN mkdir -p /app/logs /app/data

# Run the consumer script
CMD ["python", "consumer.py"]
