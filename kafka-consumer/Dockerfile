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
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the .env file from the project root (one level up from the Dockerfile's context)
# to /app/.env in the container.
COPY ../.env /app/.env

# Copy the application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data

# Run the consumer script
CMD ["python", "consumer.py"]
