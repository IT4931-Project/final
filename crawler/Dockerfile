FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create directories for data and logs
RUN mkdir -p /app/data/raw /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the crawler on container start
CMD ["python", "crawler.py"]
