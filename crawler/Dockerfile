FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY ./crawler/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the .env file from the project root (which is now the build context)
# to /app/.env in the container.
COPY .env /app/.env

# Copy source code from the crawler subdirectory
COPY ./crawler/ /app/

# Create directories for data and logs
RUN mkdir -p /app/data/raw /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the crawler on container start
CMD ["python", "crawler.py"]
