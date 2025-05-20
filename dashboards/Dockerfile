FROM python:3.8-slim

WORKDIR /app

# Create log directory
RUN mkdir -p /app/logs

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dashboard files
COPY *.py /app/
COPY *.ndjson /app/

# Set up entrypoint
ENTRYPOINT ["python", "dashboard_import.py"]
