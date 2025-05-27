FROM python:3.8-slim

WORKDIR /app

# Create log directory
RUN mkdir -p /app/logs

# Install dependencies
COPY ./dashboards/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the .env file from the project root (which will be the build context)
# to /app/.env in the container.
COPY .env /app/.env

# Copy dashboard files from the dashboards subdirectory
COPY ./dashboards/*.py /app/
COPY ./dashboards/*.ndjson /app/

# Set up entrypoint
ENTRYPOINT ["python", "generate_and_import.py"]
