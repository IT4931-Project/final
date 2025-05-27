FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    lsb-release \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI to manage containers
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create app directories
RUN mkdir -p /app /app/logs /app/data

# Set working directory
WORKDIR /app

# Copy requirements
COPY ./docker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY .env /app/.env

COPY ./docker/scheduler.py /app/
COPY ./docker/entrypoint-scheduler.sh /app/

RUN chmod +x /app/entrypoint-scheduler.sh

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint-scheduler.sh"]