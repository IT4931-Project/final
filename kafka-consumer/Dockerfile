FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    librdkafka-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./kafka-consumer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY .env /app/.env

COPY ./kafka-consumer/ /app/

RUN mkdir -p /app/logs /app/data

CMD ["python", "consumer.py"]
