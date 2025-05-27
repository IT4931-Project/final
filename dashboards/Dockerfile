FROM python:3.8-slim

WORKDIR /app

RUN mkdir -p /app/logs

COPY ./dashboards/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY .env /app/.env

COPY ./dashboards/*.py /app/
COPY ./dashboards/*.ndjson /app/

ENTRYPOINT ["python", "dashboard_import.py"]
