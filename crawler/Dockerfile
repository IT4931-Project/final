FROM python:3.9-slim

WORKDIR /app

COPY ./crawler/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY .env /app/.env

COPY ./crawler/ /app/

RUN mkdir -p /app/data/raw /app/logs

ENV PYTHONUNBUFFERED=1

CMD ["python", "crawler.py"]
