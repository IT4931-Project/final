FROM mongo:5.0

RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    iputils-ping \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip

RUN pip3 install pymongo dnspython

RUN mkdir -p /scripts

COPY ./mongo-setup/mongo_init.py /scripts/mongo_init.py

RUN chmod +x /scripts/mongo_init.py

EXPOSE 27017

CMD ["python3", "/scripts/mongo_init.py"]
