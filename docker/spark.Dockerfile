FROM openjdk:11-jdk-slim

# Install Python and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-dev \
    build-essential \
    wget \
    procps \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for Python (if they don't exist)
RUN if [ ! -e /usr/bin/python ]; then ln -s /usr/bin/python3 /usr/bin/python; fi && \
    if [ ! -e /usr/bin/pip ]; then ln -s /usr/bin/pip3 /usr/bin/pip; fi

# Set working directory
WORKDIR /opt

# Download and install Spark
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy configuration files
COPY ./docker/spark-defaults.conf ${SPARK_HOME}/conf/
COPY ./docker/spark-env.sh ${SPARK_HOME}/conf/

RUN mkdir -p /app/data /app/logs

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} \
    numpy \
    pandas \
    pyarrow \
    fastparquet \
    python-dotenv

WORKDIR /app

COPY ./docker/entrypoint-spark.sh /
RUN chmod +x /entrypoint-spark.sh

EXPOSE 8080 7077 6066 8081

ENTRYPOINT ["/entrypoint-spark.sh"]
