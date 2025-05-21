# MongoDB Sharded Cluster Setup Container with Python
FROM mongo:5.0

# Install Python and required packages
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    wait-for-it \
    dnsutils \
    iputils-ping \
    netcat \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip

# Install Python dependencies
RUN pip3 install pymongo dnspython retry

# Create script directory
RUN mkdir -p /scripts

# Copy Python initialization script
COPY mongo_init.py /scripts/mongo_init.py

# Copy wait-for-mongodb script
RUN echo '#!/bin/bash\n\
echo "Waiting for MongoDB servers to be ready..."\n\
timeout=120\n\
counter=0\n\
echo "Testing connection to mongo-config1..."\n\
until ping -c1 mongo-config1 >/dev/null 2>&1; do\n\
  >&2 echo "Ping to mongo-config1 failed - sleeping"\n\
  sleep 2\n\
  counter=$((counter+2))\n\
  if [ "$counter" -gt "$timeout" ]; then break; fi\n\
done\n\
if [ "$counter" -gt "$timeout" ]; then\n\
  >&2 echo "Timed out waiting for MongoDB servers to be pingable. Starting anyway..."\n\
else\n\
  >&2 echo "MongoDB servers appear to be pingable. Continuing..."\n\
fi\n\
\n\
exec "$@"' > /scripts/wait-for-mongodb.sh

# Set execution permissions
RUN chmod +x /scripts/mongo_init.py
RUN chmod +x /scripts/wait-for-mongodb.sh

# Expose MongoDB default port
EXPOSE 27017

# Run the Python script when container starts, but first wait for MongoDB services
CMD ["/scripts/wait-for-mongodb.sh", "python3", "/scripts/mongo_init.py"]
