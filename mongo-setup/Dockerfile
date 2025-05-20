# MongoDB Sharded Cluster Setup Container with Python
FROM mongo:5.0

# Install Python and required packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wait-for-it \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install pymongo

# Copy Python initialization script
COPY mongo_init.py /mongo_init.py

# Set execution permissions
RUN chmod +x /mongo_init.py

# Expose MongoDB default port
EXPOSE 27017

# Run the Python script when container starts
CMD ["python3", "/mongo_init.py"]
