# MongoDB Single Instance Setup Container with Python
FROM mongo:5.0

# Install Python and required packages
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    iputils-ping \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip

# Install Python dependencies
RUN pip3 install pymongo dnspython

# Create script directory
RUN mkdir -p /scripts

# Copy Python initialization script
COPY ./mongo-setup/mongo_init.py /scripts/mongo_init.py

# Set execution permissions
RUN chmod +x /scripts/mongo_init.py

# Expose MongoDB default port
EXPOSE 27017

# By default, run the Python setup script
CMD ["python3", "/scripts/mongo_init.py"]
