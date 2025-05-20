# TODO: optimize this messed up mongo


# MongoDB Setup Dockerfile
FROM mongo:latest

# Copy setup and optimization scripts into the container
COPY setup.sh /docker-entrypoint-initdb.d/setup.sh
COPY optimization.sh /docker-entrypoint-initdb.d/optimization.sh

# Ensure scripts are executable
RUN chmod +x /docker-entrypoint-initdb.d/setup.sh /docker-entrypoint-initdb.d/optimization.sh

# Use the default MongoDB entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# Expose MongoDB default port
EXPOSE 27017
