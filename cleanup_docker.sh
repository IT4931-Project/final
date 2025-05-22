#!/bin/bash

echo "Stopping all running Docker containers..."
docker stop $(docker ps -aq)

echo "Removing all Docker containers..."
docker rm $(docker ps -aq)

echo "Removing all Docker images..."
docker rmi $(docker images -q)

echo "Removing all Docker volumes..."
docker volume rm $(docker volume ls -q)

echo "Removing all Docker networks..."
# Keep the default networks: bridge, host, none
docker network rm $(docker network ls | grep -vE '^(NETWORK ID|bridge|host|none)' | awk '{print $1}')

echo "Pruning Docker system (removes unused data)..."
docker system prune -af --volumes

echo "Bringing down Docker Compose services and removing volumes..."
# Check if docker-compose.yml exists before running docker compose down
if [ -f docker-compose.yml ]; then
  docker compose down --volumes --remove-orphans
else
  echo "docker-compose.yml not found, skipping 'docker compose down'."
fi

echo "Docker cleanup complete."