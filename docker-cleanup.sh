#!/bin/bash

# Docker Cleanup Script - Modified
# This script removes:
# - All stopped/running containers
# - All networks (except default)
# - All volumes
# - All build cache
# - Docker images older than 48 hours
# - Docker images tagged as <none> (dangling images)

echo "🧹 Starting Docker cleanup process..."

echo ""
echo "⚠️  WARNING: This script will perform significant Docker cleanup!"
echo "The following actions will be taken:"
echo "  - Stop all running containers"
echo "  - Remove all containers"
echo "  - Prune all networks (except default: bridge, host, none)"
echo "  - Prune all volumes"
echo "  - Prune all build cache"
echo "  - Remove Docker images newer than 48 hours (created in the last 2 days)"
echo "  - Remove Docker images tagged as <none> (dangling images)"
echo ""
read -p "Are you sure you want to continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cleanup cancelled by user."
    exit 1
fi

echo ""
echo "🛑 Stopping all running containers..."
docker stop $(docker ps -aq) 2>/dev/null || echo "  No running containers to stop or already stopped."

echo ""
echo "🗑️  Removing all containers..."
docker rm $(docker ps -aq) 2>/dev/null || echo "  No containers to remove."

echo ""
echo "🔌 Removing all networks (except default ones)..."
docker network prune --force 2>/dev/null || echo "  No custom networks to remove or error during network pruning."

echo ""
echo "💾 Removing all volumes..."
docker volume prune --force 2>/dev/null || echo "  No volumes to remove or error during volume pruning."

echo ""
echo "🧽 Removing all build cache..."
docker builder prune --all --force 2>/dev/null || echo "  No build cache to remove or error during builder cache pruning."

echo ""
echo "📦 Removing specific Docker images..."

echo "  Attempting to remove images newer than 48 hours (created in the last 2 days)..."
# This command removes all images (dangling or not, used by other images or not, as long as not used by containers)
# created since 48 hours ago (i.e., in the last 2 days). Since containers are already removed, this should be effective.
docker image prune -a --force --filter "since=48h" 2>/dev/null || echo "  No images newer than 48 hours found, or an error occurred."

echo "  Attempting to remove <none> (dangling) images..."
# This command removes all dangling images.
docker image prune --force --filter "dangling=true" 2>/dev/null || echo "  No dangling images found, or an error occurred during their removal."

echo ""
echo "📊 Current Docker status after cleanup:"
# Using grep -c . to count lines accurately, defaulting to 0 if no output
echo "  Containers: $(docker ps -aq | grep -c . || echo 0)"
echo "  Images: $(docker images -q | grep -c . || echo 0)"
echo "  Volumes: $(docker volume ls -q | grep -c . || echo 0)"
echo "  Networks: $(docker network ls --format '{{.Name}}' | grep -vE '^(bridge|host|none)$' | grep -c . || echo 0)"

echo ""
echo "✅ Docker cleanup process completed."
echo "💡 Targeted images (newer than 48h, <none>) and other Docker resources have been processed."
