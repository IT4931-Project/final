#!/bin/bash

# Docker Cleanup Script - Modified
# This script removes:
# - All stopped/running containers
# - All networks (except default)
# - All volumes
# - All build cache
# - Docker images newer than 48 hours (created in the last 2 days)
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
ALL_VOLUMES=$(docker volume ls -q)
if [ -n "$ALL_VOLUMES" ]; then
  echo "  The following volumes will be attempted for removal:"
  # Display volume names, one per line, indented
  echo "$ALL_VOLUMES" | tr ' ' '\n' | sed 's/^/    /'
  echo "  Executing: docker volume rm --force $ALL_VOLUMES"
  # SC2086: We want word splitting for $ALL_VOLUMES
  # shellcheck disable=SC2086
  docker volume rm --force $ALL_VOLUMES
else
  echo "  No volumes found to remove."
fi

echo ""
echo "🧽 Removing all build cache..."
docker builder prune --all --force 2>/dev/null || echo "  No build cache to remove or error during builder cache pruning."

echo ""
echo "📦 Removing specific Docker images..."

echo "  Attempting to remove images newer than 48 hours (created in the last 2 days)..."
# Get POSIX timestamp for 48 hours ago. GNU date is assumed (common in WSL).
CUTOFF_TIMESTAMP=$(date -d "48 hours ago" +%s)
echo "  DEBUG: CUTOFF_TIMESTAMP = $CUTOFF_TIMESTAMP"

# Get all image IDs
ALL_IMAGE_IDS=$(docker images -q)

if [ -z "$ALL_IMAGE_IDS" ]; then
    echo "  No images found."
else
    IMAGE_IDS_TO_DELETE=""
    # Loop through each image ID
    for img_id in $ALL_IMAGE_IDS; do
      # Get creation timestamp in ISO 8601 format
      iso_created_at=$(docker inspect --format='{{.Created}}' "$img_id" 2>/dev/null)

      if [ -n "$iso_created_at" ]; then
        echo "  DEBUG: Processing img_id = $img_id, iso_created_at = $iso_created_at"
        # Convert image creation time to POSIX timestamp.
        img_ts=$(date -d "$iso_created_at" +%s 2>/dev/null)
        echo "  DEBUG: Converted img_ts = $img_ts"

        # If conversion was successful and image is newer than cutoff:
        if [ -n "$img_ts" ] && [ "$img_ts" -gt "$CUTOFF_TIMESTAMP" ]; then
          echo "  DEBUG: Adding $img_id to delete list (img_ts $img_ts > CUTOFF_TIMESTAMP $CUTOFF_TIMESTAMP)."
          IMAGE_IDS_TO_DELETE="$IMAGE_IDS_TO_DELETE $img_id"
        else
          echo "  DEBUG: Not adding $img_id (img_ts $img_ts <= CUTOFF_TIMESTAMP $CUTOFF_TIMESTAMP or img_ts is empty)."
        fi
      else
        echo "  Warning: Could not inspect image ID $img_id to get creation date (iso_created_at is empty). Skipping."
      fi
    done

    if [ -n "$IMAGE_IDS_TO_DELETE" ]; then
      echo "  The following image IDs (newer than 48 hours) will be attempted for removal:"
      # Display IDs, one per line, indented for readability.
      # Using echo and tr to list IDs one per line for clarity before passing to xargs or rmi
      echo "$IMAGE_IDS_TO_DELETE" | tr ' ' '\n' | sed 's/^/    /'
      echo "  Executing: docker rmi --force $IMAGE_IDS_TO_DELETE"
      # Remove the identified images.
      # SC2086: We want word splitting for $IMAGE_IDS_TO_DELETE as it's a space-separated list of IDs.
      # shellcheck disable=SC2086
      docker rmi --force $IMAGE_IDS_TO_DELETE
    else
      echo "  No images found that are newer than 48 hours."
    fi
fi

echo ""
echo "  Attempting to remove <none> (dangling) images..."
# This command removes all dangling images.
echo "  Executing: docker image prune --force --filter \"dangling=true\""
docker image prune --force --filter "dangling=true"

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
