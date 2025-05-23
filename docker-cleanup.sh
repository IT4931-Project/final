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
echo "  Executing: docker volume prune --force"
docker volume prune --force

echo ""
echo "🧽 Removing all build cache..."
docker builder prune --all --force 2>/dev/null || echo "  No build cache to remove or error during builder cache pruning."

echo ""
echo "📦 Removing specific Docker images..."

echo "  Attempting to remove images newer than 48 hours (created in the last 2 days)..."
# Get POSIX timestamp for 48 hours ago. GNU date is assumed (common in WSL).
CUTOFF_TIMESTAMP=$(date -d "48 hours ago" +%s)

# Create a temporary file to hold image IDs and their creation timestamps.
TMP_IMAGE_LIST_FILE=$(mktemp)
if [ -z "$TMP_IMAGE_LIST_FILE" ] || [ ! -f "$TMP_IMAGE_LIST_FILE" ]; then
    echo "  Error: Failed to create a temporary file. Skipping removal of newer images."
else
    docker images --format "{{.ID}}\t{{.CreatedAt}}" > "$TMP_IMAGE_LIST_FILE"

    IMAGE_IDS_TO_DELETE=""
    while IFS=$'\t' read -r img_id img_created_at; do
      # Ensure img_id and img_created_at are not empty
      if [ -n "$img_id" ] && [ -n "$img_created_at" ]; then
        # Convert image creation time to POSIX timestamp.
        # Suppress errors from date conversion in case of unexpected format.
        img_ts=$(date -d "$img_created_at" +%s 2>/dev/null)

        # If conversion was successful and image is newer than cutoff:
        if [ -n "$img_ts" ] && [ "$img_ts" -gt "$CUTOFF_TIMESTAMP" ]; then
          IMAGE_IDS_TO_DELETE="$IMAGE_IDS_TO_DELETE $img_id"
        fi
      fi
    done < "$TMP_IMAGE_LIST_FILE"

    # Clean up the temporary file.
    rm "$TMP_IMAGE_LIST_FILE"

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
