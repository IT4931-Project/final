#!/bin/bash

# Docker Complete Cleanup Script for Development Environment
# This script removes everything from Docker while preserving base images from Dockerfiles

echo "🧹 Starting Docker cleanup process..."

# Function to extract base images from Dockerfiles
extract_base_images() {
    echo "📋 Extracting base images from Dockerfiles..."
    local base_images=()
    
    # Find all Dockerfiles and extract FROM statements
    while IFS= read -r -d '' dockerfile; do
        if [[ -f "$dockerfile" ]]; then
            echo "  Checking: $dockerfile"
            while IFS= read -r line; do
                if [[ $line =~ ^FROM[[:space:]]+([^[:space:]]+) ]]; then
                    image="${BASH_REMATCH[1]}"
                    # Skip multi-stage build aliases (contains 'as')
                    if [[ ! $line =~ [[:space:]]as[[:space:]] ]]; then
                        base_images+=("$image")
                        echo "    Found base image: $image"
                    fi
                fi
            done < "$dockerfile"
        fi
    done < <(find . -name "Dockerfile*" -print0)
    
    printf '%s\n' "${base_images[@]}" | sort -u
}

# Store base images to preserve
BASE_IMAGES_FILE=$(mktemp)
extract_base_images > "$BASE_IMAGES_FILE"

echo ""
echo "🔒 Base images to preserve:"
cat "$BASE_IMAGES_FILE" | sed 's/^/  - /'

echo ""
echo "⚠️  WARNING: This will remove ALL Docker data except base images listed above!"
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cleanup cancelled."
    rm -f "$BASE_IMAGES_FILE"
    exit 1
fi

echo ""
echo "🛑 Stopping all running containers..."
docker stop $(docker ps -aq) 2>/dev/null || echo "  No running containers to stop"

echo ""
echo "🗑️  Removing all containers..."
docker rm $(docker ps -aq) 2>/dev/null || echo "  No containers to remove"

echo ""
echo "🔌 Removing all networks (except default ones)..."
docker network prune -f

echo ""
echo "💾 Removing all volumes..."
docker volume prune -f

echo ""
echo "🧽 Removing all build cache..."
docker builder prune -af

echo ""
echo "📦 Removing all images except base images..."
# Get all image IDs
ALL_IMAGES=$(docker images -q)

if [[ -n "$ALL_IMAGES" ]]; then
    # Get base image IDs to preserve
    PRESERVE_IDS=""
    while IFS= read -r base_image; do
        if [[ -n "$base_image" ]]; then
            # Try to get image ID, ignore errors if image doesn't exist locally
            IMAGE_ID=$(docker images -q "$base_image" 2>/dev/null)
            if [[ -n "$IMAGE_ID" ]]; then
                PRESERVE_IDS="$PRESERVE_IDS $IMAGE_ID"
                echo "  Preserving: $base_image ($IMAGE_ID)"
            fi
        fi
    done < "$BASE_IMAGES_FILE"
    
    # Remove all images except preserved ones
    if [[ -n "$PRESERVE_IDS" ]]; then
        # Create a list of images to remove (exclude preserved ones)
        IMAGES_TO_REMOVE=""
        for img_id in $ALL_IMAGES; do
            if [[ ! " $PRESERVE_IDS " =~ " $img_id " ]]; then
                IMAGES_TO_REMOVE="$IMAGES_TO_REMOVE $img_id"
            fi
        done
        
        if [[ -n "$IMAGES_TO_REMOVE" ]]; then
            echo "  Removing non-base images..."
            docker rmi -f $IMAGES_TO_REMOVE 2>/dev/null || echo "  Some images couldn't be removed (may be in use)"
        else
            echo "  No non-base images to remove"
        fi
    else
        echo "  No base images found locally, removing all images..."
        docker rmi -f $ALL_IMAGES 2>/dev/null || echo "  Some images couldn't be removed"
    fi
else
    echo "  No images found"
fi

echo ""
echo "🧹 Final cleanup - removing dangling images and unused data..."
docker system prune -af

echo ""
echo "📊 Current Docker status:"
echo "  Containers: $(docker ps -aq | wc -l)"
echo "  Images: $(docker images -q | wc -l)"
echo "  Volumes: $(docker volume ls -q | wc -l)"
echo "  Networks: $(docker network ls --format 'table {{.Name}}' | grep -v 'bridge\|host\|none' | wc -l)"

# Cleanup temporary file
rm -f "$BASE_IMAGES_FILE"

echo ""
echo "✅ Docker cleanup completed!"
echo "💡 Base images from Dockerfiles have been preserved for future builds."