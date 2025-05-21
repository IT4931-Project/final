#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting Financial Big Data Scheduler..."

# Wait for other services to be up
echo "Waiting for other services to be ready..."
sleep 15

# Start the scheduler
echo "Starting scheduler..."
python /app/scheduler.py

# Keep the container running if scheduler exits
tail -f /dev/null
