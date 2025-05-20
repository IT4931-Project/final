#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting Financial Big Data Scheduler..."

# Wait for other services to be up first
sleep 15

# Make sure the scheduler script is executable
chmod +x /app/scheduler.py

# Start the scheduler
python /app/scheduler.py

# Keep the container running
tail -f /dev/null