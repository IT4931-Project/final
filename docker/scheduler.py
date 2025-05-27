#!/usr/bin/env python3
"""
script de chay cac job tren docker container
1. Crawler job - 10 minutes
2. ETL job - 12 hours
"""

import os
import time
import datetime
import logging
import schedule
import docker
import sys
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/scheduler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("scheduler")

# Load environment variables
load_dotenv()

# Connect to Docker daemon
client = docker.from_env()

# Store last run timestamps
last_run_times = {
    "crawler": None,
    "etl": None
}

# Maximum runtime allowed (in seconds) before forcefully stopping a container
# 30 minutes for crawler (generous timeout since we're doing parallel processing now)
# 6 hours for ETL job
MAX_RUNTIME = {
    "crawler": 30 * 60,  # 30 minutes
    "etl": 6 * 60 * 60   # 6 hours
}

def get_container_runtime(container):
    """
    Calculate how long a container has been running
    
    Args:
        container: Docker container object
        
    Returns:
        int: Runtime in seconds, or 0 if container is not running
    """
    if container.status != 'running':
        return 0
        
    # Get container info which includes start time
    container_info = container.attrs
    
    # Extract start time
    started_at = container_info.get('State', {}).get('StartedAt', '')
    if not started_at:
        return 0
        
    # Convert to datetime and calculate difference
    try:
        start_time = datetime.datetime.fromisoformat(started_at.replace('Z', '+00:00'))
        now = datetime.datetime.now(datetime.timezone.utc)
        runtime = (now - start_time).total_seconds()
        return runtime
    except Exception as e:
        logger.error(f"Error calculating container runtime: {str(e)}")
        return 0

def run_container(service_name):
    """
    Run a specific container using Docker API
    
    Args:
        service_name (str): Name of the service to run (crawler, etl)
    """
    try:
        logger.info(f"Starting {service_name} job at {datetime.datetime.now()}")
        
        # Find the container
        container_name = f"finance_{service_name}"
        
        try:
            container = client.containers.get(container_name)
            
            # Check if container is running and how long it's been running
            if container.status == 'running':
                runtime = get_container_runtime(container)
                max_allowed_runtime = MAX_RUNTIME.get(service_name, 60*60)  # Default 1 hour
                
                # If crawler is still running but within reasonable time, let it complete
                if service_name == "crawler" and runtime < max_allowed_runtime:
                    logger.info(f"Container {container_name} is still running (for {runtime:.2f} seconds). Allowing it to continue.")
                    return False  # Indicate that we didn't start a new job
                # For ETL or if container running too long, stop it
                else:
                    logger.info(f"Container {container_name} has been running for {runtime:.2f} seconds. Stopping it...")
                    container.stop()
                    logger.info(f"Container {container_name} stopped.")
                    
                    # Remove the container to create a fresh instance
                    container.remove()
                    logger.info(f"Removed existing container {container_name}")
            else:
                # Container exists but not running - remove it
                container.remove()
                logger.info(f"Removed existing stopped container {container_name}")
                
        except docker.errors.NotFound:
            logger.info(f"Container {container_name} not found, will create a new one")
        
        # Execute a new container instance with the same configuration as in docker-compose
        logger.info(f"Creating and starting container {container_name}...")
        
        # Use the correct image name with hyphen instead of underscore
        image_name = f"final-{service_name}:latest"
        logger.info(f"Using image: {image_name}")
        
        # Run the container with the correct image name
        new_container = client.containers.run(
            image_name,
            name=container_name,
            detach=True,
            network="finance_network",
            volumes={
                '/app/data': {'bind': '/app/data', 'mode': 'rw'},
                '/app/logs': {'bind': '/app/logs', 'mode': 'rw'},
                '/app/configs': {'bind': '/app/configs', 'mode': 'rw'}
            }
        )
        
        # Update the last run time
        last_run_times[service_name] = datetime.datetime.now()
        
        logger.info(f"Successfully started {service_name} job with container ID: {new_container.id[:12]}")
        return True
        
    except Exception as e:
        logger.error(f"Error running {service_name} job: {str(e)}")
        # Fallback to docker-compose if direct container run fails
        try:
            logger.info(f"Attempting to use docker-compose to start {service_name} service...")
            os.system(f"docker-compose up -d {service_name}")
            # Update the last run time even when using docker-compose
            last_run_times[service_name] = datetime.datetime.now()
            return True
        except Exception as compose_error:
            logger.error(f"Docker-compose attempt also failed: {str(compose_error)}")
            return False

def run_crawler_job():
    """Run the crawler job to fetch financial data"""
    # Check if previous crawler is still running within reasonable time
    try:
        container_name = "finance_crawler"
        try:
            container = client.containers.get(container_name)
            if container.status == 'running':
                runtime = get_container_runtime(container)
                
                # If it's been running for less than the max allowed time, let it continue
                if runtime < MAX_RUNTIME["crawler"]:
                    logger.info(f"Crawler is still running for {runtime:.2f} seconds, which is within the allowed limit of {MAX_RUNTIME['crawler']} seconds. Skipping new job.")
                    return False
                    
                # Otherwise, stop and restart
                logger.info(f"Crawler has been running for {runtime:.2f} seconds, which exceeds reasonable time. Restarting.")
        except docker.errors.NotFound:
            # Container doesn't exist, so we'll create a new one
            pass
    except Exception as e:
        logger.error(f"Error checking crawler status: {str(e)}")
    
    # Run the crawler job normally
    return run_container("crawler")

def run_etl_job():
    """Run the ETL job to process raw data"""
    return run_container("etl")

def setup_schedule():
    """Configure the schedule for all jobs"""
    crawler_schedule = os.getenv("CRAWLER_SCHEDULE", "*/10 * * * *")
    etl_schedule = os.getenv("ETL_SCHEDULE", "0 */12 * * *")
    
    # Crawler: every 10 minutes
    schedule.every(24).hours.do(run_crawler_job)
    logger.info("Scheduled crawler job: every 10 minutes")
    
    # ETL: every 12 hours
    schedule.every(12).hours.do(run_etl_job)
    logger.info("Scheduled ETL job: every 12 hours")

def main():
    """Main function to set up and run the scheduler"""
    logger.info("Starting Financial Big Data Scheduler")
    
    # Setup schedules
    setup_schedule()
    
    # Run initial jobs to provide initial data
    logger.info("Running initial jobs to provide initial data...")
    run_crawler_job()
    
    # Wait a bit before initial ETL
    time.sleep(300)  # 5 minute
    run_etl_job()
    
    # Enter the main scheduling loop
    logger.info("Entering main scheduling loop...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
