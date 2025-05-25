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
            # Check if container is running and stop it if needed
            if container.status == 'running':
                logger.info(f"Container {container_name} is already running. Stopping it...")
                container.stop()
                logger.info(f"Container {container_name} stopped.")
                
            # Remove the container if it exists to create a fresh instance
            container.remove()
            logger.info(f"Removed existing container {container_name}")
            
        except docker.errors.NotFound:
            logger.info(f"Container {container_name} not found, will create a new one")
        
        # Execute a new container instance with the same configuration as in docker-compose
        # This triggers a job run without modifications to the original setup
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
        
        logger.info(f"Successfully started {service_name} job with container ID: {new_container.id[:12]}")
        return True
        
    except Exception as e:
        logger.error(f"Error running {service_name} job: {str(e)}")
        # Fallback to docker-compose if direct container run fails
        try:
            logger.info(f"Attempting to use docker-compose to start {service_name} service...")
            os.system(f"docker-compose up -d {service_name}")
            return True
        except Exception as compose_error:
            logger.error(f"Docker-compose attempt also failed: {str(compose_error)}")
            return False

def run_crawler_job():
    """Run the crawler job to fetch financial data"""
    return run_container("crawler")

def run_etl_job():
    """Run the ETL job to process raw data"""
    return run_container("etl")

def setup_schedule():
    """Configure the schedule for all jobs"""
    crawler_schedule = os.getenv("CRAWLER_SCHEDULE", "*/10 * * * *")
    etl_schedule = os.getenv("ETL_SCHEDULE", "0 */12 * * *")
    
    # Convert cron expressions to schedule format
    # For simplicity, we're using direct schedule methods rather than parsing cron
    
    # Crawler: every 10 minutes
    schedule.every(10).minutes.do(run_crawler_job)
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
    time.sleep(60)  # 1 minute
    run_etl_job()
    
    # Enter the main scheduling loop
    logger.info("Entering main scheduling loop...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
