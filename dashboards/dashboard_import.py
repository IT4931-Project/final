#!/usr/bin/env python3
"""
Script to import Kibana dashboards
"""
import os
import json
import logging
import requests
import time
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/dashboard_import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("dashboard_import")

# Load environment variables
# Explicitly load .env from /app/.env and override existing env vars
# This requires .env to be copied to /app/.env in the Dockerfile
load_dotenv(dotenv_path='/app/.env', override=True)

# Configuration
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch-master')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
ELASTICSEARCH_USERNAME = os.getenv('ELASTICSEARCH_USERNAME', 'elastic')
ELASTICSEARCH_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD', 'devpassword123') # Updated fallback
KIBANA_HOST = os.getenv('KIBANA_HOST', 'kibana') # Kibana host often matches the service name
KIBANA_PORT = int(os.getenv('KIBANA_PORT', 5601))

def wait_for_kibana():
    """Wait for Kibana to be ready"""
    kibana_url = f"http://{KIBANA_HOST}:{KIBANA_PORT}/api/status"
    retries = 30
    for i in range(retries):
        try:
            response = requests.get(kibana_url, auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD))
            if response.status_code == 200:
                logger.info("Kibana is ready")
                return True
        except Exception as e:
            logger.warning(f"Kibana not ready yet: {str(e)}")
        
        logger.info(f"Waiting for Kibana... ({i+1}/{retries})")
        time.sleep(10)
    
    logger.error("Kibana did not become ready in time")
    return False

def import_dashboards():
    """Import dashboards from the dashboards directory"""
    dashboard_dir = os.path.dirname(os.path.realpath(__file__))
    success_count = 0
    
    for filename in os.listdir(dashboard_dir):
        if filename.endswith('.ndjson'):
            file_path = os.path.join(dashboard_dir, filename)
            
            url = f"http://{KIBANA_HOST}:{KIBANA_PORT}/api/saved_objects/_import"
            headers = {"kbn-xsrf": "true"}
            
            try:
                with open(file_path, 'rb') as f:
                    files = {'file': (filename, f)}
                    response = requests.post(
                        url,
                        headers=headers,
                        files=files,
                        auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
                        params={"overwrite": "true"}
                    )
                
                if response.status_code == 200:
                    logger.info(f"Successfully imported dashboard: {filename}")
                    success_count += 1
                else:
                    logger.error(f"Failed to import dashboard {filename}: {response.text}")
            except Exception as e:
                logger.error(f"Error importing dashboard {filename}: {str(e)}")
    
    return success_count > 0

def main():
    """Main function to import dashboards"""
    logger.info("Starting Kibana dashboard import")
    
    # Wait for Kibana to be ready
    if not wait_for_kibana():
        logger.error("Timed out waiting for Kibana")
        return False
    
    # Import dashboards
    # Note: If there are no .ndjson files in the dashboards directory,
    # this function will still run but import nothing, which is acceptable.
    if not import_dashboards():
        logger.error("Failed to import dashboards")
        return False
    
    logger.info("Dashboard import completed successfully")
    return True

if __name__ == "__main__":
    main()
