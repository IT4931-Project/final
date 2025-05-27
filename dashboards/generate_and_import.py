#!/usr/bin/env python3
"""
Script to generate and import Kibana dashboards
This script runs dashboard_generator.py to create dashboard definitions
and then uses dashboard_import.py to import them into Kibana
"""
import os
import sys
import logging
import subprocess
import time
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("generate_and_import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("generate_and_import")

def run_dashboard_generator():
    """Run the dashboard generator script to create dashboard definitions"""
    logger.info("Starting dashboard generation process")
    
    # Get the directory of the current script
    current_dir = Path(__file__).parent.absolute()
    generator_script = current_dir / "dashboard_generator.py"
    
    # Check if script exists
    if not generator_script.exists():
        logger.error(f"Dashboard generator script not found at {generator_script}")
        return False
    
    # Make the script executable
    try:
        generator_script.chmod(0o755)
    except Exception as e:
        logger.warning(f"Could not make script executable: {e}. Continuing anyway.")
    
    # Run the generator script
    try:
        logger.info(f"Running dashboard generator script: {generator_script}")
        result = subprocess.run(
            [sys.executable, str(generator_script)],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info("Dashboard generator completed successfully")
        logger.debug(f"Output: {result.stdout}")
        
        # Check if the NDJSON file was created
        ndjson_file = current_dir / "dashboard.ndjson"
        if not ndjson_file.exists():
            logger.error(f"Dashboard NDJSON file not found at {ndjson_file}")
            return False
        
        logger.info(f"Dashboard NDJSON file created at {ndjson_file}")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Dashboard generator failed with error code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error running dashboard generator: {str(e)}")
        return False

def run_dashboard_import():
    """Run the dashboard import script to import dashboards into Kibana"""
    logger.info("Starting dashboard import process")
    
    # Get the directory of the current script
    current_dir = Path(__file__).parent.absolute()
    import_script = current_dir / "dashboard_import.py"
    
    # Check if script exists
    if not import_script.exists():
        logger.error(f"Dashboard import script not found at {import_script}")
        return False
    
    # Make the script executable
    try:
        import_script.chmod(0o755)
    except Exception as e:
        logger.warning(f"Could not make script executable: {e}. Continuing anyway.")
    
    # Run the import script
    try:
        logger.info(f"Running dashboard import script: {import_script}")
        result = subprocess.run(
            [sys.executable, str(import_script)],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info("Dashboard import completed successfully")
        logger.debug(f"Output: {result.stdout}")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Dashboard import failed with error code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error running dashboard import: {str(e)}")
        return False

def main():
    """Main function to generate and import Kibana dashboards"""
    logger.info("Starting generate and import process")
    
    # Generate dashboards
    if not run_dashboard_generator():
        logger.error("Dashboard generation failed, aborting import")
        return False
    
    # Wait a bit to ensure the file is fully written
    time.sleep(2)
    
    # Import dashboards
    if not run_dashboard_import():
        logger.error("Dashboard import failed")
        return False
    
    logger.info("Dashboard generation and import completed successfully")
    logger.info("Open Kibana and navigate to Dashboards to see the imported dashboards")
    return True

if __name__ == "__main__":
    main()