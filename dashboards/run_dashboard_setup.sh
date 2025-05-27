#!/bin/bash
# Script to run dashboard generation and import locally for testing
# This is an alternative to the Docker-based approach, useful for development

# Set strict error handling
set -e

# Function for timestamped logging
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Set working directory to the script's directory
cd "$(dirname "$0")"
SCRIPT_DIR="$(pwd)"
log "Working directory: $SCRIPT_DIR"

# Create logs directory if it doesn't exist (needed for dashboard_import.py)
mkdir -p logs
log "Created logs directory"

# Set up environment variables if not already set
# These default values are for local testing only
export ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST:-"localhost"}
export ELASTICSEARCH_PORT=${ELASTICSEARCH_PORT:-9200}
export KIBANA_HOST=${KIBANA_HOST:-"localhost"}
export KIBANA_PORT=${KIBANA_PORT:-5601}
export ES_INDEX_PREFIX=${ES_INDEX_PREFIX:-"processed_stock_data"}
export STOCK_SYMBOLS=${STOCK_SYMBOLS:-"AAPL,MSFT,GOOG,AMZN,TSLA"}

log "Using Elasticsearch at $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT"
log "Using Kibana at $KIBANA_HOST:$KIBANA_PORT"
log "Using index prefix: $ES_INDEX_PREFIX"
log "Using stock symbols: $STOCK_SYMBOLS"

# Ensure the Python scripts are executable
chmod +x dashboard_generator.py dashboard_import.py generate_and_import.py
log "Made Python scripts executable"

# Check if Elasticsearch is available
log "Testing connection to Elasticsearch..."
if curl -s "http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_cluster/health" > /dev/null; then
    log "✅ Elasticsearch is available"
else
    log "❌ Error: Cannot connect to Elasticsearch at $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT"
    log "Please ensure Elasticsearch is running and accessible"
    exit 1
fi

# Check if Kibana is available
log "Testing connection to Kibana..."
if curl -s "http://$KIBANA_HOST:$KIBANA_PORT/api/status" > /dev/null; then
    log "✅ Kibana is available"
else
    log "❌ Error: Cannot connect to Kibana at $KIBANA_HOST:$KIBANA_PORT"
    log "Please ensure Kibana is running and accessible"
    exit 1
fi

# Option 1: Run each script separately
run_separately() {
    # Run the dashboard generator
    log "Generating dashboards..."
    python dashboard_generator.py
    if [ $? -ne 0 ]; then
        log "❌ Error: Dashboard generation failed"
        exit 1
    fi
    log "✅ Dashboard generation completed successfully"

    # Wait a bit to ensure the file is fully written
    sleep 2

    # Run the dashboard import
    log "Importing dashboards into Kibana..."
    python dashboard_import.py
    if [ $? -ne 0 ]; then
        log "❌ Error: Dashboard import failed"
        exit 1
    fi
    log "✅ Dashboard import completed successfully"
}

# Option 2: Use the orchestration script
run_orchestration() {
    log "Running dashboard generation and import using orchestration script..."
    python generate_and_import.py
    if [ $? -ne 0 ]; then
        log "❌ Error: Dashboard generation and import process failed"
        exit 1
    fi
    log "✅ Dashboard generation and import process completed successfully"
}

# Use the orchestration script by default
run_orchestration

log "🎉 Dashboard setup complete!"
log "You can now access your dashboards in Kibana at http://$KIBANA_HOST:$KIBANA_PORT/app/dashboards"