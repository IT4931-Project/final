# Kibana Dashboard Generator for Financial Data

This repository contains a suite of tools to programmatically generate and deploy Kibana dashboards for financial data analysis. The system creates visualizations for stock market data including price charts, volume, technical indicators, and economic data.

## Table of Contents

- [System Overview](#system-overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Installation](#installation)
- [Usage](#usage)
  - [Docker Deployment](#docker-deployment)
  - [Local Development](#local-development)
- [Dashboard Descriptions](#dashboard-descriptions)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Extending the System](#extending-the-system)

## System Overview

This system automates the creation and deployment of Kibana dashboards for financial data analysis. It connects to an Elasticsearch instance containing processed stock market data, generates dashboard configurations programmatically, and imports them into Kibana.

The workflow follows these steps:
1. ETL process (separate component) loads financial data into Elasticsearch
2. Dashboard generator creates visualization configurations based on the data structure
3. Generated configurations are exported as NDJSON files
4. Dashboard importer loads these configurations into Kibana

## Features

- **Automated Dashboard Generation**: Create complex Kibana dashboards without manual configuration
- **Multiple Technical Indicators**: Visualizations for various technical analysis metrics:
  - Candlestick price charts
  - Volume analysis
  - Moving averages (5, 20, 50, 200-day SMA)
  - RSI (Relative Strength Index)
  - MACD (Moving Average Convergence/Divergence)
  - Bollinger Bands
- **Performance Metrics**: Daily, weekly, and monthly price changes
- **Economic Indicators**: Treasury yields, CPI, unemployment, VIX
- **Interactive Filters**: Stock symbol selector and date range controls
- **Containerized Deployment**: Docker setup for easy deployment
- **Local Development Tools**: Shell script for local testing

## Prerequisites

- Python 3.8+
- Elasticsearch 7.x or 8.x
- Kibana 7.x or 8.x (matching your Elasticsearch version)
- Docker (for containerized deployment)
- ETL pipeline that processes financial data into Elasticsearch (see the related `spark-job` directory)

## Configuration

The system is configured through environment variables which can be set in a `.env` file or directly in the environment:

| Variable | Description | Default |
|----------|-------------|---------|
| `ELASTICSEARCH_HOST` | Elasticsearch hostname/IP | `elasticsearch-master` |
| `ELASTICSEARCH_PORT` | Elasticsearch port | `9200` |
| `KIBANA_HOST` | Kibana hostname/IP | `kibana` |
| `KIBANA_PORT` | Kibana port | `5601` |
| `ES_INDEX_PREFIX` | Prefix for Elasticsearch indices | `processed_stock_data` |
| `STOCK_SYMBOLS` | Comma-separated list of stock symbols to include | `AAPL,MSFT,GOOG,AMZN,TSLA` |

## Installation

### Clone the repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### Install dependencies

```bash
cd dashboards
pip install -r requirements.txt
```

## Usage

There are two main ways to use this system:

### Docker Deployment

1. Build the Docker image:
```bash
docker build -t kibana-dashboard-generator -f dashboards/Dockerfile .
```

2. Run the container:
```bash
docker run --network your-network-name \
  -e ELASTICSEARCH_HOST=elasticsearch-host \
  -e KIBANA_HOST=kibana-host \
  kibana-dashboard-generator
```

### Local Development

1. Ensure Elasticsearch and Kibana are running and accessible
2. Configure environment variables in a `.env` file or export them directly
3. Run the setup script:
```bash
cd dashboards
chmod +x run_dashboard_setup.sh
./run_dashboard_setup.sh
```

For manual execution of individual components:

```bash
# Generate dashboard definitions
python dashboard_generator.py

# Import dashboards into Kibana
python dashboard_import.py

# Or run both steps in sequence
python generate_and_import.py
```

## Dashboard Descriptions

The generated dashboard includes the following visualizations:

### Stock Price Analysis
- **Candlestick Chart**: Shows OHLC (Open, High, Low, Close) price data
- **Volume Chart**: Displays trading volume over time
- **Moving Averages**: Multiple SMAs (5, 20, 50, 200-day) plotted with price

### Technical Indicators
- **RSI Chart**: Relative Strength Index with overbought/oversold thresholds
- **MACD Chart**: Moving Average Convergence/Divergence with signal line and histogram
- **Performance Metrics**: Daily, weekly, and monthly percentage changes

### Economic Context (if data available)
- **Economic Indicators**: Treasury yields, CPI, unemployment rates, and VIX index

### Interactive Controls
- **Stock Symbol Selector**: Filter dashboard by stock symbol
- **Date Range Selector**: Adjust the time period for analysis

## Architecture

The dashboard generation system consists of several components:

### 1. Dashboard Generator (`dashboard_generator.py`)
- Creates visualization configurations based on the Elasticsearch data structure
- Generates NDJSON file with dashboard definition

### 2. Dashboard Importer (`dashboard_import.py`)
- Imports the generated NDJSON file into Kibana using the Saved Objects API
- Handles authentication and error reporting

### 3. Orchestration Script (`generate_and_import.py`)
- Coordinates the generator and importer
- Provides unified logging and error handling

### 4. Supporting Components
- `Dockerfile`: Containerizes the application
- `run_dashboard_setup.sh`: Shell script for local testing
- Environment configuration

## Troubleshooting

### Connection Issues
- Ensure Elasticsearch and Kibana are running and accessible
- Check that hostname/IP and port configurations are correct
- Verify network connectivity between services

### Missing Data
- Confirm that the ETL process has successfully loaded data into Elasticsearch
- Check that the index names match the expected pattern (`{ES_INDEX_PREFIX}_{symbol}`)
- Verify the data structure includes required fields (open, high, low, close, volume, trading_date)

### Import Failures
- Check Kibana logs for detailed error messages
- Ensure Kibana's Saved Objects API is accessible
- Verify that the generated NDJSON file is valid

### Script Execution Problems
- Ensure Python scripts have execute permission (`chmod +x *.py`)
- Check for missing dependencies in requirements.txt
- Verify environment variables are properly set

## Extending the System

### Adding New Visualizations
1. Create a new function in `dashboard_generator.py` following the pattern of existing visualizations
2. Add the visualization to the dashboard layout in the `create_dashboard()` function
3. Update tests and documentation

### Supporting Additional Data Types
1. Examine the data structure in Elasticsearch
2. Modify or add visualization functions to accommodate the new data
3. Update the dashboard layout to include new visualizations

### Customizing the Dashboard Layout
1. Modify the `create_dashboard()` function in `dashboard_generator.py`
2. Adjust panel positions and sizes by updating the `panelsJSON` configuration

### Adding Authentication
1. Update the connection logic in both generator and importer scripts
2. Add environment variables for authentication credentials
3. Modify the Dockerfile to securely handle credentials