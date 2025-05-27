#!/usr/bin/env python3
"""
Script to generate Kibana dashboards for stock data and economic indicators
"""
import os
import json
import logging
import requests
import time
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dashboard_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("dashboard_generator")

# Load environment variables
load_dotenv()

# Configuration
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch-master')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
KIBANA_HOST = os.getenv('KIBANA_HOST', 'kibana')
KIBANA_PORT = int(os.getenv('KIBANA_PORT', 5601))
ES_INDEX_PREFIX = os.getenv('ES_INDEX_PREFIX', 'processed_stock_data')
STOCK_SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA').split(',')

# Kibana dashboard and visualization IDs
DASHBOARD_ID = "stock_analysis_dashboard"
INDEX_PATTERN_ID = "stock_data_pattern"

def wait_for_elasticsearch():
    """Wait for Elasticsearch to be ready"""
    es_url = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}/_cluster/health"
    retries = 30
    for i in range(retries):
        try:
            response = requests.get(es_url)
            if response.status_code == 200:
                logger.info("Elasticsearch is ready")
                return True
        except Exception as e:
            logger.warning(f"Elasticsearch not ready yet: {str(e)}")
        
        logger.info(f"Waiting for Elasticsearch... ({i+1}/{retries})")
        time.sleep(5)
    
    logger.error("Elasticsearch did not become ready in time")
    return False

def wait_for_kibana():
    """Wait for Kibana to be ready"""
    kibana_url = f"http://{KIBANA_HOST}:{KIBANA_PORT}/api/status"
    retries = 30
    for i in range(retries):
        try:
            response = requests.get(kibana_url)
            if response.status_code == 200:
                logger.info("Kibana is ready")
                return True
        except Exception as e:
            logger.warning(f"Kibana not ready yet: {str(e)}")
        
        logger.info(f"Waiting for Kibana... ({i+1}/{retries})")
        time.sleep(10)
    
    logger.error("Kibana did not become ready in time")
    return False

def get_index_mappings():
    """Get mappings for the Elasticsearch indices"""
    try:
        # Try to get mappings for all indices with the prefix
        url = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}/{ES_INDEX_PREFIX}_*/_mapping"
        response = requests.get(url)
        
        if response.status_code == 200:
            mappings = response.json()
            logger.info(f"Retrieved mappings for {len(mappings)} indices")
            return mappings
        else:
            logger.warning(f"Failed to get mappings: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error getting index mappings: {str(e)}")
        return None

def create_index_pattern():
    """Create Kibana index pattern for stock data"""
    try:
        # Create index pattern for all stock indices
        pattern_object = {
            "attributes": {
                "title": f"{ES_INDEX_PREFIX}_*",
                "timeFieldName": "trading_date"
            }
        }
        
        index_pattern = {
            "type": "index-pattern",
            "id": INDEX_PATTERN_ID,
            "attributes": pattern_object["attributes"]
        }
        
        return index_pattern
    except Exception as e:
        logger.error(f"Error creating index pattern: {str(e)}")
        return None

def create_stock_price_visualization(symbol="*"):
    """Create a stock price candlestick visualization"""
    try:
        vis_id = f"stock_price_viz_{symbol}" if symbol != "*" else "stock_price_viz"
        
        # Create a visualization for candlestick/OHLC chart
        visualization = {
            "type": "visualization",
            "id": vis_id,
            "attributes": {
                "title": f"Stock Price - {symbol}" if symbol != "*" else "Stock Price",
                "visState": json.dumps({
                    "title": f"Stock Price - {symbol}" if symbol != "*" else "Stock Price",
                    "type": "vega",
                    "params": {
                        "spec": json.dumps({
                            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
                            "data": {"name": "table"},
                            "mark": {"type": "bar", "tooltip": True},
                            "encoding": {
                                "x": {"field": "trading_date", "type": "temporal", "title": "Date"},
                                "y": {"field": "close", "type": "quantitative", "title": "Price"},
                                "color": {
                                    "condition": {
                                        "test": "datum.open < datum.close",
                                        "value": "green"
                                    },
                                    "value": "red"
                                },
                                "tooltip": [
                                    {"field": "trading_date", "type": "temporal", "title": "Date"},
                                    {"field": "open", "type": "quantitative", "title": "Open"},
                                    {"field": "high", "type": "quantitative", "title": "High"},
                                    {"field": "low", "type": "quantitative", "title": "Low"},
                                    {"field": "close", "type": "quantitative", "title": "Close"},
                                    {"field": "volume", "type": "quantitative", "title": "Volume"}
                                ]
                            }
                        })
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "schema": "metric",
                            "params": {}
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "description": "Stock price candlestick chart",
                "savedSearchId": None,
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {
                            "query": symbol != "*" and f"symbol: {symbol}" or "",
                            "language": "kuery"
                        },
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating stock price visualization: {str(e)}")
        return None

def create_volume_visualization(symbol="*"):
    """Create a volume chart visualization"""
    try:
        vis_id = f"volume_viz_{symbol}" if symbol != "*" else "volume_viz"
        
        visualization = {
            "type": "visualization",
            "id": vis_id,
            "attributes": {
                "title": f"Volume - {symbol}" if symbol != "*" else "Volume",
                "visState": json.dumps({
                    "title": f"Volume - {symbol}" if symbol != "*" else "Volume",
                    "type": "histogram",
                    "params": {
                        "type": "histogram",
                        "grid": {"categoryLines": False},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "Volume"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": True,
                                "type": "histogram",
                                "mode": "stacked",
                                "data": {"label": "Volume", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": True
                            }
                        ],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False,
                        "labels": {"show": False},
                        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"},
                        "dimensions": {
                            "x": {
                                "accessor": 0,
                                "format": {"id": "date", "params": {"pattern": "YYYY-MM-DD"}},
                                "params": {"date": True, "interval": "P1D", "format": "YYYY-MM-DD", "bounds": {"min": "2020-06-01", "max": "2021-06-01"}},
                                "aggType": "date_histogram"
                            },
                            "y": [{"accessor": 1, "format": {"id": "number"}, "params": {}, "aggType": "sum"}]
                        }
                    }),
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "sum",
                            "schema": "metric",
                            "params": {"field": "volume"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "trading_date",
                                "timeRange": {"from": "now-1y", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "description": "Stock trading volume chart",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {
                            "query": symbol != "*" and f"symbol: {symbol}" or "",
                            "language": "kuery"
                        },
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating volume visualization: {str(e)}")
        return None

def create_moving_averages_visualization(symbol="*"):
    """Create a moving averages visualization"""
    try:
        vis_id = f"moving_avg_viz_{symbol}" if symbol != "*" else "moving_avg_viz"
        
        visualization = {
            "type": "visualization",
            "id": vis_id,
            "attributes": {
                "title": f"Moving Averages - {symbol}" if symbol != "*" else "Moving Averages",
                "visState": json.dumps({
                    "title": f"Moving Averages - {symbol}" if symbol != "*" else "Moving Averages",
                    "type": "line",
                    "params": {
                        "type": "line",
                        "grid": {"categoryLines": False},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "Price"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "Price", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "SMA 5", "id": "2"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 1.5,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "SMA 20", "id": "3"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 1.5,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "SMA 50", "id": "4"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 1.5,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "SMA 200", "id": "5"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 1.5,
                                "showCircles": False
                            }
                        ],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False,
                        "labels": {},
                        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"}
                    }),
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "close"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "sma_5"}
                        },
                        {
                            "id": "3",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "sma_20"}
                        },
                        {
                            "id": "4",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "sma_50"}
                        },
                        {
                            "id": "5",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "sma_200"}
                        },
                        {
                            "id": "6",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "trading_date",
                                "timeRange": {"from": "now-1y", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "description": "Stock price with moving averages",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {
                            "query": symbol != "*" and f"symbol: {symbol}" or "",
                            "language": "kuery"
                        },
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating moving averages visualization: {str(e)}")
        return None

def create_rsi_visualization(symbol="*"):
    """Create an RSI visualization"""
    try:
        vis_id = f"rsi_viz_{symbol}" if symbol != "*" else "rsi_viz"
        
        visualization = {
            "type": "visualization",
            "id": vis_id,
            "attributes": {
                "title": f"RSI - {symbol}" if symbol != "*" else "RSI",
                "visState": json.dumps({
                    "title": f"RSI - {symbol}" if symbol != "*" else "RSI",
                    "type": "line",
                    "params": {
                        "type": "line",
                        "grid": {"categoryLines": True, "valueAxis": "ValueAxis-1"},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal", "defaultYExtents": False, "setYExtents": True, "min": 0, "max": 100},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "RSI"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "RSI", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            }
                        ],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False,
                        "labels": {},
                        "thresholdLine": {"show": True, "value": 70, "width": 1, "style": "dashed", "color": "red"},
                        "thresholdLine2": {"show": True, "value": 30, "width": 1, "style": "dashed", "color": "green"}
                    }),
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "rsi"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "trading_date",
                                "timeRange": {"from": "now-1y", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({
                    "vis": {
                        "colors": {"RSI": "#6ED0E0"},
                        "legendOpen": True
                    }
                }),
                "description": "Relative Strength Index (RSI)",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {
                            "query": symbol != "*" and f"symbol: {symbol}" or "",
                            "language": "kuery"
                        },
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating RSI visualization: {str(e)}")
        return None

def create_macd_visualization(symbol="*"):
    """Create a MACD visualization"""
    try:
        vis_id = f"macd_viz_{symbol}" if symbol != "*" else "macd_viz"
        
        visualization = {
            "type": "visualization",
            "id": vis_id,
            "attributes": {
                "title": f"MACD - {symbol}" if symbol != "*" else "MACD",
                "visState": json.dumps({
                    "title": f"MACD - {symbol}" if symbol != "*" else "MACD",
                    "type": "line",
                    "params": {
                        "type": "line",
                        "grid": {"categoryLines": False, "valueAxis": "ValueAxis-1"},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "MACD"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "MACD", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "Signal", "id": "2"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 1,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "histogram",
                                "mode": "normal",
                                "data": {"label": "Histogram", "id": "3"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "showCircles": False
                            }
                        ],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False
                    }),
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "macd"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "signal_line"}
                        },
                        {
                            "id": "3",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "macd_histogram"}
                        },
                        {
                            "id": "4",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "trading_date",
                                "timeRange": {"from": "now-1y", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({
                    "vis": {
                        "colors": {
                            "MACD": "#447EBC",
                            "Signal": "#E0752D", 
                            "Histogram": "#7EB26D"
                        },
                        "legendOpen": True
                    }
                }),
                "description": "MACD indicator with signal line and histogram",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {
                            "query": symbol != "*" and f"symbol: {symbol}" or "",
                            "language": "kuery"
                        },
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating MACD visualization: {str(e)}")
        return None

def create_economic_indicators_visualization():
    """Create a visualization for economic indicators"""
    try:
        visualization = {
            "type": "visualization",
            "id": "economic_indicators_viz",
            "attributes": {
                "title": "Economic Indicators",
                "visState": json.dumps({
                    "title": "Economic Indicators",
                    "type": "line",
                    "params": {
                        "type": "line",
                        "grid": {"categoryLines": False},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "Value"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "Treasury 10Y", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "CPI", "id": "2"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "Unemployment", "id": "3"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            },
                            {
                                "show": True,
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "VIX", "id": "4"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "lineWidth": 2,
                                "showCircles": False
                            }
                        ],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False,
                        "labels": {},
                        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"}
                    }),
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "treasury_10y"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "cpi"}
                        },
                        {
                            "id": "3",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "unemployment_rate"}
                        },
                        {
                            "id": "4",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "vix"}
                        },
                        {
                            "id": "5",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "trading_date",
                                "timeRange": {"from": "now-1y", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "description": "Key economic indicators chart",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {"query": "", "language": "kuery"},
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating economic indicators visualization: {str(e)}")
        return None

def create_stock_performance_visualization(symbol="*"):
    """Create a stock performance visualization"""
    try:
        vis_id = f"performance_viz_{symbol}" if symbol != "*" else "performance_viz"
        
        visualization = {
            "type": "visualization",
            "id": vis_id,
            "attributes": {
                "title": f"Performance Metrics - {symbol}" if symbol != "*" else "Performance Metrics",
                "visState": json.dumps({
                    "title": f"Performance Metrics - {symbol}" if symbol != "*" else "Performance Metrics",
                    "type": "metric",
                    "params": {
                        "addTooltip": True,
                        "addLegend": False,
                        "type": "metric",
                        "metric": {
                            "percentageMode": True,
                            "useRanges": False,
                            "colorSchema": "Green to Red",
                            "metricColorMode": "None",
                            "colorsRange": [{"from": 0, "to": 10000}],
                            "labels": {"show": True},
                            "invertColors": False,
                            "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 60}
                        }
                    }),
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "day_change_pct", "customLabel": "Daily Change"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "week_change_pct", "customLabel": "Weekly Change"}
                        },
                        {
                            "id": "3",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "month_change_pct", "customLabel": "Monthly Change"}
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "description": "Stock performance metrics",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {
                            "query": symbol != "*" and f"symbol: {symbol}" or "",
                            "language": "kuery"
                        },
                        "filter": [
                            {
                                "meta": {
                                    "index": INDEX_PATTERN_ID,
                                    "type": "range",
                                    "key": "trading_date",
                                    "value": "now-30d to now",
                                    "params": {"gte": "now-30d", "lt": "now"},
                                    "disabled": False,
                                    "negate": False
                                },
                                "range": {
                                    "trading_date": {"gte": "now-30d", "lt": "now"}
                                }
                            }
                        ],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                    })
                }
            },
            "references": [
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return visualization
    except Exception as e:
        logger.error(f"Error creating stock performance visualization: {str(e)}")
        return None

def create_stock_selector():
    """Create a stock symbol selector dashboard control"""
    try:
        control = {
            "type": "visualization",
            "id": "stock_selector",
            "attributes": {
                "title": "Stock Symbol Selector",
                "visState": json.dumps({
                    "title": "Stock Symbol Selector",
                    "type": "input_control_vis",
                    "params": {
                        "controls": [
                            {
                                "id": "1",
                                "indexPattern": INDEX_PATTERN_ID,
                                "fieldName": "symbol",
                                "label": "Stock Symbol",
                                "type": "list",
                                "options": {
                                    "type": "terms",
                                    "multiselect": False,
                                    "dynamicOptions": True,
                                    "size": 10,
                                    "order": "desc"
                                }
                            }
                        ],
                        "updateFiltersOnChange": True,
                        "useTimeFilter": True,
                        "pinFilters": False
                    }),
                    "aggs": []
                }),
                "uiStateJSON": "{}",
                "description": "Stock symbol selector",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": [
                {
                    "name": "control_0_index_pattern",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return control
    except Exception as e:
        logger.error(f"Error creating stock selector: {str(e)}")
        return None

def create_date_range_selector():
    """Create a date range selector dashboard control"""
    try:
        control = {
            "type": "visualization",
            "id": "date_range_selector",
            "attributes": {
                "title": "Date Range Selector",
                "visState": json.dumps({
                    "title": "Date Range Selector",
                    "type": "input_control_vis",
                    "params": {
                        "controls": [
                            {
                                "id": "1",
                                "indexPattern": INDEX_PATTERN_ID,
                                "fieldName": "trading_date",
                                "label": "Date Range",
                                "type": "range",
                                "options": {
                                    "decimalPlaces": 0,
                                    "step": 1
                                }
                            }
                        ],
                        "updateFiltersOnChange": True,
                        "useTimeFilter": True,
                        "pinFilters": False
                    }),
                    "aggs": []
                }),
                "uiStateJSON": "{}",
                "description": "Date range selector",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": [
                {
                    "name": "control_0_index_pattern",
                    "type": "index-pattern",
                    "id": INDEX_PATTERN_ID
                }
            ]
        }
        
        return control
    except Exception as e:
        logger.error(f"Error creating date range selector: {str(e)}")
        return None

def create_dashboard(visualizations):
    """Create a dashboard with the provided visualizations"""
    try:
        # Create dashboard object
        dashboard = {
            "type": "dashboard",
            "id": DASHBOARD_ID,
            "attributes": {
                "title": "Stock Market Analysis Dashboard",
                "hits": 0,
                "description": "Comprehensive dashboard for stock market analysis with technical indicators and economic data",
                "panelsJSON": json.dumps([
                    # First row: Selectors
                    {
                        "panelIndex": "1",
                        "gridData": {
                            "x": 0,
                            "y": 0,
                            "w": 12,
                            "h": 4,
                            "i": "1"
                        },
                        "embeddableConfig": {},
                        "id": "stock_selector",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    {
                        "panelIndex": "2",
                        "gridData": {
                            "x": 12,
                            "y": 0,
                            "w": 12,
                            "h": 4,
                            "i": "2"
                        },
                        "embeddableConfig": {},
                        "id": "date_range_selector",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    # Second row: Performance metrics
                    {
                        "panelIndex": "3",
                        "gridData": {
                            "x": 0,
                            "y": 4,
                            "w": 24,
                            "h": 4,
                            "i": "3"
                        },
                        "embeddableConfig": {},
                        "id": "performance_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    # Third row: Stock price and volume
                    {
                        "panelIndex": "4",
                        "gridData": {
                            "x": 0,
                            "y": 8,
                            "w": 16,
                            "h": 10,
                            "i": "4"
                        },
                        "embeddableConfig": {},
                        "id": "stock_price_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    {
                        "panelIndex": "5",
                        "gridData": {
                            "x": 16,
                            "y": 8,
                            "w": 8,
                            "h": 10,
                            "i": "5"
                        },
                        "embeddableConfig": {},
                        "id": "volume_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    # Fourth row: Moving averages
                    {
                        "panelIndex": "6",
                        "gridData": {
                            "x": 0,
                            "y": 18,
                            "w": 24,
                            "h": 8,
                            "i": "6"
                        },
                        "embeddableConfig": {},
                        "id": "moving_avg_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    # Fifth row: Technical indicators
                    {
                        "panelIndex": "7",
                        "gridData": {
                            "x": 0,
                            "y": 26,
                            "w": 12,
                            "h": 8,
                            "i": "7"
                        },
                        "embeddableConfig": {},
                        "id": "rsi_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    {
                        "panelIndex": "8",
                        "gridData": {
                            "x": 12,
                            "y": 26,
                            "w": 12,
                            "h": 8,
                            "i": "8"
                        },
                        "embeddableConfig": {},
                        "id": "macd_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    },
                    # Sixth row: Economic indicators
                    {
                        "panelIndex": "9",
                        "gridData": {
                            "x": 0,
                            "y": 34,
                            "w": 24,
                            "h": 8,
                            "i": "9"
                        },
                        "embeddableConfig": {},
                        "id": "economic_indicators_viz",
                        "type": "visualization",
                        "version": "7.10.0"
                    }
                ]),
                "optionsJSON": json.dumps({
                    "useMargins": True,
                    "hidePanelTitles": False
                }),
                "version": 1,
                "timeRestore": True,
                "timeTo": "now",
                "timeFrom": "now-1y",
                "refreshInterval": {
                    "pause": True,
                    "value": 0
                },
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": []
        }
        
        # Add references to visualizations
        for i, vis in enumerate(visualizations):
            if vis is not None:
                dashboard["references"].append({
                    "name": f"panel_{i}_visualization",
                    "type": "visualization",
                    "id": vis["id"]
                })
        
        return dashboard
    except Exception as e:
        logger.error(f"Error creating dashboard: {str(e)}")
        return None

def create_ndjson_file(objects):
    """Create a NDJSON file from Kibana objects"""
    try:
        ndjson_content = ""
        for obj in objects:
            if obj is not None:
                ndjson_content += json.dumps(obj) + "\n"
        
        # Save to file
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dashboard.ndjson")
        with open(file_path, "w") as f:
            f.write(ndjson_content)
        
        logger.info(f"Successfully created NDJSON file: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error creating NDJSON file: {str(e)}")
        return None

def main():
    """Main function to generate Kibana dashboards"""
    logger.info("Starting Kibana dashboard generation")
    
    # Wait for Elasticsearch
    if not wait_for_elasticsearch():
        logger.error("Timed out waiting for Elasticsearch")
        return False
    
    # Wait for Kibana
    if not wait_for_kibana():
        logger.error("Timed out waiting for Kibana")
        return False
    
    # Get index mappings
    mappings = get_index_mappings()
    if mappings is None:
        logger.warning("Could not get index mappings, proceeding with default dashboard objects")
    
    # Create index pattern
    index_pattern = create_index_pattern()
    
    # Create visualizations
    stock_price_viz = create_stock_price_visualization()
    volume_viz = create_volume_visualization()
    moving_avg_viz = create_moving_averages_visualization()
    rsi_viz = create_rsi_visualization()
    macd_viz = create_macd_visualization()
    performance_viz = create_stock_performance_visualization()
    economic_viz = create_economic_indicators_visualization()
    stock_selector = create_stock_selector()
    date_selector = create_date_range_selector()
    
    # Create symbol-specific visualizations for each symbol
    symbol_visualizations = []
    for symbol in STOCK_SYMBOLS:
        symbol_visualizations.extend([
            create_stock_price_visualization(symbol),
            create_volume_visualization(symbol),
            create_moving_averages_visualization(symbol),
            create_rsi_visualization(symbol),
            create_macd_visualization(symbol),
            create_stock_performance_visualization(symbol)
        ])
    
    # Create dashboard
    dashboard = create_dashboard([
        stock_selector,
        date_selector,
        performance_viz,
        stock_price_viz,
        volume_viz,
        moving_avg_viz,
        rsi_viz,
        macd_viz,
        economic_viz
    ])
    
    # Collect all objects
    all_objects = [
        index_pattern,
        stock_price_viz,
        volume_viz,
        moving_avg_viz,
        rsi_viz,
        macd_viz,
        performance_viz,
        economic_viz,
        stock_selector,
        date_selector,
        dashboard
    ]
    all_objects.extend(symbol_visualizations)
    
    # Create NDJSON file
    ndjson_file = create_ndjson_file(all_objects)
    if ndjson_file:
        logger.info("Dashboard generation completed successfully")
        return True
    else:
        logger.error("Failed to create dashboard NDJSON file")
        return False

if __name__ == "__main__":
    main()