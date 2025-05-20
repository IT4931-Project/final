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
load_dotenv()

# Configuration
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch-master')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
ELASTICSEARCH_USERNAME = os.getenv('ELASTICSEARCH_USERNAME', 'elastic')
ELASTICSEARCH_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD', 'changeme')
KIBANA_HOST = os.getenv('KIBANA_HOST', 'kibana')
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

def create_index_pattern():
    """Create index pattern for stock prediction data"""
    url = f"http://{KIBANA_HOST}:{KIBANA_PORT}/api/saved_objects/index-pattern"
    headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
    data = {
        "attributes": {
            "title": "stock_predictions*",
            "timeFieldName": "prediction_date"
        }
    }
    
    try:
        response = requests.post(
            url, 
            headers=headers,
            json=data,
            auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
        )
        
        if response.status_code in (200, 201):
            logger.info("Successfully created index pattern")
            return True
        else:
            logger.error(f"Failed to create index pattern: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error creating index pattern: {str(e)}")
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

def create_visualizations():
    """Create necessary visualizations for the dashboard"""
    visualizations = [
        {
            "id": "price-compare-visualization",
            "type": "line",
            "title": "Predicted vs Actual Prices",
            "index_pattern": "stock_predictions*",
            "x_axis_field": "target_date",
            "y_axis_fields": ["predicted_price", "actual_price"],
            "split_series": "symbol"
        },
        {
            "id": "prediction-accuracy",
            "type": "metric",
            "title": "Prediction Accuracy",
            "index_pattern": "stock_predictions*",
            "aggregation": "average",
            "field": "accuracy_percent"
        },
        {
            "id": "latest-predictions",
            "type": "table",
            "title": "Latest Predictions",
            "index_pattern": "stock_predictions*",
            "sort_field": "prediction_date",
            "sort_order": "desc",
            "fields": ["symbol", "prediction_date", "target_date", "predicted_price"]
        }
    ]
    
    success_count = 0
    for viz in visualizations:
        # Create visualization request based on type
        if viz["type"] == "line":
            body = create_line_visualization(viz)
        elif viz["type"] == "metric":
            body = create_metric_visualization(viz)
        elif viz["type"] == "table":
            body = create_table_visualization(viz)
        else:
            logger.warning(f"Unknown visualization type: {viz['type']}")
            continue
        
        # Save visualization
        url = f"http://{KIBANA_HOST}:{KIBANA_PORT}/api/saved_objects/visualization/{viz['id']}"
        headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=body,
                auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
            )
            
            if response.status_code in (200, 201):
                logger.info(f"Successfully created visualization: {viz['title']}")
                success_count += 1
            else:
                logger.error(f"Failed to create visualization {viz['title']}: {response.text}")
        except Exception as e:
            logger.error(f"Error creating visualization {viz['title']}: {str(e)}")
    
    return success_count == len(visualizations)

def create_line_visualization(config):
    """Create a line visualization definition"""
    return {
        "attributes": {
            "title": config["title"],
            "visState": json.dumps({
                "title": config["title"],
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
                            "data": {"label": "Predicted", "id": "1"},
                            "valueAxis": "ValueAxis-1",
                            "drawLinesBetweenPoints": True,
                            "lineWidth": 2,
                            "showCircles": True
                        },
                        {
                            "show": True,
                            "type": "line",
                            "mode": "normal",
                            "data": {"label": "Actual", "id": "2"},
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
                    "dimensions": {
                        "x": {
                            "accessor": 0,
                            "format": {"id": "date", "params": {"pattern": "YYYY-MM-DD"}},
                            "params": {"date": True, "interval": "P1D", "format": "YYYY-MM-DD"},
                            "aggType": "date_histogram"
                        },
                        "y": [
                            {"accessor": 1, "format": {"id": "number"}, "params": {}, "aggType": "avg"},
                            {"accessor": 2, "format": {"id": "number"}, "params": {}, "aggType": "avg"}
                        ],
                        "series": [
                            {
                                "accessor": 3,
                                "format": {"id": "string"},
                                "params": {},
                                "aggType": "terms"
                            }
                        ]
                    }
                },
                "aggs": [
                    {
                        "id": "1",
                        "enabled": True,
                        "type": "avg",
                        "schema": "metric",
                        "params": {"field": config["y_axis_fields"][0]}
                    },
                    {
                        "id": "2",
                        "enabled": True,
                        "type": "avg",
                        "schema": "metric",
                        "params": {"field": config["y_axis_fields"][1]}
                    },
                    {
                        "id": "3",
                        "enabled": True,
                        "type": "date_histogram",
                        "schema": "segment",
                        "params": {
                            "field": config["x_axis_field"],
                            "timeRange": {"from": "now-30d", "to": "now"},
                            "useNormalizedEsInterval": True,
                            "interval": "auto",
                            "drop_partials": False,
                            "min_doc_count": 1,
                            "extended_bounds": {}
                        }
                    },
                    {
                        "id": "4",
                        "enabled": True,
                        "type": "terms",
                        "schema": "group",
                        "params": {
                            "field": config["split_series"],
                            "orderBy": "1",
                            "order": "desc",
                            "size": 5,
                            "otherBucket": False,
                            "otherBucketLabel": "Other",
                            "missingBucket": False,
                            "missingBucketLabel": "Missing"
                        }
                    }
                ]
            }),
            "uiStateJSON": "{}",
            "description": "",
            "savedSearchId": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {
                        "query": "",
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
                "id": config["index_pattern"]
            }
        ]
    }

def create_metric_visualization(config):
    """Create a metric visualization definition"""
    return {
        "attributes": {
            "title": config["title"],
            "visState": json.dumps({
                "title": config["title"],
                "type": "metric",
                "params": {
                    "addTooltip": True,
                    "addLegend": False,
                    "type": "metric",
                    "metric": {
                        "percentageMode": False,
                        "useRanges": False,
                        "colorSchema": "Green to Red",
                        "metricColorMode": "None",
                        "colorsRange": [{"from": 0, "to": 10000}],
                        "labels": {"show": True},
                        "invertColors": False,
                        "style": {
                            "bgFill": "#000",
                            "bgColor": False,
                            "labelColor": False,
                            "subText": "",
                            "fontSize": 60
                        }
                    }
                },
                "aggs": [
                    {
                        "id": "1",
                        "enabled": True,
                        "type": config["aggregation"],
                        "schema": "metric",
                        "params": {
                            "field": config["field"],
                            "customLabel": "Average Accuracy (%)"
                        }
                    }
                ]
            }),
            "uiStateJSON": "{}",
            "description": "",
            "savedSearchId": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {
                        "query": "",
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
                "id": config["index_pattern"]
            }
        ]
    }

def create_table_visualization(config):
    """Create a table visualization definition"""
    return {
        "attributes": {
            "title": config["title"],
            "visState": json.dumps({
                "title": config["title"],
                "type": "table",
                "params": {
                    "perPage": 10,
                    "showPartialRows": False,
                    "showMetricsAtAllLevels": False,
                    "sort": {
                        "columnIndex": 0,
                        "direction": "desc"
                    },
                    "showTotal": False,
                    "totalFunc": "sum",
                    "dimensions": {
                        "metrics": [{"accessor": 3, "format": {"id": "number"}, "params": {}, "aggType": "count"}],
                        "buckets": []
                    }
                },
                "aggs": [
                    {
                        "id": "1",
                        "enabled": True,
                        "type": "count",
                        "schema": "metric",
                        "params": {}
                    },
                    {
                        "id": "2",
                        "enabled": True,
                        "type": "terms",
                        "schema": "bucket",
                        "params": {
                            "field": config["fields"][0],
                            "orderBy": config["sort_field"],
                            "order": config["sort_order"],
                            "size": 20,
                            "otherBucket": False,
                            "otherBucketLabel": "Other",
                            "missingBucket": False,
                            "missingBucketLabel": "Missing"
                        }
                    },
                    {
                        "id": "3",
                        "enabled": True,
                        "type": "terms",
                        "schema": "bucket",
                        "params": {
                            "field": config["fields"][1],
                            "orderBy": "1",
                            "order": "desc",
                            "size": 5,
                            "otherBucket": False,
                            "otherBucketLabel": "Other",
                            "missingBucket": False,
                            "missingBucketLabel": "Missing"
                        }
                    },
                    {
                        "id": "4",
                        "enabled": True,
                        "type": "terms",
                        "schema": "bucket",
                        "params": {
                            "field": config["fields"][2],
                            "orderBy": "1",
                            "order": "desc",
                            "size": 5,
                            "otherBucket": False,
                            "otherBucketLabel": "Other",
                            "missingBucket": False,
                            "missingBucketLabel": "Missing"
                        }
                    },
                    {
                        "id": "5",
                        "enabled": True,
                        "type": "avg",
                        "schema": "metric",
                        "params": {
                            "field": config["fields"][3]
                        }
                    }
                ]
            }),
            "uiStateJSON": json.dumps({
                "vis": {
                    "params": {
                        "sort": {
                            "columnIndex": 0,
                            "direction": "desc"
                        }
                    }
                }
            }),
            "description": "",
            "savedSearchId": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {
                        "query": "",
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
                "id": config["index_pattern"]
            }
        ]
    }

def main():
    """Main function to import dashboards"""
    logger.info("Starting Kibana dashboard import")
    
    # Wait for Kibana to be ready
    if not wait_for_kibana():
        logger.error("Timed out waiting for Kibana")
        return False
    
    # Create index pattern
    if not create_index_pattern():
        logger.error("Failed to create index pattern")
        return False
    
    # Create visualizations
    if not create_visualizations():
        logger.warning("Failed to create all visualizations")
    
    # Import dashboards
    if not import_dashboards():
        logger.error("Failed to import dashboards")
        return False
    
    logger.info("Dashboard import completed successfully")
    return True

if __name__ == "__main__":
    main()
