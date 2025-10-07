#!/usr/bin/env python3
"""
Elasticsearch Index Setup Script
Creates and configures the brand-complaints index for AI agent
"""

import os
import json
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ES_URL = os.environ.get('ELASTICSEARCH_URL', 'http://35.202.249.118:9200')
ES_INDEX = 'brand-complaints-2025'

def load_mapping():
    """Load index mapping from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'index_mapping.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def create_index():
    """Create Elasticsearch index with proper mapping"""
    
    # Delete existing index if it exists
    delete_response = requests.delete(f"{ES_URL}/{ES_INDEX}")
    if delete_response.status_code in [200, 404]:
        logger.info(f"Cleaned up existing index (if any)")
    
    # Load mapping configuration
    mapping_config = load_mapping()
    
    # Create new index
    response = requests.put(
        f"{ES_URL}/{ES_INDEX}",
        headers={'Content-Type': 'application/json'},
        json=mapping_config
    )
    
    if response.status_code == 200:
        result = response.json()
        logger.info(f"Successfully created index '{ES_INDEX}'")
        logger.info(f"Acknowledged: {result.get('acknowledged')}")
        return True
    else:
        logger.error(f"Failed to create index: {response.status_code} - {response.text}")
        return False

def test_index():
    """Test the created index"""
    
    # Check index exists
    response = requests.get(f"{ES_URL}/{ES_INDEX}")
    if response.status_code == 200:
        logger.info(f"Index '{ES_INDEX}' exists and is accessible")
        
        # Get index info
        info = response.json()
        settings = info[ES_INDEX]['settings']
        mappings = info[ES_INDEX]['mappings']
        
        logger.info(f"Shards: {settings['index']['number_of_shards']}")
        logger.info(f"Replicas: {settings['index']['number_of_replicas']}")
        logger.info(f"Mapped fields: {len(mappings['properties'])}")
        
        return True
    else:
        logger.error(f"Index test failed: {response.status_code}")
        return False

def main():
    """Main setup function"""
    logger.info("Setting up Elasticsearch index for AI agent...")
    
    if create_index():
        if test_index():
            logger.info("✅ Elasticsearch setup complete!")
        else:
            logger.error("❌ Index test failed")
    else:
        logger.error("❌ Index creation failed")

if __name__ == "__main__":
    main()
