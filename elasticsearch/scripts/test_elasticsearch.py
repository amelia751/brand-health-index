#!/usr/bin/env python3
"""
Elasticsearch Test Script
Tests all Elasticsearch functionality for AI agent
"""

import os
import json
import requests
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ES_URL = os.environ.get('ELASTICSEARCH_URL', 'http://35.202.249.118:9200')
ES_INDEX = 'brand-complaints-2025'

def test_elasticsearch_connection():
    """Test basic Elasticsearch connection"""
    try:
        response = requests.get(f"{ES_URL}/_cluster/health", timeout=10)
        if response.status_code == 200:
            health = response.json()
            logger.info(f"✅ Elasticsearch connection: {health['status']}")
            return True
        else:
            logger.error(f"❌ Connection failed: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Connection error: {e}")
        return False

def test_index_exists():
    """Test if our index exists"""
    try:
        response = requests.get(f"{ES_URL}/{ES_INDEX}")
        if response.status_code == 200:
            logger.info(f"✅ Index '{ES_INDEX}' exists")
            return True
        else:
            logger.error(f"❌ Index '{ES_INDEX}' not found")
            return False
    except Exception as e:
        logger.error(f"❌ Index check error: {e}")
        return False

def test_index_sample_data():
    """Add sample data for testing"""
    
    sample_docs = [
        {
            "event_id": "test_reddit_001",
            "brand_id": "chase",
            "source": "reddit",
            "ts_event": "2025-10-07T15:00:00Z",
            "text": "Chase bank has terrible customer service and high fees",
            "sentiment": -0.7,
            "severity": 0.6,
            "topics": ["customer_service", "fees"],
            "nlp_confidence": 0.85,
            "subreddit": "personalfinance",
            "author": "test_user_1"
        },
        {
            "event_id": "test_cfpb_001", 
            "brand_id": "bank_of_america",
            "source": "cfpb",
            "ts_event": "2025-10-07T14:30:00Z",
            "text": "Issue: Account opening, closing, or management | Complaint: Unable to open account due to system errors",
            "sentiment": -0.5,
            "severity": 0.4,
            "topics": ["account_lock", "ux"],
            "nlp_confidence": 0.78
        },
        {
            "event_id": "test_reddit_002",
            "brand_id": "td_bank",
            "source": "reddit", 
            "ts_event": "2025-10-07T13:15:00Z",
            "text": "TD Bank has great mobile app and friendly tellers",
            "sentiment": 0.8,
            "severity": 0.1,
            "topics": ["mobile_app", "customer_service"],
            "nlp_confidence": 0.92,
            "subreddit": "TDBank",
            "author": "test_user_2"
        }
    ]
    
    # Index sample documents
    for doc in sample_docs:
        try:
            response = requests.post(
                f"{ES_URL}/{ES_INDEX}/_doc/{doc['event_id']}",
                headers={'Content-Type': 'application/json'},
                json=doc
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ Indexed sample doc: {doc['event_id']}")
            else:
                logger.error(f"❌ Failed to index {doc['event_id']}: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ Error indexing {doc['event_id']}: {e}")
    
    # Refresh index
    requests.post(f"{ES_URL}/{ES_INDEX}/_refresh")
    logger.info("🔄 Index refreshed")

def test_search_queries():
    """Test various search queries for AI agent"""
    
    test_queries = [
        {
            "name": "Basic text search",
            "query": {
                "query": {"match": {"text": "customer service"}},
                "size": 5
            }
        },
        {
            "name": "Brand filter search", 
            "query": {
                "query": {
                    "bool": {
                        "must": [{"match": {"text": "bank"}}],
                        "filter": [{"term": {"brand_id": "chase"}}]
                    }
                },
                "size": 5
            }
        },
        {
            "name": "Sentiment range search",
            "query": {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"sentiment": {"gte": -1, "lte": -0.5}}}
                        ]
                    }
                },
                "sort": [{"sentiment": {"order": "asc"}}],
                "size": 5
            }
        },
        {
            "name": "Multi-source aggregation",
            "query": {
                "size": 0,
                "aggs": {
                    "by_source": {
                        "terms": {"field": "source"}
                    },
                    "by_brand": {
                        "terms": {"field": "brand_id"}
                    },
                    "avg_sentiment": {
                        "avg": {"field": "sentiment"}
                    }
                }
            }
        }
    ]
    
    for test in test_queries:
        try:
            response = requests.post(
                f"{ES_URL}/{ES_INDEX}/_search",
                headers={'Content-Type': 'application/json'},
                json=test["query"]
            )
            
            if response.status_code == 200:
                result = response.json()
                hits = result.get('hits', {}).get('total', {}).get('value', 0)
                logger.info(f"✅ {test['name']}: {hits} results")
                
                # Show aggregations if present
                if 'aggregations' in result:
                    aggs = result['aggregations']
                    for agg_name, agg_data in aggs.items():
                        if 'buckets' in agg_data:
                            buckets = agg_data['buckets']
                            logger.info(f"   {agg_name}: {len(buckets)} buckets")
                        elif 'value' in agg_data:
                            logger.info(f"   {agg_name}: {agg_data['value']:.3f}")
                            
            else:
                logger.error(f"❌ {test['name']} failed: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ {test['name']} error: {e}")

def test_ai_agent_queries():
    """Test specific queries the AI agent will use"""
    
    # Simulate AI agent evidence retrieval
    agent_query = {
        "query": {
            "bool": {
                "must": [
                    {"multi_match": {
                        "query": "fees customer service",
                        "fields": ["text^2", "topics"]
                    }}
                ],
                "filter": [
                    {"terms": {"brand_id": ["chase", "bank_of_america"]}},
                    {"range": {"ts_event": {"gte": "2025-10-01"}}}
                ]
            }
        },
        "sort": [
            {"sentiment": {"order": "asc"}},  # Most negative first
            {"severity": {"order": "desc"}},   # Most severe first
            {"_score": {"order": "desc"}}      # Most relevant first
        ],
        "size": 10,
        "_source": ["event_id", "brand_id", "text", "sentiment", "severity", "ts_event", "source"]
    }
    
    try:
        response = requests.post(
            f"{ES_URL}/{ES_INDEX}/_search",
            headers={'Content-Type': 'application/json'},
            json=agent_query
        )
        
        if response.status_code == 200:
            result = response.json()
            hits = result['hits']['hits']
            logger.info(f"✅ AI Agent query: {len(hits)} evidence documents")
            
            # Show sample results
            for i, hit in enumerate(hits[:3]):
                source = hit['_source']
                logger.info(f"   Evidence {i+1}: {source['brand_id']} | sentiment: {source['sentiment']} | {source['text'][:60]}...")
                
        else:
            logger.error(f"❌ AI Agent query failed: {response.text}")
            
    except Exception as e:
        logger.error(f"❌ AI Agent query error: {e}")

def main():
    """Run all Elasticsearch tests"""
    
    logger.info("🧪 Starting Elasticsearch Tests for AI Agent...")
    
    # Basic connectivity
    if not test_elasticsearch_connection():
        return
    
    # Index existence
    if not test_index_exists():
        logger.info("Creating index first...")
        from setup_index import create_index
        if not create_index():
            return
    
    # Add sample data
    test_index_sample_data()
    
    # Test search functionality
    test_search_queries()
    
    # Test AI agent specific queries
    test_ai_agent_queries()
    
    logger.info("✅ All Elasticsearch tests completed!")

if __name__ == "__main__":
    main()
