#!/usr/bin/env python3
"""
Elasticsearch Loader for AI Agent
Loads Reddit data from BigQuery into Elasticsearch for hybrid search
"""

import os
import json
import logging
from typing import List, Dict, Any
from google.cloud import bigquery
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'trendle-469110')
BQ_DATASET = 'brand_health_raw'
BQ_TABLE = 'reddit_events'
ES_URL = os.environ.get('ELASTICSEARCH_URL', 'http://35.202.249.118:9200')
ES_INDEX = 'brand-complaints-2025'

class ElasticsearchLoader:
    """Load Reddit data into Elasticsearch for AI agent"""
    
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.es_url = ES_URL
        self.es_index = ES_INDEX
    
    def extract_data_from_bigquery(self, limit: int = None) -> List[Dict[str, Any]]:
        """Extract Reddit data from BigQuery"""
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        query = f"""
        SELECT 
            event_id,
            brand_id,
            source,
            ts_event,
            text,
            sentiment,
            severity,
            topics,
            nlp_confidence,
            nlp_model,
            geo_country,
            content_hash,
            JSON_EXTRACT_SCALAR(metadata, '$.subreddit') as subreddit,
            JSON_EXTRACT_SCALAR(metadata, '$.author') as author,
            JSON_EXTRACT_SCALAR(metadata, '$.permalink') as url,
            CAST(JSON_EXTRACT_SCALAR(metadata, '$.score') AS INT64) as score,
            CAST(JSON_EXTRACT_SCALAR(metadata, '$.num_comments') AS INT64) as num_comments
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE text IS NOT NULL 
        AND LENGTH(text) > 10
        ORDER BY ts_event DESC
        {limit_clause}
        """
        
        logger.info(f"Extracting data from BigQuery: {query}")
        
        query_job = self.bq_client.query(query)
        results = query_job.result()
        
        records = []
        for row in results:
            record = {
                'event_id': row.event_id,
                'brand_id': row.brand_id,
                'source': row.source,
                'ts_event': row.ts_event.isoformat() if row.ts_event else None,
                'text': row.text,
                'sentiment': float(row.sentiment) if row.sentiment is not None else 0.0,
                'severity': float(row.severity) if row.severity is not None else 0.0,
                'topics': row.topics.split(',') if row.topics else [],
                'nlp_confidence': float(row.nlp_confidence) if row.nlp_confidence is not None else 0.0,
                'nlp_model': row.nlp_model,
                'geo_country': row.geo_country,
                'content_hash': row.content_hash,
                'subreddit': row.subreddit,
                'author': row.author,
                'url': row.url,
                'score': row.score,
                'num_comments': row.num_comments
            }
            records.append(record)
        
        logger.info(f"Extracted {len(records)} records from BigQuery")
        return records
    
    def bulk_index_to_elasticsearch(self, records: List[Dict[str, Any]], batch_size: int = 100):
        """Bulk index records to Elasticsearch"""
        
        total_indexed = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Create bulk request body
            bulk_body = []
            for record in batch:
                # Index action
                bulk_body.append(json.dumps({
                    "index": {
                        "_index": self.es_index,
                        "_id": record['event_id']
                    }
                }))
                
                # Document body
                bulk_body.append(json.dumps(record))
            
            bulk_data = '\n'.join(bulk_body) + '\n'
            
            # Send bulk request
            try:
                response = requests.post(
                    f"{self.es_url}/_bulk",
                    headers={'Content-Type': 'application/x-ndjson'},
                    data=bulk_data,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    errors = [item for item in result.get('items', []) if 'error' in item.get('index', {})]
                    
                    if errors:
                        logger.warning(f"Batch {i//batch_size + 1}: {len(errors)} errors out of {len(batch)} documents")
                        for error in errors[:3]:  # Show first 3 errors
                            logger.warning(f"Error: {error}")
                    else:
                        logger.info(f"Batch {i//batch_size + 1}: Successfully indexed {len(batch)} documents")
                    
                    total_indexed += len(batch) - len(errors)
                else:
                    logger.error(f"Batch {i//batch_size + 1}: HTTP {response.status_code} - {response.text}")
                    
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1}: Error indexing to Elasticsearch: {e}")
        
        logger.info(f"Total documents indexed: {total_indexed}")
        return total_indexed
    
    def verify_elasticsearch_data(self):
        """Verify data was loaded correctly"""
        
        try:
            # Get index stats
            response = requests.get(f"{self.es_url}/{self.es_index}/_count")
            if response.status_code == 200:
                count = response.json()['count']
                logger.info(f"Elasticsearch index '{self.es_index}' contains {count} documents")
            
            # Test search
            search_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"text": "bank"}}
                        ],
                        "filter": [
                            {"range": {"sentiment": {"gte": -1, "lte": 1}}}
                        ]
                    }
                },
                "size": 3,
                "sort": [{"ts_event": {"order": "desc"}}]
            }
            
            response = requests.post(
                f"{self.es_url}/{self.es_index}/_search",
                headers={'Content-Type': 'application/json'},
                json=search_query
            )
            
            if response.status_code == 200:
                results = response.json()
                hits = results['hits']['hits']
                logger.info(f"Test search returned {len(hits)} results")
                
                for hit in hits:
                    source = hit['_source']
                    logger.info(f"Sample: {source['brand_id']} - sentiment: {source['sentiment']} - {source['text'][:100]}...")
            
        except Exception as e:
            logger.error(f"Error verifying Elasticsearch data: {e}")
    
    def load_all_data(self):
        """Main method to load all data from BigQuery to Elasticsearch"""
        
        logger.info("Starting Elasticsearch data loading process...")
        
        # Extract data from BigQuery
        records = self.extract_data_from_bigquery()
        
        if not records:
            logger.warning("No data found in BigQuery")
            return
        
        # Load to Elasticsearch
        indexed_count = self.bulk_index_to_elasticsearch(records)
        
        # Verify the load
        self.verify_elasticsearch_data()
        
        logger.info(f"Data loading complete. {indexed_count} documents indexed.")

def main():
    """Main function"""
    loader = ElasticsearchLoader()
    loader.load_all_data()

if __name__ == "__main__":
    main()
