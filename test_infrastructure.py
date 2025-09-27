#!/usr/bin/env python3
"""
Test script to verify Brand Health Index infrastructure is working
"""

import os
import json
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub_v1

def test_gcs_bucket():
    """Test GCS bucket access and write a test file"""
    print("🧪 Testing GCS bucket access...")
    
    bucket_name = "brand-health-raw-data-dev-20241201"
    client = storage.Client()
    
    try:
        bucket = client.bucket(bucket_name)
        
        # Test write
        test_data = {
            "test": True,
            "timestamp": datetime.utcnow().isoformat(),
            "brand_id": "td",
            "message": "Infrastructure test successful"
        }
        
        blob_path = f"test/infrastructure_test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(test_data), content_type='application/json')
        
        # Test read
        downloaded_data = json.loads(blob.download_as_text())
        
        print(f"✅ GCS bucket test successful!")
        print(f"   - Wrote test file: gs://{bucket_name}/{blob_path}")
        print(f"   - Data: {downloaded_data}")
        
        return True
        
    except Exception as e:
        print(f"❌ GCS bucket test failed: {e}")
        return False

def test_bigquery_datasets():
    """Test BigQuery dataset access"""
    print("\n🧪 Testing BigQuery datasets...")
    
    client = bigquery.Client()
    
    try:
        # List datasets
        datasets = list(client.list_datasets())
        dataset_ids = [d.dataset_id for d in datasets]
        
        required_datasets = ['fivetran_raw', 'brand_health_dev']
        
        for dataset_id in required_datasets:
            if dataset_id in dataset_ids:
                print(f"✅ Dataset {dataset_id} exists")
                
                # Test table creation
                table_id = f"brand-health-index.{dataset_id}.infrastructure_test"
                
                schema = [
                    bigquery.SchemaField("test_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("message", "STRING", mode="NULLABLE"),
                ]
                
                table = bigquery.Table(table_id, schema=schema)
                table = client.create_table(table, exists_ok=True)
                
                # Insert test data
                rows_to_insert = [{
                    "test_id": f"test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                    "timestamp": datetime.utcnow(),
                    "message": "Infrastructure test successful"
                }]
                
                errors = client.insert_rows_json(table, rows_to_insert)
                if not errors:
                    print(f"   - Successfully wrote test data to {dataset_id}")
                else:
                    print(f"   - Errors writing to {dataset_id}: {errors}")
                    
            else:
                print(f"❌ Dataset {dataset_id} missing")
                return False
                
        return True
        
    except Exception as e:
        print(f"❌ BigQuery test failed: {e}")
        return False

def test_pubsub_topics():
    """Test Pub/Sub topic access"""
    print("\n🧪 Testing Pub/Sub topics...")
    
    project_id = "brand-health-index"
    publisher = pubsub_v1.PublisherClient()
    
    required_topics = [
        'twitter-data-fetch',
        'reddit-data-fetch', 
        'trends-data-fetch',
        'cfpb-data-fetch'
    ]
    
    try:
        for topic_name in required_topics:
            topic_path = publisher.topic_path(project_id, topic_name)
            
            # Test publish
            test_message = json.dumps({
                "test": True,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "infrastructure_test"
            })
            
            future = publisher.publish(topic_path, test_message.encode('utf-8'))
            message_id = future.result()
            
            print(f"✅ Published test message to {topic_name} (ID: {message_id})")
            
        return True
        
    except Exception as e:
        print(f"❌ Pub/Sub test failed: {e}")
        return False

def main():
    """Run all infrastructure tests"""
    print("🚀 Brand Health Index - Infrastructure Test")
    print("=" * 50)
    
    tests = [
        ("GCS Bucket", test_gcs_bucket),
        ("BigQuery", test_bigquery_datasets), 
        ("Pub/Sub", test_pubsub_topics)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        results[test_name] = test_func()
    
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All infrastructure tests passed! Your Brand Health Index pipeline is ready.")
        print("\nNext steps:")
        print("1. Add API credentials to Secret Manager")
        print("2. Build and deploy Cloud Run services") 
        print("3. Set up Fivetran connectors")
        print("4. Deploy dbt transformations")
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
    
    return all_passed

if __name__ == "__main__":
    main()
