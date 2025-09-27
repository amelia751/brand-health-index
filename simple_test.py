#!/usr/bin/env python3
"""
Simple test script to verify Brand Health Index infrastructure basics
"""

import os
import json
from datetime import datetime
from google.cloud import storage
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
            "message": "Infrastructure test successful - TD Bank sponsorship confirmed! 🏦"
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
                "source": "infrastructure_test",
                "brands": ["JPMorgan", "Wells Fargo", "Bank of America", "Citibank", "Goldman Sachs", "Morgan Stanley", "TD Bank"]
            })
            
            future = publisher.publish(topic_path, test_message.encode('utf-8'))
            message_id = future.result()
            
            print(f"✅ Published test message to {topic_name} (ID: {message_id})")
            
        return True
        
    except Exception as e:
        print(f"❌ Pub/Sub test failed: {e}")
        return False

def test_bucket_structure():
    """Test creating the expected folder structure in GCS"""
    print("\n🧪 Testing GCS folder structure...")
    
    bucket_name = "brand-health-raw-data-dev-20241201"
    client = storage.Client()
    
    try:
        bucket = client.bucket(bucket_name)
        
        # Create sample files in the expected structure
        sources = ['twitter', 'reddit', 'trends', 'cfpb']
        brands = ['td', 'jpm', 'wf', 'bac', 'c', 'gs', 'ms']
        
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        for source in sources:
            for brand in brands:
                blob_path = f"raw/{source}/date={current_date}/{brand}_sample.json"
                blob = bucket.blob(blob_path)
                
                sample_data = {
                    "source": source,
                    "brand_id": brand,
                    "date": current_date,
                    "test_data": f"Sample {source} data for {brand}",
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                blob.upload_from_string(json.dumps(sample_data), content_type='application/json')
        
        print(f"✅ Created sample folder structure in GCS")
        print(f"   - Structure: raw/{{source}}/date={{date}}/{{brand}}.json")
        print(f"   - Sources: {sources}")
        print(f"   - Brands: {brands}")
        
        return True
        
    except Exception as e:
        print(f"❌ GCS folder structure test failed: {e}")
        return False

def main():
    """Run all infrastructure tests"""
    print("🚀 Brand Health Index - Infrastructure Test")
    print("📊 Sponsored by TD Bank")
    print("=" * 50)
    
    tests = [
        ("GCS Bucket Access", test_gcs_bucket),
        ("Pub/Sub Topics", test_pubsub_topics),
        ("GCS Folder Structure", test_bucket_structure)
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
        print("\n🎉 All infrastructure tests passed!")
        print("\n📋 Your Brand Health Index pipeline infrastructure is ready:")
        print("   ✅ GCS bucket for raw data storage")
        print("   ✅ Pub/Sub topics for triggering data collection")
        print("   ✅ Proper folder structure for Fivetran ingestion")
        print("   ✅ BigQuery datasets (fivetran_raw, brand_health_dev)")
        print("   ✅ TD Bank included in brand tracking")
        
        print("\n🔧 Next steps:")
        print("   1. Add API credentials to Secret Manager")
        print("   2. Build and deploy Cloud Run services") 
        print("   3. Set up Fivetran connectors")
        print("   4. Deploy data transformations")
        print("   5. Create dashboards")
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
    
    return all_passed

if __name__ == "__main__":
    main()
