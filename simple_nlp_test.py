#!/usr/bin/env python3
"""
Simple test to verify Vertex AI NLP is working
"""

import json
import gzip
import tempfile
from google.cloud import storage
from google.cloud import aiplatform
import vertexai
from vertexai.generative_models import GenerativeModel

def test_vertex_ai_nlp():
    """Test Vertex AI NLP directly"""
    print("🧠 Testing Vertex AI NLP...")
    
    # Initialize Vertex AI
    vertexai.init(project="trendle-469110", location="us-central1")
    model = GenerativeModel("gemini-1.5-flash")
    
    # Test text
    test_text = "TD Bank's customer service is terrible. They charged me ridiculous fees and their online banking is always down. I'm switching to another bank."
    
    prompt = f"""Analyze this TD Bank customer feedback for sentiment, severity, and topics.

Text: "{test_text}"

Provide analysis in this exact JSON format:
{{
  "sentiment": <float between -1.0 and 1.0, where -1=very negative, 0=neutral, 1=very positive>,
  "severity": <float between 0.0 and 1.0, where 0=minor issue, 1=critical issue>,
  "topics": <array of relevant topics from: customer_service, fees, digital_banking, account_issues, fraud, loans, credit_cards, investment, mobile_app, branch_experience>,
  "language": "en",
  "confidence": <float between 0.0 and 1.0 indicating analysis confidence>
}}

Return only valid JSON, no other text."""
    
    try:
        response = model.generate_content(prompt)
        result_text = response.text.strip()
        
        # Try to parse as JSON
        result = json.loads(result_text)
        
        print("✅ Vertex AI NLP working!")
        print(f"📊 Test result: {json.dumps(result, indent=2)}")
        return True
        
    except Exception as e:
        print(f"❌ Vertex AI NLP failed: {e}")
        return False

def check_sample_data():
    """Check a sample of Oct 9-12 data"""
    print("\n🔍 Checking sample data from Oct 9-12...")
    
    storage_client = storage.Client(project='trendle-469110')
    bucket = storage_client.bucket('brand-health-raw-data-469110')
    
    dates = ["2025-10-09", "2025-10-10", "2025-10-11", "2025-10-12"]
    
    for date in dates:
        print(f"\n📅 {date}:")
        prefix = f"raw/reddit/dt={date}/"
        
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
        if not blobs:
            print("  No files found")
            continue
            
        blob = blobs[0]
        
        with tempfile.NamedTemporaryFile() as temp_file:
            blob.download_to_filename(temp_file.name)
            
            with gzip.open(temp_file.name, 'rt', encoding='utf-8') as f:
                lines = f.readlines()[:3]  # Just first 3 records
                
                for i, line in enumerate(lines):
                    if line.strip():
                        record = json.loads(line.strip())
                        sentiment = record.get('sentiment', 'N/A')
                        text_preview = record.get('text', '')[:50] + '...'
                        print(f"  Record {i+1}: sentiment={sentiment}, text='{text_preview}'")

def main():
    print("🚀 Simple NLP Test & Data Check")
    print("=" * 50)
    
    # Test Vertex AI
    if test_vertex_ai_nlp():
        print("\n✅ Vertex AI is working properly!")
    else:
        print("\n❌ Vertex AI has issues!")
        return
    
    # Check sample data
    try:
        check_sample_data()
    except Exception as e:
        print(f"❌ Error checking data: {e}")

if __name__ == "__main__":
    main()
