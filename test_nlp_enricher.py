#!/usr/bin/env python3
"""
Test script for NLP enricher to verify Vertex AI sentiment analysis works
"""

import sys
import os
sys.path.append('cloud-functions/reddit-fetcher')

# Sample TD Bank Reddit data for testing
SAMPLE_DATA = [
    {
        "event_id": "reddit_t3_test1",
        "ts_event": "2025-10-12T10:00:00Z",
        "brand_id": "td_bank",
        "source": "reddit",
        "text": "TD Bank customer service is absolutely terrible. Waited 2 hours on hold and they couldn't even help me with a simple account question. Worst bank ever!",
        "metadata": {"subreddit": "personalfinance", "score": -5}
    },
    {
        "event_id": "reddit_t3_test2", 
        "ts_event": "2025-10-12T10:01:00Z",
        "brand_id": "td_bank",
        "source": "reddit",
        "text": "Love TD Bank's mobile app! Super easy to deposit checks and transfer money. The interface is clean and fast.",
        "metadata": {"subreddit": "banking", "score": 12}
    },
    {
        "event_id": "reddit_t3_test3",
        "ts_event": "2025-10-12T10:02:00Z", 
        "brand_id": "td_bank",
        "source": "reddit",
        "text": "TD Bank charged me a $35 overdraft fee even though I had overdraft protection. This is the third time this month. Seriously considering switching banks.",
        "metadata": {"subreddit": "povertyfinance", "score": 3}
    },
    {
        "event_id": "reddit_t3_test4",
        "ts_event": "2025-10-12T10:03:00Z",
        "brand_id": "td_bank", 
        "source": "reddit",
        "text": "TD Bank mortgage rates are pretty competitive right now. Got pre-approved quickly and the loan officer was helpful throughout the process.",
        "metadata": {"subreddit": "realestate", "score": 8}
    },
    {
        "event_id": "reddit_t3_test5",
        "ts_event": "2025-10-12T10:04:00Z",
        "brand_id": "td_bank",
        "source": "reddit", 
        "text": "TD Bank ATM ate my card and now I'm stuck without access to my money over the weekend. No branch nearby is open. This is a nightmare.",
        "metadata": {"subreddit": "mildlyinfuriating", "score": 1}
    }
]

def test_nlp_enricher():
    """Test the NLP enricher with sample data"""
    try:
        # Import the NLP enricher
        from main_nlp import NLPEnricher
        
        print("🧪 Testing NLP Enricher...")
        print("=" * 50)
        
        # Initialize enricher
        enricher = NLPEnricher()
        
        # Test each sample
        for i, sample in enumerate(SAMPLE_DATA, 1):
            print(f"\n📝 Test {i}: {sample['event_id']}")
            print(f"Text: {sample['text'][:100]}...")
            
            # Enrich the sample
            enriched = enricher._enrich_with_nlp([sample])
            
            if enriched and len(enriched) > 0:
                result = enriched[0]
                print(f"✅ Sentiment: {result.get('sentiment', 'N/A')}")
                print(f"✅ Severity: {result.get('severity', 'N/A')}")
                print(f"✅ Topics: {result.get('topics', 'N/A')}")
                print(f"✅ Confidence: {result.get('nlp_confidence', 'N/A')}")
                print(f"✅ Model: {result.get('nlp_model', 'N/A')}")
            else:
                print("❌ No enrichment returned")
        
        print("\n" + "=" * 50)
        print("🎯 NLP Enricher Test Complete!")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("Make sure main_nlp.py is in the correct location")
        return False
    except Exception as e:
        print(f"❌ Error testing NLP enricher: {e}")
        return False

if __name__ == "__main__":
    success = test_nlp_enricher()
    if success:
        print("\n✅ NLP enricher is working!")
    else:
        print("\n❌ NLP enricher has issues - needs debugging")
