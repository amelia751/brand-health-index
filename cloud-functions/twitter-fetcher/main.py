"""
Twitter/X API v2 Data Fetcher
Fetches tweets mentioning financial brands and writes to GCS for Fivetran ingestion
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from google.cloud import storage
from google.cloud import secretmanager
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'brand-health-raw-data')
SECRET_NAME = os.environ.get('TWITTER_SECRET_NAME', 'twitter-bearer-token')

# Financial brands to monitor 
FINANCIAL_BRANDS = {
    'jpm': ['JPMorgan', 'Chase Bank', '@Chase'],
    'wf': ['Wells Fargo', '@WellsFargo'],
    'bac': ['Bank of America', '@BankofAmerica', 'BofA'],
    'c': ['Citibank', 'Citi', '@Citi'],
    'gs': ['Goldman Sachs', '@GoldmanSachs'],
    'ms': ['Morgan Stanley', '@MorganStanley'],
    'td': ['TD Bank', '@TDBank_US', '@TDBank', 'Toronto-Dominion']
}

class TwitterFetcher:
    def __init__(self):
        self.bearer_token = self._get_secret()
        self.base_url = "https://api.twitter.com/2"
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
        self.storage_client = storage.Client()
        
    def _get_secret(self) -> str:
        """Retrieve Twitter bearer token from Secret Manager"""
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    
    def _build_query(self, brand_terms: List[str]) -> str:
        """Build Twitter search query for brand mentions"""
        # Combine brand terms with OR, exclude retweets
        terms = ' OR '.join([f'"{term}"' for term in brand_terms])
        return f"({terms}) -is:retweet lang:en"
    
    def fetch_tweets(self, brand_id: str, brand_terms: List[str], 
                    since_date: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Fetch tweets for a specific brand"""
        
        query = self._build_query(brand_terms)
        
        params = {
            'query': query,
            'max_results': min(max_results, 100),  # API limit
            'start_time': since_date,
            'tweet.fields': 'id,text,author_id,created_at,lang,public_metrics,possibly_sensitive,geo',
            'user.fields': 'id,username,location,verified',
            'expansions': 'author_id,geo.place_id'
        }
        
        url = f"{self.base_url}/tweets/search/recent"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            tweets = []
            
            if 'data' in data:
                users = {u['id']: u for u in data.get('includes', {}).get('users', [])}
                places = {p['id']: p for p in data.get('includes', {}).get('places', [])}
                
                for tweet in data['data']:
                    # Enrich with user and place data
                    user = users.get(tweet['author_id'], {})
                    place = places.get(tweet.get('geo', {}).get('place_id'), {})
                    
                    processed_tweet = {
                        'brand_id': brand_id,
                        'tweet_id': tweet['id'],
                        'ts_event': tweet['created_at'],
                        'author_id': tweet['author_id'],
                        'author_username': user.get('username'),
                        'author_verified': user.get('verified', False),
                        'author_location': user.get('location'),
                        'text': tweet['text'],
                        'lang': tweet.get('lang', 'en'),
                        'like_count': tweet['public_metrics']['like_count'],
                        'reply_count': tweet['public_metrics']['reply_count'],
                        'retweet_count': tweet['public_metrics']['retweet_count'],
                        'quote_count': tweet['public_metrics']['quote_count'],
                        'possibly_sensitive': tweet.get('possibly_sensitive', False),
                        'geo_country': place.get('country'),
                        'geo_place_id': tweet.get('geo', {}).get('place_id'),
                        'collected_at': datetime.utcnow().isoformat()
                    }
                    tweets.append(processed_tweet)
            
            logger.info(f"Fetched {len(tweets)} tweets for brand {brand_id}")
            return tweets
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching tweets for {brand_id}: {e}")
            return []
    
    def save_to_gcs(self, tweets: List[Dict[str, Any]], brand_id: str, date_str: str):
        """Save tweets to GCS in NDJSON format for Fivetran"""
        if not tweets:
            logger.info(f"No tweets to save for brand {brand_id}")
            return
            
        bucket = self.storage_client.bucket(BUCKET_NAME)
        
        # Create path: raw/twitter/date=YYYY-MM-DD/brand_id.ndjson
        blob_path = f"raw/twitter/date={date_str}/{brand_id}.ndjson"
        blob = bucket.blob(blob_path)
        
        # Convert to newline-delimited JSON
        ndjson_content = '\n'.join([json.dumps(tweet) for tweet in tweets])
        
        blob.upload_from_string(ndjson_content, content_type='application/x-ndjson')
        logger.info(f"Saved {len(tweets)} tweets to gs://{BUCKET_NAME}/{blob_path}")

@functions_framework.http
def fetch_twitter_data(request):
    """Cloud Function entry point"""
    try:
        # Parse request parameters
        request_json = request.get_json(silent=True) or {}
        
        # Default to yesterday's data
        target_date = request_json.get('date')
        if not target_date:
            yesterday = datetime.utcnow() - timedelta(days=1)
            target_date = yesterday.strftime('%Y-%m-%d')
        
        # Calculate since_time for API (ISO format)
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        since_time = date_obj.isoformat() + 'Z'
        
        fetcher = TwitterFetcher()
        
        # Fetch data for each brand
        total_tweets = 0
        for brand_id, brand_terms in FINANCIAL_BRANDS.items():
            tweets = fetcher.fetch_tweets(brand_id, brand_terms, since_time)
            fetcher.save_to_gcs(tweets, brand_id, target_date)
            total_tweets += len(tweets)
        
        return {
            'status': 'success',
            'date': target_date,
            'total_tweets': total_tweets,
            'brands_processed': len(FINANCIAL_BRANDS)
        }, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_twitter_data: {e}")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    # For local testing
    from flask import Flask, request
    app = Flask(__name__)
    app.route('/', methods=['POST'])(fetch_twitter_data)
    app.run(debug=True, port=8080)
