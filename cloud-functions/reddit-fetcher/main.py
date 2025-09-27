"""
Reddit API Data Fetcher
Fetches Reddit posts and comments mentioning financial brands
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import praw
from google.cloud import storage
from google.cloud import secretmanager
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'brand-health-raw-data')
REDDIT_SECRET_NAME = os.environ.get('REDDIT_SECRET_NAME', 'reddit-credentials')

# Financial brands and relevant subreddits 
FINANCIAL_BRANDS = {
    'jpm': ['JPMorgan', 'Chase', 'Chase Bank'],
    'wf': ['Wells Fargo', 'WellsFargo'],
    'bac': ['Bank of America', 'BankofAmerica', 'BofA'],
    'c': ['Citibank', 'Citi', 'Citicorp'],
    'gs': ['Goldman Sachs', 'Goldman'],
    'ms': ['Morgan Stanley'],
    'td': ['TD Bank', 'Toronto-Dominion', 'TD Canada', 'TD US']
}

RELEVANT_SUBREDDITS = [
    'personalfinance', 'banking', 'CreditCards', 'investing',
    'financialindependence', 'stocks', 'SecurityBank', 'complaints',
    'legaladvice', 'povertyfinance', 'StudentLoans'
]

class RedditFetcher:
    def __init__(self):
        self.reddit = self._initialize_reddit()
        self.storage_client = storage.Client()
        
    def _get_reddit_credentials(self) -> Dict[str, str]:
        """Retrieve Reddit API credentials from Secret Manager"""
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{REDDIT_SECRET_NAME}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return json.loads(response.payload.data.decode("UTF-8"))
    
    def _initialize_reddit(self) -> praw.Reddit:
        """Initialize Reddit API client"""
        creds = self._get_reddit_credentials()
        return praw.Reddit(
            client_id=creds['client_id'],
            client_secret=creds['client_secret'],
            user_agent=creds['user_agent'],
            username=creds.get('username'),
            password=creds.get('password')
        )
    
    def search_brand_mentions(self, brand_id: str, brand_terms: List[str], 
                            since_timestamp: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for brand mentions across relevant subreddits"""
        messages = []
        
        for subreddit_name in RELEVANT_SUBREDDITS:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Search for each brand term
                for term in brand_terms:
                    query = f'"{term}"'
                    
                    # Search recent posts
                    for submission in subreddit.search(query, sort='new', time_filter='day', limit=limit//len(brand_terms)):
                        if submission.created_utc >= since_timestamp:
                            post_data = self._process_submission(submission, brand_id, subreddit_name)
                            if post_data:
                                messages.append(post_data)
                            
                            # Process top comments
                            submission.comments.replace_more(limit=2)
                            for comment in submission.comments.list()[:5]:  # Top 5 comments
                                if any(term.lower() in comment.body.lower() for term in brand_terms):
                                    comment_data = self._process_comment(comment, brand_id, subreddit_name)
                                    if comment_data:
                                        messages.append(comment_data)
                        
            except Exception as e:
                logger.error(f"Error searching {subreddit_name} for {brand_id}: {e}")
                continue
        
        logger.info(f"Found {len(messages)} Reddit messages for brand {brand_id}")
        return messages
    
    def _process_submission(self, submission, brand_id: str, subreddit_name: str) -> Optional[Dict[str, Any]]:
        """Process Reddit submission into standardized format"""
        try:
            return {
                'brand_id': brand_id,
                'reddit_id': submission.id,
                'ts_event': datetime.fromtimestamp(submission.created_utc).isoformat(),
                'subreddit': subreddit_name,
                'author': str(submission.author) if submission.author else '[deleted]',
                'type': 'post',
                'title': submission.title,
                'body': submission.selftext,
                'score': submission.score,
                'num_comments': submission.num_comments,
                'upvote_ratio': submission.upvote_ratio,
                'url': submission.url,
                'permalink': f"https://reddit.com{submission.permalink}",
                'lang': 'en',  # Assuming English for now
                'geo_country': None,  # Reddit doesn't provide geo data
                'collected_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error processing submission {submission.id}: {e}")
            return None
    
    def _process_comment(self, comment, brand_id: str, subreddit_name: str) -> Optional[Dict[str, Any]]:
        """Process Reddit comment into standardized format"""
        try:
            return {
                'brand_id': brand_id,
                'reddit_id': comment.id,
                'ts_event': datetime.fromtimestamp(comment.created_utc).isoformat(),
                'subreddit': subreddit_name,
                'author': str(comment.author) if comment.author else '[deleted]',
                'type': 'comment',
                'title': None,
                'body': comment.body,
                'score': comment.score,
                'num_comments': None,
                'upvote_ratio': None,
                'url': None,
                'permalink': f"https://reddit.com{comment.permalink}",
                'lang': 'en',
                'geo_country': None,
                'collected_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error processing comment {comment.id}: {e}")
            return None
    
    def save_to_gcs(self, messages: List[Dict[str, Any]], brand_id: str, date_str: str):
        """Save Reddit messages to GCS in NDJSON format for Fivetran"""
        if not messages:
            logger.info(f"No Reddit messages to save for brand {brand_id}")
            return
            
        bucket = self.storage_client.bucket(BUCKET_NAME)
        
        # Create path: raw/reddit/date=YYYY-MM-DD/brand_id.ndjson
        blob_path = f"raw/reddit/date={date_str}/{brand_id}.ndjson"
        blob = bucket.blob(blob_path)
        
        # Convert to newline-delimited JSON
        ndjson_content = '\n'.join([json.dumps(message) for message in messages])
        
        blob.upload_from_string(ndjson_content, content_type='application/x-ndjson')
        logger.info(f"Saved {len(messages)} Reddit messages to gs://{BUCKET_NAME}/{blob_path}")

@functions_framework.http
def fetch_reddit_data(request):
    """Cloud Function entry point"""
    try:
        # Parse request parameters
        request_json = request.get_json(silent=True) or {}
        
        # Default to yesterday's data
        target_date = request_json.get('date')
        if not target_date:
            yesterday = datetime.utcnow() - timedelta(days=1)
            target_date = yesterday.strftime('%Y-%m-%d')
        
        # Calculate since timestamp
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        since_timestamp = int(date_obj.timestamp())
        
        fetcher = RedditFetcher()
        
        # Fetch data for each brand
        total_messages = 0
        for brand_id, brand_terms in FINANCIAL_BRANDS.items():
            messages = fetcher.search_brand_mentions(brand_id, brand_terms, since_timestamp)
            fetcher.save_to_gcs(messages, brand_id, target_date)
            total_messages += len(messages)
        
        return {
            'status': 'success',
            'date': target_date,
            'total_messages': total_messages,
            'brands_processed': len(FINANCIAL_BRANDS)
        }, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_reddit_data: {e}")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    # For Cloud Run deployment
    import os
    from flask import Flask, request
    
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def handle_request():
        return fetch_reddit_data(request)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        return {'status': 'healthy'}, 200
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
