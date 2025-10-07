"""
Idempotent Reddit API Data Fetcher
Fetches Reddit posts and comments with natural IDs and state tracking
Implements pagination and deduplication to avoid duplicates
"""

import os
import json
import logging
import hashlib
import gzip
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import time
import uuid

import praw
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'brand-health-raw-data')
REDDIT_SECRET_NAME = os.environ.get('REDDIT_SECRET_NAME', 'reddit-credentials')
BQ_DATASET = os.environ.get('BQ_DATASET', 'brand_health_raw')
REDDIT_REQUESTS_PER_MINUTE = int(os.environ.get('REDDIT_REQUESTS_PER_MINUTE', '100'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1000'))

# Rate limiting
REQUEST_DELAY = 60.0 / REDDIT_REQUESTS_PER_MINUTE  # seconds between requests

# Financial institutions to track
FINANCIAL_BRANDS = {
    'td_bank': ['TD Bank', 'TD'],
    'bank_of_america': ['Bank of America', 'BofA', 'BoA'],
    'capital_one': ['Capital One'],
    'chase': ['Chase', 'Chase Bank'],
    'citibank': ['Citibank', 'Citi'],
    'citizens_bank': ['Citizens Bank'],
    'mt_bank': ['M&T Bank', 'M&T'],
    'pnc': ['PNC', 'PNC Bank'],
    'santander': ['Santander', 'Santander Bank'],
    'wells_fargo': ['Wells Fargo', 'WellsFargo'],
    'keybank': ['KeyBank', 'Key Bank'],
    'regions_bank': ['Regions Bank', 'Regions'],
    'truist': ['Truist', 'Truist Bank']
}

# Subreddit categories mapping
RELEVANT_SUBREDDITS = [
    # General Personal Finance
    'personalfinance', 'financialindependence', 'frugal', 'povertyfinance', 'finance',
    # Credit Cards
    'creditcards', 'churning', 'loans', 'personalcredit',
    # Mortgages
    'realestate', 'mortgages', 'firsttimehomebuyer',
    # Student Loans
    'studentloans',
    # Retirement & Investing
    '401k', 'investing',
    # Bank-Specific Subreddits
    'TDBank', 'TD_Bank', 'BankofAmerica', 'CapitalOne', 'Chase', 'Citi',
    'CitizensBank', 'MTB_Bank', 'PNCBank', 'SantanderBank', 'WellsFargo',
    'KeyBank', 'RegionsBank', 'TruistBank'
]

class IngestionState:
    """Manages ingestion state for idempotent processing"""
    
    def __init__(self, bq_client: bigquery.Client):
        self.bq_client = bq_client
        self.dataset_id = BQ_DATASET
        self.table_id = 'ingest_state'
        self._ensure_state_table()
    
    def _ensure_state_table(self):
        """Create ingestion state table if it doesn't exist"""
        table_ref = self.bq_client.dataset(self.dataset_id).table(self.table_id)
        
        try:
            self.bq_client.get_table(table_ref)
            logger.info("Ingestion state table exists")
        except Exception:
            # Create table
            schema = [
                bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("cursor_iso", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("tie_breaker_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            table = self.bq_client.create_table(table)
            logger.info(f"Created ingestion state table: {table.table_id}")
    
    def get_state(self, source: str) -> Tuple[Optional[str], Optional[str]]:
        """Get the last cursor and tie-breaker ID for a source"""
        query = f"""
        SELECT cursor_iso, tie_breaker_id
        FROM `{PROJECT_ID}.{self.dataset_id}.{self.table_id}`
        WHERE source = @source
        ORDER BY updated_at DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source)
            ]
        )
        
        try:
            results = list(self.bq_client.query(query, job_config=job_config))
            if results:
                row = results[0]
                return row.cursor_iso, row.tie_breaker_id
            return None, None
        except Exception as e:
            logger.warning(f"Could not get state for {source}: {e}")
            return None, None
    
    def update_state(self, source: str, cursor_iso: str, tie_breaker_id: str):
        """Update the cursor state for a source"""
        query = f"""
        INSERT INTO `{PROJECT_ID}.{self.dataset_id}.{self.table_id}`
        (source, cursor_iso, tie_breaker_id, updated_at)
        VALUES (@source, @cursor_iso, @tie_breaker_id, CURRENT_TIMESTAMP())
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("cursor_iso", "STRING", cursor_iso),
                bigquery.ScalarQueryParameter("tie_breaker_id", "STRING", tie_breaker_id),
            ]
        )
        
        try:
            self.bq_client.query(query, job_config=job_config)
            logger.info(f"Updated state for {source}: cursor={cursor_iso}, tie_breaker={tie_breaker_id}")
        except Exception as e:
            logger.error(f"Could not update state for {source}: {e}")

class IdempotentRedditFetcher:
    """Idempotent Reddit fetcher with natural IDs and state tracking"""
    
    def __init__(self):
        self.reddit = self._initialize_reddit()
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.state_manager = IngestionState(self.bq_client)
        self.request_count = 0
        self.start_time = time.time()
        
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
    
    def _rate_limit(self):
        """Enforce rate limiting"""
        self.request_count += 1
        elapsed = time.time() - self.start_time
        expected_time = self.request_count * REQUEST_DELAY
        
        if elapsed < expected_time:
            sleep_time = expected_time - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
    
    def _generate_event_id(self, reddit_type: str, reddit_id: str) -> str:
        """Generate stable event ID for Reddit posts/comments"""
        if reddit_type == 'post':
            return f"reddit_t3_{reddit_id}"
        elif reddit_type == 'comment':
            return f"reddit_t1_{reddit_id}"
        else:
            return f"reddit_{reddit_type}_{reddit_id}"
    
    def _generate_content_hash(self, text: str) -> str:
        """Generate content hash for detecting edits"""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]
    
    def fetch_incremental_posts_and_comments(self, 
                                           subreddit_name: str, 
                                           brand_terms: List[str],
                                           since_timestamp: Optional[int] = None,
                                           initial_fetch: bool = False) -> List[Dict[str, Any]]:
        """Fetch posts and comments incrementally with pagination"""
        
        all_messages = []
        source_key = f"reddit_{subreddit_name}"
        
        # Get last state
        last_cursor, last_tie_breaker = self.state_manager.get_state(source_key)
        
        # Calculate overlap window (2 hours for clock skew tolerance)
        overlap_seconds = 2 * 3600
        if since_timestamp and not initial_fetch:
            since_timestamp = max(0, since_timestamp - overlap_seconds)
        elif last_cursor and not initial_fetch:
            last_dt = datetime.fromisoformat(last_cursor.replace('Z', '+00:00'))
            since_timestamp = int((last_dt.timestamp() - overlap_seconds))
        else:
            # Initial fetch: get last 7 days
            since_timestamp = int((datetime.utcnow() - timedelta(days=7)).timestamp())
        
        logger.info(f"Fetching {subreddit_name} since {datetime.fromtimestamp(since_timestamp)} (initial={initial_fetch})")
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            max_timestamp = since_timestamp
            max_tie_breaker = last_tie_breaker or ""
            
            # Search for each brand term
            for brand_id, terms in FINANCIAL_BRANDS.items():
                if not any(term in brand_terms for term in terms):
                    continue  # Skip if this brand's terms aren't in the search
                
                for term in terms:
                    if term not in brand_terms:
                        continue
                        
                    query = f'"{term}"'
                    
                    try:
                        self._rate_limit()
                        
                        # Search posts with pagination
                        limit = 500 if initial_fetch else 100
                        submissions = list(subreddit.search(query, sort='new', time_filter='all', limit=limit))
                        
                        for submission in submissions:
                            if submission.created_utc < since_timestamp:
                                continue
                                
                            # Process post
                            post_data = self._process_submission(submission, brand_id, subreddit_name)
                            if post_data:
                                all_messages.append(post_data)
                                
                                # Update max timestamp and tie-breaker
                                if submission.created_utc > max_timestamp or \
                                   (submission.created_utc == max_timestamp and submission.id > max_tie_breaker):
                                    max_timestamp = submission.created_utc
                                    max_tie_breaker = submission.id
                            
                            # Process comments with rate limiting
                            try:
                                self._rate_limit()
                                submission.comments.replace_more(limit=2)
                                
                                for comment in submission.comments.list()[:10]:  # Top 10 comments
                                    if comment.created_utc < since_timestamp:
                                        continue
                                        
                                    if any(t.lower() in comment.body.lower() for t in terms):
                                        comment_data = self._process_comment(comment, brand_id, subreddit_name)
                                        if comment_data:
                                            all_messages.append(comment_data)
                                            
                                            # Update max timestamp and tie-breaker
                                            if comment.created_utc > max_timestamp or \
                                               (comment.created_utc == max_timestamp and comment.id > max_tie_breaker):
                                                max_timestamp = comment.created_utc
                                                max_tie_breaker = comment.id
                                                
                            except Exception as e:
                                logger.warning(f"Error processing comments for {submission.id}: {e}")
                                continue
                        
                    except Exception as e:
                        logger.error(f"Error searching {subreddit_name} for '{term}': {e}")
                        continue
            
            # Update state if we processed any messages
            if max_timestamp > since_timestamp:
                max_cursor_iso = datetime.fromtimestamp(max_timestamp).isoformat() + 'Z'
                self.state_manager.update_state(source_key, max_cursor_iso, max_tie_breaker)
                
        except Exception as e:
            logger.error(f"Error processing subreddit {subreddit_name}: {e}")
        
        logger.info(f"Fetched {len(all_messages)} messages from {subreddit_name}")
        return all_messages
    
    def _process_submission(self, submission, brand_id: str, subreddit_name: str) -> Optional[Dict[str, Any]]:
        """Process Reddit submission into standardized format with natural ID"""
        try:
            event_id = self._generate_event_id('post', submission.id)
            text = f"{submission.title}\n\n{submission.selftext}".strip()
            
            return {
                'event_id': event_id,
                'ts_event': datetime.fromtimestamp(submission.created_utc).isoformat() + 'Z',
                'brand_id': brand_id,
                'source': 'reddit',
                'geo_country': None,  # Reddit doesn't provide geo data
                'text': text,
                'content_hash': self._generate_content_hash(text),
                'metadata': {
                    'reddit_id': submission.id,
                    'reddit_type': 'post',
                    'subreddit': subreddit_name,
                    'author': str(submission.author) if submission.author else '[deleted]',
                    'title': submission.title,
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'upvote_ratio': submission.upvote_ratio,
                    'url': submission.url,
                    'permalink': f"https://reddit.com{submission.permalink}",
                    'edited': bool(submission.edited) if hasattr(submission, 'edited') else False
                },
                '_ingested_at': datetime.utcnow().isoformat() + 'Z',
                '_source_run': f"reddit_fetcher_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            }
        except Exception as e:
            logger.error(f"Error processing submission {submission.id}: {e}")
            return None
    
    def _process_comment(self, comment, brand_id: str, subreddit_name: str) -> Optional[Dict[str, Any]]:
        """Process Reddit comment into standardized format with natural ID"""
        try:
            event_id = self._generate_event_id('comment', comment.id)
            text = comment.body.strip()
            
            return {
                'event_id': event_id,
                'ts_event': datetime.fromtimestamp(comment.created_utc).isoformat() + 'Z',
                'brand_id': brand_id,
                'source': 'reddit',
                'geo_country': None,
                'text': text,
                'content_hash': self._generate_content_hash(text),
                'metadata': {
                    'reddit_id': comment.id,
                    'reddit_type': 'comment',
                    'subreddit': subreddit_name,
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'score': comment.score,
                    'permalink': f"https://reddit.com{comment.permalink}",
                    'parent_id': comment.parent_id,
                    'edited': bool(comment.edited) if hasattr(comment, 'edited') else False
                },
                '_ingested_at': datetime.utcnow().isoformat() + 'Z',
                '_source_run': f"reddit_fetcher_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            }
        except Exception as e:
            logger.error(f"Error processing comment {comment.id}: {e}")
            return None
    
    def save_to_gcs_partitioned(self, messages: List[Dict[str, Any]], run_timestamp: str):
        """Save messages to GCS in partitioned format with gzip compression"""
        if not messages:
            logger.info("No messages to save")
            return
        
        # Group messages by date
        messages_by_date = {}
        for msg in messages:
            event_date = msg['ts_event'][:10]  # Extract YYYY-MM-DD
            if event_date not in messages_by_date:
                messages_by_date[event_date] = []
            messages_by_date[event_date].append(msg)
        
        bucket = self.storage_client.bucket(BUCKET_NAME)
        saved_files = []
        
        for date_str, date_messages in messages_by_date.items():
            # Create partitioned path with run timestamp and UUID for uniqueness
            run_id = f"{run_timestamp}-{uuid.uuid4().hex[:8]}"
            blob_path = f"raw/reddit/dt={date_str}/part-{run_id}.jsonl.gz"
            
            # Convert to NDJSON and compress
            ndjson_content = '\n'.join([json.dumps(msg, sort_keys=True) for msg in date_messages])
            compressed_content = gzip.compress(ndjson_content.encode('utf-8'))
            
            # Upload to GCS
            blob = bucket.blob(blob_path)
            blob.upload_from_string(compressed_content, content_type='application/gzip')
            
            saved_files.append(f"gs://{BUCKET_NAME}/{blob_path}")
            logger.info(f"Saved {len(date_messages)} messages to gs://{BUCKET_NAME}/{blob_path}")
        
        return saved_files

@functions_framework.http
def fetch_reddit_data_idempotent(request):
    """Cloud Function entry point for idempotent Reddit fetching"""
    try:
        # Parse request parameters
        request_json = request.get_json(silent=True) or {}
        
        # Parameters
        initial_fetch = request_json.get('initial_fetch', False)
        target_date = request_json.get('date')
        subreddits = request_json.get('subreddits', RELEVANT_SUBREDDITS)
        
        # For initial fetch or if no target date, process multiple days
        if initial_fetch or not target_date:
            since_timestamp = None  # Will use state or default to 7 days
        else:
            # Calculate since timestamp for specific date
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            since_timestamp = int(date_obj.timestamp())
        
        fetcher = IdempotentRedditFetcher()
        run_timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        
        # Collect all brand terms for efficient searching
        all_brand_terms = []
        for terms in FINANCIAL_BRANDS.values():
            all_brand_terms.extend(terms)
        
        all_messages = []
        processed_subreddits = 0
        
        # Process each subreddit
        for subreddit_name in subreddits:
            try:
                messages = fetcher.fetch_incremental_posts_and_comments(
                    subreddit_name=subreddit_name,
                    brand_terms=all_brand_terms,
                    since_timestamp=since_timestamp,
                    initial_fetch=initial_fetch
                )
                all_messages.extend(messages)
                processed_subreddits += 1
                
                # Log progress
                if processed_subreddits % 5 == 0:
                    logger.info(f"Processed {processed_subreddits}/{len(subreddits)} subreddits, {len(all_messages)} total messages")
                    
            except Exception as e:
                logger.error(f"Error processing subreddit {subreddit_name}: {e}")
                continue
        
        # Save to GCS
        saved_files = fetcher.save_to_gcs_partitioned(all_messages, run_timestamp)
        
        return {
            'status': 'success',
            'run_timestamp': run_timestamp,
            'total_messages': len(all_messages),
            'subreddits_processed': processed_subreddits,
            'files_saved': len(saved_files) if saved_files else 0,
            'initial_fetch': initial_fetch,
            'api_requests_made': fetcher.request_count
        }, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_reddit_data_idempotent: {e}")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    # For Cloud Run deployment
    import os
    from flask import Flask, request
    
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def handle_request():
        return fetch_reddit_data_idempotent(request)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        return {'status': 'healthy'}, 200
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
