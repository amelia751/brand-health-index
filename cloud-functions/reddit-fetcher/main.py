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
import requests

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

# Financial institutions to track - ULTRA COMPREHENSIVE TD Bank keywords
FINANCIAL_BRANDS = {
    'td_bank': [
        # === OFFICIAL CORPORATE NAMES ===
        'TD Bank', 'TD Bank N.A.', 'TD Bank USA', 'TD Bank US', 'TD Bank National Association',
        'Toronto Dominion Bank', 'Toronto-Dominion Bank', 'Toronto Dominion', 'Toronto-Dominion',
        'The Toronto-Dominion Bank', 'TD Bank Group',
        
        # === ABBREVIATIONS & VARIATIONS ===
        'TD', 'TDB', 'TD-Bank', 'T-D', 'T.D.', 'T D Bank', 'T.D. Bank',
        'TD Banknorth', 'TD Waterhouse', 'TD Securities',
        
        # === MAJOR SUBSIDIARIES & DIVISIONS ===
        'TD Canada Trust', 'TD Ameritrade', 'TD Auto Finance', 'TD Wealth Management',
        'TD Direct Investing', 'TD Insurance', 'TD Commercial Banking', 'TD Private Banking',
        'TD Asset Management', 'TD Securities', 'TD Investment Services',
        
        # === SPECIFIC CREDIT CARD PRODUCTS ===
        'TD Cash Credit Card', 'TD Cash Back Credit Card', 'TD Double Up Credit Card',
        'TD First Class Travel Visa', 'TD Aeroplan Visa', 'TD Business Cash Back Visa',
        'TD Business Travel Visa', 'TD Student Credit Card', 'TD Secured Credit Card',
        'TD Rewards Credit Card', 'TD Platinum Travel Visa', 'TD Gold Elite Visa',
        
        # === CHECKING ACCOUNT PRODUCTS ===
        'TD Convenience Checking', 'TD Beyond Checking', 'TD Premier Checking',
        'TD Student Checking', 'TD 60+ Checking', 'TD Business Checking',
        'TD Simple Business Checking', 'TD Commercial Checking',
        
        # === SAVINGS & INVESTMENT PRODUCTS ===
        'TD Simple Savings', 'TD Growth Money Market', 'TD Special Rate CD',
        'TD Choice Promotional CD', 'TD Signature Select', 'TD IRA',
        'TD Traditional IRA', 'TD Roth IRA', 'TD SEP IRA', 'TD Simple IRA',
        
        # === MORTGAGE & LOAN PRODUCTS ===
        'TD Mortgage', 'TD Home Equity', 'TD Personal Loan', 'TD Student Loan',
        'TD Auto Loan', 'TD Business Loan', 'TD Line of Credit', 'TD HELOC',
        'TD Fixed Rate Mortgage', 'TD Adjustable Rate Mortgage', 'TD Jumbo Mortgage',
        
        # === DIGITAL SERVICES ===
        'TD EasyWeb', 'TD Mobile App', 'TD Online Banking', 'TD Digital Banking',
        'TD Mobile Deposit', 'TD Zelle', 'TD Bill Pay', 'TD MySpend',
        'TD GoalAssist', 'TD Clari', 'TD Mobile Banking',
        
        # === CUSTOMER SERVICE & LOCATIONS ===
        'TD Customer Service', 'TD Branch', 'TD ATM', 'TD Teller', 'TD Call Center',
        'TD Customer Care', 'TD Support', 'TD Help Desk', 'TD Phone Banking',
        
        # === CONTEXTUAL PATTERNS (case variations) ===
        'TD checking', 'TD savings', 'TD credit', 'TD debit', 'TD card',
        'TD account', 'TD online', 'TD app', 'TD mortgage', 'TD loan',
        'td bank', 'td', 'td checking', 'td savings', 'td credit card',
        'td online banking', 'td mobile app', 'td customer service',
        
        # === COMMON MISSPELLINGS & SLANG ===
        'TD Bankz', 'T D Bank', 'TD Can Trust', 'TD Ameritrd', 'TD Amertrade',
        'Toronto Dom', 'Toronto-Dom', 'TD Banc', 'TDBank', 'TD_Bank',
        
        # === REGIONAL & COLLOQUIAL REFERENCES ===
        'TD Green', 'TD Canada', 'TD US', 'TD USA', 'TD American',
        'TD North', 'TD East Coast', 'TD New England', 'TD Northeast',
        
        # === BUSINESS & COMMERCIAL SERVICES ===
        'TD Business Banking', 'TD Commercial', 'TD Corporate Banking',
        'TD Treasury Services', 'TD Merchant Services', 'TD Business Credit Card',
        'TD Commercial Real Estate', 'TD Equipment Finance',
        
        # === INVESTMENT & WEALTH SERVICES ===
        'TD Wealth', 'TD Investment', 'TD Portfolio', 'TD Financial Planning',
        'TD Private Investment Counsel', 'TD Asset Management USA',
        'TD Epoch', 'TD Greystone', 'TDAM',
        
        # === HISTORICAL & LEGACY NAMES ===
        'Commerce Bank', 'TD Commerce', 'Banknorth', 'TD Banknorth Group'
    ]
}

# TD Bank focused subreddits - comprehensive coverage
RELEVANT_SUBREDDITS = [
    # Core Personal Finance (high TD Bank activity)
    'personalfinance', 'banking', 'povertyfinance', 'frugal',
    
    # TD-Specific Subreddits
    'TDBank', 'TD_Bank', 'TDBankCanada',
    
    # Canadian Finance (TD's major market)
    'PersonalFinanceCanada', 'PersonalFinanceForCanadians', 'CanadianInvestor',
    'ontario', 'toronto', 'canada', 'CanadaPublicServants',
    
    # US Finance (TD's US operations)
    'financialindependence', 'CRedit', 'StudentLoans',
    
    # Product-specific (TD services)
    'creditcards', 'mortgages', 'investing', 'realestate',
    
    # Regional (TD Bank locations)
    'boston', 'newyork', 'philadelphia', 'newjersey', 'connecticut',
    'maine', 'newhampshire', 'vermont', 'massachusetts', 'rhodeisland',
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
    
    def detect_brand_mentions(self, text: str, debug: bool = False) -> Dict[str, Any]:
        """
        Enhanced brand detection with case-insensitive matching and diagnostics
        Returns: {
            'brand_id': str or None,
            'matched_terms': List[str],
            'match_positions': List[Dict],
            'confidence_score': float,
            'debug_info': Dict (if debug=True)
        }
        """
        if not text or len(text.strip()) < 2:
            return {'brand_id': None, 'matched_terms': [], 'match_positions': [], 'confidence_score': 0.0}
        
        text_lower = text.lower()
        results = {
            'brand_id': None,
            'matched_terms': [],
            'match_positions': [],
            'confidence_score': 0.0
        }
        
        if debug:
            results['debug_info'] = {
                'text_length': len(text),
                'search_text': text_lower[:200] + '...' if len(text_lower) > 200 else text_lower,
                'brands_checked': [],
                'all_matches_found': []
            }
        
        # Check each brand
        for brand_id, terms in FINANCIAL_BRANDS.items():
            brand_matches = []
            brand_positions = []
            
            if debug:
                results['debug_info']['brands_checked'].append(brand_id)
            
            # Check each term for this brand (case-insensitive)
            for term in terms:
                term_lower = term.lower()
                
                # Find all occurrences of this term
                start_pos = 0
                while True:
                    pos = text_lower.find(term_lower, start_pos)
                    if pos == -1:
                        break
                    
                    # Check word boundaries for better matching
                    is_word_boundary = True
                    if pos > 0 and text_lower[pos-1].isalnum():
                        is_word_boundary = False
                    if pos + len(term_lower) < len(text_lower) and text_lower[pos + len(term_lower)].isalnum():
                        is_word_boundary = False
                    
                    # For short terms like "TD", be more strict about word boundaries
                    if len(term_lower) <= 3 and not is_word_boundary:
                        start_pos = pos + 1
                        continue
                    
                    # Record the match
                    match_info = {
                        'term': term,
                        'position': pos,
                        'length': len(term),
                        'context': text[max(0, pos-20):pos+len(term)+20],
                        'word_boundary': is_word_boundary,
                        'confidence': 1.0 if is_word_boundary else 0.7
                    }
                    
                    brand_matches.append(term)
                    brand_positions.append(match_info)
                    
                    if debug:
                        results['debug_info']['all_matches_found'].append({
                            'brand_id': brand_id,
                            'term': term,
                            'position': pos,
                            'context': match_info['context']
                        })
                    
                    start_pos = pos + 1
            
            # If this brand has matches, calculate confidence
            if brand_matches:
                # Calculate confidence based on:
                # 1. Number of unique terms matched
                # 2. Quality of matches (word boundaries)
                # 3. Length/specificity of matched terms
                
                unique_matches = list(set(brand_matches))
                avg_confidence = sum(pos['confidence'] for pos in brand_positions) / len(brand_positions)
                term_specificity = sum(len(term) for term in unique_matches) / len(unique_matches)
                
                confidence = (
                    0.4 * min(1.0, len(unique_matches) / 3) +  # Diversity bonus
                    0.4 * avg_confidence +  # Match quality
                    0.2 * min(1.0, term_specificity / 10)  # Term specificity
                )
                
                # If this is the best match so far, update results
                if confidence > results['confidence_score']:
                    results['brand_id'] = brand_id
                    results['matched_terms'] = unique_matches
                    results['match_positions'] = brand_positions
                    results['confidence_score'] = confidence
        
        return results
    
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
                                
                            # Use enhanced brand detection for posts
                            post_text = f"{submission.title}\n\n{submission.selftext}".strip()
                            brand_detection = self.detect_brand_mentions(post_text)
                            
                            if brand_detection['brand_id'] == brand_id and brand_detection['confidence_score'] > 0.3:
                                post_data = self._process_submission(submission, brand_id, subreddit_name)
                                if post_data:
                                    # Add brand detection metadata
                                    post_data['metadata']['brand_detection'] = {
                                        'matched_terms': brand_detection['matched_terms'],
                                        'confidence_score': brand_detection['confidence_score'],
                                        'match_count': len(brand_detection['match_positions'])
                                    }
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
                                    
                                    # Use enhanced brand detection for comments
                                    brand_detection = self.detect_brand_mentions(comment.body)
                                    if brand_detection['brand_id'] == brand_id and brand_detection['confidence_score'] > 0.3:
                                        comment_data = self._process_comment(comment, brand_id, subreddit_name)
                                        if comment_data:
                                            # Add brand detection metadata
                                            comment_data['metadata']['brand_detection'] = {
                                                'matched_terms': brand_detection['matched_terms'],
                                                'confidence_score': brand_detection['confidence_score'],
                                                'match_count': len(brand_detection['match_positions'])
                                            }
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
    
    def _enrich_with_nlp(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich messages with NLP analysis using Vertex AI"""
        if not messages:
            return messages
        
        try:
            # Import NLP enrichment function
            from main_nlp import enrich_reddit_records
            
            # Process messages with NLP
            enriched_messages = enrich_reddit_records(messages)
            logger.info(f"Successfully enriched {len(messages)} messages with NLP analysis")
            return enriched_messages
            
        except Exception as e:
            logger.error(f"NLP enrichment failed: {e}")
            # Return original messages with default NLP values
            for msg in messages:
                msg.update({
                    'sentiment': 0.0,
                    'severity': 0.0,
                    'topics': [],
                    'language': 'en',
                    'nlp_confidence': 0.0,
                    'nlp_model': 'none',
                    'nlp_version': 'v1.0',
                    'nlp_processed_at': datetime.utcnow().isoformat() + 'Z',
                    'nlp_error': str(e)
                })
            return messages
    
    def save_to_gcs_partitioned(self, messages: List[Dict[str, Any]], run_timestamp: str):
        """Save messages to GCS in partitioned format with gzip compression"""
        if not messages:
            logger.info("No messages to save")
            return
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
        
        # Collect all messages
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
        
        # Enrich all messages with NLP analysis
        logger.info(f"Starting NLP enrichment for {len(all_messages)} messages")
        enriched_messages = fetcher._enrich_with_nlp(all_messages)
        
        # Save enriched data to GCS
        saved_files = fetcher.save_to_gcs_partitioned(enriched_messages, run_timestamp)
        
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
