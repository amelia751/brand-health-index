"""
Google Trends Data Fetcher
Fetches search interest data for financial brands using PyTrends
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import storage
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'brand-health-raw-data')

# Comprehensive financial product categories for Brand Health Index analysis
# Covers the full spectrum of financial services from basic banking to wealth management
# Consistent categories enable accurate brand comparison and market share analysis across all segments
FINANCIAL_CATEGORIES = [
    # Basic Banking Services
    'savings account',         # Basic banking - universal need
    'checking account',        # Basic banking - universal need
    'mobile banking',          # Digital experience - growing importance
    'online banking',          # Digital banking platform
    
    # Consumer Credit Products  
    'credit card',            # Consumer credit - high search volume
    'personal loan',          # Consumer credit - comparison shopping
    'auto loan',              # Vehicle financing
    'student loan',           # Education financing
    'home equity loan',       # Home-secured lending
    
    # Real Estate & Mortgages
    'mortgage rates',         # Major life decision - high consideration
    'home loan',              # Real estate financing
    'refinance',              # Mortgage refinancing
    'first time home buyer',  # Entry-level real estate
    
    # Business & Commercial
    'business loan',          # Small business financing
    'business banking',       # Commercial services
    'merchant services',      # Payment processing
    'business credit card',   # Commercial credit
    
    # Investment & Wealth Management
    'investment advisor',     # Professional investment guidance
    'wealth management',      # High net worth services
    'private banking',        # Premium banking services
    'retirement planning',    # Long-term financial planning
    'financial advisor',      # Personal finance guidance
    '401k rollover',          # Retirement account management
    
    # Specialized Services
    'trust services',         # Estate planning and trusts
    'foreign exchange',       # International banking
    'treasury services',      # Corporate cash management
    'insurance',              # Financial protection products
    'cd rates',               # Certificate of deposit
    'money market account'    # High-yield savings alternative
]

# Financial brands and search terms
# Using COMPREHENSIVE and CONSISTENT category terms across all brands for accurate comparison
# All brands now track the complete spectrum of financial services
FINANCIAL_BRANDS = {
    'jpm': {
        'brand_terms': ['JPMorgan Chase', 'Chase Bank'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    },
    'wf': {
        'brand_terms': ['Wells Fargo'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    },
    'bac': {
        'brand_terms': ['Bank of America'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    },
    'c': {
        'brand_terms': ['Citibank', 'Citi'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    },
    'gs': {
        'brand_terms': ['Goldman Sachs'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    },
    'ms': {
        'brand_terms': ['Morgan Stanley'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    },
    'td': {
        'brand_terms': ['TD Bank', 'Toronto-Dominion Bank'],
        'category_terms': FINANCIAL_CATEGORIES  # Use all comprehensive categories
    }
}

# Geographic regions to track
GEO_REGIONS = ['US', 'US-NY', 'US-CA', 'US-TX', 'US-FL']

class TrendsFetcher:
    def __init__(self):
        self.pytrends = TrendReq(hl='en-US', tz=360)
        self.storage_client = storage.Client()
    
    def fetch_trends_data(self, brand_id: str, terms_config: Dict[str, List[str]], 
                         timeframe: str = 'now 7-d') -> List[Dict[str, Any]]:
        """Fetch Google Trends data for brand and category terms"""
        all_trends = []
        
        # Fetch data for limited geographic regions to avoid rate limits
        limited_regions = ['US']  # Start with just US to avoid rate limiting
        for geo in limited_regions:
            try:
                # 1. Fetch pure brand terms (brand awareness)
                for term in terms_config['brand_terms']:
                    trends_data = self._fetch_single_term(brand_id, term, geo, timeframe, 'brand')
                    all_trends.extend(trends_data)
                
                # 2. Fetch category terms (market size) - limited for testing
                # Use only high-priority categories to avoid rate limits
                priority_categories = [
                    'savings account', 'checking account', 'credit card', 'mortgage rates'
                ]
                for term in priority_categories:
                    if term in terms_config['category_terms']:
                        trends_data = self._fetch_single_term(brand_id, term, geo, timeframe, 'category')
                        all_trends.extend(trends_data)
                
                # 3. Fetch brand + category combinations (brand consideration)
                # Use primary brand term and limit to high-value categories to avoid rate limits
                primary_brand = terms_config['brand_terms'][0]
                
                # High-priority categories for brand consideration analysis
                priority_categories = [
                    'savings account', 'checking account', 'credit card', 'mortgage rates',
                    'personal loan', 'business banking', 'investment advisor', 'wealth management'
                ]
                
                for category_term in priority_categories:
                    if category_term in FINANCIAL_CATEGORIES:  # Ensure it's in our full list
                        combo_term = f"{primary_brand} {category_term}"
                        trends_data = self._fetch_single_term(brand_id, combo_term, geo, timeframe, 'brand_category')
                        all_trends.extend(trends_data)
                        
                        # Also try reverse order for better coverage
                        reverse_combo = f"{category_term} {primary_brand}"
                        trends_data = self._fetch_single_term(brand_id, reverse_combo, geo, timeframe, 'category_brand')
                        all_trends.extend(trends_data)
                    
            except Exception as e:
                logger.error(f"Error fetching trends for {brand_id} in {geo}: {e}")
                continue
        
        logger.info(f"Fetched {len(all_trends)} trend data points for brand {brand_id}")
        return all_trends
    
    def _fetch_single_term(self, brand_id: str, keyword: str, geo: str, 
                          timeframe: str, keyword_type: str) -> List[Dict[str, Any]]:
        """Fetch trends data for a single term"""
        try:
            # Rate limiting to avoid hitting Google Trends limits
            time.sleep(2.0)  # 2 second delay between requests to avoid 429 errors
            
            # Build payload for pytrends
            self.pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo=geo)
            
            # Get interest over time
            interest_df = self.pytrends.interest_over_time()
            
            if interest_df.empty:
                logger.warning(f"No trends data for '{keyword}' in {geo}")
                return []
            
            trends_data = []
            for timestamp, row in interest_df.iterrows():
                trend_point = {
                    'brand_id': brand_id,
                    'keyword': keyword,
                    'keyword_type': keyword_type,  # 'brand', 'category', 'brand_category', 'category_brand'
                    'geo': geo,
                    'ts_event': timestamp.isoformat(),
                    'value': int(row[keyword]) if keyword in row else 0,
                    'category': None,  # Could be added if using category filters
                    'is_brand_keyword': keyword_type in ['brand', 'brand_category', 'category_brand'],
                    'collected_at': datetime.utcnow().isoformat()
                }
                trends_data.append(trend_point)
            
            # Add related queries if available
            try:
                related_queries = self.pytrends.related_queries()
                if keyword in related_queries and related_queries[keyword]['top'] is not None:
                    # Store related queries as metadata (could be used for insights)
                    for _, trend_point in enumerate(trends_data):
                        trend_point['related_queries_top'] = related_queries[keyword]['top']['query'].tolist()[:5]
            except:
                pass  # Related queries are optional
            
            return trends_data
            
        except Exception as e:
            logger.error(f"Error fetching trend for '{keyword}' in {geo}: {e}")
            return []
    
    def get_trending_searches(self, geo: str = 'US') -> List[Dict[str, Any]]:
        """Get trending searches (for context/market insights)"""
        try:
            trending_df = self.pytrends.trending_searches(pn=geo.lower())
            
            trending_data = []
            for _, row in trending_df.head(20).iterrows():  # Top 20 trending
                trending_data.append({
                    'geo': geo,
                    'query': row[0],
                    'rank': _ + 1,
                    'ts_event': datetime.utcnow().isoformat(),
                    'collected_at': datetime.utcnow().isoformat()
                })
            
            return trending_data
            
        except Exception as e:
            logger.error(f"Error fetching trending searches for {geo}: {e}")
            return []
    
    def save_to_gcs(self, trends_data: List[Dict[str, Any]], brand_id: str, date_str: str):
        """Save trends data to GCS in NDJSON format for Fivetran"""
        if not trends_data:
            logger.info(f"No trends data to save for brand {brand_id}")
            return
            
        bucket = self.storage_client.bucket(BUCKET_NAME)
        
        # Create path: raw/trends/date=YYYY-MM-DD/brand_id.ndjson
        blob_path = f"raw/trends/date={date_str}/{brand_id}.ndjson"
        blob = bucket.blob(blob_path)
        
        # Convert to newline-delimited JSON
        ndjson_content = '\n'.join([json.dumps(trend) for trend in trends_data])
        
        blob.upload_from_string(ndjson_content, content_type='application/x-ndjson')
        logger.info(f"Saved {len(trends_data)} trend data points to gs://{BUCKET_NAME}/{blob_path}")
    
    def save_trending_searches(self, trending_data: List[Dict[str, Any]], date_str: str):
        """Save trending searches data to GCS"""
        if not trending_data:
            return
            
        bucket = self.storage_client.bucket(BUCKET_NAME)
        blob_path = f"raw/trends/date={date_str}/trending_searches.ndjson"
        blob = bucket.blob(blob_path)
        
        ndjson_content = '\n'.join([json.dumps(trend) for trend in trending_data])
        blob.upload_from_string(ndjson_content, content_type='application/x-ndjson')
        logger.info(f"Saved {len(trending_data)} trending searches to gs://{BUCKET_NAME}/{blob_path}")

@functions_framework.http
def fetch_trends_data(request):
    """Cloud Function entry point"""
    try:
        # Parse request parameters
        request_json = request.get_json(silent=True) or {}
        
        # Default to yesterday's data
        target_date = request_json.get('date')
        if not target_date:
            yesterday = datetime.utcnow() - timedelta(days=1)
            target_date = yesterday.strftime('%Y-%m-%d')
        
        # Determine timeframe (default to last 7 days for daily runs)
        timeframe = request_json.get('timeframe', 'now 7-d')
        
        fetcher = TrendsFetcher()
        
        # Fetch data for each brand
        total_trends = 0
        for brand_id, terms_config in FINANCIAL_BRANDS.items():
            trends_data = fetcher.fetch_trends_data(brand_id, terms_config, timeframe)
            fetcher.save_to_gcs(trends_data, brand_id, target_date)
            total_trends += len(trends_data)
        
        # Fetch trending searches for market context
        trending_searches = []
        for geo in ['US']:  # Focus on US for now
            trending = fetcher.get_trending_searches(geo)
            trending_searches.extend(trending)
        
        if trending_searches:
            fetcher.save_trending_searches(trending_searches, target_date)
        
        return {
            'status': 'success',
            'date': target_date,
            'timeframe': timeframe,
            'total_trends': total_trends,
            'trending_searches': len(trending_searches),
            'brands_processed': len(FINANCIAL_BRANDS)
        }, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_trends_data: {e}")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    # For Cloud Run deployment
    import os
    from flask import Flask, request
    
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def handle_request():
        return fetch_trends_data(request)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        return {'status': 'healthy'}, 200
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
