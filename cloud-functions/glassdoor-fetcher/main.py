"""
Glassdoor Company Data Fetcher using RapidAPI
Fetches company reviews, ratings, and employee sentiment from Glassdoor via official API
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from google.cloud import storage, secretmanager
import functions_framework
from flask import Flask, request

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'brand-health-index')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'brand-health-raw-data-dev-20241201')
SECRET_NAME = 'glassdoor-rapidapi-credentials'

# Financial institutions and their search terms for Glassdoor
FINANCIAL_COMPANIES = {
    'jpm': {
        'company_names': ['JPMorgan Chase', 'Chase Bank', 'JPMorgan'],
        'search_terms': ['jpmorgan chase', 'chase bank', 'jpmorgan']
    },
    'wf': {
        'company_names': ['Wells Fargo', 'Wells Fargo Bank'],
        'search_terms': ['wells fargo', 'wells fargo bank']
    },
    'bac': {
        'company_names': ['Bank of America', 'Bank of America Corporation'],
        'search_terms': ['bank of america', 'bofa']
    },
    'c': {
        'company_names': ['Citibank', 'Citi', 'Citigroup'],
        'search_terms': ['citibank', 'citi', 'citigroup']
    },
    'gs': {
        'company_names': ['Goldman Sachs', 'The Goldman Sachs Group'],
        'search_terms': ['goldman sachs', 'goldman']
    },
    'ms': {
        'company_names': ['Morgan Stanley', 'Morgan Stanley & Co'],
        'search_terms': ['morgan stanley']
    },
    'td': {
        'company_names': ['TD Bank', 'Toronto-Dominion Bank', 'TD Bank US'],
        'search_terms': ['td bank', 'toronto dominion', 'td bank us']
    }
}

class GlassdoorAPIFetcher:
    def __init__(self):
        self.storage_client = storage.Client()
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.session = requests.Session()
        
        # Load API credentials from Secret Manager
        self._load_credentials()
        
        # Setup session headers
        self._setup_session()
        
    def _load_credentials(self):
        """Load RapidAPI credentials from Google Secret Manager"""
        try:
            secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
            response = self.secret_client.access_secret_version(request={"name": secret_path})
            credentials_data = response.payload.data.decode("UTF-8")
            
            # Parse credentials
            credentials = {}
            for line in credentials_data.strip().split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    credentials[key] = value
            
            self.rapidapi_key = credentials.get('RAPIDAPI_KEY')
            self.rapidapi_host = credentials.get('RAPIDAPI_HOST')
            
            if not self.rapidapi_key or not self.rapidapi_host:
                raise ValueError("Missing RapidAPI credentials")
                
            logger.info("✅ Successfully loaded RapidAPI credentials from Secret Manager")
            
        except Exception as e:
            logger.error(f"❌ Failed to load credentials: {e}")
            raise
    
    def _setup_session(self):
        """Setup session with RapidAPI headers"""
        self.session.headers.update({
            'X-RapidAPI-Key': self.rapidapi_key,
            'X-RapidAPI-Host': self.rapidapi_host,
            'Accept': 'application/json',
            'User-Agent': 'Brand-Health-Index/1.0'
        })
        
        logger.info("🔧 Session configured for RapidAPI")
    
    def _save_to_gcs(self, data: List[Dict[str, Any]], brand_id: str, data_type: str):
        """Save fetched data to Google Cloud Storage"""
        if not data:
            logger.info(f"No {data_type} data to save for {brand_id}")
            return
            
        today_str = datetime.utcnow().strftime('%Y-%m-%d')
        timestamp_str = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        bucket = self.storage_client.bucket(BUCKET_NAME)
        blob_name = f"raw/glassdoor/date={today_str}/{brand_id}_{data_type}_{timestamp_str}.ndjson"
        blob = bucket.blob(blob_name)
        
        # Write as NDJSON (newline-delimited JSON)
        ndjson_data = "\n".join([json.dumps(item) for item in data])
        blob.upload_from_string(ndjson_data, content_type='application/x-ndjson')
        
        logger.info(f"💾 Saved {len(data)} {data_type} records for {brand_id} to {blob_name}")
    
    def search_companies(self, brand_id: str, search_terms: List[str]) -> List[Dict[str, Any]]:
        """Search for companies using RapidAPI"""
        all_companies = []
        
        for search_term in search_terms:
            try:
                logger.info(f"🔍 Searching for companies: '{search_term}'")
                
                params = {
                    'query': search_term,
                    'limit': 10
                }
                
                response = self.session.get(
                    f"https://{self.rapidapi_host}/company-search",
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == 'OK' and 'data' in data:
                        companies = data['data']
                        logger.info(f"✅ Found {len(companies)} companies for '{search_term}'")
                        
                        # Add metadata to each company
                        for company in companies:
                            company.update({
                                'brand_id': brand_id,
                                'search_term': search_term,
                                'fetched_at': datetime.utcnow().isoformat(),
                                'data_source': 'rapidapi_glassdoor'
                            })
                            
                        all_companies.extend(companies)
                    else:
                        logger.warning(f"⚠️ No companies found for '{search_term}': {data}")
                        
                elif response.status_code == 429:
                    logger.warning("⚠️ Rate limit hit - waiting...")
                    time.sleep(60)  # Wait 1 minute for rate limit reset
                    continue
                    
                else:
                    logger.error(f"❌ API error {response.status_code}: {response.text}")
                    
                # Rate limiting - be respectful to the API
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error searching for '{search_term}': {e}")
                continue
                
        return all_companies
    
    def get_company_overview(self, company_id: str, brand_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed company overview"""
        try:
            logger.info(f"📊 Getting overview for company ID: {company_id}")
            
            params = {'company_id': company_id}
            
            response = self.session.get(
                f"https://{self.rapidapi_host}/company-overview",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'OK' and 'data' in data:
                    overview = data['data']
                    
                    # Add metadata
                    overview.update({
                        'brand_id': brand_id,
                        'company_id': company_id,
                        'fetched_at': datetime.utcnow().isoformat(),
                        'data_source': 'rapidapi_glassdoor',
                        'data_type': 'company_overview'
                    })
                    
                    logger.info(f"✅ Retrieved overview for company {company_id}")
                    return overview
                else:
                    logger.warning(f"⚠️ No overview data: {data}")
                    
            elif response.status_code == 429:
                logger.warning("⚠️ Rate limit hit for overview")
                time.sleep(60)
                return None
                
            else:
                logger.error(f"❌ Overview API error {response.status_code}: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ Error getting overview for {company_id}: {e}")
            
        return None
    
    def get_company_reviews(self, company_id: str, brand_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get company reviews with Brand Health Index focus - comprehensive employee review data"""
        all_reviews = []
        page = 1
        max_pages = 5  # Increased to get more review data
        
        while page <= max_pages:
            try:
                logger.info(f"📝 Getting BHI reviews for company {company_id}, page {page}")
                
                params = {
                    'company_id': company_id,
                    'page': page,
                    'limit': min(limit, 50),  # API limit per page
                    'sort': 'date',  # Get most recent reviews first
                    'employment_status': 'ANY',  # Include current and former employees
                    'language': 'en'
                }
                
                response = self.session.get(
                    f"https://{self.rapidapi_host}/company-reviews",
                    params=params,
                    timeout=45
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == 'OK' and 'data' in data:
                        raw_reviews = data['data']
                        
                        if not raw_reviews:
                            logger.info(f"📭 No more reviews on page {page}")
                            break
                        
                        # Transform reviews for Brand Health Index schema
                        bhi_reviews = []
                        for review in raw_reviews:
                            bhi_review = self._transform_review_for_bhi(review, brand_id, company_id)
                            if bhi_review:
                                bhi_reviews.append(bhi_review)
                        
                        all_reviews.extend(bhi_reviews)
                        logger.info(f"✅ Processed {len(bhi_reviews)} BHI reviews from page {page}")
                        
                        # Check if we should continue to next page
                        if len(raw_reviews) < params['limit']:
                            logger.info("📄 Reached last page of reviews")
                            break
                            
                    else:
                        logger.warning(f"⚠️ No review data on page {page}: {data}")
                        break
                        
                elif response.status_code == 429:
                    logger.warning("⚠️ Rate limit hit for reviews - waiting 2 minutes...")
                    time.sleep(120)  # Wait 2 minutes for rate limit reset
                    continue
                    
                else:
                    logger.error(f"❌ Reviews API error {response.status_code}: {response.text}")
                    break
                    
                # Rate limiting between pages - be more conservative
                time.sleep(5)
                page += 1
                
            except Exception as e:
                logger.error(f"❌ Error getting reviews page {page} for {company_id}: {e}")
                break
                
        logger.info(f"🎯 Total BHI reviews collected for company {company_id}: {len(all_reviews)}")
        return all_reviews
    
    def _transform_review_for_bhi(self, raw_review: Dict[str, Any], brand_id: str, company_id: str) -> Optional[Dict[str, Any]]:
        """Transform raw Glassdoor review to Brand Health Index schema"""
        try:
            # Extract review date
            review_date = raw_review.get('date') or raw_review.get('review_date') or raw_review.get('created_at')
            if review_date:
                try:
                    # Handle different date formats
                    if isinstance(review_date, str):
                        from datetime import datetime
                        # Try common date formats
                        for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%m/%d/%Y']:
                            try:
                                ts_event = datetime.strptime(review_date, fmt).isoformat()
                                break
                            except ValueError:
                                continue
                        else:
                            ts_event = review_date  # Use as-is if can't parse
                    else:
                        ts_event = review_date
                except:
                    ts_event = datetime.utcnow().isoformat()  # Fallback to now
            else:
                ts_event = datetime.utcnow().isoformat()
            
            # Build Brand Health Index review record
            bhi_review = {
                # Core identifiers
                'brand_id': brand_id,
                'review_id': raw_review.get('id') or raw_review.get('review_id') or f"{company_id}_{hash(str(raw_review))}",
                'ts_event': ts_event,
                
                # Key BHI ratings (1-5 scale)
                'overall_rating': self._safe_float(raw_review.get('rating') or raw_review.get('overall_rating')),
                'work_life_balance': self._safe_float(raw_review.get('work_life_balance_rating')),
                'comp_benefits': self._safe_float(raw_review.get('compensation_benefits_rating') or raw_review.get('compensation_and_benefits_rating')),
                'culture_values': self._safe_float(raw_review.get('culture_values_rating') or raw_review.get('culture_and_values_rating')),
                'career_opportunities': self._safe_float(raw_review.get('career_opportunities_rating')),
                'senior_management': self._safe_float(raw_review.get('senior_management_rating')),
                
                # Approval ratings (0-1 scale)
                'ceo_approval': self._safe_approval(raw_review.get('ceo_approval') or raw_review.get('approve_of_ceo')),
                'recommend_to_friend': self._safe_approval(raw_review.get('recommend_to_friend') or raw_review.get('recommend')),
                
                # Employee context
                'employment_status': self._extract_employment_status(raw_review),
                'location': raw_review.get('location') or raw_review.get('employee_location'),
                'job_title': raw_review.get('job_title') or raw_review.get('position'),
                'employment_length': raw_review.get('employment_length') or raw_review.get('length_of_employment'),
                
                # Review content for NLP
                'review_title': raw_review.get('title') or raw_review.get('headline'),
                'pros': raw_review.get('pros'),
                'cons': raw_review.get('cons'),
                'advice_to_management': raw_review.get('advice_to_management') or raw_review.get('advice_to_mgmt'),
                'review_text': self._combine_review_text(raw_review),
                
                # Metadata
                'company_id': company_id,
                'fetched_at': datetime.utcnow().isoformat(),
                'data_source': 'rapidapi_glassdoor',
                'data_type': 'employee_review'
            }
            
            return bhi_review
            
        except Exception as e:
            logger.warning(f"⚠️ Error transforming review: {e}")
            return None
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_approval(self, value) -> Optional[float]:
        """Convert approval rating to 0-1 scale"""
        if value is None:
            return None
        try:
            val = float(value)
            # If it's already 0-1, return as-is
            if 0 <= val <= 1:
                return val
            # If it's percentage (0-100), convert to 0-1
            elif 0 <= val <= 100:
                return val / 100.0
            # If it's 1-5 scale, convert to 0-1
            elif 1 <= val <= 5:
                return (val - 1) / 4.0
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    def _extract_employment_status(self, review: Dict[str, Any]) -> Optional[str]:
        """Extract employment status in standardized format"""
        status = review.get('employment_status') or review.get('status') or review.get('employee_status')
        if status:
            status_lower = str(status).lower()
            if 'current' in status_lower:
                return 'current'
            elif 'former' in status_lower:
                return 'former'
            elif 'intern' in status_lower:
                return 'intern'
        return status
    
    def _combine_review_text(self, review: Dict[str, Any]) -> str:
        """Combine all review text for NLP analysis"""
        text_parts = []
        
        title = review.get('title') or review.get('headline')
        if title:
            text_parts.append(f"Title: {title}")
            
        pros = review.get('pros')
        if pros:
            text_parts.append(f"Pros: {pros}")
            
        cons = review.get('cons')
        if cons:
            text_parts.append(f"Cons: {cons}")
            
        advice = review.get('advice_to_management') or review.get('advice_to_mgmt')
        if advice:
            text_parts.append(f"Advice: {advice}")
        
        return " | ".join(text_parts)
    
    def fetch_company_data(self, brand_id: str, company_config: Dict[str, List[str]]) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch comprehensive company data for a brand"""
        logger.info(f"🏦 Starting data collection for {brand_id}")
        
        results = {
            'companies': [],
            'overviews': [],
            'reviews': []
        }
        
        # Step 1: Search for companies
        companies = self.search_companies(brand_id, company_config['search_terms'])
        results['companies'] = companies
        
        if not companies:
            logger.warning(f"⚠️ No companies found for {brand_id}")
            return results
            
        # Step 2: Get detailed data for top companies
        for company in companies[:3]:  # Limit to top 3 matches to avoid excessive API calls
            company_id = company.get('company_id') or company.get('id')
            
            if not company_id:
                logger.warning(f"⚠️ No company ID found for: {company}")
                continue
                
            # Get company overview
            overview = self.get_company_overview(str(company_id), brand_id)
            if overview:
                results['overviews'].append(overview)
                
            # Get company reviews
            reviews = self.get_company_reviews(str(company_id), brand_id)
            results['reviews'].extend(reviews)
            
            # Rate limiting between companies
            time.sleep(5)
            
        logger.info(f"🎉 Completed data collection for {brand_id}: "
                   f"{len(results['companies'])} companies, "
                   f"{len(results['overviews'])} overviews, "
                   f"{len(results['reviews'])} reviews")
        
        return results

@functions_framework.http
def fetch_glassdoor_data(request):
    """Cloud Function entry point for Glassdoor API data fetching"""
    try:
        fetcher = GlassdoorAPIFetcher()
        
        # Parse request parameters
        request_json = request.get_json(silent=True) or {}
        
        # Handle Pub/Sub message format
        if 'message' in request_json:
            import base64
            message_data = request_json['message'].get('data', '')
            if message_data:
                try:
                    decoded_data = base64.b64decode(message_data).decode('utf-8')
                    request_json = json.loads(decoded_data)
                    logger.info(f"📨 Decoded Pub/Sub message: {request_json}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error decoding Pub/Sub message: {e}")
                    return {'status': 'error', 'message': f'Invalid Pub/Sub message: {e}'}, 400
            else:
                logger.warning("⚠️ Empty Pub/Sub message data")
        
        # Get brands to fetch (default to all)
        brands_to_fetch = request_json.get('brands', list(FINANCIAL_COMPANIES.keys()))
        
        logger.info(f"🚀 Starting Glassdoor API data collection for brands: {brands_to_fetch}")
        
        all_results = {}
        
        for brand_id in brands_to_fetch:
            if brand_id not in FINANCIAL_COMPANIES:
                logger.warning(f"⚠️ Unknown brand: {brand_id}")
                continue
                
            try:
                company_config = FINANCIAL_COMPANIES[brand_id]
                results = fetcher.fetch_company_data(brand_id, company_config)
                
                # Save each data type to GCS with BHI naming
                if results['companies']:
                    fetcher._save_to_gcs(results['companies'], brand_id, 'company_search')
                    
                if results['overviews']:
                    fetcher._save_to_gcs(results['overviews'], brand_id, 'company_overview')
                    
                if results['reviews']:
                    # Save employee reviews for Brand Health Index
                    fetcher._save_to_gcs(results['reviews'], brand_id, 'reviews')
                    logger.info(f"💎 Saved {len(results['reviews'])} BHI employee reviews for {brand_id}")
                
                all_results[brand_id] = {
                    'companies_count': len(results['companies']),
                    'overviews_count': len(results['overviews']),
                    'reviews_count': len(results['reviews'])
                }
                
            except Exception as e:
                logger.error(f"❌ Error processing {brand_id}: {e}")
                all_results[brand_id] = {'error': str(e)}
                continue
        
        logger.info(f"🎉 Glassdoor API data collection completed: {all_results}")
        
        return {
            'status': 'success',
            'message': 'Glassdoor API data collection completed',
            'results': all_results,
            'timestamp': datetime.utcnow().isoformat()
        }, 200
        
    except Exception as e:
        logger.error(f"❌ Fatal error in Glassdoor fetcher: {e}")
        return {
            'status': 'error',
            'message': f'Glassdoor fetcher failed: {str(e)}',
            'timestamp': datetime.utcnow().isoformat()
        }, 500

# Flask app for Cloud Run
app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def handle_request():
    return fetch_glassdoor_data(request)

@app.route('/health', methods=['GET'])
def health_check():
    return {'status': 'healthy', 'service': 'glassdoor-rapidapi-fetcher'}, 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
