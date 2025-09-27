"""
CFPB Consumer Complaints Data Fetcher
Fetches consumer complaint data from CFPB's Socrata API
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from google.cloud import storage
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'brand-health-raw-data')

# CFPB API configuration
CFPB_API_BASE = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

# Financial institutions mapping (company names as they appear in CFPB data) 
FINANCIAL_COMPANIES = {
    'jpm': [
        'JPMORGAN CHASE & CO.',
        'JPMORGAN CHASE BANK, N.A.',
        'CHASE BANK USA, N.A.'
    ],
    'wf': [
        'WELLS FARGO & COMPANY',
        'WELLS FARGO BANK, N.A.',
        'WELLS FARGO FINANCIAL NATIONAL BANK'
    ],
    'bac': [
        'BANK OF AMERICA, NATIONAL ASSOCIATION',
        'BANK OF AMERICA CORPORATION',
        'FIA CARD SERVICES, N.A.'
    ],
    'c': [
        'CITIBANK, N.A.',
        'CITICORP',
        'CITIGROUP INC.'
    ],
    'gs': [
        'GOLDMAN SACHS BANK USA',
        'THE GOLDMAN SACHS GROUP, INC.'
    ],
    'ms': [
        'MORGAN STANLEY & CO. LLC',
        'MORGAN STANLEY BANK, N.A.',
        'MORGAN STANLEY'
    ],
    'td': [
        'TD BANK US HOLDING COMPANY',
        'TD BANK, N.A.',
        'TD BANK USA, NATIONAL ASSOCIATION'
    ]
}

class CFPBFetcher:
    def __init__(self):
        self.storage_client = storage.Client()
        self.session = requests.Session()
        # Set proper headers for CFPB API
        self.session.headers.update({
            'User-Agent': 'Brand-Health-Index-Pipeline/1.0 (Consumer Complaint Analysis)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
    
    def fetch_complaints(self, brand_id: str, company_names: List[str], 
                        date_received_gte: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch complaints for specific companies from CFPB API"""
        
        all_complaints = []
        
        for company_name in company_names:
            try:
                # Rate limiting to avoid overwhelming CFPB API
                time.sleep(1.0)  # 1 second delay between requests
                
                # Build query parameters
                params = {
                    'company': company_name,
                    'date_received_gte': date_received_gte,
                    'size': min(limit, 1000),  # API limit
                    'sort': 'date_received_desc'
                }
                
                logger.info(f"Fetching complaints for {company_name} since {date_received_gte}")
                response = self.session.get(CFPB_API_BASE, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'hits' in data and 'hits' in data['hits']:
                    for hit in data['hits']['hits']:
                        complaint = self._process_complaint(hit['_source'], brand_id)
                        if complaint:
                            all_complaints.append(complaint)
                
                logger.info(f"Fetched {len(data.get('hits', {}).get('hits', []))} complaints for {company_name}")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching complaints for {company_name}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing {company_name}: {e}")
                continue
        
        # Remove duplicates based on complaint_id
        unique_complaints = {}
        for complaint in all_complaints:
            complaint_id = complaint['complaint_id']
            if complaint_id not in unique_complaints:
                unique_complaints[complaint_id] = complaint
        
        final_complaints = list(unique_complaints.values())
        logger.info(f"Total unique complaints for brand {brand_id}: {len(final_complaints)}")
        
        return final_complaints
    
    def _process_complaint(self, complaint_data: Dict[str, Any], brand_id: str) -> Optional[Dict[str, Any]]:
        """Process CFPB complaint into standardized format"""
        try:
            # Map CFPB fields to our schema
            processed = {
                'brand_id': brand_id,
                'complaint_id': complaint_data.get('complaint_id'),
                'ts_event': self._parse_date(complaint_data.get('date_received')),
                'product': complaint_data.get('product'),
                'sub_product': complaint_data.get('sub_product'),
                'issue': complaint_data.get('issue'),
                'sub_issue': complaint_data.get('sub_issue'),
                'consumer_complaint_narrative': complaint_data.get('consumer_complaint_narrative'),
                'company_response_to_consumer': complaint_data.get('company_response_to_consumer'),
                'timely_response': complaint_data.get('timely_response', '').lower() == 'yes',
                'consumer_disputed': self._parse_boolean(complaint_data.get('consumer_disputed')),
                'submitted_via': complaint_data.get('submitted_via'),
                'date_sent_to_company': self._parse_date(complaint_data.get('date_sent_to_company')),
                'company_public_response': complaint_data.get('company_public_response'),
                'tags': complaint_data.get('tags'),
                'state': complaint_data.get('state'),
                'zip_code': complaint_data.get('zip_code'),
                'geo_country': 'US',  # CFPB is US-only
                'collected_at': datetime.utcnow().isoformat()
            }
            
            # Calculate severity score based on available indicators
            processed['severity_score'] = self._calculate_severity_score(complaint_data)
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing complaint {complaint_data.get('complaint_id', 'unknown')}: {e}")
            return None
    
    def _parse_date(self, date_string: Optional[str]) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_string:
            return None
        try:
            # CFPB dates are typically in YYYY-MM-DD format
            dt = datetime.strptime(date_string, '%Y-%m-%d')
            return dt.isoformat()
        except:
            return date_string  # Return as-is if parsing fails
    
    def _parse_boolean(self, value: Optional[str]) -> Optional[bool]:
        """Parse string boolean values"""
        if not value:
            return None
        return value.lower() in ['yes', 'true', '1']
    
    def _calculate_severity_score(self, complaint_data: Dict[str, Any]) -> float:
        """Calculate a severity score (0-1) based on complaint characteristics"""
        score = 0.0
        
        # Base score for having a complaint
        score += 0.3
        
        # Higher severity for certain products
        high_severity_products = ['mortgage', 'debt collection', 'credit reporting']
        product = complaint_data.get('product', '').lower()
        if any(hsp in product for hsp in high_severity_products):
            score += 0.2
        
        # Consumer disputed adds severity
        if complaint_data.get('consumer_disputed', '').lower() == 'yes':
            score += 0.2
        
        # Untimely response adds severity
        if complaint_data.get('timely_response', '').lower() != 'yes':
            score += 0.1
        
        # Has narrative (consumer took time to write) adds severity
        if complaint_data.get('consumer_complaint_narrative'):
            score += 0.1
        
        # Public response suggests more serious complaint
        if complaint_data.get('company_public_response'):
            score += 0.1
        
        return min(score, 1.0)  # Cap at 1.0
    
    def save_to_gcs(self, complaints: List[Dict[str, Any]], brand_id: str, date_str: str):
        """Save complaints to GCS in NDJSON format for Fivetran"""
        if not complaints:
            logger.info(f"No complaints to save for brand {brand_id}")
            return
            
        bucket = self.storage_client.bucket(BUCKET_NAME)
        
        # Create path: raw/cfpb/date=YYYY-MM-DD/brand_id.ndjson
        blob_path = f"raw/cfpb/date={date_str}/{brand_id}.ndjson"
        blob = bucket.blob(blob_path)
        
        # Convert to newline-delimited JSON
        ndjson_content = '\n'.join([json.dumps(complaint) for complaint in complaints])
        
        blob.upload_from_string(ndjson_content, content_type='application/x-ndjson')
        logger.info(f"Saved {len(complaints)} complaints to gs://{BUCKET_NAME}/{blob_path}")

@functions_framework.http
def fetch_cfpb_data(request):
    """Cloud Function entry point"""
    try:
        # Parse request parameters - handle both HTTP and Pub/Sub triggers
        request_json = request.get_json(silent=True) or {}
        
        # Handle Pub/Sub message format
        if 'message' in request_json:
            import base64
            message_data = request_json['message'].get('data', '')
            if message_data:
                try:
                    decoded_data = base64.b64decode(message_data).decode('utf-8')
                    request_json = json.loads(decoded_data)
                    logger.info(f"Decoded Pub/Sub message: {request_json}")
                except Exception as e:
                    logger.warning(f"Failed to decode Pub/Sub message: {e}")
                    request_json = {}
        
        # Default to yesterday's data
        target_date = request_json.get('date')
        if not target_date:
            yesterday = datetime.utcnow() - timedelta(days=1)
            target_date = yesterday.strftime('%Y-%m-%d')
        
        # CFPB date format for API query (look back 7 days to catch any delayed reports)
        lookback_date = datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=7)
        date_received_gte = lookback_date.strftime('%Y-%m-%d')
        
        fetcher = CFPBFetcher()
        
        # Fetch data for each brand
        total_complaints = 0
        for brand_id, company_names in FINANCIAL_COMPANIES.items():
            complaints = fetcher.fetch_complaints(brand_id, company_names, date_received_gte)
            fetcher.save_to_gcs(complaints, brand_id, target_date)
            total_complaints += len(complaints)
        
        return {
            'status': 'success',
            'date': target_date,
            'lookback_date': date_received_gte,
            'total_complaints': total_complaints,
            'brands_processed': len(FINANCIAL_COMPANIES)
        }, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_cfpb_data: {e}")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    # For Cloud Run deployment
    import os
    from flask import Flask, request
    
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def handle_request():
        return fetch_cfpb_data(request)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        return {'status': 'healthy'}, 200
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
