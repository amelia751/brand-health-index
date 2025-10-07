"""
CFPB Complaints Fetcher
Fetches consumer complaints from CFPB using the same bank aliases as Reddit fetcher
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
PROJECT_ID = os.environ.get('PROJECT_ID')
GCS_BUCKET = os.environ.get('GCS_BUCKET', 'brand-health-raw-data')
BQ_DATASET = os.environ.get('BQ_DATASET', 'brand_health_raw')

# CFPB-specific company name mappings (based on ACTUAL CFPB API data)
CFPB_COMPANY_MAPPING = {
    'chase': [
        'JPMORGAN CHASE & CO.'
    ],
    'bank_of_america': [
        'BANK OF AMERICA, NATIONAL ASSOCIATION'
    ],
    'wells_fargo': [
        'WELLS FARGO & COMPANY'
    ],
    'capital_one': [
        'CAPITAL ONE FINANCIAL CORPORATION'
    ],
    'citibank': [
        'CITIBANK, N.A.'
    ],
    'pnc': [
        'PNC Bank N.A.'
    ],
    'santander': [
        'SANTANDER BANK, NATIONAL ASSOCIATION',
        'SANTANDER HOLDINGS USA, INC.'
    ],
    # These need to be verified with more CFPB data
    'td_bank': [
        'TD BANK USA, NATIONAL ASSOCIATION'
    ],
    'citizens_bank': [
        'CITIZENS BANK, NATIONAL ASSOCIATION'
    ],
    'mt_bank': [
        'M&T BANK CORPORATION'
    ],
    'keybank': [
        'KEYBANK NATIONAL ASSOCIATION'
    ],
    'regions_bank': [
        'REGIONS BANK'
    ],
    'truist': [
        'TRUIST BANK'
    ]
}

# CFPB API Configuration
CFPB_API_BASE = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

class CFPBFetcher:
    """Fetches CFPB complaints data with same brand mapping as Reddit"""
    
    def __init__(self):
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.bucket = self.storage_client.bucket(GCS_BUCKET)
    
    def get_brand_id_from_company(self, company_name: str) -> Optional[str]:
        """Map CFPB company name to our standardized brand_id"""
        if not company_name:
            return None
        
        # Check exact matches first (case-insensitive)
        for brand_id, company_names in CFPB_COMPANY_MAPPING.items():
            for cfpb_name in company_names:
                if company_name.upper() == cfpb_name.upper():
                    return brand_id
        
        # Check partial matches as fallback
        company_upper = company_name.upper()
        for brand_id, company_names in CFPB_COMPANY_MAPPING.items():
            for cfpb_name in company_names:
                if cfpb_name.upper() in company_upper or company_upper in cfpb_name.upper():
                    return brand_id
        
        return None
    
    def fetch_cfpb_complaints(self, 
                            since_date: Optional[str] = None,
                            limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch complaints from CFPB API"""
        
        # Default to last 30 days if no date specified
        if not since_date:
            since_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"Fetching CFPB complaints since {since_date}")
        
        # Build API parameters (CFPB API format)
        params = {
            'date_received_min': since_date,
            'size': limit
        }
        
        try:
            response = requests.get(CFPB_API_BASE, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            
            logger.info(f"Retrieved {len(hits)} complaints from CFPB API")
            
            # Process and filter for our target banks
            processed_complaints = []
            all_companies = set()
            
            for hit in hits:
                source = hit.get('_source', {})
                
                # Extract key fields
                company_name = source.get('company', '')
                all_companies.add(company_name)
                brand_id = self.get_brand_id_from_company(company_name)
                
                # Log company names for debugging
                if not brand_id:
                    logger.info(f"Unmapped company: {company_name}")
                else:
                    logger.info(f"Mapped company: {company_name} → {brand_id}")
                
                # Only include complaints for our target banks
                if not brand_id:
                    continue
                
                # Create standardized record
                complaint = {
                    'event_id': f"cfpb_{source.get('complaint_id', '')}",
                    'ts_event': self._parse_date(source.get('date_received')),
                    'brand_id': brand_id,
                    'source': 'cfpb',
                    'geo_country': 'US',
                    'text': self._build_complaint_text(source),
                    'content_hash': self._generate_content_hash(source.get('complaint_id', '')),
                    'metadata': {
                        'complaint_id': source.get('complaint_id'),
                        'company': company_name,
                        'product': source.get('product'),
                        'issue': source.get('issue'),
                        'sub_issue': source.get('sub_issue'),
                        'state': source.get('state'),
                        'zip_code': source.get('zip_code'),
                        'submitted_via': source.get('submitted_via'),
                        'company_response': source.get('company_response_to_consumer'),
                        'timely_response': source.get('timely_response'),
                        'consumer_disputed': source.get('consumer_disputed'),
                        'consumer_consent_provided': source.get('consumer_consent_provided')
                    },
                    '_ingested_at': datetime.utcnow().isoformat() + 'Z',
                    '_source_run': f"cfpb_fetcher_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                }
                
                processed_complaints.append(complaint)
            
            logger.info(f"Processed {len(processed_complaints)} complaints for target banks")
            logger.info(f"All companies found: {sorted(list(all_companies))[:20]}")  # Show first 20
            return processed_complaints
            
        except Exception as e:
            logger.error(f"Error fetching CFPB data: {e}")
            return []
    
    def _parse_date(self, date_str: str) -> str:
        """Parse CFPB date format to ISO format"""
        if not date_str:
            return datetime.utcnow().isoformat() + 'Z'
        
        try:
            # CFPB typically uses YYYY-MM-DD format
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.isoformat() + 'Z'
        except:
            return datetime.utcnow().isoformat() + 'Z'
    
    def _build_complaint_text(self, source: Dict[str, Any]) -> str:
        """Build complaint text from available fields"""
        parts = []
        
        # Add issue information
        if source.get('issue'):
            parts.append(f"Issue: {source['issue']}")
        
        if source.get('sub_issue'):
            parts.append(f"Sub-issue: {source['sub_issue']}")
        
        # Add complaint narrative if available
        if source.get('consumer_complaint_narrative'):
            parts.append(f"Complaint: {source['consumer_complaint_narrative']}")
        
        # Add product information
        if source.get('product'):
            parts.append(f"Product: {source['product']}")
        
        return ' | '.join(parts) if parts else f"CFPB complaint for {source.get('company', 'Unknown')}"
    
    def _generate_content_hash(self, complaint_id: str) -> str:
        """Generate content hash for deduplication"""
        return hashlib.sha256(complaint_id.encode('utf-8')).hexdigest()[:16]
    
    def save_to_gcs_partitioned(self, complaints: List[Dict[str, Any]], run_timestamp: str):
        """Save complaints to GCS in partitioned format"""
        if not complaints:
            logger.info("No complaints to save")
            return []
        
        # Group by date
        complaints_by_date = {}
        for complaint in complaints:
            event_date = complaint['ts_event'][:10]  # Extract YYYY-MM-DD
            if event_date not in complaints_by_date:
                complaints_by_date[event_date] = []
            complaints_by_date[event_date].append(complaint)
        
        saved_files = []
        
        for date_str, date_complaints in complaints_by_date.items():
            # Create filename
            filename = f"raw/cfpb/dt={date_str}/part-{run_timestamp}-{hashlib.md5(date_str.encode()).hexdigest()[:8]}.jsonl.gz"
            
            # Convert to JSONL
            jsonl_content = '\n'.join(json.dumps(complaint) for complaint in date_complaints)
            
            # Upload to GCS with gzip compression
            blob = self.bucket.blob(filename)
            blob.upload_from_string(
                jsonl_content,
                content_type='application/gzip',
                content_encoding='gzip'
            )
            
            saved_files.append(filename)
            logger.info(f"Saved {len(date_complaints)} complaints to gs://{GCS_BUCKET}/{filename}")
        
        return saved_files

@functions_framework.http
def fetch_cfpb_data(request):
    """Cloud Function entry point for CFPB data fetching"""
    
    try:
        # Parse request
        request_json = request.get_json(silent=True) or {}
        
        # Get parameters
        since_date = request_json.get('since_date')  # YYYY-MM-DD format
        limit = request_json.get('limit', 1000)
        
        # Create run timestamp
        run_timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        
        logger.info(f"Starting CFPB fetch - since_date: {since_date}, limit: {limit}")
        
        # Initialize fetcher
        fetcher = CFPBFetcher()
        
        # Fetch complaints
        complaints = fetcher.fetch_cfpb_complaints(since_date=since_date, limit=limit)
        
        # Save to GCS
        saved_files = fetcher.save_to_gcs_partitioned(complaints, run_timestamp)
        
        # Return results
        result = {
            'status': 'success',
            'run_timestamp': run_timestamp,
            'total_complaints': len(complaints),
            'files_saved': len(saved_files),
            'since_date': since_date,
            'brands_found': list(set(c['brand_id'] for c in complaints))
        }
        
        logger.info(f"CFPB fetch complete: {result}")
        return result, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_cfpb_data: {e}")
        return {'status': 'error', 'message': str(e)}, 500

# Test function
def test_cfpb_fetcher():
    """Test the CFPB fetcher locally"""
    
    print("🧪 Testing CFPB Fetcher...")
    
    # Test brand mapping
    fetcher = CFPBFetcher()
    
    test_companies = [
        "BANK OF AMERICA, NATIONAL ASSOCIATION",
        "JPMORGAN CHASE & CO.",
        "WELLS FARGO & COMPANY",
        "CITIBANK, N.A.",
        "CAPITAL ONE FINANCIAL CORPORATION",
        "TD BANK USA, NATIONAL ASSOCIATION",
        "Random Bank Not In Our List"
    ]
    
    print("\n📋 Testing brand mapping:")
    for company in test_companies:
        brand_id = fetcher.get_brand_id_from_company(company)
        status = "✅" if brand_id else "❌"
        print(f"{status} {company} → {brand_id}")
    
    # Test API call (small sample)
    print(f"\n🌐 Testing CFPB API call...")
    complaints = fetcher.fetch_cfpb_complaints(limit=10)
    
    if complaints:
        print(f"✅ Successfully fetched {len(complaints)} complaints")
        
        # Show sample
        sample = complaints[0]
        print(f"\n📄 Sample complaint:")
        print(f"   Event ID: {sample['event_id']}")
        print(f"   Brand: {sample['brand_id']}")
        print(f"   Date: {sample['ts_event']}")
        print(f"   Text: {sample['text'][:100]}...")
        
        # Show brand distribution
        brands = {}
        for c in complaints:
            brands[c['brand_id']] = brands.get(c['brand_id'], 0) + 1
        
        print(f"\n📊 Brand distribution:")
        for brand, count in brands.items():
            print(f"   {brand}: {count}")
    else:
        print("❌ No complaints fetched")
    
    print(f"\n✅ CFPB Fetcher test complete!")

if __name__ == "__main__":
    test_cfpb_fetcher()