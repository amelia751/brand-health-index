#!/usr/bin/env python3
"""
Test CFPB Company Name Mappings
Verify that our bank aliases work with actual CFPB data
"""

import requests
import json

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

def get_brand_id_from_company(company_name: str) -> str:
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

def test_cfpb_mappings():
    """Test CFPB company name mappings with real API data"""
    
    print("🧪 Testing CFPB Company Name Mappings...")
    
    # Get sample data from CFPB API
    try:
        response = requests.get(
            "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?size=200",
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        hits = data.get('hits', {}).get('hits', [])
        
        print(f"✅ Retrieved {len(hits)} complaints from CFPB API")
        
        # Test mappings
        mapped_companies = {}
        unmapped_companies = set()
        all_companies = set()
        
        for hit in hits:
            source = hit.get('_source', {})
            company_name = source.get('company', '')
            
            if company_name:
                all_companies.add(company_name)
                brand_id = get_brand_id_from_company(company_name)
                
                if brand_id:
                    if brand_id not in mapped_companies:
                        mapped_companies[brand_id] = []
                    mapped_companies[brand_id].append(company_name)
                else:
                    unmapped_companies.add(company_name)
        
        print(f"\n📊 Results from {len(all_companies)} unique companies:")
        
        # Show successful mappings
        if mapped_companies:
            print(f"\n✅ Successfully mapped {len(mapped_companies)} brands:")
            for brand_id, companies in mapped_companies.items():
                unique_companies = list(set(companies))
                print(f"   {brand_id}: {unique_companies}")
        
        # Show unmapped companies (first 10)
        if unmapped_companies:
            print(f"\n❌ Unmapped companies (showing first 10 of {len(unmapped_companies)}):")
            for company in sorted(list(unmapped_companies))[:10]:
                print(f"   {company}")
        
        # Test specific companies we expect
        print(f"\n🎯 Testing specific expected companies:")
        test_companies = [
            "JPMORGAN CHASE & CO.",
            "BANK OF AMERICA, NATIONAL ASSOCIATION", 
            "WELLS FARGO & COMPANY",
            "CAPITAL ONE FINANCIAL CORPORATION",
            "CITIBANK, N.A."
        ]
        
        for company in test_companies:
            brand_id = get_brand_id_from_company(company)
            status = "✅" if brand_id else "❌"
            print(f"   {status} {company} → {brand_id}")
        
        return len(mapped_companies) > 0
        
    except Exception as e:
        print(f"❌ Error testing CFPB mappings: {e}")
        return False

if __name__ == "__main__":
    success = test_cfpb_mappings()
    if success:
        print(f"\n🎉 CFPB mapping test completed successfully!")
    else:
        print(f"\n💥 CFPB mapping test failed!")
