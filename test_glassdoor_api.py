#!/usr/bin/env python3
"""
Test script to debug Glassdoor RapidAPI responses
"""
import requests
import json

# RapidAPI credentials
RAPIDAPI_KEY = "b3cb474e98msh2a8d5b84ad7894dp11151fjsna7af7ca1b78c"
RAPIDAPI_HOST = "real-time-glassdoor-data.p.rapidapi.com"

def test_company_search():
    """Test company search endpoint"""
    print("🔍 Testing Company Search...")
    
    headers = {
        'X-RapidAPI-Key': RAPIDAPI_KEY,
        'X-RapidAPI-Host': RAPIDAPI_HOST,
        'Accept': 'application/json'
    }
    
    params = {
        'query': 'jpmorgan chase',
        'limit': 3
    }
    
    response = requests.get(
        f"https://{RAPIDAPI_HOST}/company-search",
        headers=headers,
        params=params
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Response: {json.dumps(data, indent=2)[:1000]}...")
        
        if data.get('status') == 'OK' and 'data' in data:
            companies = data['data']
            if companies:
                company = companies[0]
                company_id = company.get('company_id') or company.get('id')
                print(f"✅ Found company_id: {company_id}")
                return company_id
    else:
        print(f"❌ Error: {response.text}")
    
    return None

def test_company_reviews(company_id):
    """Test company reviews endpoint"""
    print(f"\n📝 Testing Company Reviews for ID: {company_id}...")
    
    headers = {
        'X-RapidAPI-Key': RAPIDAPI_KEY,
        'X-RapidAPI-Host': RAPIDAPI_HOST,
        'Accept': 'application/json'
    }
    
    params = {
        'company_id': str(company_id),
        'page': 1,
        'limit': 5,
        'sort': 'date',
        'employment_status': 'ANY',
        'language': 'en'
    }
    
    response = requests.get(
        f"https://{RAPIDAPI_HOST}/company-reviews",
        headers=headers,
        params=params
    )
    
    print(f"Status: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Response: {json.dumps(data, indent=2)[:2000]}...")
        
        if data.get('status') == 'OK' and 'data' in data:
            reviews = data['data']
            print(f"✅ Found {len(reviews)} reviews")
            if reviews:
                print(f"Sample review keys: {list(reviews[0].keys())}")
        else:
            print(f"⚠️ No reviews data: {data}")
    else:
        print(f"❌ Error: {response.text}")

if __name__ == "__main__":
    print("🧪 Testing Glassdoor RapidAPI...")
    
    # Test company search first
    company_id = test_company_search()
    
    if company_id:
        # Test reviews for the found company
        test_company_reviews(company_id)
    else:
        print("❌ Could not get company ID, skipping reviews test")
