#!/usr/bin/env python3
"""
Twitter API Test Script
Tests Twitter API v2 functionality and stores scraped data locally
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BEARER_TOKEN = os.environ.get('TWITTER_BEARER_TOKEN')
if not BEARER_TOKEN:
    logger.warning("TWITTER_BEARER_TOKEN environment variable not set. Please set it to test the API.")
    logger.info("Make sure you have a .env file with TWITTER_BEARER_TOKEN=your_token_here")
else:
    logger.info(f"✅ Twitter Bearer Token loaded (length: {len(BEARER_TOKEN)})")

# TD Bank specific search terms
TD_BANK_TERMS = [
    'TD Bank', 
    '@TDBank_US', 
    '@TDBank', 
    'Toronto-Dominion',
    'TD Canada Trust',
    'TD Ameritrade',
    'TD Securities',
    'TD Wealth',
    'TD Insurance',
    'TD Direct Investing'
]

class TwitterAPITester:
    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token
        self.base_url = "https://api.twitter.com/2"
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
        self.output_dir = Path("twitter_test_data")
        self.output_dir.mkdir(exist_ok=True)
        
    def _build_query(self, brand_terms: List[str]) -> str:
        """Build Twitter search query for brand mentions"""
        # Combine brand terms with OR, exclude retweets
        terms = ' OR '.join([f'"{term}"' for term in brand_terms])
        return f"({terms}) -is:retweet lang:en"
    
    def test_api_connection(self) -> bool:
        """Test basic API connection"""
        logger.info("🔍 Testing Twitter API connection...")
        
        try:
            # Test with a simple user lookup
            url = f"{self.base_url}/users/by/username/twitter"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                logger.info("✅ Twitter API connection successful!")
                return True
            else:
                logger.error(f"❌ API connection failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ API connection error: {e}")
            return False
    
    def fetch_tweets_batch(self, since_date: str, max_results: int = 100, next_token: str = None) -> tuple:
        """Fetch a single batch of tweets for TD Bank"""
        
        query = self._build_query(TD_BANK_TERMS)
        
        params = {
            'query': query,
            'max_results': min(max_results, 100),  # API limit
            'start_time': since_date,
            'tweet.fields': 'id,text,author_id,created_at,lang,public_metrics,possibly_sensitive,geo',
            'user.fields': 'id,username,location,verified',
            'expansions': 'author_id,geo.place_id'
        }
        
        if next_token:
            params['next_token'] = next_token
        
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
                        'brand_id': 'td',
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
            
            # Get next token for pagination
            next_token = data.get('meta', {}).get('next_token')
            
            return tweets, next_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching TD Bank tweets: {e}")
            return [], None

    def fetch_tweets(self, since_date: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Fetch tweets for TD Bank with pagination to get maximum data"""
        
        logger.info(f"🔍 Fetching TD Bank tweets (max {max_results}): {self._build_query(TD_BANK_TERMS)}")
        
        all_tweets = []
        next_token = None
        batch_count = 0
        max_batches = 10  # Limit to prevent excessive API calls
        
        while len(all_tweets) < max_results and batch_count < max_batches:
            batch_count += 1
            logger.info(f"📦 Fetching batch {batch_count}...")
            
            tweets, next_token = self.fetch_tweets_batch(since_date, 100, next_token)
            
            if not tweets:
                logger.info("No more tweets found")
                break
                
            all_tweets.extend(tweets)
            logger.info(f"   ✅ Got {len(tweets)} tweets (total: {len(all_tweets)})")
            
            if not next_token:
                logger.info("No more pages available")
                break
        
        # Limit to requested amount
        if len(all_tweets) > max_results:
            all_tweets = all_tweets[:max_results]
        
        logger.info(f"✅ Fetched {len(all_tweets)} TD Bank tweets total")
        return all_tweets
    
    def save_tweets_locally(self, tweets: List[Dict[str, Any]], date_str: str):
        """Save tweets to local JSON files"""
        if not tweets:
            logger.info("No TD Bank tweets to save")
            return
            
        # Create date directory
        date_dir = self.output_dir / f"date={date_str}"
        date_dir.mkdir(exist_ok=True)
        
        # Save as JSON file
        output_file = date_dir / "td_bank_tweets.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tweets, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Saved {len(tweets)} TD Bank tweets to {output_file}")
    
    def save_summary_report(self, results: Dict[str, Any], date_str: str):
        """Save a summary report of the test results"""
        summary_file = self.output_dir / f"summary_{date_str}.json"
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Summary report saved to {summary_file}")
    
    def run_td_bank_test(self, days_back: int = 7, max_tweets: int = 100):
        """Run a focused test for TD Bank tweets"""
        logger.info("🚀 Starting TD Bank Twitter API Test")
        logger.info("🏦 Sponsored by TD Bank")
        logger.info("=" * 50)
        
        # Test API connection first
        if not self.test_api_connection():
            logger.error("❌ API connection failed. Please check your bearer token.")
            return False
        
        # Calculate target date
        target_date = datetime.utcnow() - timedelta(days=days_back)
        date_str = target_date.strftime('%Y-%m-%d')
        since_time = target_date.isoformat() + 'Z'
        
        logger.info(f"📅 Testing data for date: {date_str}")
        logger.info(f"🔍 Fetching up to {max_tweets} TD Bank tweets")
        logger.info(f"🎯 Search terms: {', '.join(TD_BANK_TERMS)}")
        
        # Initialize results tracking
        results = {
            'test_date': datetime.utcnow().isoformat(),
            'target_date': date_str,
            'search_terms': TD_BANK_TERMS,
            'total_tweets': 0,
            'status': 'pending'
        }
        
        try:
            logger.info(f"\n🏦 Fetching TD Bank tweets...")
            tweets = self.fetch_tweets(since_time, max_tweets)
            
            if tweets:
                self.save_tweets_locally(tweets, date_str)
                results['total_tweets'] = len(tweets)
                results['status'] = 'success'
                
                # Log sample tweets
                logger.info(f"\n📝 Sample tweets found:")
                for i, tweet in enumerate(tweets[:3], 1):
                    logger.info(f"   {i}. @{tweet['author_username']}: {tweet['text'][:80]}...")
                    logger.info(f"      👍 {tweet['like_count']} | 🔄 {tweet['retweet_count']} | 💬 {tweet['reply_count']}")
            else:
                logger.warning("   ⚠️ No TD Bank tweets found")
                results['status'] = 'no_data'
                
        except Exception as e:
            logger.error(f"   ❌ Error fetching TD Bank tweets: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        # Save summary report
        self.save_summary_report(results, date_str)
        
        # Print final results
        logger.info("\n" + "=" * 50)
        logger.info("📊 TD Bank Test Results:")
        logger.info(f"   📝 Total tweets collected: {results['total_tweets']}")
        logger.info(f"   📁 Data saved to: {self.output_dir}")
        logger.info(f"   🎯 Search terms used: {len(TD_BANK_TERMS)} terms")
        
        return results['status'] == 'success'

def main():
    """Main function to run the TD Bank Twitter API test"""
    print("🐦 TD Bank Twitter API Test Script")
    print("🏦 Brand Health Index - Sponsored by TD Bank")
    print("=" * 50)
    
    # Check for bearer token
    if not BEARER_TOKEN:
        print("❌ Error: TWITTER_BEARER_TOKEN environment variable not set")
        print("\nTo set your Twitter Bearer Token:")
        print("1. Go to https://developer.twitter.com/")
        print("2. Create a new app or use existing app")
        print("3. Generate a Bearer Token")
        print("4. Set the environment variable:")
        print("   Windows: set TWITTER_BEARER_TOKEN=your_token_here")
        print("   Linux/Mac: export TWITTER_BEARER_TOKEN=your_token_here")
        return False
    
    # Create tester instance
    tester = TwitterAPITester(BEARER_TOKEN)
    
    # Run the TD Bank focused test
    success = tester.run_td_bank_test(days_back=7, max_tweets=100)
    
    if success:
        print("\n🎉 TD Bank Twitter API test completed successfully!")
        print(f"📁 Check the 'twitter_test_data' folder for TD Bank tweets")
        print(f"🎯 Search terms used: {', '.join(TD_BANK_TERMS)}")
    else:
        print("\n⚠️ TD Bank Twitter API test had issues. Check the logs above.")
    
    return success

if __name__ == "__main__":
    main()
