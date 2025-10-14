"""
NLP Enrichment Service using Vertex AI
Integrates sentiment analysis, severity scoring, and topic extraction
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import re

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import vertexai
    from vertexai.generative_models import GenerativeModel, SafetySetting
    VERTEX_AVAILABLE = True
except ImportError:
    VERTEX_AVAILABLE = False
    logger.warning("Vertex AI not available, using fallback sentiment analysis")
from google.cloud import bigquery
import functions_framework

# Logging already configured above

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
BQ_DATASET = os.environ.get('BQ_DATASET', 'brand_health_raw')
VERTEX_LOCATION = os.environ.get('VERTEX_LOCATION', 'us-central1')

# Initialize Vertex AI
vertexai.init(project=PROJECT_ID, location=VERTEX_LOCATION)

# TD Bank specific topics taxonomy (focused on their services and common issues)
FINANCIAL_TOPICS = [
    # Core banking services
    "checking_account", "savings_account", "mobile_app", "online_banking", "atm",
    
    # TD Bank specific services
    "td_ameritrade", "td_auto_finance", "td_mortgage", "td_credit_card",
    "td_student_loans", "td_business_banking",
    
    # Common TD Bank issues
    "fees", "overdraft", "account_lock", "fraud", "security_breach",
    "customer_service", "branch_service", "phone_support", "wait_times",
    
    # User experience
    "ux", "website_issues", "app_crashes", "login_problems", "outage",
    "system_down", "maintenance",
    
    # Financial products
    "interest_rates", "rewards", "cashback", "credit_score", "loan_approval",
    "mortgage_rates", "refinancing", "investment_options",
    
    # Cross-border (TD's US-Canada operations)
    "cross_border_banking", "currency_exchange", "international_transfers",
    "canada_us_banking"
]

class NLPEnricher:
    """NLP enrichment using Vertex AI Gemini with fallback"""
    
    def __init__(self):
        if VERTEX_AVAILABLE:
            try:
                self.model = GenerativeModel("gemini-1.5-flash")
                self.vertex_enabled = True
                logger.info("Vertex AI Gemini initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Vertex AI: {e}")
                self.vertex_enabled = False
        else:
            self.vertex_enabled = False
            
        self.bq_client = bigquery.Client()
        
        # Safety settings for financial content
        if self.vertex_enabled:
            self.safety_settings = [
                SafetySetting(
                    category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
                ),
                SafetySetting(
                    category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
                ),
            ]
    
    def analyze_text_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Analyze a batch of texts for sentiment, severity, and topics"""
        results = []
        
        for text in texts:
            try:
                analysis = self._analyze_single_text(text)
                results.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing text: {e}")
                # Return default values on error
                results.append({
                    'sentiment': 0.0,
                    'severity': 0.0,
                    'topics': [],
                    'language': 'en',
                    'confidence': 0.0,
                    'error': str(e)
                })
        
        return results
    
    def _analyze_single_text(self, text: str) -> Dict[str, Any]:
        """Analyze single text using Vertex AI Gemini or fallback"""
        
        # Clean text
        cleaned_text = self._clean_text(text)
        if len(cleaned_text.strip()) < 10:
            return {
                'sentiment': 0.0,
                'severity': 0.0,
                'topics': [],
                'language': 'en',
                'confidence': 0.0
            }
        
        # Use Vertex AI if available, otherwise fallback
        if self.vertex_enabled:
            return self._analyze_with_vertex(cleaned_text)
        else:
            return self._analyze_with_fallback(cleaned_text)
    
    def _analyze_with_vertex(self, text: str) -> Dict[str, Any]:
        """Analyze text using Vertex AI Gemini"""
        # Create analysis prompt
        prompt = self._create_analysis_prompt(text)
        
        try:
            response = self.model.generate_content(
                prompt,
                safety_settings=self.safety_settings,
                generation_config={
                    "temperature": 0.1,
                    "top_p": 0.8,
                    "top_k": 40,
                    "max_output_tokens": 200,
                }
            )
            
            # Parse response
            return self._parse_gemini_response(response.text)
            
        except Exception as e:
            logger.error(f"Vertex AI API error: {e}")
            return self._analyze_with_fallback(text)
    
    def _analyze_with_fallback(self, text: str) -> Dict[str, Any]:
        """Simple fallback sentiment analysis"""
        # Simple keyword-based analysis
        text_lower = text.lower()
        
        # Sentiment keywords
        positive_words = ['good', 'great', 'excellent', 'love', 'amazing', 'perfect', 'best', 'happy', 'satisfied']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'horrible', 'angry', 'frustrated', 'disappointed']
        
        # Problem severity keywords
        severe_words = ['fraud', 'scam', 'stolen', 'hack', 'security', 'breach', 'locked', 'frozen', 'emergency']
        
        # Topic detection
        topics = []
        for topic in FINANCIAL_TOPICS:
            if topic.replace('_', ' ') in text_lower or topic in text_lower:
                topics.append(topic)
        
        # Calculate sentiment
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            sentiment = min(0.8, pos_count * 0.2)
        elif neg_count > pos_count:
            sentiment = max(-0.8, -neg_count * 0.2)
        else:
            sentiment = 0.0
        
        # Calculate severity
        severity = min(0.8, sum(1 for word in severe_words if word in text_lower) * 0.3)
        
        return {
            'sentiment': sentiment,
            'severity': severity,
            'topics': topics[:3],  # Max 3 topics
            'language': 'en',
            'confidence': 0.6  # Lower confidence for fallback
        }
    
    def _clean_text(self, text: str) -> str:
        """Clean text for analysis"""
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Limit length
        return text[:2000].strip()
    
    def _create_analysis_prompt(self, text: str) -> str:
        """Create TD Bank-specific analysis prompt for Gemini"""
        topics_str = ", ".join(FINANCIAL_TOPICS)
        
        return f"""Analyze this TD Bank customer feedback for sentiment, severity, and topics.

Context: This is customer feedback about TD Bank (Toronto Dominion Bank), a major North American bank with operations in Canada and the US. TD Bank offers checking/savings accounts, credit cards, mortgages, auto loans, investment services (TD Ameritrade), and cross-border banking.

Text: "{text}"

Provide analysis in this exact JSON format:
{{
  "sentiment": <float between -1.0 and 1.0, where -1=very negative, 0=neutral, 1=very positive>,
  "severity": <float between 0.0 and 1.0, where 0=minor issue, 1=critical issue>,
  "topics": <array of relevant topics from: {topics_str}>,
  "language": "en",
  "confidence": <float between 0.0 and 1.0 indicating analysis confidence>
}}

TD Bank-specific analysis guidelines:
- sentiment: Consider TD Bank's reputation for customer service, fees, and digital banking
- severity: 0.0=minor complaint (slow service), 0.5=moderate (fee disputes), 1.0=critical (fraud, account lockouts)
- topics: Focus on TD-specific services and common pain points
- Consider context: Canadian vs US operations, cross-border banking issues
- Account for TD Bank nicknames: "TD", "Toronto Dominion", "TD Ameritrade" references
- Recognize product-specific feedback: TD Auto Finance, TD Mortgage, TD Credit Cards

Return only valid JSON, no other text."""
    
    def _parse_gemini_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Gemini response into structured data"""
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                result = json.loads(json_str)
                
                # Validate and clean result
                return {
                    'sentiment': max(-1.0, min(1.0, float(result.get('sentiment', 0.0)))),
                    'severity': max(0.0, min(1.0, float(result.get('severity', 0.0)))),
                    'topics': result.get('topics', [])[:3],  # Max 3 topics
                    'language': result.get('language', 'en'),
                    'confidence': max(0.0, min(1.0, float(result.get('confidence', 0.5))))
                }
            else:
                raise ValueError("No JSON found in response")
                
        except Exception as e:
            logger.error(f"Error parsing Gemini response: {e}")
            return {
                'sentiment': 0.0,
                'severity': 0.0,
                'topics': [],
                'language': 'en',
                'confidence': 0.0
            }

def enrich_reddit_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrich Reddit records with NLP analysis"""
    enricher = NLPEnricher()
    
    # Extract texts for batch processing
    texts = []
    for record in records:
        # Combine title and body for posts, or just body for comments
        if 'title' in record and record['title']:
            text = f"{record['title']}\n\n{record.get('body', '')}"
        else:
            text = record.get('text', record.get('body', ''))
        texts.append(text)
    
    # Get NLP analysis
    analyses = enricher.analyze_text_batch(texts)
    
    # Combine records with analysis
    enriched_records = []
    for record, analysis in zip(records, analyses):
        enriched_record = record.copy()
        enriched_record.update({
            'sentiment': analysis['sentiment'],
            'severity': analysis['severity'],
            'topics': analysis['topics'],
            'language': analysis['language'],
            'nlp_confidence': analysis['confidence'],
            'nlp_model': 'vertex-ai-gemini-1.5-flash',
            'nlp_version': 'v1.0',
            'nlp_processed_at': datetime.utcnow().isoformat() + 'Z'
        })
        
        # Add error info if present
        if 'error' in analysis:
            enriched_record['nlp_error'] = analysis['error']
        
        enriched_records.append(enriched_record)
    
    return enriched_records

@functions_framework.http
def enrich_nlp_data(request):
    """Cloud Function entry point for NLP enrichment"""
    try:
        # Parse request
        request_json = request.get_json(silent=True) or {}
        
        # Get records to process
        records = request_json.get('records', [])
        if not records:
            return {'status': 'error', 'message': 'No records provided'}, 400
        
        # Enrich records
        enriched_records = enrich_reddit_records(records)
        
        return {
            'status': 'success',
            'records_processed': len(enriched_records),
            'enriched_records': enriched_records
        }, 200
        
    except Exception as e:
        logger.error(f"Error in enrich_nlp_data: {e}")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    # For Cloud Run deployment
    import os
    from flask import Flask, request
    
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def handle_request():
        return enrich_nlp_data(request)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        return {'status': 'healthy'}, 200
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
