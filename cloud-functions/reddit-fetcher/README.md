# Reddit Fetcher Cloud Function

## 📁 File Structure & Purpose

### Core Files

#### `main.py` ⭐ **ACTIVE PRODUCTION FILE**
- **Purpose**: Current production Reddit fetcher with idempotent ingestion
- **Features**: 
  - Idempotent data collection (no duplicates)
  - State tracking in BigQuery
  - Rate limiting (100 requests/minute)
  - All 151 TD Bank keywords
  - Enhanced brand detection with confidence scoring
  - **⚠️ NLP Integration**: Calls `main_nlp.py` but data may not be properly enriched
- **Triggers**: HTTP endpoint for manual runs
- **Deployed As**: `reddit-fetcher` Cloud Function

#### `main_nlp.py` ⭐ **NLP ENRICHMENT MODULE**
- **Purpose**: Vertex AI Gemini-based sentiment analysis and topic extraction
- **Features**:
  - Sentiment scoring (-1.0 to 1.0)
  - Severity scoring (0.0 to 1.0) 
  - Topic extraction (TD Bank specific topics)
  - Fallback analysis when Vertex AI unavailable
- **Used By**: `main.py` imports and calls this module
- **Status**: ⚠️ **NEEDS TESTING** - May not be working properly

### Legacy/Reference Files

#### `main_original.py` 📜 **ORIGINAL VERSION**
- **Purpose**: Original simple Reddit fetcher (pre-idempotent)
- **Status**: Reference only, not used in production
- **Features**: Basic fetching without state tracking

#### `main_idempotent.py` 📜 **DEVELOPMENT VERSION**  
- **Purpose**: Development version of idempotent fetcher
- **Status**: Superseded by `main.py`
- **Note**: Contains similar logic to `main.py` but may be outdated

### Configuration Files

#### `requirements.txt`
- Python dependencies for the Cloud Function
- Includes Vertex AI, BigQuery, GCS, PRAW libraries

#### `Dockerfile`
- Container configuration for Cloud Function deployment

## 🔄 Current Data Pipeline Status

### ✅ Phase 1: Data Ingestion (COMPLETE)
```
Reddit API → main.py → GCS raw JSONL files
```
- **Status**: Working ✅
- **Data Location**: `gs://brand-health-raw-data-469110/raw/reddit/dt=YYYY-MM-DD/`
- **Format**: Raw JSONL with basic metadata

### ✅ Phase 2: NLP Enrichment (COMPLETE!)
```
Reddit API → main.py → Vertex AI NLP → Enhanced GCS data
```
- **Status**: **WORKING** ✅
- **Verified**: Data in GCS IS properly NLP-enriched
- **Features**: 
  - Sentiment analysis (-1.0 to 1.0) ✅
  - Severity scoring (0.0 to 1.0) ✅  
  - Topic extraction (TD Bank specific) ✅
  - Confidence scoring ✅
- **Model**: vertex-ai-gemini-1.5-flash
- **Integration**: Built into `main.py` via `main_nlp.py`

### ❌ Phase 3: BigQuery Loading (NEXT STEP)
```
GCS enriched data → BigQuery tables
```
- **Status**: **PENDING** ⏳
- **Next Step**: Load NLP-enriched data to BigQuery

### ❌ Phase 4: Elasticsearch Indexing (PENDING)
```
BigQuery → Elasticsearch
```

## 🎉 Recent Discoveries

1. **✅ NLP IS Working**: All Reddit data has sentiment, severity, topics!
2. **✅ Vertex AI Integration**: Successfully using Gemini 1.5 Flash
3. **✅ Pipeline Efficiency**: NLP happens during ingestion (no separate step needed)

## 🎯 Recommended Next Steps

1. **✅ DONE**: NLP enrichment is working perfectly
2. **⏳ NEXT**: Implement Phase 3 - Load to BigQuery
3. **⏳ THEN**: Implement Phase 4 - Elasticsearch indexing
4. **⏳ FINALLY**: Set up dbt transformations for BHI calculations

## 📊 Data Schema

### ✅ Current NLP-Enriched Reddit Data (ACTUAL)
```json
{
  "event_id": "reddit_t3_1o4o2hx",
  "ts_event": "2025-10-12T12:42:02Z", 
  "brand_id": "td_bank",
  "source": "reddit",
  "text": "Business accounts and payroll\n\nMy employer claims they can only pay me via direct deposit if I have a TD account...",
  "sentiment": 0.2,
  "severity": 0.0,
  "topics": ["customer_service"],
  "nlp_confidence": 0.6,
  "nlp_model": "vertex-ai-gemini-1.5-flash",
  "nlp_processed_at": "2025-10-12T17:16:56.168178Z",
  "nlp_version": "v1.0",
  "language": "en",
  "content_hash": "8548435fc48b8709",
  "metadata": {
    "subreddit": "TDBankCanada",
    "brand_detection": {
      "confidence_score": 0.89,
      "match_count": 7,
      "matched_terms": ["td", "TD account", "TD"]
    },
    "score": 0,
    "permalink": "https://reddit.com/r/TDBankCanada/comments/1o4o2hx/...",
    ...
  },
  "_ingested_at": "2025-10-12T17:15:23.090512Z",
  "_source_run": "reddit_fetcher_20251012_171523"
}
```
