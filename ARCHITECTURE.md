# Brand Health Index - Revised Architecture

## System Overview

```
Data Sources → Cloud Functions/Run → GCS (NDJSON/Parquet)
           → BigQuery (raw) → dbt (curated, BHI) → Looker (dashboards)
                           ↘ Vertex AI (batch NLP enrich)
Data Sources (text) ────────────────────────────────→ Elastic (hybrid search)
                                                      ↑ embeddings from Vertex/Cloud Run
Agent (Cloud Run + Gemini): uses BigQuery (metrics) + Elastic (evidence), pushes Slack/Jira
```

## 1. Ingestion (No Fivetran)

### Data Sources
- **Reddit**: Posts and comments mentioning financial brands
- **CFPB (Socrata)**: Consumer complaint database
- **Google Trends (PyTrends)**: Search volume and interest trends
- **Trustpilot/Glassdoor**: Review exports (optional)

### Ingestion Pattern
Cloud Functions/Run fetchers → write newline-delimited JSON (gzip) to GCS:

```
gs://bhi-raw/raw/{source}/dt=YYYY-MM-DD/part-HHMMSS-UUID.jsonl.gz
```

### Common Record Shape
```json
{
  "event_id": "reddit_c_abcd1234",
  "ts_event": "2025-10-05T23:41:12Z",
  "brand_id": "citi",
  "source": "reddit",
  "geo_country": "US",
  "text": "Citi app keeps crashing...",
  "metadata": {"subreddit":"personalfinance", "permalink":"..."}
}
```

### BigQuery Loading
**Option A**: External tables over GCS for raw, then CTAS into native BQ tables.

**Option B (recommended)**: Small Cloud Run loader that issues bq load jobs into partitioned tables:
- `raw_reddit_events`
- `raw_cfpb_complaints` 
- `raw_trends_timeseries`
- `raw_trustpilot_reviews`

Partition on `DATE(ts_event)`, cluster by `brand_id`, `source`.

## 2. NLP Enrichment with Vertex AI

Add sentiment, severity, and topics to each text record before indexing to Elastic.

### Batch Flow (daily or hourly)
1. Query yesterday's raw text from BigQuery
2. Chunk to safe token sizes, call Vertex AI (Gemini or Text model)
3. Output: `sentiment ∈ [-1,1]`, `severity ∈ [0,1]`, `topics = [string]`, `language`
4. Write enrichments back into BQ side tables (`*_enriched`)
5. Include enrichments when indexing to Elastic

**Auditability**: Store `nlp_model`, `nlp_version`, and `score_confidence`.

### Few-shot Topic Schema (Finance-oriented)
```
["fees","fraud","mobile_app","branch_service","mortgage","cards","onboarding",
 "support_latency","outage","ux","account_lock","atm","overdraft","chargeback"]
```

## 3. dbt Models (Metrics & BHI)

### Project Layout
```
models/
  raw/            -- optional external views
  staging/        -- tidy, typed tables
  curated/        -- daily metrics per source
  bhi/            -- unified metrics + BHI calc
```

### Curated Daily Tables
- `reddit_daily_brand`: posts, authors, engagement_score, sentiment_index, volume_index, topic_shares
- `cfpb_daily_brand`: complaints, severity_avg, timely_rate, dispute_rate, complaints_index
- `trends_daily_brand`: rsv_avg, rsv_trend_7d, visibility_index
- `reviews_daily_brand`: review_count, rating_avg, employee_review_index, customer_review_index

### Unified & BHI
- `brand_daily_metrics`: join all sources on brand_id, date
- `brand_daily_bhi`: final score + component breakdown

### BHI Calculation Example
```sql
with m as (
  select
    brand_id, date,
    0.5*social_sentiment_index + 0.5*social_volume_index as social,
    search_visibility_index as search,
    complaints_index as complaints,
    0.6*customer_review_index + 0.4*employee_review_index as reviews
  from {{ ref('brand_daily_metrics') }}
)
select
  brand_id, date,
  0.30*social + 0.20*search + 0.25*complaints + 0.25*reviews as bhi_score,
  social, search, complaints, reviews,
  0.30 as w_social, 0.20 as w_search, 0.25 as w_complaints, 0.25 as w_reviews,
  'v1.0' as calc_version,
  current_timestamp() as updated_at
from m;
```

## 4. Elasticsearch for Hybrid Search (Agent Evidence)

Index textual sources (Reddit, CFPB narratives, Trustpilot/Glassdoor text) for retrieval.

### Index Mapping
```json
{
  "settings": {"index": {"knn": true}},
  "mappings": {
    "properties": {
      "brand_id":   {"type":"keyword"},
      "source":     {"type":"keyword"},
      "ts_event":   {"type":"date"},
      "text":       {"type":"text"},
      "topics":     {"type":"keyword"},
      "sentiment":  {"type":"float"},
      "severity":   {"type":"float"},
      "dense_vec":  {"type":"dense_vector", "dims": 768, "index": true, "similarity": "cosine"},
      "sparse_vec": {"type":"rank_features"},
      "url":        {"type":"keyword"},
      "geo_country":{"type":"keyword"}
    }
  }
}
```

### Embeddings Options
- **Option A (fastest)**: Use ELSER (Elastic's learned sparse encoder) → store to sparse_vec automatically
- **Option B (custom dense)**: Generate embeddings in Cloud Run worker (E5/Instructor or Vertex text-embedding)

### Hybrid Retrieval Query
```json
{
  "knn": { "field": "dense_vec", "query_vector": [/* embedding */], "k": 100, "num_candidates": 1000 },
  "query": {
    "bool": {
      "must": [{"match": {"text": "mobile app login outage"}}],
      "filter": [
        {"term": {"brand_id": "citi"}},
        {"range": {"ts_event": {"gte": "now-30d"}}}
      ]
    }
  }
}
```

## 5. The Agent (Cloud Run + Gemini)

**Goal**: Brand Complaints Copilot that answers "why", shows evidence, and suggests actions.

### Agent Tools
- **BigQuery tool**: SQL against curated tables (delta, top topics, BHI trend)
- **Elastic tool**: Hybrid search for representative quotes/snippets
- **Actions tool**: Slack alert / Jira ticket / email draft

### Agent Reasoning Flow
1. Parse brand + time range + intent (trend, root cause, compare)
2. BigQuery: pull metric deltas → top topics, geos, sources contributing to change
3. Elastic: retrieve ~10–20 maximally diverse snippets (MMR) per top topic
4. Gemini: produce grounded explanation with inline citations (links), plus 3–5 recommended actions
5. (Optional) Act: user clicks "Open Jira" or "Notify Slack" → agent posts artifacts

## 6. Dashboards (Looker)

Connect Looker directly to BigQuery curated models.

### Core Tiles
- **BHI Overview**: current BHI, WoW/PoP deltas, sparkline (28/90d)
- **Component Breakdown**: Social/Search/Complaints/Reviews bars
- **Complaint Topics**: Top topics by volume & severity, WoW change
- **Geo Heatmap**: complaints per million customers (by state/DMA)
- **Source Lens**: Reddit vs CFPB vs Trustpilot mix

### Drill-through
Click a topic → deep-dive page, "Open in Agent" button (passes brand+topic+date to the agent).

## 7. Alerts & Operations

- BQ scheduled queries compute anomaly flags
- Cloud Function posts Slack alerts with mini-explanations + "Ask the Agent" deep link
- Runbook: minimal manual steps for API keys rotation, backfills, schema changes

## 8. Security, PII & Governance

- **Secret Manager**: for API keys
- **PII redaction**: step before Elastic indexing (regex/Vertex PII)
- **Row-level IAM**: in BigQuery if adding private sources
- **Lineage**: keep `ingest_job_id`, `nlp_model_version`, `calc_version` columns

## 9. Idempotent Ingestion Strategy

### Natural IDs & Cursors (Source-specific)

#### Reddit
- **Natural keys**: Post: `t3_<post_id>`, Comment: `t1_<comment_id>`
- **Incremental cursor**: `created_utc` (keep `id_max_seen` as tie-breaker)
- **Query pattern**: overlap window (last 2 hours) to survive clock skew; de-dupe downstream

#### CFPB (Socrata API)
- **Natural key**: `complaint_id`
- **Incremental cursor**: `date_received` or `date_submitted` plus `complaint_id` tie-breaker
- **Overlap window**: re-pull last 3–7 days; upsert by `complaint_id`

#### Google Trends (PyTrends)
- **Natural key**: `(brand_id, keyword, geo, ts_event)` composite
- **Pattern**: refetch fixed window (last 90 days) and MERGE by composite key

### Ingestion State Persistence
```sql
CREATE TABLE ingest_state (
  source STRING,           -- 'reddit_comments', 'cfpb', ...
  cursor_iso STRING,       -- '2025-10-06T02:00:00Z'
  tie_breaker_id STRING,   -- last seen ID for same-timestamp ties
  updated_at TIMESTAMP
);
```

### Land-then-merge Pattern
1. **GCS object names**: `gs://bhi-raw/raw/reddit_comments/dt=2025-10-06/run=06-00/part-0001.jsonl.gz`
2. **BigQuery staging → target** (per run)
3. **Idempotent upsert**: keep the latest version of the same record

### MERGE Example
```sql
MERGE `dataset.raw_reddit_comments` T
USING (SELECT * FROM `dataset.stage_reddit_comments`) S
ON T.event_id = S.event_id
WHEN MATCHED AND COALESCE(S.edited, S.ts_event) > COALESCE(T.edited, T.ts_event)
  THEN UPDATE SET /* all fields */
WHEN NOT MATCHED THEN INSERT ROW;
```

## 10. Demo Flow

1. **Dashboard (Looker)**: BHI down 12 pts for Brand X this week; complaints "mobile_app" up 61% WoW
2. **Click "Open in Agent"**
3. **Agent shows**: root-cause narrative + citations from Reddit/CFPB + action plan
4. **One-click actions**: "Notify Slack" (exec channel) + "Create Jira" (pre-filled ticket with evidence)
5. **Optional**: Forecast next 14 days in BHI given current trend (BigQuery ML or Vertex)

## Project Scope: trendle-469110

This architecture is implemented for Google Cloud project `trendle-469110` with focus on:
- Cost optimization (smallest GCP instances)
- Idempotent ingestion to avoid duplicates
- Reddit-first implementation with pagination support
- Rate limit compliance (100 requests/minute for Reddit API)
- Scalable foundation for additional data sources
