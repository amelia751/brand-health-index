# Brand Health Index - Architecture Overview

## System Architecture

The Brand Health Index (BHI) pipeline is designed as a modern, cloud-native data platform that ingests, processes, and analyzes brand perception data from multiple sources to generate composite health scores for financial institutions.

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Data Sources  │    │  Cloud Functions │    │   Fivetran      │    │   BigQuery   │
│                 │    │                  │    │                 │    │              │
│ • Twitter API   │───▶│ • Twitter Fetcher│───▶│ • GCS Files     │───▶│ • Raw Tables │
│ • Reddit API    │    │ • Reddit Fetcher │    │   Connectors    │    │ • Staging    │
│ • Google Trends │    │ • Trends Fetcher │    │ • Schema Mgmt   │    │ • Marts      │
│ • CFPB API      │    │ • CFPB Fetcher   │    │ • Incremental   │    │ • BHI Tables │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────┘
                                │                                              │
                                ▼                                              │
                       ┌─────────────────┐                                     │
                       │ Cloud Scheduler │                                     │
                       │                 │                                     │
                       │ • Daily Jobs    │                                     │
                       │ • Pub/Sub       │                                     │
                       │ • Monitoring    │                                     │
                       └─────────────────┘                                     │
                                                                               │
                                                                               ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Dashboards    │◀───│      dbt         │◀───│  Vertex AI      │◀───│    GCS       │
│                 │    │                  │    │                 │    │              │
│ • Looker        │    │ • Transformations│    │ • Sentiment     │    │ • Raw Data   │
│ • Looker Studio │    │ • Data Quality   │    │ • NLP Models    │    │ • Staging    │
│ • Custom Apps   │    │ • BHI Calculation│    │ • Batch Scoring │    │ • Lifecycle  │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────┘
```

## Core Components

### 1. Data Ingestion Layer

#### Cloud Functions (Data Fetchers)
- **Purpose**: Extract data from external APIs and social platforms
- **Technology**: Python Cloud Run services triggered by Pub/Sub
- **Sources**:
  - Twitter API v2 (tweets, engagement metrics)
  - Reddit API (posts, comments, sentiment indicators)
  - Google Trends (search interest, relative volume)
  - CFPB Socrata API (consumer complaints)

#### Scheduling & Orchestration
- **Cloud Scheduler**: Daily cron jobs trigger data collection
- **Pub/Sub Topics**: Decouple scheduling from execution
- **Error Handling**: Retry logic and dead letter queues

### 2. Data Storage & Loading

#### Google Cloud Storage (GCS)
- **Raw Data Lake**: Partitioned by date and source
- **File Format**: Newline-delimited JSON (NDJSON)
- **Lifecycle**: Automatic archival and deletion policies
- **Structure**:
  ```
  gs://bucket/raw/
  ├── twitter/date=2023-12-01/jpm.ndjson
  ├── reddit/date=2023-12-01/wf.ndjson
  ├── trends/date=2023-12-01/bac.ndjson
  └── cfpb/date=2023-12-01/c.ndjson
  ```

#### Fivetran
- **Purpose**: Reliable, managed ELT from GCS to BigQuery
- **Features**: 
  - Schema drift handling
  - Incremental loading
  - Data lineage tracking
  - Built-in monitoring

### 3. Data Warehouse

#### BigQuery
- **Raw Layer** (`fivetran_raw`): Immutable source data
- **Staging Layer** (`staging`): Cleaned and normalized data
- **Marts Layer** (`marts`): Business logic and aggregations
- **BHI Layer** (`bhi`): Final Brand Health Index calculations

#### Data Organization
```sql
-- Raw Tables (Fivetran managed)
fivetran_raw.twitter_posts
fivetran_raw.reddit_messages  
fivetran_raw.trends_timeseries
fivetran_raw.cfpb_complaints

-- Staging Tables (dbt managed)
staging.stg_twitter_daily
staging.stg_reddit_daily
staging.stg_trends_daily
staging.stg_cfpb_daily

-- Business Tables (dbt managed)
marts.brand_daily_metrics
bhi.brand_daily_bhi
```

### 4. Data Transformation

#### dbt (Data Build Tool)
- **Models**: SQL-based transformations with Jinja templating
- **Testing**: Data quality tests and assertions
- **Documentation**: Auto-generated data catalog
- **Lineage**: Dependency graphs and impact analysis

#### Transformation Layers
1. **Raw → Staging**: Data cleaning, type casting, basic validation
2. **Staging → Marts**: Business logic, aggregations, metric calculations
3. **Marts → BHI**: Composite scoring, rankings, trend analysis

### 5. Machine Learning & NLP

#### Vertex AI (Future Enhancement)
- **Sentiment Analysis**: Advanced NLP models for social media text
- **Topic Classification**: Categorize complaints and social mentions
- **Anomaly Detection**: Identify unusual patterns in brand metrics
- **Batch Prediction**: Process large volumes efficiently

### 6. Analytics & Visualization

#### Business Intelligence
- **Looker**: Enterprise BI with governance and security
- **Looker Studio**: Self-service analytics and dashboards
- **Custom Applications**: API-driven apps for specific use cases

#### Key Metrics & KPIs
- Brand Health Index (0-100 composite score)
- Component scores (Social, Search, Complaints, Reviews)
- Trend analysis (7-day, 28-day changes)
- Competitive rankings and benchmarks

## Data Flow

### Daily Processing Cycle

1. **2:00 AM UTC** - Twitter data collection
2. **3:00 AM UTC** - Reddit data collection  
3. **4:00 AM UTC** - Google Trends data collection
4. **5:00 AM UTC** - CFPB complaints data collection
5. **6:00 AM UTC** - Fivetran sync (GCS → BigQuery)
6. **7:00 AM UTC** - dbt transformations run
7. **8:00 AM UTC** - Dashboards refresh

### Real-time Capabilities (Future)
- Streaming ingestion via Pub/Sub and Dataflow
- Near real-time sentiment analysis
- Alert triggers for significant BHI changes

## Brand Health Index Calculation

### Component Weights
```python
BHI = (
    0.30 * Social_Score +      # Twitter + Reddit sentiment
    0.20 * Search_Score +      # Google Trends visibility  
    0.25 * Complaints_Score +  # CFPB complaint metrics
    0.15 * Reviews_Customer +  # Trustpilot scores (future)
    0.10 * Reviews_Employee    # Glassdoor scores (future)
)
```

### Scoring Methodology
- **Normalization**: All components scaled to 0-100
- **Peer Comparison**: Percentile ranking within date/geography
- **Quality Flags**: Sample size, freshness, anomaly detection
- **Trend Analysis**: 7-day and 28-day change calculations

## Security & Compliance

### Data Privacy
- **PII Handling**: Automatic redaction using Cloud DLP
- **Access Control**: IAM roles and BigQuery column-level security
- **Audit Logging**: Complete data lineage and access tracking

### API Security
- **Secret Management**: Google Secret Manager for API keys
- **Service Accounts**: Least-privilege access principles
- **Network Security**: VPC controls and private endpoints

## Scalability & Performance

### Horizontal Scaling
- **Cloud Run**: Auto-scaling based on demand
- **BigQuery**: Serverless, automatically scales compute
- **Parallel Processing**: Independent fetchers for each data source

### Cost Optimization
- **Storage Lifecycle**: Automatic archival and deletion
- **BigQuery Slots**: Reserved capacity for predictable workloads  
- **Caching**: Materialized views for frequently accessed data

## Monitoring & Observability

### Data Quality
- **dbt Tests**: Automated validation of data integrity
- **Great Expectations**: Advanced data profiling and testing
- **Anomaly Detection**: Statistical outlier identification

### System Health
- **Cloud Monitoring**: Infrastructure metrics and alerting
- **Log Analysis**: Centralized logging and error tracking
- **SLA Monitoring**: Data freshness and pipeline success rates

### Business Metrics
- **BHI Trends**: Automated alerts for significant changes
- **Data Coverage**: Monitoring for missing or incomplete data
- **Competitive Analysis**: Benchmark tracking and reporting

## Future Enhancements

### Additional Data Sources
- **News & Media**: Brand mentions in news articles
- **Social Platforms**: LinkedIn, TikTok, Instagram
- **Review Sites**: Glassdoor, Indeed, Trustpilot
- **Financial Data**: Stock prices, analyst ratings

### Advanced Analytics
- **Predictive Models**: Forecast BHI trends and outcomes
- **Causal Analysis**: Identify drivers of brand health changes
- **Simulation**: What-if scenarios for strategic planning

### Real-time Capabilities
- **Streaming Pipeline**: Kafka/Pub/Sub for real-time ingestion
- **Live Dashboards**: Real-time BHI updates and alerts
- **API Services**: RESTful APIs for application integration
