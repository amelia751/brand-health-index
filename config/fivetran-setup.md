# Fivetran Setup Guide

This guide explains how to configure Fivetran connectors for the Brand Health Index pipeline.

## Overview

Fivetran serves as the reliable ELT layer between your raw data in Google Cloud Storage and BigQuery. It handles:
- Schema detection and evolution
- Incremental loading
- Error handling and retries
- Data lineage tracking

## Prerequisites

1. Fivetran account with BigQuery connector access
2. GCP project with BigQuery API enabled
3. Service account with appropriate permissions
4. GCS bucket with raw data (deployed via Terraform)

## BigQuery Connection Setup

### 1. Create Service Account for Fivetran

```bash
# Create service account
gcloud iam service-accounts create fivetran-bigquery \
    --display-name="Fivetran BigQuery Service Account"

# Get the service account email
export FIVETRAN_SA="fivetran-bigquery@YOUR_PROJECT_ID.iam.gserviceaccount.com"

# Grant necessary roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:$FIVETRAN_SA" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:$FIVETRAN_SA" \
    --role="roles/bigquery.jobUser"

# Create and download key
gcloud iam service-accounts keys create fivetran-key.json \
    --iam-account=$FIVETRAN_SA
```

### 2. Configure BigQuery Destination in Fivetran

1. Log into Fivetran dashboard
2. Go to **Destinations** → **Add Destination**
3. Select **Google BigQuery**
4. Fill in the connection details:
   - **Project ID**: Your GCP project ID
   - **Dataset Location**: US (or your preferred region)
   - **Authentication**: Service Account Key
   - **Service Account Key**: Upload the `fivetran-key.json` file

## GCS Files Connectors

Create separate connectors for each data source to maintain clear data lineage.

### Twitter Data Connector

1. **Create New Connector**
   - Source: **Google Cloud Storage**
   - Destination: Your BigQuery connection

2. **Configuration**
   ```
   Connector Name: Twitter Brand Data
   Bucket Name: your-brand-health-bucket-name
   Folder Path: raw/twitter/
   File Pattern: *.ndjson
   ```

3. **Schema Settings**
   ```
   Destination Schema: fivetran_raw
   Destination Table: twitter_posts
   Primary Key: tweet_id
   ```

4. **Advanced Settings**
   ```
   File Type: JSON (Newline Delimited)
   Compression: Auto-detect
   Skip Header: false
   Archive Strategy: Leave files in place
   Sync Frequency: Every hour
   ```

### Reddit Data Connector

1. **Create New Connector**
   - Source: **Google Cloud Storage**
   - Destination: Your BigQuery connection

2. **Configuration**
   ```
   Connector Name: Reddit Brand Data
   Bucket Name: your-brand-health-bucket-name
   Folder Path: raw/reddit/
   File Pattern: *.ndjson
   ```

3. **Schema Settings**
   ```
   Destination Schema: fivetran_raw
   Destination Table: reddit_messages
   Primary Key: reddit_id
   ```

### Google Trends Connector

1. **Create New Connector**
   - Source: **Google Cloud Storage**
   - Destination: Your BigQuery connection

2. **Configuration**
   ```
   Connector Name: Google Trends Data
   Bucket Name: your-brand-health-bucket-name
   Folder Path: raw/trends/
   File Pattern: *.ndjson
   ```

3. **Schema Settings**
   ```
   Destination Schema: fivetran_raw
   Destination Table: trends_timeseries
   Primary Key: brand_id, keyword, geo, ts_event
   ```

### CFPB Complaints Connector

1. **Create New Connector**
   - Source: **Google Cloud Storage**
   - Destination: Your BigQuery connection

2. **Configuration**
   ```
   Connector Name: CFPB Complaints Data
   Bucket Name: your-brand-health-bucket-name
   Folder Path: raw/cfpb/
   File Pattern: *.ndjson
   ```

3. **Schema Settings**
   ```
   Destination Schema: fivetran_raw
   Destination Table: cfpb_complaints
   Primary Key: complaint_id
   ```

## Schema Management

### Expected Schemas

#### Twitter Posts (`fivetran_raw.twitter_posts`)
```sql
CREATE TABLE fivetran_raw.twitter_posts (
  brand_id STRING,
  tweet_id STRING,
  ts_event TIMESTAMP,
  author_id STRING,
  author_username STRING,
  author_verified BOOLEAN,
  author_location STRING,
  text STRING,
  lang STRING,
  like_count INTEGER,
  reply_count INTEGER,
  retweet_count INTEGER,
  quote_count INTEGER,
  possibly_sensitive BOOLEAN,
  geo_country STRING,
  geo_place_id STRING,
  collected_at TIMESTAMP,
  _fivetran_synced TIMESTAMP
);
```

#### Reddit Messages (`fivetran_raw.reddit_messages`)
```sql
CREATE TABLE fivetran_raw.reddit_messages (
  brand_id STRING,
  reddit_id STRING,
  ts_event TIMESTAMP,
  subreddit STRING,
  author STRING,
  type STRING,
  title STRING,
  body STRING,
  score INTEGER,
  num_comments INTEGER,
  upvote_ratio FLOAT,
  url STRING,
  permalink STRING,
  lang STRING,
  geo_country STRING,
  collected_at TIMESTAMP,
  _fivetran_synced TIMESTAMP
);
```

#### Trends Timeseries (`fivetran_raw.trends_timeseries`)
```sql
CREATE TABLE fivetran_raw.trends_timeseries (
  brand_id STRING,
  keyword STRING,
  geo STRING,
  ts_event TIMESTAMP,
  value INTEGER,
  category STRING,
  is_brand_keyword BOOLEAN,
  related_queries_top ARRAY<STRING>,
  collected_at TIMESTAMP,
  _fivetran_synced TIMESTAMP
);
```

#### CFPB Complaints (`fivetran_raw.cfpb_complaints`)
```sql
CREATE TABLE fivetran_raw.cfpb_complaints (
  brand_id STRING,
  complaint_id STRING,
  ts_event TIMESTAMP,
  product STRING,
  sub_product STRING,
  issue STRING,
  sub_issue STRING,
  consumer_complaint_narrative STRING,
  company_response_to_consumer STRING,
  timely_response BOOLEAN,
  consumer_disputed BOOLEAN,
  submitted_via STRING,
  date_sent_to_company TIMESTAMP,
  company_public_response STRING,
  tags STRING,
  state STRING,
  zip_code STRING,
  geo_country STRING,
  severity_score FLOAT,
  collected_at TIMESTAMP,
  _fivetran_synced TIMESTAMP
);
```

## Monitoring and Alerts

### Fivetran Dashboard Monitoring

1. **Sync Status**: Monitor connector sync success/failure
2. **Row Counts**: Track daily ingestion volumes
3. **Schema Changes**: Alert on unexpected schema drift
4. **Sync Frequency**: Ensure connectors run on schedule

### Custom Monitoring Queries

```sql
-- Check data freshness by source
SELECT 
  'twitter' as source,
  COUNT(*) as rows_today,
  MAX(_fivetran_synced) as last_sync
FROM fivetran_raw.twitter_posts 
WHERE DATE(_fivetran_synced) = CURRENT_DATE()

UNION ALL

SELECT 
  'reddit' as source,
  COUNT(*) as rows_today,
  MAX(_fivetran_synced) as last_sync
FROM fivetran_raw.reddit_messages 
WHERE DATE(_fivetran_synced) = CURRENT_DATE()

UNION ALL

SELECT 
  'trends' as source,
  COUNT(*) as rows_today,
  MAX(_fivetran_synced) as last_sync
FROM fivetran_raw.trends_timeseries 
WHERE DATE(_fivetran_synced) = CURRENT_DATE()

UNION ALL

SELECT 
  'cfpb' as source,
  COUNT(*) as rows_today,
  MAX(_fivetran_synced) as last_sync
FROM fivetran_raw.cfpb_complaints 
WHERE DATE(_fivetran_synced) = CURRENT_DATE();
```

### Alert Configuration

Set up alerts in Fivetran for:
- **Sync Failures**: Immediate notification
- **Schema Changes**: Review and approve changes
- **Volume Anomalies**: Significant increases/decreases in row counts
- **Latency Issues**: Delays in data processing

## Best Practices

### 1. Incremental Loading
- Use `_fivetran_synced` timestamp for incremental processing
- Partition tables by date for better performance
- Set appropriate sync frequencies (hourly for real-time, daily for batch)

### 2. Schema Evolution
- Enable automatic schema updates for new fields
- Review schema changes before applying to production
- Use dbt to handle schema changes gracefully

### 3. Cost Optimization
- Monitor Monthly Active Rows (MAR) usage
- Use appropriate sync frequencies
- Archive old data that's no longer needed

### 4. Data Quality
- Set up data validation tests in dbt
- Monitor for duplicate records
- Validate primary key uniqueness

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify service account permissions
   - Check key file validity
   - Ensure BigQuery API is enabled

2. **Schema Mismatch**
   - Compare expected vs actual schemas
   - Check for data type conflicts
   - Review JSON parsing errors

3. **Sync Failures**
   - Check GCS bucket permissions
   - Verify file formats and structure
   - Review Fivetran error logs

4. **Performance Issues**
   - Optimize file sizes (10-100MB recommended)
   - Use appropriate partitioning
   - Consider parallel processing

### Support Resources

- **Fivetran Documentation**: https://fivetran.com/docs
- **BigQuery Connector Guide**: https://fivetran.com/docs/databases/google-bigquery
- **GCS Files Connector**: https://fivetran.com/docs/files/google-cloud-storage
