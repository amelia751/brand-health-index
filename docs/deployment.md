# Brand Health Index - Deployment Guide

This guide walks you through deploying the Brand Health Index pipeline on Google Cloud Platform.

## Prerequisites

1. **Google Cloud Platform Account**
   - Active GCP project with billing enabled
   - Project owner or editor permissions

2. **Local Development Tools**
   - [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
   - [Terraform](https://developer.hashicorp.com/terraform/downloads) (>= 1.0)
   - [Docker](https://docs.docker.com/get-docker/)
   - [dbt](https://docs.getdbt.com/docs/core/installation) (>= 1.5)

3. **API Access**
   - Twitter API v2 Bearer Token
   - Reddit API credentials (client_id, client_secret, user_agent)

## Step 1: Initial Setup

### 1.1 Clone and Setup Project
```bash
git clone <your-repo>
cd brand-health-index

# Authenticate with Google Cloud
gcloud auth login
gcloud auth application-default login

# Set your project
gcloud config set project YOUR_PROJECT_ID
```

### 1.2 Configure Terraform Variables
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values:
# - project_id: Your GCP project ID
# - region: Your preferred region (default: us-central1)
# - environment: dev or prod
# - brand_health_bucket_name: Unique bucket name for raw data
```

## Step 2: Infrastructure Deployment

### 2.1 Deploy Infrastructure with Terraform
```bash
cd terraform

# Initialize Terraform
terraform init

# Plan the deployment
terraform plan

# Apply the infrastructure
terraform apply
```

This creates:
- GCS bucket for raw data storage
- BigQuery datasets (fivetran_raw, brand_health_dev, brand_health_prod)
- Pub/Sub topics for triggering data fetchers
- Cloud Scheduler jobs for daily runs
- Service accounts and IAM permissions
- Secret Manager secrets (empty - to be populated)

### 2.2 Store API Credentials in Secret Manager

#### Twitter Bearer Token
```bash
# Store your Twitter Bearer Token
echo "your-twitter-bearer-token" | gcloud secrets versions add twitter-bearer-token --data-file=-
```

#### Reddit API Credentials
```bash
# Create a JSON file with Reddit credentials
cat > reddit-creds.json << EOF
{
  "client_id": "your-reddit-client-id",
  "client_secret": "your-reddit-client-secret",
  "user_agent": "BrandHealthIndex/1.0 by YourUsername"
}
EOF

# Store in Secret Manager
gcloud secrets versions add reddit-credentials --data-file=reddit-creds.json

# Clean up the file
rm reddit-creds.json
```

## Step 3: Deploy Cloud Functions

### 3.1 Build and Deploy Container Images
```bash
# Build and push Twitter fetcher
cd cloud-functions/twitter-fetcher
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/twitter-fetcher

# Build and push Reddit fetcher
cd ../reddit-fetcher
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/reddit-fetcher

# Build and push Trends fetcher
cd ../trends-fetcher
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/trends-fetcher

# Build and push CFPB fetcher
cd ../cfpb-fetcher
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/cfpb-fetcher
```

### 3.2 Update Cloud Run Services
After building images, update the Terraform configuration and apply:
```bash
cd terraform
terraform apply
```

## Step 4: Setup Fivetran

### 4.1 Create Fivetran Account
1. Sign up for [Fivetran](https://fivetran.com)
2. Connect to your BigQuery project
3. Grant Fivetran access to your BigQuery datasets

### 4.2 Configure GCS Files Connectors
For each data source, create a GCS Files connector:

1. **Twitter Connector**
   - Source: Google Cloud Storage
   - Bucket: `your-bucket-name`
   - Prefix: `raw/twitter/`
   - File Pattern: `*.ndjson`
   - Destination Table: `fivetran_raw.twitter_posts`

2. **Reddit Connector**
   - Bucket: `your-bucket-name`
   - Prefix: `raw/reddit/`
   - Destination Table: `fivetran_raw.reddit_messages`

3. **Trends Connector**
   - Bucket: `your-bucket-name`
   - Prefix: `raw/trends/`
   - Destination Table: `fivetran_raw.trends_timeseries`

4. **CFPB Connector**
   - Bucket: `your-bucket-name`
   - Prefix: `raw/cfpb/`
   - Destination Table: `fivetran_raw.cfpb_complaints`

## Step 5: Setup dbt

### 5.1 Configure dbt Profiles
```bash
cd dbt

# Create profiles directory if it doesn't exist
mkdir -p ~/.dbt

# Copy the profiles.yml template
cp profiles.yml ~/.dbt/profiles.yml

# Edit ~/.dbt/profiles.yml and set:
# - DBT_GOOGLE_BIGQUERY_PROJECT: Your project ID
# - DBT_GOOGLE_BIGQUERY_KEYFILE: Path to service account key (or use gcloud auth)
```

### 5.2 Setup Environment Variables
```bash
export DBT_GOOGLE_BIGQUERY_PROJECT=YOUR_PROJECT_ID
# If using service account key file:
# export DBT_GOOGLE_BIGQUERY_KEYFILE=/path/to/service-account-key.json
```

### 5.3 Install Dependencies and Run dbt
```bash
# Install dbt dependencies
dbt deps

# Test connection
dbt debug

# Run seeds (brand dictionary)
dbt seed

# Run models
dbt run

# Run tests
dbt test
```

## Step 6: Testing the Pipeline

### 6.1 Manual Trigger Test
```bash
# Trigger each function manually
gcloud pubsub topics publish twitter-data-fetch --message='{"test": true}'
gcloud pubsub topics publish reddit-data-fetch --message='{"test": true}'
gcloud pubsub topics publish trends-data-fetch --message='{"test": true}'
gcloud pubsub topics publish cfpb-data-fetch --message='{"test": true}'
```

### 6.2 Check Data Flow
1. **Raw Data**: Check GCS bucket for new files
2. **Fivetran**: Verify data appears in BigQuery raw tables
3. **dbt**: Run transformations and check final BHI table

```bash
# Check if data is flowing
bq query --use_legacy_sql=false "
SELECT 
  brand_id, 
  date, 
  bhi_score,
  bhi_rank
FROM brand_health_dev.brand_daily_bhi 
ORDER BY date DESC, bhi_score DESC 
LIMIT 10"
```

## Step 7: Setup Monitoring and Alerts

### 7.1 Cloud Monitoring
```bash
# Create uptime checks for Cloud Run services
gcloud alpha monitoring uptime create twitter-fetcher-check \
  --resource-type=gce_instance \
  --hostname=twitter-fetcher-hash-uc.a.run.app

# Set up log-based alerts for function failures
gcloud logging sinks create function-errors \
  bigquery.googleapis.com/projects/YOUR_PROJECT_ID/datasets/logs \
  --log-filter='severity>=ERROR AND resource.type="cloud_run_revision"'
```

### 7.2 dbt Documentation
```bash
cd dbt
dbt docs generate
dbt docs serve
```

## Step 8: Production Considerations

### 8.1 Environment Separation
- Use separate projects for dev/staging/prod
- Implement proper IAM roles and service account permissions
- Set up CI/CD pipelines for code deployment

### 8.2 Cost Optimization
- Set up budget alerts
- Use BigQuery slot reservations for predictable workloads
- Implement data lifecycle policies on GCS

### 8.3 Data Quality
- Set up Great Expectations for data validation
- Implement data freshness monitoring
- Create alerting for anomalies in BHI scores

## Troubleshooting

### Common Issues

1. **Cloud Function Timeouts**
   - Increase timeout in Terraform configuration
   - Optimize API calls and reduce batch sizes

2. **BigQuery Permission Errors**
   - Verify service account has BigQuery Data Editor role
   - Check dataset-level permissions

3. **Fivetran Sync Failures**
   - Verify GCS bucket permissions
   - Check file format and schema consistency

4. **dbt Model Failures**
   - Run `dbt debug` to check connections
   - Verify source table schemas match expectations

### Monitoring Queries
```sql
-- Check data freshness
SELECT 
  source_table,
  MAX(collected_at) as last_update,
  DATETIME_DIFF(CURRENT_DATETIME(), MAX(collected_at), HOUR) as hours_since_update
FROM (
  SELECT 'twitter' as source_table, collected_at FROM fivetran_raw.twitter_posts
  UNION ALL
  SELECT 'reddit' as source_table, collected_at FROM fivetran_raw.reddit_messages
  UNION ALL
  SELECT 'trends' as source_table, collected_at FROM fivetran_raw.trends_timeseries
  UNION ALL
  SELECT 'cfpb' as source_table, collected_at FROM fivetran_raw.cfpb_complaints
)
GROUP BY source_table
ORDER BY hours_since_update DESC;
```

## Next Steps

1. **Setup Looker/Looker Studio** for visualization
2. **Implement NLP models** with Vertex AI for better sentiment analysis
3. **Add more data sources** (Glassdoor, Trustpilot, news articles)
4. **Create alerting** for significant BHI changes
5. **Build API endpoints** for real-time BHI access
