# Brand Health Index Pipeline

A comprehensive data pipeline for calculating Brand Health Index (BHI) scores for financial institutions using multiple data sources.

## Architecture Overview

```
Data Sources → Cloud Functions → GCS → Fivetran → BigQuery → dbt → Looker
```

### Components

- **Fivetran**: Data ingestion and loading to BigQuery
- **BigQuery**: Data warehouse and analytics engine  
- **Cloud Functions**: API data fetchers for custom sources
- **dbt**: Data transformation and modeling
- **Vertex AI**: NLP processing (sentiment, toxicity)
- **Looker**: Business intelligence and dashboards

### Data Sources

1. **Twitter/X API v2** - Social sentiment and mentions
2. **Reddit API** - Community discussions and sentiment
3. **Google Trends** - Search visibility and interest
4. **CFPB Complaints** - Consumer complaint data
5. **Trustpilot/Glassdoor** - Customer and employee reviews

## Project Structure

```
brand-health-index/
├── cloud-functions/           # All GCP Cloud Functions
│   ├── twitter-fetcher/      # Twitter API v2 data fetcher
│   ├── reddit-fetcher/       # Reddit API data fetcher
│   ├── trends-fetcher/       # Google Trends data fetcher
│   └── cfpb-fetcher/         # CFPB complaints fetcher
├── dbt/                      # Data transformation models
│   ├── models/
│   │   ├── raw/             # Raw data models
│   │   ├── staging/         # Staging transformations
│   │   ├── marts/           # Final business logic
│   │   └── bhi/             # Brand Health Index calculations
├── terraform/               # Infrastructure as Code
├── config/                  # Configuration files
└── docs/                   # Documentation
```

## Brand Health Index Components

The BHI is calculated from these normalized components (0-100 scale):

- **Social Sentiment** (30%): Twitter + Reddit sentiment analysis
- **Search Visibility** (20%): Google Trends relative search volume
- **Complaints** (25%): CFPB consumer complaints (inverted)
- **Reviews** (25%): Customer (Trustpilot) + Employee (Glassdoor) ratings

## Quick Start

1. Set up GCP project and enable APIs
2. Deploy infrastructure with Terraform
3. Configure Fivetran connectors
4. Deploy Cloud Functions
5. Set up dbt transformations
6. Configure dashboards in Looker

See `/docs/deployment.md` for detailed setup instructions.
