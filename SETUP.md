# Brand Health Index - Setup Guide

## 🚀 Quick Start

### 1. Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate
```

### 2. Install Dependencies
```bash
# Install all dependencies
pip install -r requirements.txt
```

### 3. Set Up Environment Variables
```bash
# Copy the example environment file
copy .env.example .env

# Edit .env with your actual values
notepad .env  # Windows
nano .env     # Linux/Mac
```

### 4. Test Your Setup
```bash
# Test Twitter API
python test_twitter_api.py

# Test Glassdoor API
python test_glassdoor_api.py

# Test infrastructure
python simple_test.py
```

## 📋 Required API Keys

### Twitter API
1. Go to [Twitter Developer Portal](https://developer.twitter.com/)
2. Create a new app
3. Generate a Bearer Token
4. Add to `.env`: `TWITTER_BEARER_TOKEN=your_token`

### Reddit API (PRAW)
1. Go to [Reddit App Preferences](https://www.reddit.com/prefs/apps)
2. Create a new app (script type)
3. Note the client ID and secret
4. Add to `.env`:
   ```
   REDDIT_CLIENT_ID=your_client_id
   REDDIT_CLIENT_SECRET=your_client_secret
   REDDIT_USER_AGENT=BrandHealthIndex/1.0
   ```

### Glassdoor API (RapidAPI)
1. Go to [RapidAPI Glassdoor](https://rapidapi.com/real-time-glassdoor-data/api/real-time-glassdoor-data)
2. Subscribe to the API
3. Get your API key
4. Add to `.env`:
   ```
   RAPIDAPI_KEY=your_rapidapi_key
   RAPIDAPI_HOST=real-time-glassdoor-data.p.rapidapi.com
   ```

### Google Cloud
1. Set up a GCP project
2. Enable required APIs (Storage, Secret Manager, Pub/Sub)
3. Create service account and download key
4. Set up authentication:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```

## 🗂️ Project Structure

```
brand-health-index/
├── cloud-functions/          # Google Cloud Functions
│   ├── twitter-fetcher/
│   ├── reddit-fetcher/
│   ├── trends-fetcher/
│   ├── glassdoor-fetcher/
│   └── cfpb-fetcher/
├── data-transformation/      # DBT models
│   └── dbt/
├── terraform/               # Infrastructure as Code
├── test_*.py               # Test scripts
├── requirements.txt        # All dependencies
├── .env.example           # Environment template
└── .env                   # Your actual environment (gitignored)
```

## 🧪 Testing

Each API has its own test script:
- `test_twitter_api.py` - Tests Twitter API and saves data locally
- `test_glassdoor_api.py` - Tests Glassdoor API
- `simple_test.py` - Tests GCS and Pub/Sub infrastructure

## 🔧 Development Workflow

1. **Local Testing**: Use test scripts to verify APIs work
2. **Data Collection**: Cloud functions collect data daily
3. **Data Processing**: DBT transforms raw data
4. **Analysis**: Create dashboards and reports

## 📊 Data Flow

```
APIs → Cloud Functions → GCS → Fivetran → BigQuery → DBT → Analytics
```

## 🚨 Troubleshooting

### Common Issues:
- **Import errors**: Make sure virtual environment is activated
- **API errors**: Check your API keys in `.env`
- **GCP errors**: Run `gcloud auth application-default login`
- **Rate limits**: APIs have rate limits, test scripts respect them

### Getting Help:
- Check the logs in each test script
- Verify your `.env` file has correct values
- Ensure all APIs are properly subscribed/activated
