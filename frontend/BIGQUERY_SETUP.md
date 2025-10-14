# BigQuery Integration Setup

This document explains how to switch from mock data to real BigQuery data in the dashboard.

## Current Status

The dashboard is currently using **mock data** to avoid browser compatibility issues with the Google Cloud BigQuery library. The data structure matches the real BigQuery schema, so switching to real data is straightforward.

## Prerequisites

1. **Service Account Key**: You need a Google Cloud service account key with BigQuery access
2. **Environment Variables**: Set up the required environment variables
3. **BigQuery Tables**: Ensure the following tables exist and have data:
   - `trendle-469110.brand_health_dev.mart_complaint_clusters`

## Setup Steps

### 1. Install BigQuery Dependency (Server-side only)

```bash
cd frontend
npm install @google-cloud/bigquery
```

### 2. Set Environment Variables

Create a `.env.local` file in the frontend directory:

```bash
# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT=trendle-469110
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json

# Optional: BigQuery Configuration
BIGQUERY_DATASET=brand_health_dev
BIGQUERY_LOCATION=US
```

### 3. Enable Real Data in API Routes

#### For Clusters Data (`src/app/api/clusters/route.ts`):

```typescript
// Comment out this line:
// return NextResponse.json(mockClusterData);

// Uncomment these lines:
const rows = await queryComplaintClusters();
return NextResponse.json(rows);
```

#### For Trends Data (`src/app/api/trends/route.ts`):

```typescript
// Comment out this line:
// return NextResponse.json(mockDailyTrends);

// Uncomment these lines:
const rows = await queryDailyTrends();
return NextResponse.json(rows);
```

### 4. Update Import Statements

In both API route files, uncomment the import:

```typescript
import { queryComplaintClusters } from '@/lib/bigquery-server';
import { queryDailyTrends } from '@/lib/bigquery-server';
```

## File Structure

```
frontend/
├── src/
│   ├── app/api/
│   │   ├── clusters/route.ts    # Clusters API endpoint
│   │   └── trends/route.ts      # Trends API endpoint
│   └── lib/
│       ├── bigquery.ts          # Client-side data fetching
│       └── bigquery-server.ts   # Server-side BigQuery queries
```

## Testing the Integration

1. **Start the development server**:
   ```bash
   npm run dev
   ```

2. **Check the API endpoints**:
   - Visit `http://localhost:3000/api/clusters`
   - Visit `http://localhost:3000/api/trends`

3. **Monitor the console** for any BigQuery connection errors

4. **Verify data in dashboard** - the dashboard should now show real data instead of mock data

## Troubleshooting

### Common Issues

1. **Authentication Error**:
   - Verify the service account key path is correct
   - Ensure the service account has BigQuery Data Viewer permissions

2. **Table Not Found**:
   - Check that the BigQuery tables exist in the specified project/dataset
   - Verify the table names match the queries in `bigquery-server.ts`

3. **Permission Denied**:
   - Ensure the service account has access to the BigQuery dataset
   - Check that billing is enabled for the Google Cloud project

### Debug Mode

Add debug logging to the API routes:

```typescript
export async function GET() {
  try {
    console.log('Fetching data from BigQuery...');
    const rows = await queryComplaintClusters();
    console.log(`Retrieved ${rows.length} rows`);
    return NextResponse.json(rows);
  } catch (error) {
    console.error('BigQuery Error:', error);
    // ... error handling
  }
}
```

## Data Schema

The BigQuery tables should have the following structure:

### mart_complaint_clusters
- `cluster_name` (STRING)
- `cluster_description` (STRING)
- `cluster_priority` (INTEGER)
- `complaint_id` (STRING)
- `source_type` (STRING)
- `complaint_text` (STRING)
- `complaint_date` (DATE)
- `severity_level` (STRING)
- `source_detail` (STRING)

## Performance Considerations

- **Caching**: Consider implementing Redis caching for frequently accessed data
- **Pagination**: Add pagination for large datasets
- **Query Optimization**: Use appropriate WHERE clauses and LIMIT statements
- **Connection Pooling**: BigQuery client handles connection pooling automatically

## Security Notes

- Never commit service account keys to version control
- Use environment variables for all sensitive configuration
- Consider using Google Cloud IAM roles instead of service account keys in production
- Implement proper error handling to avoid exposing sensitive information

## Rollback to Mock Data

If you need to rollback to mock data:

1. Comment out the BigQuery import and query calls
2. Uncomment the mock data returns
3. The dashboard will continue working with sample data

This allows for easy switching between development (mock) and production (real) data modes.
