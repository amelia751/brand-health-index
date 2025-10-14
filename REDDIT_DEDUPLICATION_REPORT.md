# Reddit Data Deduplication Report

**Date**: October 14, 2025  
**Project**: TD Bank Health Monitor  
**Issue**: Duplicate Reddit posts/comments in BigQuery

---

## 🚨 **Problem Identified**

Your Reddit data contained **massive duplicates**:
- **430 total rows** in `brand_health_raw.reddit_events`
- **Only 86 unique events** 
- **344 duplicate rows (80% duplicates!)**

### **Most Duplicated Posts:**
- `reddit_t3_1o0g1kw`: **36 duplicates** (Credit card spending question)
- `reddit_t3_1nyhhs9`: **28 duplicates** (TD Bank teller position)
- `reddit_t3_1nvmfwp`: **14 duplicates** (Non-Canadian citizen pre-approval)
- `reddit_t3_1o4i8wl`: **12 duplicates** (Overdraft fees question)

---

## ✅ **Solution Implemented**

### **1. Data Backup**
- Created `brand_health_raw.reddit_events_backup` with original data
- **430 rows preserved** for rollback if needed

### **2. Deduplication Strategy**
- Used `ROW_NUMBER()` with `PARTITION BY event_id`
- **Kept the most recent version** based on:
  1. `nlp_processed_at DESC` (latest NLP processing)
  2. `ts_event DESC` (latest timestamp)

### **3. Results**
- ✅ **86 unique Reddit events** (100% clean)
- ✅ **0 duplicates remaining**
- ✅ **344 duplicate rows removed**
- ✅ **Original table replaced** with clean data

---

## 📊 **Impact on Dashboard**

Your complaint clustering dashboard will now show:
- **More accurate complaint counts**
- **Proper trend analysis** (no inflated numbers)
- **Realistic cluster sizes**
- **Better data quality** for insights

---

## 🔧 **Root Cause Analysis**

The duplicates likely occurred due to:

1. **Multiple Reddit Fetcher Runs**: Running the same fetching job multiple times
2. **Overlapping Date Ranges**: Fetching the same posts across different time periods
3. **No Deduplication Logic**: Missing `INSERT ... WHERE NOT EXISTS` logic
4. **Append-Only Loading**: Using `WRITE_APPEND` without checking for existing records

---

## 🛡️ **Prevention Strategies**

### **1. Update Reddit Fetcher Code**

Add deduplication logic to your Reddit fetcher:

```python
# In your Reddit fetcher (cloud-functions/reddit-fetcher/main.py)
def upload_to_bigquery(events):
    # Before inserting, check for existing events
    existing_ids = get_existing_event_ids(events)
    new_events = [e for e in events if e['event_id'] not in existing_ids]
    
    if new_events:
        # Only insert new events
        insert_events(new_events)
    else:
        print("No new events to insert")

def get_existing_event_ids(events):
    event_ids = [e['event_id'] for e in events]
    query = f"""
    SELECT DISTINCT event_id 
    FROM `brand_health_raw.reddit_events` 
    WHERE event_id IN UNNEST({event_ids})
    """
    # Return list of existing IDs
```

### **2. Use MERGE Instead of INSERT**

```sql
MERGE `brand_health_raw.reddit_events` T
USING (SELECT * FROM temp_reddit_data) S
ON T.event_id = S.event_id
WHEN NOT MATCHED THEN
  INSERT (event_id, text, ts_event, ...) 
  VALUES (S.event_id, S.text, S.ts_event, ...)
```

### **3. Add Unique Constraints**

```sql
-- Add clustering to improve deduplication performance
CREATE OR REPLACE TABLE `brand_health_raw.reddit_events`
CLUSTER BY event_id AS
SELECT * FROM `brand_health_raw.reddit_events`
```

### **4. Scheduled Deduplication**

Create a daily cleanup job:

```sql
-- Daily deduplication job
CREATE OR REPLACE TABLE `brand_health_raw.reddit_events` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id 
      ORDER BY nlp_processed_at DESC NULLS LAST
    ) as row_num
  FROM `brand_health_raw.reddit_events`
)
WHERE row_num = 1
```

---

## 📈 **Monitoring & Alerts**

Set up monitoring to catch future duplicates:

```sql
-- Weekly duplicate check query
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT event_id) as unique_events,
  COUNT(*) - COUNT(DISTINCT event_id) as duplicate_count,
  ROUND((COUNT(*) - COUNT(DISTINCT event_id)) / COUNT(*) * 100, 2) as duplicate_percentage
FROM `brand_health_raw.reddit_events`
```

**Alert if**: `duplicate_percentage > 5%`

---

## 🎯 **Next Steps**

1. **✅ COMPLETED**: Deduplicated existing data
2. **🔄 RECOMMENDED**: Update Reddit fetcher with deduplication logic
3. **🔄 RECOMMENDED**: Set up monitoring alerts
4. **🔄 RECOMMENDED**: Schedule weekly cleanup jobs
5. **🔄 RECOMMENDED**: Test fetcher with small batches first

---

## 📁 **Files Created**

- `reddit_events_backup` - Original data backup (430 rows)
- `deduplicate_reddit.sql` - Deduplication script
- `REDDIT_DEDUPLICATION_REPORT.md` - This report

---

## 🚀 **Dashboard Status**

Your TD Bank complaint clustering dashboard is now running with **clean, deduplicated data**:

- **Real complaint counts** (no inflation from duplicates)
- **Accurate trend analysis**
- **Proper cluster distributions**
- **Reliable insights** for decision making

**Dashboard URL**: `http://localhost:3000`

---

**✅ Deduplication Complete - Your data is now clean and ready for analysis!**
