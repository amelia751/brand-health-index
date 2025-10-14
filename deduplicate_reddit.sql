-- Reddit Data Deduplication Script
-- This script removes duplicate Reddit events while keeping the most recent version

-- Step 1: Create a backup of the original table
CREATE OR REPLACE TABLE `brand_health_raw.reddit_events_backup` AS
SELECT * FROM `brand_health_raw.reddit_events`;

-- Step 2: Create a deduplicated version
-- We'll keep the row with the latest nlp_processed_at timestamp for each event_id
CREATE OR REPLACE TABLE `brand_health_raw.reddit_events_deduped` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id 
      ORDER BY 
        nlp_processed_at DESC NULLS LAST,
        ts_event DESC,
        metadata.created_utc DESC NULLS LAST
    ) as row_num
  FROM `brand_health_raw.reddit_events`
)
WHERE row_num = 1;

-- Step 3: Verify the deduplication worked
SELECT 
  'Original' as table_name,
  COUNT(*) as total_rows,
  COUNT(DISTINCT event_id) as unique_events,
  COUNT(*) - COUNT(DISTINCT event_id) as duplicate_count
FROM `brand_health_raw.reddit_events`

UNION ALL

SELECT 
  'Deduplicated' as table_name,
  COUNT(*) as total_rows,
  COUNT(DISTINCT event_id) as unique_events,
  COUNT(*) - COUNT(DISTINCT event_id) as duplicate_count
FROM `brand_health_raw.reddit_events_deduped`

ORDER BY table_name;
