{{ config(
    materialized='view',
    description='Raw Reddit posts and comments data with complaint text extraction'
) }}

select
    event_id,
    brand_id,
    source,
    text,
    sentiment,
    severity,
    topics,
    ts_event,
    DATE(ts_event) as event_date,
    nlp_confidence,
    nlp_model,
    nlp_processed_at,
    language,
    geo_country,
    
    -- Extract metadata fields
    JSON_EXTRACT_SCALAR(metadata, '$.subreddit') as subreddit,
    JSON_EXTRACT_SCALAR(metadata, '$.author') as author,
    JSON_EXTRACT_SCALAR(metadata, '$.reddit_type') as reddit_type,
    JSON_EXTRACT_SCALAR(metadata, '$.title') as title,
    CAST(JSON_EXTRACT_SCALAR(metadata, '$.score') AS INT64) as score,
    CAST(JSON_EXTRACT_SCALAR(metadata, '$.num_comments') AS INT64) as num_comments,
    
    -- Text processing fields
    LENGTH(text) as text_length,
    CASE 
        WHEN text IS NOT NULL AND LENGTH(text) > 10 THEN true
        ELSE false
    END as has_meaningful_text,
    
    -- Complaint indicators
    CASE 
        WHEN LOWER(text) LIKE '%problem%' 
          OR LOWER(text) LIKE '%issue%'
          OR LOWER(text) LIKE '%complaint%'
          OR LOWER(text) LIKE '%error%'
          OR LOWER(text) LIKE '%wrong%'
          OR LOWER(text) LIKE '%bad%'
          OR LOWER(text) LIKE '%terrible%'
          OR LOWER(text) LIKE '%awful%'
          OR LOWER(text) LIKE '%frustrated%'
          OR LOWER(text) LIKE '%angry%'
          OR sentiment < 0
        THEN true
        ELSE false
    END as is_complaint

from {{ source('brand_health_raw', 'reddit_events') }}

where 
    text IS NOT NULL
    AND LENGTH(text) > 10
    AND brand_id = 'td_bank'
