{{ config(
    materialized='view',
    description='Unified complaints data from Reddit and CFPB sources for clustering analysis'
) }}

-- Reddit complaints
select
    event_id as complaint_id,
    'reddit' as source_type,
    brand_id,
    text as complaint_text,
    event_date as complaint_date,
    sentiment,
    severity,
    topics,
    subreddit as source_detail,
    author,
    score,
    text_length,
    is_complaint,
    CASE 
        WHEN severity > 0.7 THEN 'high'
        WHEN severity > 0.3 THEN 'medium'
        ELSE 'low'
    END as severity_level,
    nlp_confidence,
    geo_country

from {{ ref('raw_reddit_events') }}
where is_complaint = true

UNION ALL

-- CFPB complaints
select
    complaint_id,
    'cfpb' as source_type,
    brand_id,
    complaint_text,
    complaint_date,
    NULL as sentiment,  -- Not available for CFPB
    CASE 
        WHEN severity_level = 'high' THEN 0.8
        WHEN severity_level = 'medium' THEN 0.5
        ELSE 0.2
    END as severity,
    [issue, sub_issue] as topics,  -- Use issue categories as topics
    product as source_detail,
    NULL as author,
    NULL as score,
    text_length,
    is_complaint,
    severity_level,
    NULL as nlp_confidence,
    'US' as geo_country  -- CFPB is US-only

from {{ ref('raw_cfpb_complaints') }}
