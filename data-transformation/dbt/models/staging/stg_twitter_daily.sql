{{ config(
    materialized='view',
    description='Daily aggregated Twitter metrics by brand and country'
) }}

with twitter_with_sentiment as (
    select 
        *,
        -- Placeholder for sentiment analysis (would be populated by Vertex AI)
        -- For now, using simple heuristics
        case
            when regexp_contains(lower(text), r'\b(love|great|excellent|amazing|best|good|thank|thanks)\b') then 0.5
            when regexp_contains(lower(text), r'\b(hate|terrible|awful|worst|bad|horrible|sucks|disappointed)\b') then -0.5
            else 0.0
        end as sentiment_score,
        
        case
            when regexp_contains(lower(text), r'\b(damn|shit|fuck|wtf|stupid|idiot)\b') then 0.8
            else 0.1
        end as toxicity_score
        
    from {{ ref('raw_twitter_posts') }}
),

daily_aggregates as (
    select
        brand_id,
        date,
        coalesce(geo_country, 'Unknown') as country,
        
        -- Volume metrics
        count(*) as posts,
        count(distinct author_id) as authors,
        sum(total_engagements) as engagements,
        
        -- Sentiment metrics
        avg(sentiment_score) as sentiment_avg,
        stddev(sentiment_score) as sentiment_stddev,
        sum(case when sentiment_score > 0.1 then 1 else 0 end) / count(*) as positive_rate,
        sum(case when sentiment_score < -0.1 then 1 else 0 end) / count(*) as negative_rate,
        
        -- Quality metrics
        avg(toxicity_score) as toxicity_avg,
        sum(case when toxicity_score > 0.7 then 1 else 0 end) / count(*) as toxicity_rate,
        
        -- Engagement metrics
        avg(total_engagements) as avg_engagements_per_post,
        sum(case when total_engagements > 10 then 1 else 0 end) / count(*) as high_engagement_rate,
        
        -- Content metrics
        avg(text_length) as avg_text_length,
        sum(case when possibly_sensitive then 1 else 0 end) / count(*) as sensitive_content_rate
        
    from twitter_with_sentiment
    group by brand_id, date, country
),

-- Calculate volume index (0-100) relative to peer brands
volume_indexed as (
    select 
        *,
        -- Volume index: percentile rank within same date across brands
        percent_rank() over (
            partition by date, country 
            order by posts
        ) * 100 as volume_index,
        
        -- Sentiment index: rescale from -1,1 to 0,100
        (sentiment_avg + 1) * 50 as sentiment_index_raw
        
    from daily_aggregates
)

select 
    brand_id,
    date,
    country,
    posts,
    authors,
    engagements,
    sentiment_avg,
    sentiment_stddev,
    positive_rate,
    negative_rate,
    toxicity_avg,
    toxicity_rate,
    avg_engagements_per_post,
    high_engagement_rate,
    avg_text_length,
    sensitive_content_rate,
    volume_index,
    
    -- Final sentiment index (0-100, clamped)
    greatest(0, least(100, sentiment_index_raw)) as sentiment_index,
    
    -- Data quality flags
    case when posts >= 10 then true else false end as min_sample_size_met,
    case when abs(sentiment_avg) > 2 * coalesce(sentiment_stddev, 0.5) then true else false end as anomaly_flag,
    true as data_freshness_ok -- Would be calculated based on collection timestamp
    
from volume_indexed
