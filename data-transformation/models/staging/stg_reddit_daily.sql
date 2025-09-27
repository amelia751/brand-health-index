{{ config(
    materialized='view',
    description='Daily aggregated Reddit metrics by brand and country'
) }}

with reddit_with_sentiment as (
    select 
        *,
        -- Placeholder for sentiment analysis (would be populated by Vertex AI)
        case
            when regexp_contains(lower(combined_text), r'\b(love|great|excellent|amazing|best|good|recommend|solid)\b') then 0.5
            when regexp_contains(lower(combined_text), r'\b(hate|terrible|awful|worst|bad|horrible|avoid|scam)\b') then -0.5
            else 0.0
        end as sentiment_score,
        
        case
            when regexp_contains(lower(combined_text), r'\b(damn|shit|fuck|wtf|stupid|idiot|asshole)\b') then 0.8
            else 0.1
        end as toxicity_score
        
    from {{ ref('raw_reddit_messages') }}
),

daily_aggregates as (
    select
        brand_id,
        date,
        coalesce(geo_country, 'Unknown') as country,
        
        -- Volume metrics
        count(*) as messages,
        count(distinct author) as unique_authors,
        count(distinct subreddit) as subreddits,
        sum(case when type = 'post' then 1 else 0 end) as posts,
        sum(case when type = 'comment' then 1 else 0 end) as comments,
        
        -- Engagement metrics (Reddit score = upvotes - downvotes)
        sum(score) as score_sum,
        avg(score) as score_avg,
        sum(case when score > 0 then 1 else 0 end) / count(*) as positive_score_rate,
        sum(case when score < 0 then 1 else 0 end) / count(*) as negative_score_rate,
        
        -- Sentiment metrics
        avg(sentiment_score) as sentiment_avg,
        stddev(sentiment_score) as sentiment_stddev,
        sum(case when sentiment_score > 0.1 then 1 else 0 end) / count(*) as positive_sentiment_rate,
        sum(case when sentiment_score < -0.1 then 1 else 0 end) / count(*) as negative_sentiment_rate,
        
        -- Quality metrics
        avg(toxicity_score) as toxicity_avg,
        sum(case when toxicity_score > 0.7 then 1 else 0 end) / count(*) as toxicity_rate,
        
        -- Content metrics
        avg(text_length) as avg_text_length,
        sum(case when upvote_ratio is not null and upvote_ratio > 0.8 then 1 else 0 end) / 
            nullif(sum(case when upvote_ratio is not null then 1 else 0 end), 0) as high_upvote_ratio_rate
        
    from reddit_with_sentiment
    group by brand_id, date, country
),

-- Calculate volume index (0-100) relative to peer brands
volume_indexed as (
    select 
        *,
        -- Volume index: percentile rank within same date across brands
        percent_rank() over (
            partition by date, country 
            order by messages
        ) * 100 as volume_index,
        
        -- Sentiment index: rescale from -1,1 to 0,100
        (sentiment_avg + 1) * 50 as sentiment_index_raw
        
    from daily_aggregates
)

select 
    brand_id,
    date,
    country,
    messages,
    unique_authors,
    subreddits,
    posts,
    comments,
    score_sum,
    score_avg,
    positive_score_rate,
    negative_score_rate,
    sentiment_avg,
    sentiment_stddev,
    positive_sentiment_rate,
    negative_sentiment_rate,
    toxicity_avg,
    toxicity_rate,
    avg_text_length,
    high_upvote_ratio_rate,
    volume_index,
    
    -- Final sentiment index (0-100, clamped)
    greatest(0, least(100, sentiment_index_raw)) as sentiment_index,
    
    -- Data quality flags
    case when messages >= 5 then true else false end as min_sample_size_met,
    case when abs(sentiment_avg) > 2 * coalesce(sentiment_stddev, 0.5) then true else false end as anomaly_flag,
    true as data_freshness_ok
    
from volume_indexed
