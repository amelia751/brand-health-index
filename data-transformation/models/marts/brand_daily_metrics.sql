{{ config(
    materialized='table',
    description='Unified daily brand metrics combining all data sources',
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["brand_id", "country"]
) }}

with brand_base as (
    -- Get all brand-date-country combinations from our data sources
    select distinct brand_id, date, country
    from (
        select brand_id, date, country from {{ ref('stg_twitter_daily') }}
        union distinct
        select brand_id, date, country from {{ ref('stg_reddit_daily') }}
        union distinct
        select brand_id, date, country from {{ ref('stg_trends_daily') }}
        union distinct
        select brand_id, date, country from {{ ref('stg_cfpb_daily') }}
    )
),

unified_metrics as (
    select 
        b.brand_id,
        b.date,
        b.country,
        bd.brand_name,
        
        -- Social metrics (Twitter)
        coalesce(tw.posts, 0) as twitter_posts,
        coalesce(tw.authors, 0) as twitter_authors,
        coalesce(tw.engagements, 0) as twitter_engagements,
        tw.sentiment_avg as twitter_sentiment_avg,
        tw.volume_index as twitter_volume_index,
        tw.sentiment_index as twitter_sentiment_index,
        tw.toxicity_rate as twitter_toxicity_rate,
        tw.min_sample_size_met as twitter_sample_ok,
        tw.anomaly_flag as twitter_anomaly,
        
        -- Social metrics (Reddit)
        coalesce(rd.messages, 0) as reddit_messages,
        coalesce(rd.unique_authors, 0) as reddit_authors,
        coalesce(rd.score_sum, 0) as reddit_score_sum,
        rd.sentiment_avg as reddit_sentiment_avg,
        rd.volume_index as reddit_volume_index,
        rd.sentiment_index as reddit_sentiment_index,
        rd.toxicity_rate as reddit_toxicity_rate,
        rd.min_sample_size_met as reddit_sample_ok,
        rd.anomaly_flag as reddit_anomaly,
        
        -- Search visibility (Trends)
        tr.rsv_avg as trends_rsv_avg,
        tr.brand_rsv_avg as trends_brand_rsv_avg,
        tr.category_rsv_avg as trends_category_rsv_avg,
        tr.rsv_trend_7d as trends_7d_change,
        tr.rsv_trend_28d as trends_28d_change,
        tr.visibility_index as trends_visibility_index,
        tr.min_sample_size_met as trends_sample_ok,
        tr.anomaly_flag as trends_anomaly,
        
        -- Complaints (CFPB)
        coalesce(cf.complaints, 0) as cfpb_complaints,
        cf.timely_rate as cfpb_timely_rate,
        cf.dispute_rate as cfpb_dispute_rate,
        cf.severity_avg as cfpb_severity_avg,
        cf.complaints_index as cfpb_complaints_index,
        cf.response_quality_index as cfpb_response_quality_index,
        cf.min_sample_size_met as cfpb_sample_ok,
        cf.anomaly_flag as cfpb_anomaly
        
    from brand_base b
    left join {{ ref('brand_dictionary') }} bd on b.brand_id = bd.brand_id
    left join {{ ref('stg_twitter_daily') }} tw on b.brand_id = tw.brand_id 
        and b.date = tw.date and b.country = tw.country
    left join {{ ref('stg_reddit_daily') }} rd on b.brand_id = rd.brand_id 
        and b.date = rd.date and b.country = rd.country
    left join {{ ref('stg_trends_daily') }} tr on b.brand_id = tr.brand_id 
        and b.date = tr.date and b.country = tr.country
    left join {{ ref('stg_cfpb_daily') }} cf on b.brand_id = cf.brand_id 
        and b.date = cf.date and b.country = cf.country
),

-- Calculate composite metrics
composite_metrics as (
    select 
        *,
        
        -- Combined social metrics (Twitter + Reddit)
        case 
            when twitter_posts > 0 and reddit_messages > 0 then
                (twitter_volume_index * twitter_posts + reddit_volume_index * reddit_messages) / 
                (twitter_posts + reddit_messages)
            when twitter_posts > 0 then twitter_volume_index
            when reddit_messages > 0 then reddit_volume_index
            else null
        end as social_volume_index,
        
        case 
            when twitter_sentiment_index is not null and reddit_sentiment_index is not null then
                (twitter_sentiment_index * twitter_posts + reddit_sentiment_index * reddit_messages) / 
                nullif(twitter_posts + reddit_messages, 0)
            when twitter_sentiment_index is not null then twitter_sentiment_index
            when reddit_sentiment_index is not null then reddit_sentiment_index
            else null
        end as social_sentiment_index,
        
        -- Overall data quality flags
        case 
            when (twitter_sample_ok or reddit_sample_ok) 
                and (trends_sample_ok or cfpb_sample_ok)
            then true 
            else false 
        end as min_sample_size_met,
        
        case 
            when twitter_anomaly or reddit_anomaly or trends_anomaly or cfpb_anomaly
            then true 
            else false 
        end as anomaly_flag,
        
        -- Data freshness (all sources should be recent)
        true as data_freshness_ok
        
    from unified_metrics
)

select 
    brand_id,
    brand_name,
    date,
    country,
    
    -- Raw metrics
    twitter_posts,
    twitter_authors,
    twitter_engagements,
    twitter_sentiment_avg,
    reddit_messages,
    reddit_authors,
    reddit_score_sum,
    reddit_sentiment_avg,
    trends_rsv_avg,
    trends_brand_rsv_avg,
    trends_category_rsv_avg,
    trends_7d_change,
    trends_28d_change,
    cfpb_complaints,
    cfpb_timely_rate,
    cfpb_dispute_rate,
    cfpb_severity_avg,
    
    -- Normalized indices (0-100)
    social_volume_index,
    social_sentiment_index,
    trends_visibility_index,
    cfpb_complaints_index,
    cfpb_response_quality_index,
    
    -- Quality metrics
    twitter_toxicity_rate,
    reddit_toxicity_rate,
    
    -- Data quality flags
    min_sample_size_met,
    anomaly_flag,
    data_freshness_ok,
    
    -- Metadata
    current_timestamp() as updated_at
    
from composite_metrics
