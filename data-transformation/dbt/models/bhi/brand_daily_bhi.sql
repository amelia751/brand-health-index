{{ config(
    materialized='table',
    description='Daily Brand Health Index (BHI) scores and component breakdowns',
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["brand_id", "country"]
) }}

with bhi_components as (
    select 
        brand_id,
        brand_name,
        date,
        country,
        
        -- Component scores (0-100 scale)
        coalesce(social_sentiment_index, 50) as social_score,
        coalesce(trends_visibility_index, 50) as search_score,
        coalesce(cfpb_complaints_index, 50) as complaints_score,
        50 as reviews_customer_score,  -- Placeholder until reviews data is added
        50 as reviews_employee_score,  -- Placeholder until reviews data is added
        
        -- Raw metrics for context
        twitter_posts + reddit_messages as social_volume,
        trends_rsv_avg,
        cfpb_complaints,
        
        -- Quality indicators
        min_sample_size_met,
        anomaly_flag,
        data_freshness_ok
        
    from {{ ref('brand_daily_metrics') }}
),

bhi_calculated as (
    select 
        *,
        
        -- BHI weights from dbt variables
        {{ var('bhi_weights.social') }} as w_social,
        {{ var('bhi_weights.search') }} as w_search,
        {{ var('bhi_weights.complaints') }} as w_complaints,
        {{ var('bhi_weights.reviews_customer') }} as w_reviews_customer,
        {{ var('bhi_weights.reviews_employee') }} as w_reviews_employee,
        
        -- Calculate weighted BHI score
        ({{ var('bhi_weights.social') }} * social_score +
         {{ var('bhi_weights.search') }} * search_score +
         {{ var('bhi_weights.complaints') }} * complaints_score +
         {{ var('bhi_weights.reviews_customer') }} * reviews_customer_score +
         {{ var('bhi_weights.reviews_employee') }} * reviews_employee_score) as bhi_score_raw
         
    from bhi_components
),

bhi_with_rankings as (
    select 
        *,
        
        -- Final BHI score (clamped to 0-100)
        greatest(0, least(100, bhi_score_raw)) as bhi_score,
        
        -- Rankings within date and country
        rank() over (
            partition by date, country 
            order by bhi_score_raw desc
        ) as bhi_rank,
        
        -- Percentile ranking
        percent_rank() over (
            partition by date, country 
            order by bhi_score_raw
        ) * 100 as bhi_percentile,
        
        -- 7-day and 28-day changes
        lag(bhi_score_raw, 7) over (
            partition by brand_id, country 
            order by date
        ) as bhi_score_7d_ago,
        
        lag(bhi_score_raw, 28) over (
            partition by brand_id, country 
            order by date
        ) as bhi_score_28d_ago
        
    from bhi_calculated
),

final_bhi as (
    select 
        brand_id,
        brand_name,
        date,
        country,
        
        -- BHI Score and Rankings
        bhi_score,
        bhi_rank,
        bhi_percentile,
        
        -- Component Scores
        social_score,
        search_score,
        complaints_score,
        reviews_customer_score,
        reviews_employee_score,
        
        -- Component Weights
        w_social,
        w_search,
        w_complaints,
        w_reviews_customer,
        w_reviews_employee,
        
        -- Trend Analysis
        case 
            when bhi_score_7d_ago is not null and bhi_score_7d_ago > 0
            then ((bhi_score - bhi_score_7d_ago) / bhi_score_7d_ago) * 100
            else null
        end as bhi_change_7d,
        
        case 
            when bhi_score_28d_ago is not null and bhi_score_28d_ago > 0
            then ((bhi_score - bhi_score_28d_ago) / bhi_score_28d_ago) * 100
            else null
        end as bhi_change_28d,
        
        -- Supporting Metrics
        social_volume,
        trends_rsv_avg,
        cfpb_complaints,
        
        -- Data Quality
        min_sample_size_met,
        anomaly_flag,
        data_freshness_ok,
        
        -- Risk Indicators
        case 
            when bhi_score < 30 then 'High Risk'
            when bhi_score < 50 then 'Medium Risk'
            when bhi_score < 70 then 'Low Risk'
            else 'Healthy'
        end as risk_category,
        
        case 
            when bhi_change_7d < -10 then 'Declining'
            when bhi_change_7d > 10 then 'Improving'
            else 'Stable'
        end as trend_direction,
        
        -- Notes for manual review
        case 
            when anomaly_flag then 'Anomaly detected - manual review recommended'
            when not min_sample_size_met then 'Insufficient data - interpret with caution'
            when bhi_score < 25 then 'Critical BHI score - immediate attention required'
            when abs(bhi_change_7d) > 20 then 'Significant change detected'
            else null
        end as notes,
        
        -- Metadata
        'v1.0' as calc_version,
        current_timestamp() as updated_at
        
    from bhi_with_rankings
)

select * from final_bhi
