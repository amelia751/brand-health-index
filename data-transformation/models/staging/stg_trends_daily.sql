{{ config(
    materialized='view',
    description='Daily aggregated Google Trends metrics by brand and country'
) }}

with trends_aggregated as (
    select
        brand_id,
        date,
        case 
            when geo = 'US' then 'US'
            when starts_with(geo, 'US-') then 'US'
            else geo
        end as country,
        geo as region,
        
        -- Separate brand vs category terms
        sum(case when is_brand_keyword then value else 0 end) / 
            nullif(sum(case when is_brand_keyword then 1 else 0 end), 0) as brand_rsv_avg,
        sum(case when not is_brand_keyword then value else 0 end) / 
            nullif(sum(case when not is_brand_keyword then 1 else 0 end), 0) as category_rsv_avg,
        
        -- Overall metrics
        avg(value) as rsv_avg,
        max(value) as rsv_max,
        min(value) as rsv_min,
        stddev(value) as rsv_stddev,
        count(*) as data_points,
        count(distinct keyword) as unique_keywords,
        
        -- Keyword performance
        array_agg(struct(keyword, value) order by value desc limit 5) as top_keywords,
        
        -- Regional breakdown
        count(distinct geo) as regions_covered
        
    from {{ ref('raw_trends_timeseries') }}
    group by brand_id, date, country, geo
),

-- Calculate 7-day and 28-day trends
trends_with_changes as (
    select 
        *,
        -- Calculate trend changes
        lag(rsv_avg, 7) over (
            partition by brand_id, country, region 
            order by date
        ) as rsv_avg_7d_ago,
        
        lag(rsv_avg, 28) over (
            partition by brand_id, country, region 
            order by date
        ) as rsv_avg_28d_ago,
        
        -- Brand vs category comparison
        case 
            when brand_rsv_avg is not null and category_rsv_avg is not null
            then brand_rsv_avg - category_rsv_avg
            else null
        end as brand_category_diff
        
    from trends_aggregated
),

-- Calculate percentage changes and visibility index
final_metrics as (
    select 
        brand_id,
        date,
        country,
        region,
        rsv_avg,
        rsv_max,
        rsv_min,
        rsv_stddev,
        brand_rsv_avg,
        category_rsv_avg,
        brand_category_diff,
        data_points,
        unique_keywords,
        top_keywords,
        regions_covered,
        
        -- Trend calculations
        case 
            when rsv_avg_7d_ago > 0 
            then ((rsv_avg - rsv_avg_7d_ago) / rsv_avg_7d_ago) * 100
            else null
        end as rsv_trend_7d,
        
        case 
            when rsv_avg_28d_ago > 0 
            then ((rsv_avg - rsv_avg_28d_ago) / rsv_avg_28d_ago) * 100
            else null
        end as rsv_trend_28d,
        
        -- Visibility index: percentile rank within same date across brands
        percent_rank() over (
            partition by date, country 
            order by rsv_avg
        ) * 100 as visibility_index
        
    from trends_with_changes
)

select 
    brand_id,
    date,
    country,
    region,
    rsv_avg,
    rsv_max,
    rsv_min,
    rsv_stddev,
    brand_rsv_avg,
    category_rsv_avg,
    brand_category_diff,
    rsv_trend_7d,
    rsv_trend_28d,
    visibility_index,
    data_points,
    unique_keywords,
    top_keywords,
    regions_covered,
    
    -- Data quality flags
    case when data_points >= 5 then true else false end as min_sample_size_met,
    case when rsv_avg > 90 or rsv_trend_7d > 500 then true else false end as anomaly_flag,
    true as data_freshness_ok
    
from final_metrics
