{{ config(
    materialized='view',
    description='Daily aggregated CFPB complaints metrics by brand'
) }}

with complaints_enhanced as (
    select 
        *,
        -- Enhanced severity scoring
        case
            when consumer_disputed and not timely_response then severity_score + 0.2
            when consumer_disputed then severity_score + 0.1
            when not timely_response then severity_score + 0.1
            else severity_score
        end as enhanced_severity_score,
        
        -- Product categorization
        case
            when lower(product) like '%mortgage%' then 'Mortgage'
            when lower(product) like '%credit card%' then 'Credit Card'
            when lower(product) like '%checking%' or lower(product) like '%savings%' then 'Deposit Account'
            when lower(product) like '%loan%' then 'Loan'
            when lower(product) like '%debt collection%' then 'Debt Collection'
            else 'Other'
        end as product_category
        
    from {{ ref('raw_cfpb_complaints') }}
),

daily_aggregates as (
    select
        brand_id,
        date,
        geo_country as country,
        
        -- Volume metrics
        count(*) as complaints,
        count(distinct complaint_id) as unique_complaints,
        count(distinct state) as states_affected,
        
        -- Response metrics
        sum(case when timely_response then 1 else 0 end) / count(*) as timely_rate,
        sum(case when consumer_disputed then 1 else 0 end) / count(*) as dispute_rate,
        sum(case when has_narrative then 1 else 0 end) / count(*) as narrative_rate,
        sum(case when company_public_response is not null then 1 else 0 end) / count(*) as public_response_rate,
        
        -- Severity metrics
        avg(severity_score) as severity_avg,
        avg(enhanced_severity_score) as enhanced_severity_avg,
        max(severity_score) as severity_max,
        stddev(severity_score) as severity_stddev,
        
        -- Product breakdown
        count(distinct product_category) as product_categories,
        mode() within group (order by product_category) as top_product_category,
        
        -- Content metrics
        avg(narrative_length) as avg_narrative_length,
        sum(case when narrative_length > 500 then 1 else 0 end) / count(*) as long_narrative_rate,
        
        -- Geographic distribution
        mode() within group (order by state) as top_state,
        count(distinct zip_code) as zip_codes_affected
        
    from complaints_enhanced
    group by brand_id, date, country
),

-- Calculate complaint rates and indices
complaints_indexed as (
    select 
        *,
        -- Complaints index: inverse percentile (fewer complaints = higher score)
        (100 - percent_rank() over (
            partition by date, country 
            order by complaints
        ) * 100) as complaints_volume_index,
        
        -- Severity index: inverse of severity (lower severity = higher score)
        (100 - percent_rank() over (
            partition by date, country 
            order by enhanced_severity_avg
        ) * 100) as complaints_severity_index,
        
        -- Response quality index
        (timely_rate * 0.6 + (1 - dispute_rate) * 0.4) * 100 as response_quality_index
        
    from daily_aggregates
)

select 
    brand_id,
    date,
    country,
    complaints,
    unique_complaints,
    states_affected,
    timely_rate,
    dispute_rate,
    narrative_rate,
    public_response_rate,
    severity_avg,
    enhanced_severity_avg,
    severity_max,
    severity_stddev,
    product_categories,
    top_product_category,
    avg_narrative_length,
    long_narrative_rate,
    top_state,
    zip_codes_affected,
    complaints_volume_index,
    complaints_severity_index,
    response_quality_index,
    
    -- Combined complaints index (volume + severity + response quality)
    (complaints_volume_index * 0.4 + 
     complaints_severity_index * 0.4 + 
     response_quality_index * 0.2) as complaints_index,
    
    -- Data quality flags
    case when complaints >= 1 then true else false end as min_sample_size_met,
    case when complaints > 100 or enhanced_severity_avg > 0.9 then true else false end as anomaly_flag,
    true as data_freshness_ok
    
from complaints_indexed
