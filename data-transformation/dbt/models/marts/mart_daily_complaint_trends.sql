{{ config(
    materialized='table',
    description='Daily complaint trends by cluster for dashboard visualization'
) }}

with daily_cluster_stats as (
    select
        complaint_date,
        cluster_name,
        cluster_description,
        cluster_priority,
        COUNT(*) as daily_complaint_count,
        COUNT(DISTINCT complaint_id) as unique_complaints,
        AVG(CASE WHEN severity_level = 'high' THEN 1.0 WHEN severity_level = 'medium' THEN 0.5 ELSE 0.0 END) as avg_severity_score,
        COUNT(CASE WHEN source_type = 'reddit' THEN 1 END) as reddit_count,
        COUNT(CASE WHEN source_type = 'cfpb' THEN 1 END) as cfpb_count,
        
        -- Sample complaints for each cluster/day
        ARRAY_AGG(
            STRUCT(
                complaint_id,
                source_type,
                SUBSTR(complaint_text, 1, 200) as complaint_preview,
                severity_level,
                source_detail
            ) 
            ORDER BY 
                CASE WHEN severity_level = 'high' THEN 1 WHEN severity_level = 'medium' THEN 2 ELSE 3 END,
                LENGTH(complaint_text) DESC
            LIMIT 5
        ) as sample_complaints,
        
        -- Top keywords/features for this cluster on this day
        SUM(has_account_terms) as account_mentions,
        SUM(has_card_terms) as card_mentions,
        SUM(has_loan_terms) as loan_mentions,
        SUM(has_fee_terms) as fee_mentions,
        SUM(has_fraud_terms) as fraud_mentions,
        SUM(has_service_terms) as service_mentions,
        SUM(has_error_terms) as error_mentions
        
    from {{ ref('mart_complaint_clusters') }}
    GROUP BY complaint_date, cluster_name, cluster_description, cluster_priority
),

date_range_stats as (
    select
        complaint_date,
        COUNT(DISTINCT cluster_name) as active_clusters,
        SUM(daily_complaint_count) as total_daily_complaints,
        AVG(avg_severity_score) as overall_daily_severity,
        
        -- Rank clusters by volume for each day
        ARRAY_AGG(
            STRUCT(
                cluster_name,
                cluster_description,
                daily_complaint_count,
                avg_severity_score,
                sample_complaints
            )
            ORDER BY daily_complaint_count DESC, avg_severity_score DESC
            LIMIT 10
        ) as top_clusters_by_volume
        
    from daily_cluster_stats
    GROUP BY complaint_date
),

weekly_trends as (
    select
        DATE_TRUNC(complaint_date, WEEK(MONDAY)) as week_start,
        cluster_name,
        cluster_description,
        SUM(daily_complaint_count) as weekly_complaint_count,
        AVG(avg_severity_score) as weekly_avg_severity,
        COUNT(DISTINCT complaint_date) as days_active_in_week,
        MIN(complaint_date) as first_complaint_in_week,
        MAX(complaint_date) as last_complaint_in_week
        
    from daily_cluster_stats
    GROUP BY DATE_TRUNC(complaint_date, WEEK(MONDAY)), cluster_name, cluster_description
)

select
    dcs.*,
    drs.active_clusters as daily_active_clusters,
    drs.total_daily_complaints,
    drs.overall_daily_severity,
    drs.top_clusters_by_volume as daily_cluster_ranking,
    
    -- Add day of week context
    EXTRACT(DAYOFWEEK FROM dcs.complaint_date) as day_of_week,
    FORMAT_DATE('%A', dcs.complaint_date) as day_name,
    
    -- Add week-over-week context
    wt.weekly_complaint_count,
    wt.weekly_avg_severity,
    wt.days_active_in_week,
    wt.week_start
    
from daily_cluster_stats dcs
LEFT JOIN date_range_stats drs ON dcs.complaint_date = drs.complaint_date
LEFT JOIN weekly_trends wt ON DATE_TRUNC(dcs.complaint_date, WEEK(MONDAY)) = wt.week_start 
    AND dcs.cluster_name = wt.cluster_name

ORDER BY dcs.complaint_date DESC, dcs.cluster_priority ASC, dcs.daily_complaint_count DESC
