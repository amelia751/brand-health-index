{{ config(
    materialized='table',
    description='Executive summary of complaint trends for dashboard display'
) }}

with recent_period as (
    select
        complaint_date,
        cluster_name,
        cluster_description,
        cluster_priority,
        daily_complaint_count,
        avg_severity_score,
        sample_complaints,
        reddit_count,
        cfpb_count
    from {{ ref('mart_daily_complaint_trends') }}
    where complaint_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),

cluster_trends as (
    select
        cluster_name,
        cluster_description,
        cluster_priority,
        COUNT(DISTINCT complaint_date) as days_active,
        SUM(daily_complaint_count) as total_complaints,
        AVG(daily_complaint_count) as avg_daily_complaints,
        AVG(avg_severity_score) as avg_severity,
        MAX(daily_complaint_count) as peak_daily_complaints,
        MIN(complaint_date) as first_seen,
        MAX(complaint_date) as last_seen,
        
        -- Trend direction (simple linear trend)
        CASE 
            WHEN COUNT(DISTINCT complaint_date) >= 7 THEN
                CASE 
                    WHEN (
                        SUM(CASE WHEN complaint_date >= DATE_SUB(MAX(complaint_date), INTERVAL 7 DAY) 
                                 THEN daily_complaint_count ELSE 0 END) / 7.0
                    ) > (
                        SUM(CASE WHEN complaint_date < DATE_SUB(MAX(complaint_date), INTERVAL 7 DAY) 
                                 THEN daily_complaint_count ELSE 0 END) / GREATEST(COUNT(DISTINCT complaint_date) - 7, 1)
                    ) * 1.2 THEN 'increasing'
                    WHEN (
                        SUM(CASE WHEN complaint_date >= DATE_SUB(MAX(complaint_date), INTERVAL 7 DAY) 
                                 THEN daily_complaint_count ELSE 0 END) / 7.0
                    ) < (
                        SUM(CASE WHEN complaint_date < DATE_SUB(MAX(complaint_date), INTERVAL 7 DAY) 
                                 THEN daily_complaint_count ELSE 0 END) / GREATEST(COUNT(DISTINCT complaint_date) - 7, 1)
                    ) * 0.8 THEN 'decreasing'
                    ELSE 'stable'
                END
            ELSE 'insufficient_data'
        END as trend_direction,
        
        -- Sample recent complaints
        ARRAY_AGG(
            STRUCT(
                complaint_date,
                daily_complaint_count,
                sample_complaints
            )
            ORDER BY complaint_date DESC
            LIMIT 7
        ) as recent_daily_samples
        
    from recent_period
    GROUP BY cluster_name, cluster_description, cluster_priority
),

daily_summary as (
    select
        complaint_date,
        SUM(daily_complaint_count) as total_complaints,
        COUNT(DISTINCT cluster_name) as active_clusters,
        AVG(avg_severity_score) as overall_severity,
        
        -- Top 3 clusters by volume each day
        ARRAY_AGG(
            STRUCT(
                cluster_name,
                cluster_description,
                daily_complaint_count,
                avg_severity_score,
                ARRAY(
                    SELECT AS STRUCT 
                        complaint_id,
                        source_type,
                        complaint_preview,
                        severity_level
                    FROM UNNEST(sample_complaints)
                    LIMIT 3
                ) as top_complaints
            )
            ORDER BY daily_complaint_count DESC
            LIMIT 3
        ) as top_clusters
        
    from recent_period
    GROUP BY complaint_date
    ORDER BY complaint_date DESC
    LIMIT 30  -- Last 30 days
)

select
    'cluster_summary' as report_type,
    CURRENT_DATETIME() as generated_at,
    ct.cluster_name,
    ct.cluster_description,
    ct.cluster_priority,
    ct.days_active,
    ct.total_complaints,
    ct.avg_daily_complaints,
    ct.avg_severity,
    ct.peak_daily_complaints,
    ct.first_seen,
    ct.last_seen,
    ct.trend_direction,
    ct.recent_daily_samples,
    NULL as complaint_date,
    NULL as daily_total_complaints,
    NULL as daily_active_clusters,
    NULL as daily_overall_severity,
    NULL as daily_top_clusters
    
from cluster_trends ct

UNION ALL

select
    'daily_summary' as report_type,
    CURRENT_DATETIME() as generated_at,
    NULL as cluster_name,
    NULL as cluster_description,
    NULL as cluster_priority,
    NULL as days_active,
    NULL as total_complaints,
    NULL as avg_daily_complaints,
    NULL as avg_severity,
    NULL as peak_daily_complaints,
    NULL as first_seen,
    NULL as last_seen,
    NULL as trend_direction,
    NULL as recent_daily_samples,
    ds.complaint_date,
    ds.total_complaints as daily_total_complaints,
    ds.active_clusters as daily_active_clusters,
    ds.overall_severity as daily_overall_severity,
    ds.top_clusters as daily_top_clusters
    
from daily_summary ds

ORDER BY 
    report_type,
    COALESCE(cluster_priority, 999),
    COALESCE(complaint_date, DATE('1900-01-01')) DESC
