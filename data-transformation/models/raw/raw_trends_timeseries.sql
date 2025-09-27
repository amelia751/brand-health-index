{{ config(
    materialized='view',
    description='Raw Google Trends data from Fivetran ingestion'
) }}

select
    brand_id,
    keyword,
    geo,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event) as ts_event,
    date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) as date,
    value,
    category,
    is_brand_keyword,
    related_queries_top,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', collected_at) as collected_at

from {{ source('fivetran_raw', 'trends_timeseries') }}

where 
    -- Data quality filters
    brand_id is not null
    and keyword is not null
    and value is not null
    and value >= 0
    
    -- Date range filter
    {% if var('start_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) >= '{{ var("start_date") }}'
    {% endif %}
    
    {% if var('end_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) <= '{{ var("end_date") }}'
    {% endif %}
