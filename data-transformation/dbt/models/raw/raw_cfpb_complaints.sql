{{ config(
    materialized='view',
    description='Raw CFPB consumer complaints data from Fivetran ingestion'
) }}

select
    brand_id,
    complaint_id,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event) as ts_event,
    date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) as date,
    product,
    sub_product,
    issue,
    sub_issue,
    consumer_complaint_narrative,
    company_response_to_consumer,
    timely_response,
    consumer_disputed,
    submitted_via,
    case 
        when date_sent_to_company is not null 
        then parse_timestamp('%Y-%m-%dT%H:%M:%E*S', date_sent_to_company)
        else null
    end as date_sent_to_company,
    company_public_response,
    tags,
    state,
    zip_code,
    geo_country,
    severity_score,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', collected_at) as collected_at,
    
    -- Add derived fields
    case 
        when consumer_complaint_narrative is not null 
        then length(consumer_complaint_narrative)
        else 0
    end as narrative_length,
    
    case 
        when consumer_complaint_narrative is not null and consumer_complaint_narrative != ''
        then true
        else false
    end as has_narrative

from {{ source('fivetran_raw', 'cfpb_complaints') }}

where 
    -- Data quality filters
    complaint_id is not null
    and brand_id is not null
    
    -- Date range filter
    {% if var('start_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) >= '{{ var("start_date") }}'
    {% endif %}
    
    {% if var('end_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) <= '{{ var("end_date") }}'
    {% endif %}
