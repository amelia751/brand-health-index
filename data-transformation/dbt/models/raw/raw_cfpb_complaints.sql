{{ config(
    materialized='view',
    description='Raw CFPB consumer complaints data with text processing'
) }}

select
    Complaint_ID as complaint_id,
    'td_bank' as brand_id,
    Consumer_complaint_narrative as complaint_text,
    Product as product,
    Sub_product as sub_product,
    Issue as issue,
    Sub_issue as sub_issue,
    Company as company,
    State as state,
    ZIP_code as zip_code,
    Company_response_to_consumer as company_response,
    Timely_response as timely_response,
    Consumer_disputed as consumer_disputed,
    Submitted_via as submitted_via,
    
    -- Date parsing
    PARSE_DATE('%m/%d/%y', Date_received) as complaint_date,
    CASE 
        WHEN Date_sent_to_company IS NOT NULL AND Date_sent_to_company != ''
        THEN PARSE_DATE('%m/%d/%y', Date_sent_to_company)
        ELSE NULL
    END as date_sent_to_company,
    
    -- Text processing fields
    LENGTH(Consumer_complaint_narrative) as text_length,
    CASE 
        WHEN Consumer_complaint_narrative IS NOT NULL 
         AND Consumer_complaint_narrative != ''
         AND LENGTH(Consumer_complaint_narrative) > 20
        THEN true
        ELSE false
    END as has_meaningful_text,
    
    -- Severity indicators based on text patterns
    CASE 
        WHEN LOWER(Consumer_complaint_narrative) LIKE '%fraud%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%scam%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%illegal%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%lawsuit%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%attorney%'
        THEN 'high'
        WHEN LOWER(Consumer_complaint_narrative) LIKE '%problem%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%issue%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%error%'
          OR LOWER(Consumer_complaint_narrative) LIKE '%wrong%'
        THEN 'medium'
        ELSE 'low'
    END as severity_level,
    
    -- Always true for CFPB data since these are all complaints
    true as is_complaint

from {{ source('brand_health_raw', 'cfpb_complaints') }}

where 
    Consumer_complaint_narrative IS NOT NULL
    AND Consumer_complaint_narrative != ''
    AND LENGTH(Consumer_complaint_narrative) > 20