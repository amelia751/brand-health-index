{{ config(
    materialized='table',
    description='Text preprocessing and feature extraction for complaint clustering'
) }}

with text_preprocessing as (
    select
        complaint_id,
        source_type,
        brand_id,
        complaint_text,
        complaint_date,
        severity_level,
        topics,
        source_detail,
        
        -- Clean and normalize text
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER(complaint_text),
                    r'[^a-zA-Z0-9\s]', ' '  -- Remove special characters
                ),
                r'\s+', ' '  -- Normalize whitespace
            ),
            r'^\s+|\s+$', ''  -- Trim
        ) as cleaned_text,
        
        -- Extract key financial terms
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(account|checking|savings|deposit)\b') THEN 1 ELSE 0 END as has_account_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(card|credit|debit|payment)\b') THEN 1 ELSE 0 END as has_card_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(loan|mortgage|finance|debt)\b') THEN 1 ELSE 0 END as has_loan_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(fee|charge|cost|money|dollar)\b') THEN 1 ELSE 0 END as has_fee_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(atm|branch|online|mobile|app)\b') THEN 1 ELSE 0 END as has_channel_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(fraud|scam|unauthorized|stolen)\b') THEN 1 ELSE 0 END as has_fraud_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(customer service|support|representative|call)\b') THEN 1 ELSE 0 END as has_service_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(error|mistake|wrong|incorrect)\b') THEN 1 ELSE 0 END as has_error_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(close|closed|closure|terminate)\b') THEN 1 ELSE 0 END as has_closure_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(dispute|disagree|unfair|unreasonable)\b') THEN 1 ELSE 0 END as has_dispute_terms,
        
        -- Sentiment indicators
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(terrible|awful|horrible|worst|hate)\b') THEN 1 ELSE 0 END as has_negative_sentiment,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'\b(frustrated|angry|upset|disappointed)\b') THEN 1 ELSE 0 END as has_emotional_terms,
        
        -- Extract common complaint patterns
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'(can.t|cannot|unable|won.t|will not)') THEN 1 ELSE 0 END as has_inability_terms,
        CASE WHEN REGEXP_CONTAINS(LOWER(complaint_text), r'(never|always|every time|constantly)') THEN 1 ELSE 0 END as has_frequency_terms,
        
        -- Text statistics
        LENGTH(complaint_text) as original_length,
        ARRAY_LENGTH(SPLIT(complaint_text, ' ')) as word_count,
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL(complaint_text, r'[.!?]')) as sentence_count
        
    from {{ ref('stg_unified_complaints') }}
),

feature_vectors as (
    select
        *,
        -- Create a simple feature vector for clustering
        CONCAT(
            CAST(has_account_terms as STRING), ',',
            CAST(has_card_terms as STRING), ',',
            CAST(has_loan_terms as STRING), ',',
            CAST(has_fee_terms as STRING), ',',
            CAST(has_channel_terms as STRING), ',',
            CAST(has_fraud_terms as STRING), ',',
            CAST(has_service_terms as STRING), ',',
            CAST(has_error_terms as STRING), ',',
            CAST(has_closure_terms as STRING), ',',
            CAST(has_dispute_terms as STRING), ',',
            CAST(has_negative_sentiment as STRING), ',',
            CAST(has_emotional_terms as STRING), ',',
            CAST(has_inability_terms as STRING), ',',
            CAST(has_frequency_terms as STRING)
        ) as feature_vector,
        
        -- Calculate feature score for simple clustering
        (has_account_terms + has_card_terms + has_loan_terms + has_fee_terms + 
         has_channel_terms + has_fraud_terms + has_service_terms + has_error_terms + 
         has_closure_terms + has_dispute_terms + has_negative_sentiment + 
         has_emotional_terms + has_inability_terms + has_frequency_terms) as feature_score
         
    from text_preprocessing
)

select * from feature_vectors
