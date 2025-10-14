{{ config(
    materialized='table',
    description='Complaint clustering analysis with interpretable cluster assignments'
) }}

with cluster_assignment as (
    select
        *,
        -- Rule-based clustering logic
        CASE
            -- Fraud/Security Cluster
            WHEN has_fraud_terms = 1 THEN 'fraud_security'
            
            -- Account/Banking Issues Cluster  
            WHEN has_account_terms = 1 AND has_error_terms = 1 THEN 'account_errors'
            WHEN has_account_terms = 1 AND has_closure_terms = 1 THEN 'account_closure'
            WHEN has_account_terms = 1 THEN 'account_issues'
            
            -- Card/Payment Issues Cluster
            WHEN has_card_terms = 1 AND has_fee_terms = 1 THEN 'card_fees'
            WHEN has_card_terms = 1 THEN 'card_issues'
            
            -- Loan/Credit Issues Cluster
            WHEN has_loan_terms = 1 AND has_dispute_terms = 1 THEN 'loan_disputes'
            WHEN has_loan_terms = 1 THEN 'loan_issues'
            
            -- Customer Service Issues Cluster
            WHEN has_service_terms = 1 AND has_emotional_terms = 1 THEN 'poor_service'
            WHEN has_service_terms = 1 THEN 'service_issues'
            
            -- Fee/Billing Issues Cluster
            WHEN has_fee_terms = 1 AND has_dispute_terms = 1 THEN 'fee_disputes'
            WHEN has_fee_terms = 1 THEN 'fee_issues'
            
            -- Channel/Technology Issues Cluster
            WHEN has_channel_terms = 1 AND has_error_terms = 1 THEN 'tech_errors'
            WHEN has_channel_terms = 1 THEN 'channel_issues'
            
            -- General Disputes
            WHEN has_dispute_terms = 1 THEN 'general_disputes'
            
            -- High Emotion/Negative Experience
            WHEN has_negative_sentiment = 1 AND has_emotional_terms = 1 THEN 'negative_experience'
            
            -- Default cluster
            ELSE 'general_complaints'
        END as cluster_name,
        
        -- Assign cluster descriptions
        CASE
            WHEN has_fraud_terms = 1 THEN 'Fraud, scams, unauthorized transactions, and security issues'
            WHEN has_account_terms = 1 AND has_error_terms = 1 THEN 'Account errors, incorrect balances, and banking mistakes'
            WHEN has_account_terms = 1 AND has_closure_terms = 1 THEN 'Account closures and termination issues'
            WHEN has_account_terms = 1 THEN 'General account management and banking issues'
            WHEN has_card_terms = 1 AND has_fee_terms = 1 THEN 'Credit/debit card fees and charges'
            WHEN has_card_terms = 1 THEN 'Credit/debit card functionality and issues'
            WHEN has_loan_terms = 1 AND has_dispute_terms = 1 THEN 'Loan disputes, disagreements, and conflicts'
            WHEN has_loan_terms = 1 THEN 'Loan, mortgage, and financing issues'
            WHEN has_service_terms = 1 AND has_emotional_terms = 1 THEN 'Poor customer service experiences'
            WHEN has_service_terms = 1 THEN 'Customer service and support issues'
            WHEN has_fee_terms = 1 AND has_dispute_terms = 1 THEN 'Fee disputes and billing disagreements'
            WHEN has_fee_terms = 1 THEN 'Fees, charges, and billing issues'
            WHEN has_channel_terms = 1 AND has_error_terms = 1 THEN 'ATM, online, and mobile app technical errors'
            WHEN has_channel_terms = 1 THEN 'ATM, branch, online, and mobile channel issues'
            WHEN has_dispute_terms = 1 THEN 'General disputes and disagreements'
            WHEN has_negative_sentiment = 1 AND has_emotional_terms = 1 THEN 'Highly negative customer experiences'
            ELSE 'General complaints and miscellaneous issues'
        END as cluster_description,
        
        -- Assign priority based on severity and cluster type
        CASE
            WHEN has_fraud_terms = 1 THEN 1  -- Highest priority
            WHEN severity_level = 'high' THEN 2
            WHEN has_emotional_terms = 1 AND has_negative_sentiment = 1 THEN 3
            WHEN has_dispute_terms = 1 THEN 4
            WHEN severity_level = 'medium' THEN 5
            ELSE 6  -- Lowest priority
        END as cluster_priority
        
    from {{ ref('stg_complaint_text_features') }}
),

cluster_summary as (
    select
        cluster_name,
        cluster_description,
        cluster_priority,
        COUNT(*) as complaint_count,
        COUNT(DISTINCT complaint_date) as days_with_complaints,
        AVG(CASE WHEN severity_level = 'high' THEN 1.0 WHEN severity_level = 'medium' THEN 0.5 ELSE 0.0 END) as avg_severity_score,
        COUNT(CASE WHEN source_type = 'reddit' THEN 1 END) as reddit_complaints,
        COUNT(CASE WHEN source_type = 'cfpb' THEN 1 END) as cfpb_complaints,
        MIN(complaint_date) as first_complaint_date,
        MAX(complaint_date) as last_complaint_date,
        ARRAY_AGG(DISTINCT source_detail IGNORE NULLS LIMIT 10) as common_sources
    from cluster_assignment
    GROUP BY cluster_name, cluster_description, cluster_priority
)

select
    ca.*,
    cs.complaint_count as cluster_total_complaints,
    cs.days_with_complaints as cluster_days_active,
    cs.avg_severity_score as cluster_avg_severity,
    cs.reddit_complaints as cluster_reddit_count,
    cs.cfpb_complaints as cluster_cfpb_count,
    cs.first_complaint_date as cluster_first_date,
    cs.last_complaint_date as cluster_last_date,
    cs.common_sources as cluster_common_sources
from cluster_assignment ca
JOIN cluster_summary cs ON ca.cluster_name = cs.cluster_name
