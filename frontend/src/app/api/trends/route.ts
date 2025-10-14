import { NextResponse } from 'next/server';
import { queryDailyTrends } from '@/lib/bigquery-server';

// Mock data for daily trends
const mockDailyTrends = [
  {
    complaint_date: '2025-10-12',
    cluster_name: 'fraud_security',
    cluster_description: 'Fraud, unauthorized transactions, and security concerns',
    daily_complaint_count: 28,
    avg_severity_score: 0.82,
    reddit_count: 12,
    cfpb_count: 16,
    sample_complaints: [
      {
        complaint_id: 'reddit_001',
        source_type: 'reddit',
        complaint_preview: 'Someone used my TD Bank card for unauthorized purchases. Called customer service but they said it would take 10 business days to investigate...',
        severity_level: 'high'
      },
      {
        complaint_id: 'cfpb_001',
        source_type: 'cfpb',
        complaint_preview: 'Fraudulent charges appeared on my account. TD Bank initially denied my dispute claim without proper investigation...',
        severity_level: 'high'
      }
    ]
  },
  {
    complaint_date: '2025-10-12',
    cluster_name: 'account_issues',
    cluster_description: 'General account management and access problems',
    daily_complaint_count: 23,
    avg_severity_score: 0.47,
    reddit_count: 15,
    cfpb_count: 8,
    sample_complaints: [
      {
        complaint_id: 'reddit_002',
        source_type: 'reddit',
        complaint_preview: 'My online banking has been down for 3 days. Cannot access my account or pay bills. This is unacceptable...',
        severity_level: 'medium'
      }
    ]
  },
  {
    complaint_date: '2025-10-11',
    cluster_name: 'fraud_security',
    cluster_description: 'Fraud, unauthorized transactions, and security concerns',
    daily_complaint_count: 22,
    avg_severity_score: 0.75,
    reddit_count: 8,
    cfpb_count: 14,
    sample_complaints: [
      {
        complaint_id: 'cfpb_003',
        source_type: 'cfpb',
        complaint_preview: 'My debit card was compromised and used for multiple transactions. TD Bank took weeks to resolve the issue...',
        severity_level: 'high'
      }
    ]
  },
  {
    complaint_date: '2025-10-11',
    cluster_name: 'card_fees',
    cluster_description: 'Credit/debit card fees and charges',
    daily_complaint_count: 19,
    avg_severity_score: 0.55,
    reddit_count: 11,
    cfpb_count: 8,
    sample_complaints: [
      {
        complaint_id: 'cfpb_002',
        source_type: 'cfpb',
        complaint_preview: 'TD Bank charged me multiple overdraft fees in one day for the same transaction. This seems like predatory behavior...',
        severity_level: 'medium'
      }
    ]
  },
  {
    complaint_date: '2025-10-10',
    cluster_name: 'account_issues',
    cluster_description: 'General account management and access problems',
    daily_complaint_count: 18,
    avg_severity_score: 0.42,
    reddit_count: 12,
    cfpb_count: 6,
    sample_complaints: [
      {
        complaint_id: 'reddit_003',
        source_type: 'reddit',
        complaint_preview: 'Tried to open a new account but the process has been stuck for weeks. No one can give me a clear answer...',
        severity_level: 'medium'
      }
    ]
  },
  {
    complaint_date: '2025-10-09',
    cluster_name: 'customer_service',
    cluster_description: 'Poor customer service experiences',
    daily_complaint_count: 15,
    avg_severity_score: 0.38,
    reddit_count: 9,
    cfpb_count: 6,
    sample_complaints: [
      {
        complaint_id: 'reddit_004',
        source_type: 'reddit',
        complaint_preview: 'Spent 2 hours on hold just to be transferred 3 times. Finally hung up without getting help...',
        severity_level: 'medium'
      }
    ]
  }
];

export async function GET() {
  try {
    // Try to fetch real BigQuery data first
    try {
      console.log('Attempting to fetch real BigQuery trends data...');
      const rows = await queryDailyTrends();
      console.log(`Successfully fetched ${rows.length} trend records from BigQuery`);
      return NextResponse.json(rows);
    } catch (bigqueryError) {
      console.warn('BigQuery trends fetch failed, falling back to mock data:', bigqueryError);
      // Fallback to mock data if BigQuery fails
      return NextResponse.json(mockDailyTrends);
    }
  } catch (error) {
    console.error('Error fetching trends data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch trends data' },
      { status: 500 }
    );
  }
}
