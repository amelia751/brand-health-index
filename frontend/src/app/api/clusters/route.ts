import { NextResponse } from 'next/server';
import { queryComplaintClusters } from '@/lib/bigquery-server';

// Mock data for now - replace with actual BigQuery implementation later
const mockClusterData = [
  {
    cluster_name: 'fraud_security',
    cluster_description: 'Fraud, unauthorized transactions, and security concerns',
    cluster_priority: 1,
    total_complaints: 189,
    days_active: 11,
    avg_severity_score: 0.78,
    reddit_complaints: 67,
    cfpb_complaints: 122,
    first_complaint: '2025-09-30',
    last_complaint: '2025-10-12'
  },
  {
    cluster_name: 'account_issues',
    cluster_description: 'General account management and access problems',
    cluster_priority: 2,
    total_complaints: 547,
    days_active: 13,
    avg_severity_score: 0.45,
    reddit_complaints: 234,
    cfpb_complaints: 313,
    first_complaint: '2025-09-30',
    last_complaint: '2025-10-12'
  },
  {
    cluster_name: 'card_fees',
    cluster_description: 'Credit/debit card fees and charges',
    cluster_priority: 3,
    total_complaints: 156,
    days_active: 12,
    avg_severity_score: 0.52,
    reddit_complaints: 89,
    cfpb_complaints: 67,
    first_complaint: '2025-09-30',
    last_complaint: '2025-10-11'
  },
  {
    cluster_name: 'customer_service',
    cluster_description: 'Poor customer service experiences',
    cluster_priority: 2,
    total_complaints: 134,
    days_active: 10,
    avg_severity_score: 0.41,
    reddit_complaints: 78,
    cfpb_complaints: 56,
    first_complaint: '2025-10-02',
    last_complaint: '2025-10-12'
  },
  {
    cluster_name: 'loan_issues',
    cluster_description: 'Loan processing and management problems',
    cluster_priority: 2,
    total_complaints: 98,
    days_active: 9,
    avg_severity_score: 0.61,
    reddit_complaints: 34,
    cfpb_complaints: 64,
    first_complaint: '2025-10-01',
    last_complaint: '2025-10-10'
  }
];

export async function GET() {
  try {
    // Try to fetch real BigQuery data first
    try {
      console.log('Attempting to fetch real BigQuery data...');
      const rows = await queryComplaintClusters();
      console.log(`Successfully fetched ${rows.length} cluster records from BigQuery`);
      return NextResponse.json(rows);
    } catch (bigqueryError) {
      console.warn('BigQuery fetch failed, falling back to mock data:', bigqueryError);
      // Fallback to mock data if BigQuery fails
      return NextResponse.json(mockClusterData);
    }
  } catch (error) {
    console.error('Error fetching cluster data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch cluster data' },
      { status: 500 }
    );
  }
}
