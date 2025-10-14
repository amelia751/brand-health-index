import { NextResponse } from 'next/server';
import { queryComplaintDetails, queryComplaintCount } from '@/lib/bigquery-server';

// Mock data for complaints with full text
const mockComplaints = [
  {
    complaint_id: 'reddit_001',
    source_type: 'reddit',
    brand_id: 'td_bank',
    complaint_text: 'Someone used my TD Bank card for unauthorized purchases totaling over $2,000. I called customer service immediately but they said it would take 10 business days to investigate. In the meantime, my account is overdrawn and I can\'t pay my bills. The representative was unhelpful and kept reading from a script. I\'ve been a loyal customer for 15 years and this is how they treat me? Absolutely unacceptable. I\'m considering switching to another bank that actually cares about protecting their customers from fraud.',
    complaint_date: '2025-10-12',
    cluster_name: 'fraud_security',
    severity_level: 'high',
    source_detail: 'r/personalfinance'
  },
  {
    complaint_id: 'cfpb_001',
    source_type: 'cfpb',
    brand_id: 'td_bank',
    complaint_text: 'Fraudulent charges appeared on my account totaling $3,500. TD Bank initially denied my dispute claim without proper investigation. They claimed the charges were authorized, but I have proof I was out of the country when these transactions occurred. After filing a formal complaint with CFPB, they finally agreed to investigate, but it has been 45 days and I still haven\'t received a refund. My credit score has been impacted and I\'ve incurred late fees on other accounts because of this.',
    complaint_date: '2025-10-12',
    cluster_name: 'fraud_security',
    severity_level: 'high',
    source_detail: 'CFPB Portal'
  },
  {
    complaint_id: 'reddit_002',
    source_type: 'reddit',
    brand_id: 'td_bank',
    complaint_text: 'My online banking has been down for 3 days. Cannot access my account or pay bills. This is unacceptable for a major bank in 2025. I called support multiple times and was told "technical issues are being resolved" but no timeline given. I have automatic payments that are going to bounce and I can\'t even check my balance. The mobile app also doesn\'t work. How is this acceptable?',
    complaint_date: '2025-10-12',
    cluster_name: 'account_issues',
    severity_level: 'medium',
    source_detail: 'r/tdbank'
  },
  {
    complaint_id: 'cfpb_003',
    source_type: 'cfpb',
    brand_id: 'td_bank',
    complaint_text: 'My debit card was compromised and used for multiple transactions at gas stations in different states. I reported this immediately to TD Bank. They took weeks to resolve the issue and during that time, they continued to decline my legitimate transactions, leaving me stranded without access to my money. The fraud department was incredibly slow to respond and kept asking me to fill out the same forms multiple times. I lost money on hotel reservations that I couldn\'t cancel because I had no access to funds.',
    complaint_date: '2025-10-11',
    cluster_name: 'fraud_security',
    severity_level: 'high',
    source_detail: 'CFPB Portal'
  },
  {
    complaint_id: 'cfpb_002',
    source_type: 'cfpb',
    brand_id: 'td_bank',
    complaint_text: 'TD Bank charged me multiple overdraft fees ($35 each) in one day for the same transaction that was pending. When the transaction finally cleared, they charged me again. I called to dispute these fees as they seem like predatory behavior, but the customer service representative was dismissive and refused to refund any of the fees. I ended up paying $175 in fees for a $50 overdraft. This is absolutely ridiculous and feels like they\'re taking advantage of customers who are already struggling financially.',
    complaint_date: '2025-10-11',
    cluster_name: 'card_fees',
    severity_level: 'medium',
    source_detail: 'CFPB Portal'
  },
  {
    complaint_id: 'reddit_003',
    source_type: 'reddit',
    brand_id: 'td_bank',
    complaint_text: 'Tried to open a new savings account but the process has been stuck in "pending review" for weeks. No one can give me a clear answer about what\'s happening or when it will be resolved. I\'ve called three times and each representative tells me something different. One said it would take 5 business days, another said 10, and the last one said they couldn\'t find my application at all. This is incredibly frustrating and unprofessional.',
    complaint_date: '2025-10-10',
    cluster_name: 'account_issues',
    severity_level: 'medium',
    source_detail: 'r/banking'
  },
  {
    complaint_id: 'reddit_004',
    source_type: 'reddit',
    brand_id: 'td_bank',
    complaint_text: 'Spent 2 hours on hold just to be transferred 3 times to different departments. Finally hung up without getting help. Their customer service is a complete joke. Every representative seemed clueless and couldn\'t answer basic questions about my account. When I asked to speak to a supervisor, I was put on hold for another 45 minutes before being disconnected. This is the worst banking experience I\'ve ever had.',
    complaint_date: '2025-10-09',
    cluster_name: 'customer_service',
    severity_level: 'medium',
    source_detail: 'r/tdbank'
  },
  {
    complaint_id: 'reddit_005',
    source_type: 'reddit',
    brand_id: 'td_bank',
    complaint_text: 'Applied for a car loan 3 weeks ago and still haven\'t heard back. I\'ve called twice and they keep saying they\'re "processing" it but won\'t give me any details. Meanwhile, the car I wanted to buy got sold to someone else because I couldn\'t get financing approved in time. Other banks approved my application within 24 hours, but TD Bank is dragging their feet. Very disappointed with their service.',
    complaint_date: '2025-10-08',
    cluster_name: 'loan_issues',
    severity_level: 'medium',
    source_detail: 'r/personalfinance'
  },
  {
    complaint_id: 'cfpb_004',
    source_type: 'cfpb',
    brand_id: 'td_bank',
    complaint_text: 'I was charged a $50 wire transfer fee that was never disclosed to me. When I initiated the transfer, the online banking system said it would be free for amounts over $1,000. After the transfer went through, they charged me $50. When I called to complain, they said the free transfer only applies to certain account types, which was never explained. This feels like deceptive business practices. I want my fee refunded.',
    complaint_date: '2025-10-07',
    cluster_name: 'card_fees',
    severity_level: 'low',
    source_detail: 'CFPB Portal'
  },
  {
    complaint_id: 'reddit_006',
    source_type: 'reddit',
    brand_id: 'td_bank',
    complaint_text: 'Received a notice that my account was being closed due to "suspicious activity" but they won\'t tell me what activity they\'re referring to. I\'ve been banking with TD for 8 years with no issues. Now they\'re holding my money hostage and I can\'t access my funds. I need this money to pay rent and they\'re telling me it will take 30 days to mail me a check. This is absolutely insane.',
    complaint_date: '2025-10-06',
    cluster_name: 'account_issues',
    severity_level: 'high',
    source_detail: 'r/legaladvice'
  },
  {
    complaint_id: 'cfpb_005',
    source_type: 'cfpb',
    brand_id: 'td_bank',
    complaint_text: 'Branch representative pressured me into opening a credit card I didn\'t want or need. They said it would "improve my relationship with the bank" and help with future loan applications. Now I\'m stuck with an annual fee and a card I never use. When I tried to close it, they said it would hurt my credit score. This feels like predatory sales tactics.',
    complaint_date: '2025-10-05',
    cluster_name: 'customer_service',
    severity_level: 'low',
    source_detail: 'CFPB Portal'
  }
];

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const cluster = searchParams.get('cluster');
    const page = parseInt(searchParams.get('page') || '1');
    const pageSize = parseInt(searchParams.get('pageSize') || '50');

    // Try to fetch real BigQuery data first
    try {
      console.log('Attempting to fetch real BigQuery complaints data...');
      
      // Get both the complaints and total count
      const [complaints, totalCount] = await Promise.all([
        queryComplaintDetails(
          cluster || undefined, 
          undefined, // dateRange
          { page, pageSize }
        ),
        queryComplaintCount(cluster || undefined)
      ]);
      
      console.log(`Successfully fetched ${complaints.length} complaint records from BigQuery (page ${page})`);
      
      // Return paginated response
      return NextResponse.json({
        complaints,
        pagination: {
          page,
          pageSize,
          totalCount,
          totalPages: Math.ceil(totalCount / pageSize),
          hasNextPage: page < Math.ceil(totalCount / pageSize),
          hasPreviousPage: page > 1
        }
      });
    } catch (bigqueryError) {
      console.warn('BigQuery complaints fetch failed, falling back to mock data:', bigqueryError);
      
      // Fallback to mock data if BigQuery fails
      let filteredComplaints = mockComplaints;
      if (cluster && cluster !== 'all') {
        filteredComplaints = mockComplaints.filter(c => c.cluster_name === cluster);
      }
      
      // Mock pagination for fallback
      const startIndex = (page - 1) * pageSize;
      const endIndex = startIndex + pageSize;
      const paginatedComplaints = filteredComplaints.slice(startIndex, endIndex);
      
      return NextResponse.json({
        complaints: paginatedComplaints,
        pagination: {
          page,
          pageSize,
          totalCount: filteredComplaints.length,
          totalPages: Math.ceil(filteredComplaints.length / pageSize),
          hasNextPage: page < Math.ceil(filteredComplaints.length / pageSize),
          hasPreviousPage: page > 1
        }
      });
    }
  } catch (error) {
    console.error('Error fetching complaints data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch complaints data' },
      { status: 500 }
    );
  }
}
