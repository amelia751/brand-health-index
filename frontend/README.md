# TD Bank Health Monitor Dashboard

A comprehensive complaint analysis dashboard built with Next.js, TypeScript, and shadcn/ui components to monitor and analyze customer complaints from Reddit and CFPB data sources.

## 🚀 Features

### 📊 Dashboard Overview
- **Real-time Metrics**: Total complaints, high-priority issues, average severity, and active monitoring period
- **Interactive Tabs**: Switch between cluster analysis, daily trends, and recent complaints
- **Responsive Design**: Optimized for desktop and mobile viewing

### 🔍 Cluster Analysis
- **13 Distinct Complaint Categories**: Automatically categorized using rule-based clustering
- **Priority Levels**: Critical, High, Medium, and Low priority classification
- **Visual Progress Bars**: Volume and severity indicators for each cluster
- **Source Breakdown**: Reddit vs CFPB complaint distribution
- **Activity Timeline**: Track complaint patterns over time

### 📈 Daily Trends Visualization
- **Interactive Charts**: Bar charts, line charts, and pie charts using Recharts
- **Multiple Views**: 
  - Volume trends over time
  - Source breakdown (Reddit vs CFPB)
  - Cluster distribution analysis
  - Severity trend monitoring
- **Filtering Options**: Filter by specific complaint clusters
- **Date Range Selection**: Customizable time periods

### 💬 Recent Complaints
- **High Priority Alerts**: Dedicated section for critical issues
- **Sample Text Preview**: Preview complaint content with severity indicators
- **Source Attribution**: Clear labeling of Reddit vs CFPB sources
- **Real-time Updates**: Latest complaint activity monitoring

### 🚨 Alerts & Notifications
- **Critical Issue Monitoring**: Automated alerts for complaint spikes
- **Trend Detection**: Early warning system for emerging issues
- **Status Tracking**: Active, investigating, and resolved alert states
- **Customizable Thresholds**: Configurable alert sensitivity

### ⚙️ Settings & Configuration
- **Data Source Management**: BigQuery connection configuration
- **Alert Preferences**: Customizable notification thresholds
- **Dashboard Customization**: Default views and refresh intervals
- **System Information**: Current status and data statistics

## 🛠️ Technology Stack

- **Frontend Framework**: Next.js 15 with App Router
- **UI Components**: shadcn/ui with Tailwind CSS
- **Charts & Visualization**: Recharts library
- **Icons**: Lucide React icons
- **Data Integration**: Google Cloud BigQuery
- **TypeScript**: Full type safety throughout the application

## 📁 Project Structure

```
frontend/
├── src/
│   ├── app/
│   │   ├── dashboard/
│   │   │   ├── alerts/page.tsx
│   │   │   ├── clusters/page.tsx
│   │   │   ├── complaints/page.tsx
│   │   │   ├── settings/page.tsx
│   │   │   ├── trends/page.tsx
│   │   │   ├── layout.tsx
│   │   │   └── page.tsx
│   │   ├── globals.css
│   │   ├── layout.tsx
│   │   └── page.tsx
│   ├── components/
│   │   ├── dashboard/
│   │   │   ├── cluster-overview-cards.tsx
│   │   │   ├── daily-trends-chart.tsx
│   │   │   ├── recent-complaints.tsx
│   │   │   └── sidebar.tsx
│   │   └── ui/
│   │       └── [shadcn components]
│   └── lib/
│       ├── bigquery.ts
│       └── utils.ts
├── components.json
├── package.json
└── README.md
```

## 🎨 Design System

### Color Scheme
- **Primary**: Blue (#3B82F6) - Main brand color
- **Success**: Green (#10B981) - Positive metrics and CFPB data
- **Warning**: Yellow (#F59E0B) - Medium priority alerts
- **Danger**: Red (#EF4444) - High priority issues and critical alerts
- **Neutral**: Gray tones for backgrounds and secondary text

### Component Library
All UI components are built using shadcn/ui for consistency:
- Cards, Buttons, Badges, Tabs
- Progress bars, Select dropdowns, Input fields
- Tables, Dialogs, Sheets, Separators
- Alert dialogs, Dropdown menus

## 📊 Data Integration

### BigQuery Connection
The dashboard connects to Google Cloud BigQuery to fetch complaint data:

```typescript
// Current tables:
- brand_health_dev.mart_complaint_clusters
- brand_health_dev.stg_unified_complaints
- brand_health_dev.raw_reddit_events
- brand_health_dev.raw_cfpb_complaints
```

### Mock Data
For development, the dashboard uses comprehensive mock data that mirrors the production schema:
- 5 complaint clusters with realistic data
- Daily trends with sample complaints
- Severity scores and source attribution

## 🚀 Getting Started

### Prerequisites
- Node.js 18+ 
- npm or yarn
- Google Cloud BigQuery access (for production data)

### Installation

1. **Install dependencies**:
   ```bash
   cd frontend
   npm install
   ```

2. **Start development server**:
   ```bash
   npm run dev
   ```

3. **Open browser**:
   Navigate to `http://localhost:3000`

### Production Setup

1. **Configure BigQuery credentials**:
   - Set up service account key
   - Update `src/lib/bigquery.ts` with production queries

2. **Environment variables**:
   ```bash
   GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
   BIGQUERY_PROJECT_ID=your-project-id
   BIGQUERY_DATASET=your-dataset
   ```

3. **Build and deploy**:
   ```bash
   npm run build
   npm start
   ```

## 📈 Complaint Clustering Categories

The dashboard automatically categorizes complaints into these clusters:

1. **Fraud & Security** (Priority 1) - Unauthorized transactions, security breaches
2. **Account Issues** (Priority 2) - Access problems, account management
3. **Card Fees** (Priority 3) - Credit/debit card charges and fees
4. **Customer Service** (Priority 2) - Poor service experiences
5. **Loan Issues** (Priority 2) - Loan processing and management problems
6. **ATM Problems** (Priority 3) - ATM-related issues
7. **Online Banking** (Priority 2) - Digital platform problems
8. **Branch Service** (Priority 3) - In-person service issues
9. **Account Closure** (Priority 2) - Account termination problems
10. **Error Handling** (Priority 2) - System errors and mistakes
11. **Payment Issues** (Priority 2) - Payment processing problems
12. **General Issues** (Priority 3) - Miscellaneous complaints
13. **Other** (Priority 3) - Uncategorized complaints

## 🔄 Data Flow

1. **Data Collection**: Reddit API + CFPB data → Cloud Functions → GCS
2. **NLP Processing**: GCS → NLP Enricher → Enhanced data → GCS
3. **Data Transformation**: GCS → BigQuery → dbt transformations → Curated tables
4. **Dashboard Display**: BigQuery → Dashboard API → React Components

## 📱 Responsive Design

The dashboard is fully responsive and optimized for:
- **Desktop**: Full feature set with multi-column layouts
- **Tablet**: Adapted layouts with touch-friendly interactions
- **Mobile**: Stacked layouts with essential information prioritized

## 🔧 Customization

### Adding New Clusters
1. Update clustering logic in dbt models
2. Add new cluster definitions to `src/lib/bigquery.ts`
3. Update color schemes and icons as needed

### Custom Visualizations
1. Install additional chart libraries if needed
2. Create new components in `src/components/dashboard/`
3. Add new routes in `src/app/dashboard/`

### Styling Changes
1. Modify Tailwind classes in components
2. Update color scheme in `globals.css`
3. Customize shadcn/ui theme variables

## 📊 Performance Considerations

- **Data Caching**: Implement Redis or in-memory caching for frequently accessed data
- **Pagination**: Add pagination for large complaint datasets
- **Lazy Loading**: Implement lazy loading for chart components
- **Real-time Updates**: Consider WebSocket connections for live data updates

## 🔒 Security

- **Authentication**: Implement user authentication for production
- **Authorization**: Role-based access control for different user types
- **Data Privacy**: Ensure complaint text is properly anonymized
- **API Security**: Secure BigQuery connections with proper credentials

## 📝 Future Enhancements

- **Real-time Data Streaming**: Live updates from BigQuery
- **Advanced Analytics**: Machine learning-based trend prediction
- **Export Functionality**: PDF/Excel report generation
- **Mobile App**: Native mobile application
- **Integration APIs**: REST/GraphQL APIs for third-party integrations
- **Advanced Filtering**: Multi-dimensional data filtering and search
- **Collaborative Features**: Comments, annotations, and team collaboration

## 🐛 Troubleshooting

### Common Issues

1. **Charts not rendering**: Ensure Recharts is properly installed
2. **BigQuery connection errors**: Verify credentials and project permissions
3. **Styling issues**: Check Tailwind CSS configuration
4. **Component errors**: Verify shadcn/ui components are properly installed

### Debug Mode
Enable debug logging by setting:
```typescript
const DEBUG = process.env.NODE_ENV === 'development';
```

## 📞 Support

For technical support or feature requests:
- Create an issue in the project repository
- Contact the development team
- Check the troubleshooting guide above

---

**Built with ❤️ for TD Bank complaint monitoring and analysis**