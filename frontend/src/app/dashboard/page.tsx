'use client';

import { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { AlertTriangle, TrendingUp, MessageSquare, Calendar } from 'lucide-react';
import { ComplaintCluster, DailyTrend, getComplaintClusters, getDailyTrends } from '@/lib/bigquery';
import { ClusterOverviewCards } from '@/components/dashboard/cluster-overview-cards';
import { DailyTrendsChart } from '@/components/dashboard/daily-trends-chart';
import { RecentComplaints } from '@/components/dashboard/recent-complaints';

export default function DashboardPage() {
  const [clusters, setClusters] = useState<ComplaintCluster[]>([]);
  const [dailyTrends, setDailyTrends] = useState<DailyTrend[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const [clustersData, trendsData] = await Promise.all([
          getComplaintClusters(),
          getDailyTrends()
        ]);
        setClusters(clustersData);
        setDailyTrends(trendsData);
      } catch (error) {
        console.error('Error loading dashboard data:', error);
      } finally {
        setLoading(false);
      }
    }

    loadData();
  }, []);

  if (loading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-6">
          <div className="h-8 bg-gray-200 rounded w-1/4"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  const totalComplaints = clusters.reduce((sum, cluster) => sum + cluster.total_complaints, 0);
  const highPriorityClusters = clusters.filter(cluster => cluster.cluster_priority === 1);
  const avgSeverity = clusters.reduce((sum, cluster) => sum + cluster.avg_severity_score, 0) / clusters.length;
  const activeDays = Math.max(...clusters.map(cluster => cluster.days_active));

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Dashboard Overview</h1>
        <p className="text-gray-600 mt-2">
          Monitor TD Bank customer complaint trends and cluster analysis
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Complaints</CardTitle>
            <MessageSquare className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{totalComplaints.toLocaleString()}</div>
            <p className="text-xs text-muted-foreground">
              Last {activeDays} days
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">High Priority Issues</CardTitle>
            <AlertTriangle className="h-4 w-4 text-red-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-600">
              {highPriorityClusters.reduce((sum, cluster) => sum + cluster.total_complaints, 0)}
            </div>
            <p className="text-xs text-muted-foreground">
              {highPriorityClusters.length} critical clusters
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Average Severity</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {(avgSeverity * 100).toFixed(1)}%
            </div>
            <p className="text-xs text-muted-foreground">
              Across all clusters
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Active Period</CardTitle>
            <Calendar className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{activeDays}</div>
            <p className="text-xs text-muted-foreground">
              Days of data
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Main Content Tabs */}
      <Tabs defaultValue="clusters" className="space-y-6">
        <TabsList>
          <TabsTrigger value="clusters">Cluster Analysis</TabsTrigger>
          <TabsTrigger value="trends">Daily Trends</TabsTrigger>
          <TabsTrigger value="recent">Recent Complaints</TabsTrigger>
        </TabsList>

        <TabsContent value="clusters" className="space-y-6">
          <ClusterOverviewCards clusters={clusters} />
        </TabsContent>

        <TabsContent value="trends" className="space-y-6">
          <DailyTrendsChart trends={dailyTrends} />
        </TabsContent>

        <TabsContent value="recent" className="space-y-6">
          <RecentComplaints trends={dailyTrends} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
