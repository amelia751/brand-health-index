'use client';

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { DailyTrend } from '@/lib/bigquery';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell } from 'recharts';
import { useState } from 'react';
import { format, parseISO, isValid } from 'date-fns';

interface DailyTrendsChartProps {
  trends: DailyTrend[];
}

export function DailyTrendsChart({ trends }: DailyTrendsChartProps) {
  const [selectedCluster, setSelectedCluster] = useState<string>('all');
  
  // Helper function to safely parse dates from BigQuery
  const parseDate = (dateValue: any): string => {
    try {
      // Handle BigQuery date objects like {"value": "2025-03-30"}
      if (dateValue && typeof dateValue === 'object' && dateValue.value) {
        return dateValue.value;
      }
      // Handle plain strings
      if (typeof dateValue === 'string') {
        return dateValue;
      }
      // Fallback
      return new Date().toISOString().split('T')[0];
    } catch (error) {
      console.warn('Date parsing error:', error);
      return new Date().toISOString().split('T')[0];
    }
  };

  // Helper function to format dates safely
  const formatDateShort = (dateValue: any): string => {
    try {
      const dateString = parseDate(dateValue);
      const date = parseISO(dateString);
      if (isValid(date)) {
        return format(date, 'MMM dd');
      }
      return dateString.substring(5); // Return MM-DD if parsing fails
    } catch (error) {
      console.warn('Date formatting error:', error);
      return 'Invalid';
    }
  };
  
  // Process data for charts
  const processedData = trends.map(trend => ({
    ...trend,
    date: formatDateShort(trend.complaint_date),
    total_count: trend.daily_complaint_count,
    severity_percentage: Math.round(trend.avg_severity_score * 100)
  }));

  // Filter data by selected cluster
  const filteredData = selectedCluster === 'all' 
    ? processedData 
    : processedData.filter(d => d.cluster_name === selectedCluster);

  // Aggregate data by date for overview
  const dateAggregatedData = processedData.reduce((acc, curr) => {
    const existing = acc.find(item => item.date === curr.date);
    if (existing) {
      existing.total_count += curr.daily_complaint_count;
      existing.reddit_count += curr.reddit_count;
      existing.cfpb_count += curr.cfpb_count;
    } else {
      acc.push({
        date: curr.date,
        total_count: curr.daily_complaint_count,
        reddit_count: curr.reddit_count,
        cfpb_count: curr.cfpb_count
      });
    }
    return acc;
  }, [] as any[]);

  // Get unique clusters for filter
  const uniqueClusters = [...new Set(trends.map(t => t.cluster_name))];

  // Cluster distribution data for pie chart
  const clusterDistribution = uniqueClusters.map(cluster => {
    const clusterData = trends.filter(t => t.cluster_name === cluster);
    const totalComplaints = clusterData.reduce((sum, t) => sum + t.daily_complaint_count, 0);
    return {
      name: cluster.replace(/_/g, ' '),
      value: totalComplaints,
      cluster_name: cluster
    };
  });

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d'];

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Daily Trends Analysis</h2>
          <p className="text-gray-600 mt-2">
            Track complaint volume and patterns over time
          </p>
        </div>
        
        <Select value={selectedCluster} onValueChange={setSelectedCluster}>
          <SelectTrigger className="w-48">
            <SelectValue placeholder="Select cluster" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Clusters</SelectItem>
            {uniqueClusters.map(cluster => (
              <SelectItem key={cluster} value={cluster}>
                {cluster.replace(/_/g, ' ')}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Tabs defaultValue="volume" className="space-y-6">
        <TabsList>
          <TabsTrigger value="volume">Volume Trends</TabsTrigger>
          <TabsTrigger value="sources">Source Breakdown</TabsTrigger>
          <TabsTrigger value="distribution">Cluster Distribution</TabsTrigger>
          <TabsTrigger value="severity">Severity Trends</TabsTrigger>
        </TabsList>

        <TabsContent value="volume" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Daily Complaint Volume</CardTitle>
              <CardDescription>
                {selectedCluster === 'all' 
                  ? 'Total complaints across all clusters' 
                  : `Complaints for ${selectedCluster.replace(/_/g, ' ')} cluster`}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={selectedCluster === 'all' ? dateAggregatedData : filteredData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip 
                    labelFormatter={(label) => `Date: ${label}`}
                    formatter={(value, name) => [value, name === 'total_count' ? 'Complaints' : name]}
                  />
                  <Bar dataKey="total_count" fill="#3B82F6" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sources" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Complaints by Source</CardTitle>
              <CardDescription>
                Breakdown of Reddit vs CFPB complaints over time
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={selectedCluster === 'all' ? dateAggregatedData : filteredData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="reddit_count" stackId="a" fill="#3B82F6" name="Reddit" />
                  <Bar dataKey="cfpb_count" stackId="a" fill="#10B981" name="CFPB" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="distribution" className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Cluster Distribution</CardTitle>
                <CardDescription>
                  Total complaints by cluster type
                </CardDescription>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={clusterDistribution}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {clusterDistribution.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Top Complaint Categories</CardTitle>
                <CardDescription>
                  Ranked by total volume
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {clusterDistribution
                    .sort((a, b) => b.value - a.value)
                    .slice(0, 5)
                    .map((cluster, index) => (
                      <div key={cluster.cluster_name} className="flex items-center justify-between">
                        <div className="flex items-center space-x-3">
                          <div className="text-sm font-medium text-gray-500">
                            #{index + 1}
                          </div>
                          <div>
                            <div className="font-medium capitalize">
                              {cluster.name}
                            </div>
                          </div>
                        </div>
                        <Badge variant="outline">
                          {cluster.value} complaints
                        </Badge>
                      </div>
                    ))}
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="severity" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Severity Trends</CardTitle>
              <CardDescription>
                Average severity score over time
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={filteredData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis domain={[0, 100]} />
                  <Tooltip 
                    formatter={(value) => [`${value}%`, 'Severity']}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="severity_percentage" 
                    stroke="#EF4444" 
                    strokeWidth={2}
                    dot={{ fill: '#EF4444' }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
