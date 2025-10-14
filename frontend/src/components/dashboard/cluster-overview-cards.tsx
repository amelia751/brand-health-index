'use client';

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { ComplaintCluster } from '@/lib/bigquery';
import { AlertTriangle, TrendingUp, Users, Calendar } from 'lucide-react';

interface ClusterOverviewCardsProps {
  clusters: ComplaintCluster[];
}

export function ClusterOverviewCards({ clusters }: ClusterOverviewCardsProps) {
  const getPriorityColor = (priority: number) => {
    switch (priority) {
      case 1: return 'destructive';
      case 2: return 'default';
      case 3: return 'secondary';
      default: return 'outline';
    }
  };

  const getPriorityIcon = (priority: number) => {
    switch (priority) {
      case 1: return <AlertTriangle className="h-4 w-4 text-red-500" />;
      case 2: return <TrendingUp className="h-4 w-4 text-yellow-500" />;
      case 3: return <Users className="h-4 w-4 text-blue-500" />;
      default: return <Calendar className="h-4 w-4 text-gray-500" />;
    }
  };

  const getPriorityLabel = (priority: number) => {
    switch (priority) {
      case 1: return 'Critical';
      case 2: return 'High';
      case 3: return 'Medium';
      default: return 'Low';
    }
  };

  const maxComplaints = Math.max(...clusters.map(c => c.total_complaints));

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Complaint Clusters</h2>
        <p className="text-gray-600 mt-2">
          Analysis of complaint patterns grouped by similarity and topic
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {clusters.map((cluster) => (
          <Card key={cluster.cluster_name} className="hover:shadow-lg transition-shadow">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  {getPriorityIcon(cluster.cluster_priority)}
                  <Badge variant={getPriorityColor(cluster.cluster_priority)}>
                    {getPriorityLabel(cluster.cluster_priority)}
                  </Badge>
                </div>
                <div className="text-right">
                  <div className="text-2xl font-bold text-gray-900">
                    {cluster.total_complaints}
                  </div>
                  <div className="text-xs text-gray-500">complaints</div>
                </div>
              </div>
              <CardTitle className="text-lg capitalize">
                {cluster.cluster_name.replace(/_/g, ' ')}
              </CardTitle>
              <CardDescription className="text-sm">
                {cluster.cluster_description}
              </CardDescription>
            </CardHeader>
            
            <CardContent className="space-y-4">
              {/* Progress bar showing relative volume */}
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>Volume</span>
                  <span>{((cluster.total_complaints / maxComplaints) * 100).toFixed(1)}%</span>
                </div>
                <Progress 
                  value={(cluster.total_complaints / maxComplaints) * 100} 
                  className="h-2"
                />
              </div>

              {/* Severity Score */}
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>Avg Severity</span>
                  <span className={`font-medium ${
                    cluster.avg_severity_score > 0.7 ? 'text-red-600' :
                    cluster.avg_severity_score > 0.4 ? 'text-yellow-600' : 'text-green-600'
                  }`}>
                    {(cluster.avg_severity_score * 100).toFixed(1)}%
                  </span>
                </div>
                <Progress 
                  value={cluster.avg_severity_score * 100} 
                  className="h-2"
                />
              </div>

              {/* Source Breakdown */}
              <div className="grid grid-cols-2 gap-4 pt-2 border-t">
                <div className="text-center">
                  <div className="text-lg font-semibold text-blue-600">
                    {cluster.reddit_complaints}
                  </div>
                  <div className="text-xs text-gray-500">Reddit</div>
                </div>
                <div className="text-center">
                  <div className="text-lg font-semibold text-green-600">
                    {cluster.cfpb_complaints}
                  </div>
                  <div className="text-xs text-gray-500">CFPB</div>
                </div>
              </div>

              {/* Activity Period */}
              <div className="flex justify-between text-xs text-gray-500 pt-2 border-t">
                <span>Active: {cluster.days_active} days</span>
                <span>Latest: {new Date(cluster.last_complaint).toLocaleDateString()}</span>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Summary Stats */}
      <Card>
        <CardHeader>
          <CardTitle>Cluster Summary</CardTitle>
          <CardDescription>
            Overall statistics across all complaint clusters
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900">
                {clusters.length}
              </div>
              <div className="text-sm text-gray-500">Total Clusters</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-red-600">
                {clusters.filter(c => c.cluster_priority === 1).length}
              </div>
              <div className="text-sm text-gray-500">Critical Issues</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-blue-600">
                {clusters.reduce((sum, c) => sum + c.reddit_complaints, 0)}
              </div>
              <div className="text-sm text-gray-500">Reddit Posts</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-green-600">
                {clusters.reduce((sum, c) => sum + c.cfpb_complaints, 0)}
              </div>
              <div className="text-sm text-gray-500">CFPB Reports</div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
