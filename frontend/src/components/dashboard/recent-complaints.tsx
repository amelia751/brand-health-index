'use client';

import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { DailyTrend } from '@/lib/bigquery';
import { MessageSquare, Calendar, AlertTriangle, ChevronDown, ChevronUp } from 'lucide-react';
import { format, parseISO, isValid } from 'date-fns';

interface RecentComplaintsProps {
  trends: DailyTrend[];
}

interface ComplaintWithMetadata {
  complaint_id: string;
  complaint_preview: string;
  severity_level: string;
  source_type: string;
  complaint_date: string;
  cluster_name: string;
  cluster_description: string;
  avg_severity_score: number;
}

export function RecentComplaints({ trends }: RecentComplaintsProps) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

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

  // Flatten all sample complaints and sort by date and severity
  const allComplaints: ComplaintWithMetadata[] = trends.flatMap(trend =>
    trend.sample_complaints.map(complaint => ({
      ...complaint,
      complaint_date: trend.complaint_date,
      cluster_name: trend.cluster_name,
      cluster_description: trend.cluster_description,
      avg_severity_score: trend.avg_severity_score
    }))
  ).sort((a, b) => {
    // Sort by date (newest first), then by severity (highest first)
    const dateA = parseDate(a.complaint_date);
    const dateB = parseDate(b.complaint_date);
    const dateCompare = new Date(dateB).getTime() - new Date(dateA).getTime();
    if (dateCompare !== 0) return dateCompare;

    const severityOrder = { 'high': 3, 'medium': 2, 'low': 1 };
    return severityOrder[b.severity_level as keyof typeof severityOrder] -
           severityOrder[a.severity_level as keyof typeof severityOrder];
  });

  const toggleExpanded = (id: string) => {
    setExpandedIds(prev => {
      const newSet = new Set(prev);
      if (newSet.has(id)) {
        newSet.delete(id);
      } else {
        newSet.add(id);
      }
      return newSet;
    });
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'high': return 'destructive';
      case 'medium': return 'default';
      case 'low': return 'secondary';
      default: return 'outline';
    }
  };

  const getSeverityIcon = (severity: string) => {
    switch (severity) {
      case 'high': return <AlertTriangle className="h-4 w-4 text-red-500" />;
      case 'medium': return <MessageSquare className="h-4 w-4 text-yellow-500" />;
      case 'low': return <MessageSquare className="h-4 w-4 text-green-500" />;
      default: return <MessageSquare className="h-4 w-4 text-gray-500" />;
    }
  };

  const getSourceColor = (source: string) => {
    switch (source) {
      case 'reddit': return 'bg-blue-100 text-blue-800';
      case 'cfpb': return 'bg-green-100 text-green-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Recent Complaints</h2>
        <p className="text-gray-600 mt-2">
          Latest customer complaints with sample text and severity analysis
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* High Priority Complaints */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <AlertTriangle className="h-5 w-5 text-red-500" />
              <span>High Priority Issues</span>
            </CardTitle>
            <CardDescription>
              Critical complaints requiring immediate attention
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {allComplaints
              .filter(complaint => complaint.severity_level === 'high')
              .slice(0, 5)
              .map((complaint, index) => {
                const isExpanded = expandedIds.has(`high-${complaint.complaint_id}-${index}`);
                return (
                  <div key={`high-${complaint.complaint_id}-${index}`} className="border rounded-lg bg-red-50 overflow-hidden">
                    <button
                      onClick={() => toggleExpanded(`high-${complaint.complaint_id}-${index}`)}
                      className="w-full p-4 text-left hover:bg-red-100 transition-colors"
                    >
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center space-x-2">
                          <Badge className={getSourceColor(complaint.source_type)}>
                            {complaint.source_type.toUpperCase()}
                          </Badge>
                          <Badge variant={getSeverityColor(complaint.severity_level)}>
                            {complaint.severity_level}
                          </Badge>
                        </div>
                        <div className="flex items-center space-x-2">
                          <div className="text-xs text-gray-500 flex items-center">
                            <Calendar className="h-3 w-3 mr-1" />
                            {formatDateShort(complaint.complaint_date)}
                          </div>
                          {isExpanded ? (
                            <ChevronUp className="h-4 w-4 text-gray-500" />
                          ) : (
                            <ChevronDown className="h-4 w-4 text-gray-500" />
                          )}
                        </div>
                      </div>

                      <div className="text-sm text-gray-700">
                        <strong className="capitalize">
                          {complaint.cluster_name.replace(/_/g, ' ')}
                        </strong>
                        <p className="mt-1 line-clamp-2">
                          {complaint.complaint_preview}...
                        </p>
                      </div>
                    </button>

                    {isExpanded && (
                      <div className="px-4 pb-4 pt-2 bg-white border-t space-y-3">
                        <div>
                          <p className="text-sm font-medium text-gray-700 mb-1">Full Preview:</p>
                          <p className="text-sm text-gray-600">{complaint.complaint_preview}</p>
                        </div>

                        <div>
                          <p className="text-sm font-medium text-gray-700 mb-1">Cluster Description:</p>
                          <p className="text-sm text-gray-600">{complaint.cluster_description}</p>
                        </div>

                        <div className="flex justify-between items-center pt-2 border-t">
                          <span className="text-xs text-gray-500">
                            ID: {complaint.complaint_id}
                          </span>
                          <span className="text-xs text-gray-500">
                            Avg Severity: {(complaint.avg_severity_score * 100).toFixed(1)}%
                          </span>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
          </CardContent>
        </Card>

        {/* All Recent Complaints */}
        <Card>
          <CardHeader>
            <CardTitle>All Recent Activity</CardTitle>
            <CardDescription>
              Latest complaints across all severity levels
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 max-h-[600px] overflow-y-auto">
            {allComplaints.slice(0, 15).map((complaint, index) => {
              const isExpanded = expandedIds.has(`all-${complaint.complaint_id}-${index}`);
              return (
                <div key={`all-${complaint.complaint_id}-${index}`}>
                  <div className="border rounded-lg overflow-hidden">
                    <button
                      onClick={() => toggleExpanded(`all-${complaint.complaint_id}-${index}`)}
                      className="w-full p-3 text-left hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-start space-x-3">
                        <div className="flex-shrink-0 mt-1">
                          {getSeverityIcon(complaint.severity_level)}
                        </div>

                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between mb-1">
                            <div className="flex items-center space-x-2">
                              <Badge className={getSourceColor(complaint.source_type)} variant="outline">
                                {complaint.source_type}
                              </Badge>
                              <span className="text-sm font-medium capitalize">
                                {complaint.cluster_name.replace(/_/g, ' ')}
                              </span>
                            </div>
                            <div className="flex items-center space-x-2">
                              <span className="text-xs text-gray-500">
                                {formatDateShort(complaint.complaint_date)}
                              </span>
                              {isExpanded ? (
                                <ChevronUp className="h-4 w-4 text-gray-500" />
                              ) : (
                                <ChevronDown className="h-4 w-4 text-gray-500" />
                              )}
                            </div>
                          </div>

                          <p className="text-sm text-gray-600 line-clamp-2">
                            {complaint.complaint_preview}...
                          </p>

                          <div className="flex items-center justify-between mt-2">
                            <Badge variant={getSeverityColor(complaint.severity_level)} className="text-xs">
                              {complaint.severity_level}
                            </Badge>
                          </div>
                        </div>
                      </div>
                    </button>

                    {isExpanded && (
                      <div className="px-3 pb-3 pt-2 bg-gray-50 border-t space-y-2">
                        <div>
                          <p className="text-xs font-medium text-gray-700 mb-1">Full Preview:</p>
                          <p className="text-sm text-gray-600">{complaint.complaint_preview}</p>
                        </div>

                        <div>
                          <p className="text-xs font-medium text-gray-700 mb-1">Cluster Description:</p>
                          <p className="text-sm text-gray-600">{complaint.cluster_description}</p>
                        </div>

                        <div className="flex justify-between items-center pt-2 border-t">
                          <span className="text-xs text-gray-500">
                            ID: {complaint.complaint_id}
                          </span>
                          <span className="text-xs text-gray-500">
                            Severity Score: {(complaint.avg_severity_score * 100).toFixed(1)}%
                          </span>
                        </div>
                      </div>
                    )}
                  </div>

                  {index < allComplaints.slice(0, 15).length - 1 && (
                    <Separator className="my-3" />
                  )}
                </div>
              );
            })}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
