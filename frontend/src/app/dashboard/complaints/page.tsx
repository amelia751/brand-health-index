'use client';

import { useEffect, useState } from 'react';
import { ComplaintDetail, getComplaintDetails, ComplaintCluster, getComplaintClusters, PaginatedComplaintsResponse } from '@/lib/bigquery';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Button } from '@/components/ui/button';
import { Pagination, PaginationContent, PaginationEllipsis, PaginationItem, PaginationLink, PaginationNext, PaginationPrevious } from '@/components/ui/pagination';
import { MessageSquare, Calendar, AlertTriangle, ChevronDown, ChevronUp, Filter } from 'lucide-react';
import { format, parseISO, isValid } from 'date-fns';

export default function ComplaintsPage() {
  const [complaints, setComplaints] = useState<ComplaintDetail[]>([]);
  const [clusters, setClusters] = useState<ComplaintCluster[]>([]);
  const [selectedCluster, setSelectedCluster] = useState<string>('all');
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [pagination, setPagination] = useState({
    page: 1,
    pageSize: 50,
    totalCount: 0,
    totalPages: 0,
    hasNextPage: false,
    hasPreviousPage: false
  });

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
  const formatDate = (dateValue: any): string => {
    try {
      const dateString = parseDate(dateValue);
      const date = parseISO(dateString);
      if (isValid(date)) {
        return format(date, 'MMM dd, yyyy');
      }
      return dateString; // Return raw string if parsing fails
    } catch (error) {
      console.warn('Date formatting error:', error);
      return 'Invalid Date';
    }
  };

  useEffect(() => {
    async function loadClusters() {
      try {
        const clustersData = await getComplaintClusters();
        setClusters(clustersData);
      } catch (error) {
        console.error('Error loading clusters:', error);
      }
    }
    loadClusters();
  }, []);

  useEffect(() => {
    async function loadComplaints() {
      setLoading(true);
      try {
        const response = await getComplaintDetails(selectedCluster, pagination.page, pagination.pageSize);
        setComplaints(response.complaints);
        setPagination(response.pagination);
      } catch (error) {
        console.error('Error loading complaints:', error);
      } finally {
        setLoading(false);
      }
    }

    loadComplaints();
  }, [selectedCluster, pagination.page]);

  // Reset to page 1 when cluster changes
  useEffect(() => {
    if (pagination.page !== 1) {
      setPagination(prev => ({ ...prev, page: 1 }));
    }
  }, [selectedCluster]);

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
      case 'reddit': return 'bg-blue-100 text-blue-800 border-blue-200';
      case 'cfpb': return 'bg-green-100 text-green-800 border-green-200';
      default: return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  const severityCounts = {
    high: complaints.filter(c => c.severity_level === 'high').length,
    medium: complaints.filter(c => c.severity_level === 'medium').length,
    low: complaints.filter(c => c.severity_level === 'low').length,
  };

  const handlePageChange = (newPage: number) => {
    setPagination(prev => ({ ...prev, page: newPage }));
  };

  const handlePageSizeChange = (newPageSize: string) => {
    setPagination(prev => ({ 
      ...prev, 
      page: 1, // Reset to first page when changing page size
      pageSize: parseInt(newPageSize) 
    }));
  };

  if (loading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-6">
          <div className="h-8 bg-gray-200 rounded w-1/4"></div>
          <div className="h-12 bg-gray-200 rounded w-64"></div>
          <div className="space-y-4">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">All Complaints</h1>
        <p className="text-gray-600 mt-2">
          Browse and filter complaints by cluster with full text details
        </p>
      </div>


      {/* Filter Section */}
      <div className="flex flex-col sm:flex-row sm:items-end gap-4 p-4 bg-gray-50 rounded-lg border">
        <div className="flex items-center gap-2">
          <Filter className="h-4 w-4 text-gray-500" />
          <span className="text-sm font-medium text-gray-700">Filters:</span>
        </div>
        <div className="flex-1">
          <label className="text-sm font-medium text-gray-700 mb-1 block">
            Cluster
          </label>
          <Select value={selectedCluster} onValueChange={setSelectedCluster}>
            <SelectTrigger className="w-full sm:w-80">
              <SelectValue placeholder="Select a cluster" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Clusters ({pagination.totalCount.toLocaleString()})</SelectItem>
              {clusters.map(cluster => (
                <SelectItem key={cluster.cluster_name} value={cluster.cluster_name}>
                  <div className="flex items-center justify-between w-full">
                    <span className="capitalize">
                      {cluster.cluster_name.replace(/_/g, ' ')}
                    </span>
                    <span className="text-xs text-gray-500 ml-4">
                      ({cluster.total_complaints})
                    </span>
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div>
          <label className="text-sm font-medium text-gray-700 mb-1 block">
            Per Page
          </label>
          <Select value={pagination.pageSize.toString()} onValueChange={handlePageSizeChange}>
            <SelectTrigger className="w-20">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="25">25</SelectItem>
              <SelectItem value="50">50</SelectItem>
              <SelectItem value="100">100</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Complaints List */}
      <Card>
        <CardHeader>
          <CardTitle>
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <MessageSquare className="h-5 w-5" />
                <span>
                  {selectedCluster === 'all'
                    ? 'All Complaints'
                    : `${selectedCluster.replace(/_/g, ' ').split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')} Complaints`}
                </span>
              </div>
              <div className="text-sm font-normal text-gray-500">
                {pagination.totalCount.toLocaleString()} total
              </div>
            </div>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {complaints.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              <MessageSquare className="h-12 w-12 mx-auto mb-4 text-gray-400" />
              <p>No complaints found for this filter</p>
            </div>
          ) : (
            complaints.map((complaint, index) => {
              const isExpanded = expandedIds.has(complaint.complaint_id);
              const severityBgColor = complaint.severity_level === 'high'
                ? 'bg-red-50'
                : complaint.severity_level === 'medium'
                ? 'bg-yellow-50'
                : 'bg-gray-50';

              return (
                <div key={complaint.complaint_id}>
                  <div className={`border rounded-lg overflow-hidden ${severityBgColor}`}>
                    <button
                      onClick={() => toggleExpanded(complaint.complaint_id)}
                      className="w-full p-4 text-left hover:opacity-80 transition-opacity"
                    >
                      <div className="flex items-start space-x-3">
                        <div className="flex-shrink-0 mt-1">
                          {getSeverityIcon(complaint.severity_level)}
                        </div>

                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between mb-2">
                            <div className="flex items-center space-x-2 flex-wrap gap-2">
                              <Badge className={getSourceColor(complaint.source_type)}>
                                {complaint.source_type.toUpperCase()}
                              </Badge>
                              <Badge variant={getSeverityColor(complaint.severity_level)}>
                                {complaint.severity_level}
                              </Badge>
                              <span className="text-sm font-medium capitalize text-gray-700">
                                {complaint.cluster_name.replace(/_/g, ' ')}
                              </span>
                            </div>
                            <div className="flex items-center space-x-2">
                              <div className="text-xs text-gray-500 flex items-center">
                                <Calendar className="h-3 w-3 mr-1" />
                                {formatDate(complaint.complaint_date)}
                              </div>
                              {isExpanded ? (
                                <ChevronUp className="h-5 w-5 text-gray-500" />
                              ) : (
                                <ChevronDown className="h-5 w-5 text-gray-500" />
                              )}
                            </div>
                          </div>

                          <p className="text-sm text-gray-600 line-clamp-2">
                            {complaint.complaint_text.substring(0, 150)}...
                          </p>
                        </div>
                      </div>
                    </button>

                    {isExpanded && (
                      <div className="px-4 pb-4 pt-2 bg-white border-t space-y-3">
                        <div>
                          <p className="text-sm font-medium text-gray-700 mb-2">Full Complaint:</p>
                          <p className="text-sm text-gray-600 leading-relaxed whitespace-pre-wrap">
                            {complaint.complaint_text}
                          </p>
                        </div>

                        <Separator />

                        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-xs">
                          <div>
                            <span className="font-medium text-gray-700">Complaint ID:</span>
                            <p className="text-gray-600 mt-1">{complaint.complaint_id}</p>
                          </div>
                          <div>
                            <span className="font-medium text-gray-700">Source:</span>
                            <p className="text-gray-600 mt-1">{complaint.source_detail || 'N/A'}</p>
                          </div>
                          <div>
                            <span className="font-medium text-gray-700">Brand:</span>
                            <p className="text-gray-600 mt-1">{complaint.brand_id}</p>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>

                  {index < complaints.length - 1 && (
                    <Separator className="my-3" />
                  )}
                </div>
              );
            })
          )}
          
          {/* Pagination Controls */}
          {pagination.totalPages > 1 && (
            <div className="mt-6 pt-6 border-t">
              <div className="flex flex-col sm:flex-row justify-between items-center space-y-4 sm:space-y-0">
                <div className="text-sm text-gray-500">
                  Showing {((pagination.page - 1) * pagination.pageSize) + 1} to{' '}
                  {Math.min(pagination.page * pagination.pageSize, pagination.totalCount)} of{' '}
                  {pagination.totalCount.toLocaleString()} complaints
                </div>
                
                <Pagination>
                  <PaginationContent>
                    <PaginationItem>
                      <PaginationPrevious 
                        href="#"
                        onClick={(e) => {
                          e.preventDefault();
                          if (pagination.hasPreviousPage) {
                            handlePageChange(pagination.page - 1);
                          }
                        }}
                        className={!pagination.hasPreviousPage ? 'pointer-events-none opacity-50' : ''}
                      />
                    </PaginationItem>
                    
                    {/* Page Numbers */}
                    {Array.from({ length: Math.min(5, pagination.totalPages) }, (_, i) => {
                      let pageNum;
                      if (pagination.totalPages <= 5) {
                        pageNum = i + 1;
                      } else if (pagination.page <= 3) {
                        pageNum = i + 1;
                      } else if (pagination.page >= pagination.totalPages - 2) {
                        pageNum = pagination.totalPages - 4 + i;
                      } else {
                        pageNum = pagination.page - 2 + i;
                      }
                      
                      return (
                        <PaginationItem key={pageNum}>
                          <PaginationLink
                            href="#"
                            onClick={(e) => {
                              e.preventDefault();
                              handlePageChange(pageNum);
                            }}
                            isActive={pageNum === pagination.page}
                          >
                            {pageNum}
                          </PaginationLink>
                        </PaginationItem>
                      );
                    })}
                    
                    {pagination.totalPages > 5 && pagination.page < pagination.totalPages - 2 && (
                      <PaginationItem>
                        <PaginationEllipsis />
                      </PaginationItem>
                    )}
                    
                    <PaginationItem>
                      <PaginationNext 
                        href="#"
                        onClick={(e) => {
                          e.preventDefault();
                          if (pagination.hasNextPage) {
                            handlePageChange(pagination.page + 1);
                          }
                        }}
                        className={!pagination.hasNextPage ? 'pointer-events-none opacity-50' : ''}
                      />
                    </PaginationItem>
                  </PaginationContent>
                </Pagination>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
