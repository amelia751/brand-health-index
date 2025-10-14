'use client';

import { useEffect, useState } from 'react';
import { ComplaintCluster, getComplaintClusters } from '@/lib/bigquery';
import { ClusterOverviewCards } from '@/components/dashboard/cluster-overview-cards';

export default function ClustersPage() {
  const [clusters, setClusters] = useState<ComplaintCluster[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const clustersData = await getComplaintClusters();
        setClusters(clustersData);
      } catch (error) {
        console.error('Error loading clusters data:', error);
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
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[...Array(6)].map((_, i) => (
              <div key={i} className="h-64 bg-gray-200 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <ClusterOverviewCards clusters={clusters} />
    </div>
  );
}
