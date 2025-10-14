'use client';

import { useEffect, useState } from 'react';
import { DailyTrend, getDailyTrends } from '@/lib/bigquery';
import { DailyTrendsChart } from '@/components/dashboard/daily-trends-chart';

export default function TrendsPage() {
  const [dailyTrends, setDailyTrends] = useState<DailyTrend[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const trendsData = await getDailyTrends();
        setDailyTrends(trendsData);
      } catch (error) {
        console.error('Error loading trends data:', error);
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
          <div className="h-96 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <DailyTrendsChart trends={dailyTrends} />
    </div>
  );
}
