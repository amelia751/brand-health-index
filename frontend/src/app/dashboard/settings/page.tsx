'use client';

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Separator } from '@/components/ui/separator';
import { Badge } from '@/components/ui/badge';
import { Settings, Database, Bell, RefreshCw } from 'lucide-react';

export default function SettingsPage() {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Settings</h1>
        <p className="text-gray-600 mt-2">
          Configure dashboard preferences and data sources
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Data Source Configuration */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Database className="h-5 w-5" />
              <span>Data Sources</span>
            </CardTitle>
            <CardDescription>
              Configure BigQuery and data refresh settings
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="project-id">BigQuery Project ID</Label>
              <Input 
                id="project-id" 
                value="trendle-469110" 
                disabled 
                className="bg-gray-50"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="dataset">Dataset</Label>
              <Input 
                id="dataset" 
                value="brand_health_dev" 
                disabled 
                className="bg-gray-50"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="refresh-interval">Data Refresh Interval</Label>
              <Select defaultValue="15min">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="5min">Every 5 minutes</SelectItem>
                  <SelectItem value="15min">Every 15 minutes</SelectItem>
                  <SelectItem value="30min">Every 30 minutes</SelectItem>
                  <SelectItem value="1hour">Every hour</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center justify-between pt-4 border-t">
              <div className="flex items-center space-x-2">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <span className="text-sm text-gray-600">Connected</span>
              </div>
              <Button variant="outline" size="sm">
                <RefreshCw className="h-4 w-4 mr-2" />
                Test Connection
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Alert Configuration */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Bell className="h-5 w-5" />
              <span>Alert Settings</span>
            </CardTitle>
            <CardDescription>
              Configure notification thresholds and preferences
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="severity-threshold">High Severity Threshold</Label>
              <Select defaultValue="70">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="60">60%</SelectItem>
                  <SelectItem value="70">70%</SelectItem>
                  <SelectItem value="80">80%</SelectItem>
                  <SelectItem value="90">90%</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="volume-threshold">Volume Spike Threshold</Label>
              <Select defaultValue="50">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="25">25% increase</SelectItem>
                  <SelectItem value="50">50% increase</SelectItem>
                  <SelectItem value="75">75% increase</SelectItem>
                  <SelectItem value="100">100% increase</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <Separator />

            <div className="space-y-3">
              <Label>Notification Channels</Label>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-sm">Email Notifications</span>
                  <Badge variant="secondary">Enabled</Badge>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm">Slack Integration</span>
                  <Badge variant="outline">Disabled</Badge>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm">SMS Alerts</span>
                  <Badge variant="outline">Disabled</Badge>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Dashboard Preferences */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Settings className="h-5 w-5" />
              <span>Dashboard Preferences</span>
            </CardTitle>
            <CardDescription>
              Customize dashboard appearance and behavior
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="default-view">Default View</Label>
              <Select defaultValue="overview">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="overview">Overview</SelectItem>
                  <SelectItem value="clusters">Cluster Analysis</SelectItem>
                  <SelectItem value="trends">Daily Trends</SelectItem>
                  <SelectItem value="complaints">Recent Complaints</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="date-range">Default Date Range</Label>
              <Select defaultValue="7days">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1day">Last 24 hours</SelectItem>
                  <SelectItem value="7days">Last 7 days</SelectItem>
                  <SelectItem value="30days">Last 30 days</SelectItem>
                  <SelectItem value="90days">Last 90 days</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="auto-refresh">Auto-refresh Dashboard</Label>
              <Select defaultValue="enabled">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="enabled">Enabled</SelectItem>
                  <SelectItem value="disabled">Disabled</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>

        {/* System Information */}
        <Card>
          <CardHeader>
            <CardTitle>System Information</CardTitle>
            <CardDescription>
              Current system status and information
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-500">Version:</span>
                <div className="font-medium">v1.0.0</div>
              </div>
              <div>
                <span className="text-gray-500">Last Updated:</span>
                <div className="font-medium">{new Date().toLocaleDateString()}</div>
              </div>
              <div>
                <span className="text-gray-500">Data Sources:</span>
                <div className="font-medium">Reddit, CFPB</div>
              </div>
              <div>
                <span className="text-gray-500">Total Records:</span>
                <div className="font-medium">1,363</div>
              </div>
            </div>

            <Separator />

            <div className="flex space-x-2">
              <Button variant="outline" size="sm">
                Export Data
              </Button>
              <Button variant="outline" size="sm">
                Download Report
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
