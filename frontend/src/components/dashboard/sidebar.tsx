'use client';

import React from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { cn } from '@/lib/utils';
import {
  BarChart3,
  TrendingUp,
  AlertTriangle,
  MessageSquare,
  Settings,
  Home
} from 'lucide-react';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';

interface NavItem {
  name: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
}

const navigation: NavItem[] = [
  { name: 'Overview', href: '/dashboard', icon: Home },
  { name: 'Cluster Analysis', href: '/dashboard/clusters', icon: BarChart3 },
  { name: 'Daily Trends', href: '/dashboard/trends', icon: TrendingUp },
  { name: 'Complaints', href: '/dashboard/complaints', icon: MessageSquare },
  { name: 'Settings', href: '/dashboard/settings', icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <div className="flex-shrink-0 flex flex-col bg-white border-r border-gray-100 w-16">
      {/* Navigation */}
      <nav className="flex-1 px-2 py-3">
        <div className="space-y-1">
          {navigation.map((item) => {
            const isActive = pathname === item.href;
            const Icon = item.icon;

            return (
              <div key={item.name}>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Link
                        href={item.href}
                        className={cn(
                          'flex items-center justify-center p-3 rounded-md transition-all duration-200 group',
                          isActive
                            ? 'bg-black text-white'
                            : 'text-gray-600 hover:text-black hover:bg-gray-50'
                        )}
                      >
                        <Icon className="h-5 w-5 transition-transform duration-200 group-hover:scale-110" />
                      </Link>
                    </TooltipTrigger>
                    <TooltipContent side="right" className="bg-gray-900 text-white border-gray-700">
                      <p>{item.name}</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </div>
            );
          })}
        </div>
      </nav>

      {/* Footer Info */}
      <div className="px-2 pb-3">
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center justify-center p-3 rounded-md text-gray-400 cursor-default">
                <div className="h-2 w-2 bg-green-500 rounded-full animate-pulse" />
              </div>
            </TooltipTrigger>
            <TooltipContent side="right" className="bg-gray-900 text-white border-gray-700">
              <p className="text-xs">Last updated: {new Date().toLocaleString()}</p>
              <p className="text-xs mt-1">Data from Reddit & CFPB</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>
    </div>
  );
}
