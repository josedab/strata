/**
 * Dashboard Page - Main Overview
 */

import { useEffect, useState } from 'react';
import {
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  ServerIcon,
  ArchiveBoxIcon,
  DocumentIcon,
  BoltIcon,
} from '@heroicons/react/24/outline';
import { clsx } from 'clsx';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import {
  getMockClusterHealth,
  getMockClusterStats,
  getMockNodes,
  getMockMetrics,
} from '@/api/client';
import type { ClusterHealth, ClusterStats, Node, DashboardMetrics } from '@/types';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let unitIndex = 0;
  let value = bytes;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }

  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

function formatNumber(num: number): string {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toFixed(0);
}

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  return `${days}d ${hours}h`;
}

interface StatCardProps {
  title: string;
  value: string;
  subtitle?: string;
  icon: React.ComponentType<{ className?: string }>;
  trend?: number;
  color?: 'blue' | 'green' | 'purple' | 'orange';
}

function StatCard({ title, value, subtitle, icon: Icon, trend, color = 'blue' }: StatCardProps) {
  const colorClasses = {
    blue: 'bg-blue-500/10 text-blue-400',
    green: 'bg-green-500/10 text-green-400',
    purple: 'bg-purple-500/10 text-purple-400',
    orange: 'bg-orange-500/10 text-orange-400',
  };

  return (
    <div className="card p-6">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm font-medium text-slate-400">{title}</p>
          <p className="mt-2 text-3xl font-semibold text-white">{value}</p>
          {subtitle && (
            <p className="mt-1 text-sm text-slate-400">{subtitle}</p>
          )}
        </div>
        <div className={clsx('p-3 rounded-lg', colorClasses[color])}>
          <Icon className="h-6 w-6" />
        </div>
      </div>
      {trend !== undefined && (
        <div className="mt-4 flex items-center">
          {trend >= 0 ? (
            <ArrowTrendingUpIcon className="h-4 w-4 text-green-400 mr-1" />
          ) : (
            <ArrowTrendingDownIcon className="h-4 w-4 text-red-400 mr-1" />
          )}
          <span
            className={clsx(
              'text-sm font-medium',
              trend >= 0 ? 'text-green-400' : 'text-red-400'
            )}
          >
            {Math.abs(trend)}%
          </span>
          <span className="text-sm text-slate-400 ml-1">vs last hour</span>
        </div>
      )}
    </div>
  );
}

interface NodeStatusProps {
  nodes: Node[];
}

function NodeStatus({ nodes }: NodeStatusProps) {
  const statusColors = {
    online: 'bg-green-500',
    offline: 'bg-red-500',
    draining: 'bg-yellow-500',
    maintenance: 'bg-blue-500',
  };

  return (
    <div className="card p-6">
      <h3 className="text-lg font-semibold text-white mb-4">Node Status</h3>
      <div className="space-y-3">
        {nodes.map((node) => (
          <div key={node.id} className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className={clsx('h-2.5 w-2.5 rounded-full', statusColors[node.status])} />
              <div>
                <p className="text-sm font-medium text-white">{node.name}</p>
                <p className="text-xs text-slate-400">{node.role}</p>
              </div>
            </div>
            <div className="text-right">
              <p className="text-sm text-slate-300">{node.cpuUsage.toFixed(0)}% CPU</p>
              <p className="text-xs text-slate-400">{node.memoryUsage.toFixed(0)}% RAM</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

interface ThroughputChartProps {
  metrics: DashboardMetrics;
}

function ThroughputChart({ metrics }: ThroughputChartProps) {
  const labels = metrics.throughputRead.data.map((d) =>
    new Date(d.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  );

  const data = {
    labels,
    datasets: [
      {
        label: 'Read',
        data: metrics.throughputRead.data.map((d) => d.value),
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        fill: true,
        tension: 0.4,
      },
      {
        label: 'Write',
        data: metrics.throughputWrite.data.map((d) => d.value),
        borderColor: '#8b5cf6',
        backgroundColor: 'rgba(139, 92, 246, 0.1)',
        fill: true,
        tension: 0.4,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top' as const,
        labels: {
          color: '#94a3b8',
        },
      },
    },
    scales: {
      x: {
        grid: {
          color: '#334155',
        },
        ticks: {
          color: '#94a3b8',
        },
      },
      y: {
        grid: {
          color: '#334155',
        },
        ticks: {
          color: '#94a3b8',
          callback: (value: number | string) => `${value} MB/s`,
        },
      },
    },
  };

  return (
    <div className="card p-6">
      <h3 className="text-lg font-semibold text-white mb-4">Throughput</h3>
      <div className="h-64">
        <Line data={data} options={options} />
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [health, setHealth] = useState<ClusterHealth | null>(null);
  const [stats, setStats] = useState<ClusterStats | null>(null);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [metrics, setMetrics] = useState<DashboardMetrics | null>(null);

  useEffect(() => {
    // Load mock data for development
    setHealth(getMockClusterHealth());
    setStats(getMockClusterStats());
    setNodes(getMockNodes());
    setMetrics(getMockMetrics());
  }, []);

  if (!health || !stats || !metrics) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Dashboard</h1>
          <p className="text-slate-400">Cluster overview and real-time metrics</p>
        </div>
        <div className="flex items-center space-x-2">
          <span
            className={clsx(
              'px-3 py-1 rounded-full text-sm font-medium',
              health.status === 'healthy'
                ? 'bg-green-500/20 text-green-400'
                : health.status === 'degraded'
                ? 'bg-yellow-500/20 text-yellow-400'
                : 'bg-red-500/20 text-red-400'
            )}
          >
            {health.status.charAt(0).toUpperCase() + health.status.slice(1)}
          </span>
          <span className="text-sm text-slate-400">
            Uptime: {formatUptime(health.uptime)}
          </span>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          title="Total Nodes"
          value={`${health.healthyNodes}/${health.nodes}`}
          subtitle="Healthy nodes"
          icon={ServerIcon}
          color="blue"
        />
        <StatCard
          title="Total Buckets"
          value={formatNumber(stats.totalBuckets)}
          icon={ArchiveBoxIcon}
          color="green"
        />
        <StatCard
          title="Total Objects"
          value={formatNumber(stats.totalObjects)}
          subtitle={formatBytes(stats.totalSizeBytes)}
          icon={DocumentIcon}
          color="purple"
        />
        <StatCard
          title="Requests/sec"
          value={formatNumber(stats.requestsPerSecond)}
          subtitle={`${formatBytes(stats.bandwidthBytesPerSecond)}/s`}
          icon={BoltIcon}
          trend={5.2}
          color="orange"
        />
      </div>

      {/* Charts and Status */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2">
          <ThroughputChart metrics={metrics} />
        </div>
        <div>
          <NodeStatus nodes={nodes} />
        </div>
      </div>

      {/* Capacity Overview */}
      <div className="card p-6">
        <h3 className="text-lg font-semibold text-white mb-4">Storage Capacity</h3>
        <div className="space-y-4">
          <div>
            <div className="flex justify-between text-sm mb-1">
              <span className="text-slate-400">Used</span>
              <span className="text-white">
                {formatBytes(stats.usedCapacityBytes)} / {formatBytes(stats.totalCapacityBytes)}
              </span>
            </div>
            <div className="h-3 bg-slate-700 rounded-full overflow-hidden">
              <div
                className="h-full bg-gradient-to-r from-primary-500 to-strata-accent"
                style={{
                  width: `${(stats.usedCapacityBytes / stats.totalCapacityBytes) * 100}%`,
                }}
              />
            </div>
          </div>
          <div className="grid grid-cols-3 gap-4 pt-2">
            <div className="text-center">
              <p className="text-2xl font-semibold text-white">
                {((stats.usedCapacityBytes / stats.totalCapacityBytes) * 100).toFixed(1)}%
              </p>
              <p className="text-xs text-slate-400">Utilization</p>
            </div>
            <div className="text-center">
              <p className="text-2xl font-semibold text-white">
                {formatBytes(stats.totalCapacityBytes - stats.usedCapacityBytes)}
              </p>
              <p className="text-xs text-slate-400">Available</p>
            </div>
            <div className="text-center">
              <p className="text-2xl font-semibold text-white">{stats.activeConnections}</p>
              <p className="text-xs text-slate-400">Active Connections</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
