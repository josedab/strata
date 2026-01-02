/**
 * Alerts Page - System Alerts and Notifications
 */

import { useState } from 'react';
import { clsx } from 'clsx';
import {
  ExclamationTriangleIcon,
  ExclamationCircleIcon,
  InformationCircleIcon,
  CheckCircleIcon,
  FunnelIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';
import type { Alert, AlertSeverity, AlertStatus } from '@/types';

const mockAlerts: Alert[] = [
  {
    id: 'alert-1',
    severity: 'critical',
    status: 'active',
    title: 'Node strata-data-03 disk usage critical',
    message: 'Disk usage has exceeded 85% threshold. Consider adding more storage or cleaning up data.',
    source: 'strata-data-03',
    timestamp: new Date(Date.now() - 15 * 60000),
  },
  {
    id: 'alert-2',
    severity: 'warning',
    status: 'active',
    title: 'High memory usage on strata-meta-01',
    message: 'Memory usage is at 78%, approaching threshold of 80%.',
    source: 'strata-meta-01',
    timestamp: new Date(Date.now() - 45 * 60000),
  },
  {
    id: 'alert-3',
    severity: 'warning',
    status: 'acknowledged',
    title: 'Increased error rate on S3 gateway',
    message: 'Error rate has increased to 2.5%, above normal baseline of 0.5%.',
    source: 'strata-gateway-01',
    timestamp: new Date(Date.now() - 2 * 3600000),
    acknowledgedAt: new Date(Date.now() - 1 * 3600000),
  },
  {
    id: 'alert-4',
    severity: 'info',
    status: 'resolved',
    title: 'Scheduled maintenance completed',
    message: 'Node strata-data-02 has been successfully rebalanced.',
    source: 'cluster-manager',
    timestamp: new Date(Date.now() - 6 * 3600000),
    resolvedAt: new Date(Date.now() - 5 * 3600000),
  },
  {
    id: 'alert-5',
    severity: 'critical',
    status: 'resolved',
    title: 'Network partition detected',
    message: 'Temporary network partition between us-east-1a and us-east-1b zones.',
    source: 'network-monitor',
    timestamp: new Date(Date.now() - 24 * 3600000),
    resolvedAt: new Date(Date.now() - 23 * 3600000),
  },
];

const severityConfig: Record<AlertSeverity, { icon: typeof ExclamationCircleIcon; color: string; bg: string }> = {
  critical: {
    icon: ExclamationCircleIcon,
    color: 'text-red-400',
    bg: 'bg-red-500/10',
  },
  warning: {
    icon: ExclamationTriangleIcon,
    color: 'text-yellow-400',
    bg: 'bg-yellow-500/10',
  },
  info: {
    icon: InformationCircleIcon,
    color: 'text-blue-400',
    bg: 'bg-blue-500/10',
  },
};

const statusConfig: Record<AlertStatus, { label: string; color: string }> = {
  active: { label: 'Active', color: 'bg-red-500/20 text-red-400' },
  acknowledged: { label: 'Acknowledged', color: 'bg-yellow-500/20 text-yellow-400' },
  resolved: { label: 'Resolved', color: 'bg-green-500/20 text-green-400' },
};

function formatRelativeTime(date: Date): string {
  const now = Date.now();
  const diff = now - date.getTime();
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days = Math.floor(diff / 86400000);

  if (minutes < 1) return 'Just now';
  if (minutes < 60) return `${minutes}m ago`;
  if (hours < 24) return `${hours}h ago`;
  return `${days}d ago`;
}

interface AlertCardProps {
  alert: Alert;
  onAcknowledge: (id: string) => void;
  onResolve: (id: string) => void;
}

function AlertCard({ alert, onAcknowledge, onResolve }: AlertCardProps) {
  const severity = severityConfig[alert.severity];
  const status = statusConfig[alert.status];
  const Icon = severity.icon;

  return (
    <div className={clsx('card p-4', alert.status === 'resolved' && 'opacity-60')}>
      <div className="flex items-start space-x-4">
        <div className={clsx('p-2 rounded-lg', severity.bg)}>
          <Icon className={clsx('h-6 w-6', severity.color)} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between mb-1">
            <h3 className="text-sm font-semibold text-white truncate">{alert.title}</h3>
            <span className={clsx('px-2 py-0.5 text-xs rounded', status.color)}>
              {status.label}
            </span>
          </div>
          <p className="text-sm text-slate-400 mb-2">{alert.message}</p>
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4 text-xs text-slate-500">
              <span>Source: {alert.source}</span>
              <span>{formatRelativeTime(alert.timestamp)}</span>
              {alert.acknowledgedAt && (
                <span>Acknowledged: {formatRelativeTime(alert.acknowledgedAt)}</span>
              )}
              {alert.resolvedAt && (
                <span>Resolved: {formatRelativeTime(alert.resolvedAt)}</span>
              )}
            </div>
            {alert.status === 'active' && (
              <div className="flex items-center space-x-2">
                <button
                  onClick={() => onAcknowledge(alert.id)}
                  className="px-3 py-1 text-xs bg-slate-700 hover:bg-slate-600 text-white rounded"
                >
                  Acknowledge
                </button>
                <button
                  onClick={() => onResolve(alert.id)}
                  className="px-3 py-1 text-xs bg-green-600 hover:bg-green-500 text-white rounded"
                >
                  Resolve
                </button>
              </div>
            )}
            {alert.status === 'acknowledged' && (
              <button
                onClick={() => onResolve(alert.id)}
                className="px-3 py-1 text-xs bg-green-600 hover:bg-green-500 text-white rounded"
              >
                Resolve
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function Alerts() {
  const [alerts, setAlerts] = useState<Alert[]>(mockAlerts);
  const [filter, setFilter] = useState<AlertStatus | 'all'>('all');
  const [severityFilter, setSeverityFilter] = useState<AlertSeverity | 'all'>('all');

  const handleAcknowledge = (id: string) => {
    setAlerts((prev) =>
      prev.map((a) =>
        a.id === id
          ? { ...a, status: 'acknowledged' as AlertStatus, acknowledgedAt: new Date() }
          : a
      )
    );
    toast.success('Alert acknowledged');
  };

  const handleResolve = (id: string) => {
    setAlerts((prev) =>
      prev.map((a) =>
        a.id === id
          ? { ...a, status: 'resolved' as AlertStatus, resolvedAt: new Date() }
          : a
      )
    );
    toast.success('Alert resolved');
  };

  const filteredAlerts = alerts
    .filter((a) => filter === 'all' || a.status === filter)
    .filter((a) => severityFilter === 'all' || a.severity === severityFilter);

  const stats = {
    total: alerts.length,
    active: alerts.filter((a) => a.status === 'active').length,
    acknowledged: alerts.filter((a) => a.status === 'acknowledged').length,
    critical: alerts.filter((a) => a.severity === 'critical' && a.status !== 'resolved').length,
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Alerts</h1>
          <p className="text-slate-400">Monitor system alerts and notifications</p>
        </div>
        {stats.critical > 0 && (
          <div className="flex items-center space-x-2 px-4 py-2 bg-red-500/20 rounded-lg">
            <ExclamationCircleIcon className="h-5 w-5 text-red-400" />
            <span className="text-red-400 font-medium">
              {stats.critical} critical alert{stats.critical > 1 ? 's' : ''}
            </span>
          </div>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-4 gap-4">
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-white">{stats.total}</p>
          <p className="text-sm text-slate-400">Total Alerts</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-red-400">{stats.active}</p>
          <p className="text-sm text-slate-400">Active</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-yellow-400">{stats.acknowledged}</p>
          <p className="text-sm text-slate-400">Acknowledged</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-red-500">{stats.critical}</p>
          <p className="text-sm text-slate-400">Critical</p>
        </div>
      </div>

      {/* Filters */}
      <div className="flex items-center space-x-4">
        <div className="flex items-center space-x-2">
          <FunnelIcon className="h-5 w-5 text-slate-400" />
          <span className="text-sm text-slate-400">Filter:</span>
        </div>
        <div className="flex space-x-2">
          {(['all', 'active', 'acknowledged', 'resolved'] as const).map((status) => (
            <button
              key={status}
              onClick={() => setFilter(status)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-sm font-medium transition-colors',
                filter === status
                  ? 'bg-primary-600 text-white'
                  : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
              )}
            >
              {status.charAt(0).toUpperCase() + status.slice(1)}
            </button>
          ))}
        </div>
        <div className="border-l border-slate-700 h-6" />
        <div className="flex space-x-2">
          {(['all', 'critical', 'warning', 'info'] as const).map((severity) => (
            <button
              key={severity}
              onClick={() => setSeverityFilter(severity)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-sm font-medium transition-colors',
                severityFilter === severity
                  ? 'bg-primary-600 text-white'
                  : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
              )}
            >
              {severity.charAt(0).toUpperCase() + severity.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {/* Alert List */}
      <div className="space-y-4">
        {filteredAlerts.map((alert) => (
          <AlertCard
            key={alert.id}
            alert={alert}
            onAcknowledge={handleAcknowledge}
            onResolve={handleResolve}
          />
        ))}

        {filteredAlerts.length === 0 && (
          <div className="card p-12 text-center">
            <CheckCircleIcon className="h-12 w-12 text-green-500 mx-auto mb-4" />
            <p className="text-slate-400">No alerts matching your filters</p>
          </div>
        )}
      </div>
    </div>
  );
}
