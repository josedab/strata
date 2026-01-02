/**
 * Nodes Page - Cluster Node Management
 */

import { useEffect, useState } from 'react';
import { clsx } from 'clsx';
import {
  ServerIcon,
  ArrowPathIcon,
  PlayIcon,
  PauseIcon,
  EllipsisVerticalIcon,
} from '@heroicons/react/24/outline';
import { Menu } from '@headlessui/react';
import toast from 'react-hot-toast';
import { getMockNodes } from '@/api/client';
import type { Node, NodeStatus, NodeRole } from '@/types';

function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let unitIndex = 0;
  let value = bytes;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

const statusConfig: Record<NodeStatus, { color: string; bgColor: string; label: string }> = {
  online: { color: 'text-green-400', bgColor: 'bg-green-500', label: 'Online' },
  offline: { color: 'text-red-400', bgColor: 'bg-red-500', label: 'Offline' },
  draining: { color: 'text-yellow-400', bgColor: 'bg-yellow-500', label: 'Draining' },
  maintenance: { color: 'text-blue-400', bgColor: 'bg-blue-500', label: 'Maintenance' },
};

const roleConfig: Record<NodeRole, { color: string; label: string }> = {
  metadata: { color: 'bg-purple-500/20 text-purple-400', label: 'Metadata' },
  data: { color: 'bg-blue-500/20 text-blue-400', label: 'Data' },
  gateway: { color: 'bg-orange-500/20 text-orange-400', label: 'Gateway' },
};

interface NodeCardProps {
  node: Node;
  onDrain: (id: string) => void;
  onResume: (id: string) => void;
}

function NodeCard({ node, onDrain, onResume }: NodeCardProps) {
  const status = statusConfig[node.status];
  const role = roleConfig[node.role];

  return (
    <div className="card p-6">
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-slate-700 rounded-lg">
            <ServerIcon className="h-6 w-6 text-slate-300" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-white">{node.name}</h3>
            <p className="text-sm text-slate-400">{node.address}</p>
          </div>
        </div>
        <Menu as="div" className="relative">
          <Menu.Button className="p-1 text-slate-400 hover:text-white rounded">
            <EllipsisVerticalIcon className="h-5 w-5" />
          </Menu.Button>
          <Menu.Items className="absolute right-0 mt-2 w-48 bg-slate-700 rounded-lg shadow-lg border border-slate-600 z-10">
            <div className="py-1">
              {node.status === 'online' && (
                <Menu.Item>
                  {({ active }) => (
                    <button
                      onClick={() => onDrain(node.id)}
                      className={clsx(
                        'flex items-center w-full px-4 py-2 text-sm',
                        active ? 'bg-slate-600 text-white' : 'text-slate-300'
                      )}
                    >
                      <PauseIcon className="h-4 w-4 mr-2" />
                      Drain Node
                    </button>
                  )}
                </Menu.Item>
              )}
              {node.status === 'draining' && (
                <Menu.Item>
                  {({ active }) => (
                    <button
                      onClick={() => onResume(node.id)}
                      className={clsx(
                        'flex items-center w-full px-4 py-2 text-sm',
                        active ? 'bg-slate-600 text-white' : 'text-slate-300'
                      )}
                    >
                      <PlayIcon className="h-4 w-4 mr-2" />
                      Resume Node
                    </button>
                  )}
                </Menu.Item>
              )}
              <Menu.Item>
                {({ active }) => (
                  <button
                    className={clsx(
                      'flex items-center w-full px-4 py-2 text-sm',
                      active ? 'bg-slate-600 text-white' : 'text-slate-300'
                    )}
                  >
                    <ArrowPathIcon className="h-4 w-4 mr-2" />
                    Refresh Status
                  </button>
                )}
              </Menu.Item>
            </div>
          </Menu.Items>
        </Menu>
      </div>

      <div className="flex items-center space-x-3 mb-4">
        <div className="flex items-center space-x-1.5">
          <div className={clsx('h-2 w-2 rounded-full', status.bgColor)} />
          <span className={clsx('text-sm font-medium', status.color)}>{status.label}</span>
        </div>
        <span className={clsx('px-2 py-0.5 text-xs font-medium rounded', role.color)}>
          {role.label}
        </span>
        {node.zone && (
          <span className="px-2 py-0.5 text-xs font-medium rounded bg-slate-700 text-slate-300">
            {node.zone}
          </span>
        )}
      </div>

      {/* Resource Metrics */}
      <div className="space-y-3">
        <div>
          <div className="flex justify-between text-sm mb-1">
            <span className="text-slate-400">CPU</span>
            <span className="text-white">{node.cpuUsage.toFixed(1)}%</span>
          </div>
          <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
            <div
              className={clsx(
                'h-full rounded-full',
                node.cpuUsage > 80 ? 'bg-red-500' : node.cpuUsage > 60 ? 'bg-yellow-500' : 'bg-green-500'
              )}
              style={{ width: `${node.cpuUsage}%` }}
            />
          </div>
        </div>
        <div>
          <div className="flex justify-between text-sm mb-1">
            <span className="text-slate-400">Memory</span>
            <span className="text-white">{node.memoryUsage.toFixed(1)}%</span>
          </div>
          <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
            <div
              className={clsx(
                'h-full rounded-full',
                node.memoryUsage > 80 ? 'bg-red-500' : node.memoryUsage > 60 ? 'bg-yellow-500' : 'bg-green-500'
              )}
              style={{ width: `${node.memoryUsage}%` }}
            />
          </div>
        </div>
        <div>
          <div className="flex justify-between text-sm mb-1">
            <span className="text-slate-400">Disk</span>
            <span className="text-white">{node.diskUsage.toFixed(1)}%</span>
          </div>
          <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
            <div
              className={clsx(
                'h-full rounded-full',
                node.diskUsage > 80 ? 'bg-red-500' : node.diskUsage > 60 ? 'bg-yellow-500' : 'bg-blue-500'
              )}
              style={{ width: `${node.diskUsage}%` }}
            />
          </div>
        </div>
      </div>

      {/* Network Stats */}
      <div className="mt-4 pt-4 border-t border-slate-700 grid grid-cols-2 gap-4">
        <div>
          <p className="text-xs text-slate-400">Network In</p>
          <p className="text-sm font-medium text-white">{formatBytes(node.networkIn)}/s</p>
        </div>
        <div>
          <p className="text-xs text-slate-400">Network Out</p>
          <p className="text-sm font-medium text-white">{formatBytes(node.networkOut)}/s</p>
        </div>
        <div>
          <p className="text-xs text-slate-400">Uptime</p>
          <p className="text-sm font-medium text-white">{formatUptime(node.uptime)}</p>
        </div>
        <div>
          <p className="text-xs text-slate-400">Version</p>
          <p className="text-sm font-medium text-white">v{node.version}</p>
        </div>
      </div>
    </div>
  );
}

export default function Nodes() {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<NodeRole | 'all'>('all');

  useEffect(() => {
    setNodes(getMockNodes());
    setLoading(false);
  }, []);

  const handleDrain = (id: string) => {
    setNodes((prev) =>
      prev.map((n) => (n.id === id ? { ...n, status: 'draining' as NodeStatus } : n))
    );
    toast.success('Node draining initiated');
  };

  const handleResume = (id: string) => {
    setNodes((prev) =>
      prev.map((n) => (n.id === id ? { ...n, status: 'online' as NodeStatus } : n))
    );
    toast.success('Node resumed');
  };

  const filteredNodes = filter === 'all' ? nodes : nodes.filter((n) => n.role === filter);

  const stats = {
    total: nodes.length,
    online: nodes.filter((n) => n.status === 'online').length,
    draining: nodes.filter((n) => n.status === 'draining').length,
    offline: nodes.filter((n) => n.status === 'offline').length,
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Nodes</h1>
          <p className="text-slate-400">Manage cluster nodes</p>
        </div>
        <button className="btn-primary flex items-center">
          <ArrowPathIcon className="h-4 w-4 mr-2" />
          Refresh
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-4 gap-4">
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-white">{stats.total}</p>
          <p className="text-sm text-slate-400">Total Nodes</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-green-400">{stats.online}</p>
          <p className="text-sm text-slate-400">Online</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-yellow-400">{stats.draining}</p>
          <p className="text-sm text-slate-400">Draining</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-3xl font-bold text-red-400">{stats.offline}</p>
          <p className="text-sm text-slate-400">Offline</p>
        </div>
      </div>

      {/* Filter */}
      <div className="flex space-x-2">
        {(['all', 'metadata', 'data', 'gateway'] as const).map((role) => (
          <button
            key={role}
            onClick={() => setFilter(role)}
            className={clsx(
              'px-4 py-2 rounded-lg text-sm font-medium transition-colors',
              filter === role
                ? 'bg-primary-600 text-white'
                : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
            )}
          >
            {role.charAt(0).toUpperCase() + role.slice(1)}
          </button>
        ))}
      </div>

      {/* Node Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {filteredNodes.map((node) => (
          <NodeCard
            key={node.id}
            node={node}
            onDrain={handleDrain}
            onResume={handleResume}
          />
        ))}
      </div>
    </div>
  );
}
