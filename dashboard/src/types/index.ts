/**
 * Type definitions for the Strata Dashboard
 */

// Cluster Health
export type HealthStatus = 'healthy' | 'degraded' | 'unhealthy';

export interface ClusterHealth {
  status: HealthStatus;
  nodes: number;
  healthyNodes: number;
  uptime: number;
  version: string;
  lastCheck: Date;
}

// Node Types
export type NodeRole = 'metadata' | 'data' | 'gateway';
export type NodeStatus = 'online' | 'offline' | 'draining' | 'maintenance';

export interface Node {
  id: string;
  name: string;
  address: string;
  role: NodeRole;
  status: NodeStatus;
  uptime: number;
  cpuUsage: number;
  memoryUsage: number;
  diskUsage: number;
  networkIn: number;
  networkOut: number;
  lastHeartbeat: Date;
  version: string;
  zone?: string;
  tags?: Record<string, string>;
}

// Bucket Types
export interface Bucket {
  name: string;
  createdAt: Date;
  region?: string;
  versioning: boolean;
  encryption: boolean;
  objectCount: number;
  totalSize: number;
  owner?: string;
  tags?: Record<string, string>;
}

export interface BucketMetrics {
  objectCount: number;
  totalSizeBytes: number;
  requestsLastHour: number;
  bandwidthLastHourBytes: number;
  getRequests: number;
  putRequests: number;
  deleteRequests: number;
}

// Object Types
export interface StorageObject {
  key: string;
  bucket: string;
  size: number;
  contentType: string;
  etag: string;
  lastModified: Date;
  storageClass: string;
  versionId?: string;
  metadata?: Record<string, string>;
}

// Cluster Statistics
export interface ClusterStats {
  totalBuckets: number;
  totalObjects: number;
  totalSizeBytes: number;
  usedCapacityBytes: number;
  totalCapacityBytes: number;
  requestsPerSecond: number;
  bandwidthBytesPerSecond: number;
  activeConnections: number;
}

// Metrics & Time Series
export interface MetricDataPoint {
  timestamp: Date;
  value: number;
}

export interface MetricSeries {
  name: string;
  data: MetricDataPoint[];
}

export interface DashboardMetrics {
  throughputRead: MetricSeries;
  throughputWrite: MetricSeries;
  latencyRead: MetricSeries;
  latencyWrite: MetricSeries;
  iops: MetricSeries;
  connections: MetricSeries;
}

// Events & Alerts
export type AlertSeverity = 'critical' | 'warning' | 'info';
export type AlertStatus = 'active' | 'acknowledged' | 'resolved';

export interface Alert {
  id: string;
  severity: AlertSeverity;
  status: AlertStatus;
  title: string;
  message: string;
  source: string;
  timestamp: Date;
  acknowledgedAt?: Date;
  resolvedAt?: Date;
}

export interface AuditEvent {
  id: string;
  timestamp: Date;
  action: string;
  resource: string;
  resourceId: string;
  actor: string;
  outcome: 'success' | 'failure';
  details?: Record<string, unknown>;
}

// User & Auth
export interface User {
  id: string;
  username: string;
  email: string;
  role: 'admin' | 'operator' | 'viewer';
  createdAt: Date;
  lastLogin?: Date;
}

export interface Session {
  user: User;
  token: string;
  expiresAt: Date;
}

// API Response Types
export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

export interface ApiError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

// Form Types
export interface CreateBucketForm {
  name: string;
  region?: string;
  versioning: boolean;
  encryption: boolean;
}

export interface UploadObjectForm {
  bucket: string;
  key: string;
  file: File;
  contentType?: string;
  metadata?: Record<string, string>;
}

// Table & Filter Types
export interface TableColumn<T> {
  key: keyof T;
  label: string;
  sortable?: boolean;
  render?: (value: T[keyof T], row: T) => React.ReactNode;
}

export interface FilterOptions {
  search?: string;
  status?: string;
  dateFrom?: Date;
  dateTo?: Date;
  tags?: Record<string, string>;
}

export interface SortOptions {
  field: string;
  direction: 'asc' | 'desc';
}

// Navigation
export interface NavItem {
  name: string;
  path: string;
  icon: React.ComponentType<{ className?: string }>;
  badge?: number;
}
