/**
 * API Client for the Strata Dashboard
 */

import axios, { AxiosInstance, AxiosError } from 'axios';
import type {
  ClusterHealth,
  ClusterStats,
  Node,
  Bucket,
  BucketMetrics,
  StorageObject,
  Alert,
  AuditEvent,
  User,
  Session,
  PaginatedResponse,
  CreateBucketForm,
  DashboardMetrics,
  MetricSeries,
  MetricDataPoint,
} from '@/types';

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

class ApiClient {
  private client: AxiosInstance;
  private token: string | null = null;

  constructor() {
    this.client = axios.create({
      baseURL: API_BASE_URL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor for auth
    this.client.interceptors.request.use((config) => {
      if (this.token) {
        config.headers.Authorization = `Bearer ${this.token}`;
      }
      return config;
    });

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error: AxiosError) => {
        if (error.response?.status === 401) {
          this.token = null;
          window.location.href = '/login';
        }
        return Promise.reject(error);
      }
    );
  }

  setToken(token: string | null) {
    this.token = token;
  }

  // Auth
  async login(username: string, password: string): Promise<Session> {
    const response = await this.client.post<Session>('/auth/login', {
      username,
      password,
    });
    this.token = response.data.token;
    return response.data;
  }

  async logout(): Promise<void> {
    await this.client.post('/auth/logout');
    this.token = null;
  }

  async getCurrentUser(): Promise<User> {
    const response = await this.client.get<User>('/auth/me');
    return response.data;
  }

  // Cluster Health & Stats
  async getClusterHealth(): Promise<ClusterHealth> {
    const response = await this.client.get<ClusterHealth>('/cluster/health');
    return response.data;
  }

  async getClusterStats(): Promise<ClusterStats> {
    const response = await this.client.get<ClusterStats>('/cluster/stats');
    return response.data;
  }

  // Nodes
  async getNodes(): Promise<Node[]> {
    const response = await this.client.get<Node[]>('/nodes');
    return response.data;
  }

  async getNode(id: string): Promise<Node> {
    const response = await this.client.get<Node>(`/nodes/${id}`);
    return response.data;
  }

  async drainNode(id: string): Promise<void> {
    await this.client.post(`/nodes/${id}/drain`);
  }

  async resumeNode(id: string): Promise<void> {
    await this.client.post(`/nodes/${id}/resume`);
  }

  // Buckets
  async getBuckets(
    page = 1,
    pageSize = 20
  ): Promise<PaginatedResponse<Bucket>> {
    const response = await this.client.get<PaginatedResponse<Bucket>>(
      '/buckets',
      {
        params: { page, pageSize },
      }
    );
    return response.data;
  }

  async getBucket(name: string): Promise<Bucket> {
    const response = await this.client.get<Bucket>(`/buckets/${name}`);
    return response.data;
  }

  async createBucket(data: CreateBucketForm): Promise<Bucket> {
    const response = await this.client.post<Bucket>('/buckets', data);
    return response.data;
  }

  async deleteBucket(name: string): Promise<void> {
    await this.client.delete(`/buckets/${name}`);
  }

  async getBucketMetrics(name: string): Promise<BucketMetrics> {
    const response = await this.client.get<BucketMetrics>(
      `/buckets/${name}/metrics`
    );
    return response.data;
  }

  // Objects
  async getObjects(
    bucket: string,
    prefix = '',
    page = 1,
    pageSize = 50
  ): Promise<PaginatedResponse<StorageObject>> {
    const response = await this.client.get<PaginatedResponse<StorageObject>>(
      `/buckets/${bucket}/objects`,
      {
        params: { prefix, page, pageSize },
      }
    );
    return response.data;
  }

  async deleteObject(bucket: string, key: string): Promise<void> {
    await this.client.delete(`/buckets/${bucket}/objects/${encodeURIComponent(key)}`);
  }

  async getPresignedUrl(
    bucket: string,
    key: string,
    method: 'GET' | 'PUT' = 'GET'
  ): Promise<string> {
    const response = await this.client.get<{ url: string }>(
      `/buckets/${bucket}/objects/${encodeURIComponent(key)}/presigned`,
      { params: { method } }
    );
    return response.data.url;
  }

  // Alerts
  async getAlerts(status?: string): Promise<Alert[]> {
    const response = await this.client.get<Alert[]>('/alerts', {
      params: { status },
    });
    return response.data;
  }

  async acknowledgeAlert(id: string): Promise<void> {
    await this.client.post(`/alerts/${id}/acknowledge`);
  }

  async resolveAlert(id: string): Promise<void> {
    await this.client.post(`/alerts/${id}/resolve`);
  }

  // Audit Events
  async getAuditEvents(
    page = 1,
    pageSize = 50,
    filters?: { action?: string; resource?: string }
  ): Promise<PaginatedResponse<AuditEvent>> {
    const response = await this.client.get<PaginatedResponse<AuditEvent>>(
      '/audit',
      {
        params: { page, pageSize, ...filters },
      }
    );
    return response.data;
  }

  // Metrics
  async getDashboardMetrics(
    period: '1h' | '6h' | '24h' | '7d' = '24h'
  ): Promise<DashboardMetrics> {
    const response = await this.client.get<DashboardMetrics>('/metrics/dashboard', {
      params: { period },
    });
    return response.data;
  }

  async getMetricSeries(
    metric: string,
    period: '1h' | '6h' | '24h' | '7d'
  ): Promise<MetricSeries> {
    const response = await this.client.get<MetricSeries>(`/metrics/${metric}`, {
      params: { period },
    });
    return response.data;
  }
}

export const api = new ApiClient();

// Mock data for development
export function getMockClusterHealth(): ClusterHealth {
  return {
    status: 'healthy',
    nodes: 5,
    healthyNodes: 5,
    uptime: 864000,
    version: '1.0.0',
    lastCheck: new Date(),
  };
}

export function getMockClusterStats(): ClusterStats {
  return {
    totalBuckets: 42,
    totalObjects: 1250000,
    totalSizeBytes: 5.2 * 1024 * 1024 * 1024 * 1024, // 5.2 TB
    usedCapacityBytes: 3.8 * 1024 * 1024 * 1024 * 1024,
    totalCapacityBytes: 10 * 1024 * 1024 * 1024 * 1024,
    requestsPerSecond: 2450,
    bandwidthBytesPerSecond: 1.2 * 1024 * 1024 * 1024,
    activeConnections: 342,
  };
}

export function getMockNodes(): Node[] {
  return [
    {
      id: 'node-1',
      name: 'strata-meta-01',
      address: '10.0.1.10:9000',
      role: 'metadata',
      status: 'online',
      uptime: 864000,
      cpuUsage: 35.2,
      memoryUsage: 62.5,
      diskUsage: 45.0,
      networkIn: 125 * 1024 * 1024,
      networkOut: 89 * 1024 * 1024,
      lastHeartbeat: new Date(),
      version: '1.0.0',
      zone: 'us-east-1a',
    },
    {
      id: 'node-2',
      name: 'strata-data-01',
      address: '10.0.1.11:9000',
      role: 'data',
      status: 'online',
      uptime: 864000,
      cpuUsage: 58.3,
      memoryUsage: 71.2,
      diskUsage: 68.5,
      networkIn: 890 * 1024 * 1024,
      networkOut: 1.2 * 1024 * 1024 * 1024,
      lastHeartbeat: new Date(),
      version: '1.0.0',
      zone: 'us-east-1a',
    },
    {
      id: 'node-3',
      name: 'strata-data-02',
      address: '10.0.1.12:9000',
      role: 'data',
      status: 'online',
      uptime: 432000,
      cpuUsage: 42.1,
      memoryUsage: 55.8,
      diskUsage: 52.3,
      networkIn: 650 * 1024 * 1024,
      networkOut: 780 * 1024 * 1024,
      lastHeartbeat: new Date(),
      version: '1.0.0',
      zone: 'us-east-1b',
    },
    {
      id: 'node-4',
      name: 'strata-gateway-01',
      address: '10.0.1.20:9000',
      role: 'gateway',
      status: 'online',
      uptime: 172800,
      cpuUsage: 28.9,
      memoryUsage: 45.2,
      diskUsage: 15.0,
      networkIn: 2.5 * 1024 * 1024 * 1024,
      networkOut: 3.1 * 1024 * 1024 * 1024,
      lastHeartbeat: new Date(),
      version: '1.0.0',
      zone: 'us-east-1a',
    },
    {
      id: 'node-5',
      name: 'strata-data-03',
      address: '10.0.1.13:9000',
      role: 'data',
      status: 'draining',
      uptime: 864000,
      cpuUsage: 15.2,
      memoryUsage: 32.1,
      diskUsage: 78.9,
      networkIn: 125 * 1024 * 1024,
      networkOut: 450 * 1024 * 1024,
      lastHeartbeat: new Date(Date.now() - 5000),
      version: '1.0.0',
      zone: 'us-east-1c',
    },
  ];
}

export function getMockMetrics(): DashboardMetrics {
  const now = Date.now();
  const generateSeries = (
    name: string,
    baseValue: number,
    variance: number
  ): MetricSeries => ({
    name,
    data: Array.from({ length: 24 }, (_, i): MetricDataPoint => ({
      timestamp: new Date(now - (23 - i) * 3600000),
      value: baseValue + (Math.random() - 0.5) * variance,
    })),
  });

  return {
    throughputRead: generateSeries('Read Throughput', 500, 200),
    throughputWrite: generateSeries('Write Throughput', 300, 150),
    latencyRead: generateSeries('Read Latency', 15, 10),
    latencyWrite: generateSeries('Write Latency', 25, 15),
    iops: generateSeries('IOPS', 5000, 2000),
    connections: generateSeries('Connections', 300, 100),
  };
}
