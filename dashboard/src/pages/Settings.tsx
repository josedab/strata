/**
 * Settings Page - System Configuration
 */

import { useState } from 'react';
import { clsx } from 'clsx';
import { useForm } from 'react-hook-form';
import toast from 'react-hot-toast';

interface GeneralSettings {
  clusterName: string;
  region: string;
  replicationFactor: number;
  erasureCoding: boolean;
}

interface SecuritySettings {
  encryptionAtRest: boolean;
  encryptionAlgorithm: 'AES256' | 'ChaCha20';
  tlsEnabled: boolean;
  minTlsVersion: '1.2' | '1.3';
  auditLogging: boolean;
}

interface NotificationSettings {
  emailEnabled: boolean;
  emailRecipients: string;
  slackEnabled: boolean;
  slackWebhook: string;
  pagerDutyEnabled: boolean;
  pagerDutyKey: string;
}

const tabs = ['General', 'Security', 'Notifications', 'Advanced'] as const;
type Tab = (typeof tabs)[number];

export default function Settings() {
  const [activeTab, setActiveTab] = useState<Tab>('General');

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-white">Settings</h1>
        <p className="text-slate-400">Configure cluster settings and preferences</p>
      </div>

      {/* Tabs */}
      <div className="border-b border-slate-700">
        <nav className="flex space-x-8">
          {tabs.map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={clsx(
                'py-3 px-1 border-b-2 font-medium text-sm transition-colors',
                activeTab === tab
                  ? 'border-primary-500 text-primary-400'
                  : 'border-transparent text-slate-400 hover:text-white'
              )}
            >
              {tab}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="card p-6">
        {activeTab === 'General' && <GeneralSettingsForm />}
        {activeTab === 'Security' && <SecuritySettingsForm />}
        {activeTab === 'Notifications' && <NotificationSettingsForm />}
        {activeTab === 'Advanced' && <AdvancedSettings />}
      </div>
    </div>
  );
}

function GeneralSettingsForm() {
  const { register, handleSubmit } = useForm<GeneralSettings>({
    defaultValues: {
      clusterName: 'strata-production',
      region: 'us-east-1',
      replicationFactor: 3,
      erasureCoding: true,
    },
  });

  const onSubmit = (data: GeneralSettings) => {
    console.log(data);
    toast.success('Settings saved');
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="space-y-6 max-w-xl">
      <div>
        <label className="label">Cluster Name</label>
        <input {...register('clusterName')} className="input" />
        <p className="mt-1 text-xs text-slate-500">
          A unique identifier for this Strata cluster
        </p>
      </div>

      <div>
        <label className="label">Primary Region</label>
        <select {...register('region')} className="input">
          <option value="us-east-1">US East (N. Virginia)</option>
          <option value="us-west-2">US West (Oregon)</option>
          <option value="eu-west-1">EU (Ireland)</option>
          <option value="ap-southeast-1">Asia Pacific (Singapore)</option>
        </select>
      </div>

      <div>
        <label className="label">Replication Factor</label>
        <input
          type="number"
          {...register('replicationFactor', { min: 1, max: 5 })}
          className="input w-24"
          min={1}
          max={5}
        />
        <p className="mt-1 text-xs text-slate-500">
          Number of copies for each chunk (1-5)
        </p>
      </div>

      <div className="flex items-center space-x-3">
        <input
          type="checkbox"
          {...register('erasureCoding')}
          className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
        />
        <div>
          <label className="text-sm font-medium text-white">Enable Erasure Coding</label>
          <p className="text-xs text-slate-500">
            Use Reed-Solomon erasure coding for storage efficiency
          </p>
        </div>
      </div>

      <div className="pt-4">
        <button type="submit" className="btn-primary">
          Save Changes
        </button>
      </div>
    </form>
  );
}

function SecuritySettingsForm() {
  const { register, handleSubmit, watch } = useForm<SecuritySettings>({
    defaultValues: {
      encryptionAtRest: true,
      encryptionAlgorithm: 'AES256',
      tlsEnabled: true,
      minTlsVersion: '1.2',
      auditLogging: true,
    },
  });

  const encryptionEnabled = watch('encryptionAtRest');
  const tlsEnabled = watch('tlsEnabled');

  const onSubmit = (data: SecuritySettings) => {
    console.log(data);
    toast.success('Security settings saved');
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="space-y-6 max-w-xl">
      <div className="space-y-4">
        <h3 className="text-lg font-medium text-white">Encryption</h3>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            {...register('encryptionAtRest')}
            className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
          />
          <div>
            <label className="text-sm font-medium text-white">Encryption at Rest</label>
            <p className="text-xs text-slate-500">Encrypt all data stored on disk</p>
          </div>
        </div>

        {encryptionEnabled && (
          <div className="ml-7">
            <label className="label">Encryption Algorithm</label>
            <select {...register('encryptionAlgorithm')} className="input">
              <option value="AES256">AES-256-GCM</option>
              <option value="ChaCha20">ChaCha20-Poly1305</option>
            </select>
          </div>
        )}
      </div>

      <div className="space-y-4 pt-4 border-t border-slate-700">
        <h3 className="text-lg font-medium text-white">TLS Configuration</h3>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            {...register('tlsEnabled')}
            className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
          />
          <div>
            <label className="text-sm font-medium text-white">Enable TLS</label>
            <p className="text-xs text-slate-500">Encrypt all network traffic</p>
          </div>
        </div>

        {tlsEnabled && (
          <div className="ml-7">
            <label className="label">Minimum TLS Version</label>
            <select {...register('minTlsVersion')} className="input">
              <option value="1.2">TLS 1.2</option>
              <option value="1.3">TLS 1.3</option>
            </select>
          </div>
        )}
      </div>

      <div className="space-y-4 pt-4 border-t border-slate-700">
        <h3 className="text-lg font-medium text-white">Audit</h3>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            {...register('auditLogging')}
            className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
          />
          <div>
            <label className="text-sm font-medium text-white">Enable Audit Logging</label>
            <p className="text-xs text-slate-500">
              Log all administrative actions for compliance
            </p>
          </div>
        </div>
      </div>

      <div className="pt-4">
        <button type="submit" className="btn-primary">
          Save Security Settings
        </button>
      </div>
    </form>
  );
}

function NotificationSettingsForm() {
  const { register, handleSubmit, watch } = useForm<NotificationSettings>({
    defaultValues: {
      emailEnabled: true,
      emailRecipients: 'ops@example.com',
      slackEnabled: false,
      slackWebhook: '',
      pagerDutyEnabled: false,
      pagerDutyKey: '',
    },
  });

  const emailEnabled = watch('emailEnabled');
  const slackEnabled = watch('slackEnabled');
  const pagerDutyEnabled = watch('pagerDutyEnabled');

  const onSubmit = (data: NotificationSettings) => {
    console.log(data);
    toast.success('Notification settings saved');
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="space-y-6 max-w-xl">
      <div className="space-y-4">
        <h3 className="text-lg font-medium text-white">Email Notifications</h3>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            {...register('emailEnabled')}
            className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
          />
          <label className="text-sm font-medium text-white">Enable Email Alerts</label>
        </div>

        {emailEnabled && (
          <div className="ml-7">
            <label className="label">Recipients</label>
            <input
              {...register('emailRecipients')}
              className="input"
              placeholder="email@example.com"
            />
            <p className="mt-1 text-xs text-slate-500">
              Comma-separated list of email addresses
            </p>
          </div>
        )}
      </div>

      <div className="space-y-4 pt-4 border-t border-slate-700">
        <h3 className="text-lg font-medium text-white">Slack Integration</h3>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            {...register('slackEnabled')}
            className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
          />
          <label className="text-sm font-medium text-white">Enable Slack Notifications</label>
        </div>

        {slackEnabled && (
          <div className="ml-7">
            <label className="label">Webhook URL</label>
            <input
              {...register('slackWebhook')}
              className="input"
              placeholder="https://hooks.slack.com/..."
            />
          </div>
        )}
      </div>

      <div className="space-y-4 pt-4 border-t border-slate-700">
        <h3 className="text-lg font-medium text-white">PagerDuty Integration</h3>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            {...register('pagerDutyEnabled')}
            className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
          />
          <label className="text-sm font-medium text-white">Enable PagerDuty</label>
        </div>

        {pagerDutyEnabled && (
          <div className="ml-7">
            <label className="label">Integration Key</label>
            <input
              {...register('pagerDutyKey')}
              type="password"
              className="input"
              placeholder="Enter integration key..."
            />
          </div>
        )}
      </div>

      <div className="pt-4">
        <button type="submit" className="btn-primary">
          Save Notification Settings
        </button>
      </div>
    </form>
  );
}

function AdvancedSettings() {
  return (
    <div className="space-y-6 max-w-xl">
      <div className="p-4 bg-yellow-500/10 border border-yellow-500/20 rounded-lg">
        <h3 className="text-sm font-medium text-yellow-400 mb-2">Warning</h3>
        <p className="text-sm text-slate-400">
          These settings can affect cluster stability. Modify with caution.
        </p>
      </div>

      <div className="space-y-4">
        <div>
          <label className="label">Raft Election Timeout (ms)</label>
          <input type="number" defaultValue={150} className="input w-32" />
        </div>

        <div>
          <label className="label">Heartbeat Interval (ms)</label>
          <input type="number" defaultValue={50} className="input w-32" />
        </div>

        <div>
          <label className="label">Max Concurrent Uploads</label>
          <input type="number" defaultValue={100} className="input w-32" />
        </div>

        <div>
          <label className="label">Chunk Size (MB)</label>
          <input type="number" defaultValue={64} className="input w-32" />
        </div>

        <div>
          <label className="label">Garbage Collection Interval (hours)</label>
          <input type="number" defaultValue={24} className="input w-32" />
        </div>
      </div>

      <div className="pt-4 flex space-x-4">
        <button className="btn-primary">Save Advanced Settings</button>
        <button className="btn-secondary">Reset to Defaults</button>
      </div>
    </div>
  );
}
