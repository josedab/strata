/**
 * Buckets Page - Bucket Management
 */

import { useState, Fragment } from 'react';
import { Link } from 'react-router-dom';
import { clsx } from 'clsx';
import {
  ArchiveBoxIcon,
  PlusIcon,
  MagnifyingGlassIcon,
  TrashIcon,
  LockClosedIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline';
import { Dialog, Transition } from '@headlessui/react';
import { useForm } from 'react-hook-form';
import toast from 'react-hot-toast';
import type { Bucket, CreateBucketForm } from '@/types';

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
  return num.toString();
}

const mockBuckets: Bucket[] = [
  {
    name: 'production-assets',
    createdAt: new Date('2024-01-15'),
    versioning: true,
    encryption: true,
    objectCount: 45230,
    totalSize: 125 * 1024 * 1024 * 1024,
    region: 'us-east-1',
  },
  {
    name: 'user-uploads',
    createdAt: new Date('2024-02-20'),
    versioning: true,
    encryption: true,
    objectCount: 892456,
    totalSize: 2.4 * 1024 * 1024 * 1024 * 1024,
    region: 'us-east-1',
  },
  {
    name: 'backups',
    createdAt: new Date('2024-01-01'),
    versioning: false,
    encryption: true,
    objectCount: 1240,
    totalSize: 890 * 1024 * 1024 * 1024,
    region: 'us-west-2',
  },
  {
    name: 'logs',
    createdAt: new Date('2024-03-10'),
    versioning: false,
    encryption: false,
    objectCount: 15789234,
    totalSize: 450 * 1024 * 1024 * 1024,
    region: 'us-east-1',
  },
  {
    name: 'ml-datasets',
    createdAt: new Date('2024-04-05'),
    versioning: true,
    encryption: true,
    objectCount: 5621,
    totalSize: 1.8 * 1024 * 1024 * 1024 * 1024,
    region: 'us-west-2',
  },
];

interface CreateBucketModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCreate: (data: CreateBucketForm) => void;
}

function CreateBucketModal({ isOpen, onClose, onCreate }: CreateBucketModalProps) {
  const { register, handleSubmit, reset, formState: { errors } } = useForm<CreateBucketForm>({
    defaultValues: {
      versioning: false,
      encryption: true,
    },
  });

  const onSubmit = (data: CreateBucketForm) => {
    onCreate(data);
    reset();
    onClose();
  };

  return (
    <Transition appear show={isOpen} as={Fragment}>
      <Dialog as="div" className="relative z-50" onClose={onClose}>
        <Transition.Child
          as={Fragment}
          enter="ease-out duration-300"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-200"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-black/50" />
        </Transition.Child>

        <div className="fixed inset-0 overflow-y-auto">
          <div className="flex min-h-full items-center justify-center p-4">
            <Transition.Child
              as={Fragment}
              enter="ease-out duration-300"
              enterFrom="opacity-0 scale-95"
              enterTo="opacity-100 scale-100"
              leave="ease-in duration-200"
              leaveFrom="opacity-100 scale-100"
              leaveTo="opacity-0 scale-95"
            >
              <Dialog.Panel className="w-full max-w-md transform overflow-hidden rounded-xl bg-slate-800 border border-slate-700 p-6 shadow-xl transition-all">
                <Dialog.Title className="text-lg font-semibold text-white mb-4">
                  Create Bucket
                </Dialog.Title>

                <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
                  <div>
                    <label className="label">Bucket Name</label>
                    <input
                      {...register('name', {
                        required: 'Bucket name is required',
                        pattern: {
                          value: /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/,
                          message: 'Invalid bucket name format',
                        },
                        minLength: { value: 3, message: 'Minimum 3 characters' },
                        maxLength: { value: 63, message: 'Maximum 63 characters' },
                      })}
                      className="input"
                      placeholder="my-bucket"
                    />
                    {errors.name && (
                      <p className="mt-1 text-sm text-red-400">{errors.name.message}</p>
                    )}
                  </div>

                  <div>
                    <label className="label">Region</label>
                    <select {...register('region')} className="input">
                      <option value="us-east-1">US East (N. Virginia)</option>
                      <option value="us-west-2">US West (Oregon)</option>
                      <option value="eu-west-1">EU (Ireland)</option>
                      <option value="ap-southeast-1">Asia Pacific (Singapore)</option>
                    </select>
                  </div>

                  <div className="flex items-center space-x-4">
                    <label className="flex items-center space-x-2">
                      <input
                        type="checkbox"
                        {...register('versioning')}
                        className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
                      />
                      <span className="text-sm text-slate-300">Enable Versioning</span>
                    </label>

                    <label className="flex items-center space-x-2">
                      <input
                        type="checkbox"
                        {...register('encryption')}
                        className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
                      />
                      <span className="text-sm text-slate-300">Enable Encryption</span>
                    </label>
                  </div>

                  <div className="flex justify-end space-x-3 pt-4">
                    <button type="button" onClick={onClose} className="btn-secondary">
                      Cancel
                    </button>
                    <button type="submit" className="btn-primary">
                      Create Bucket
                    </button>
                  </div>
                </form>
              </Dialog.Panel>
            </Transition.Child>
          </div>
        </div>
      </Dialog>
    </Transition>
  );
}

export default function Buckets() {
  const [buckets, setBuckets] = useState<Bucket[]>(mockBuckets);
  const [search, setSearch] = useState('');
  const [createModalOpen, setCreateModalOpen] = useState(false);

  const filteredBuckets = buckets.filter((b) =>
    b.name.toLowerCase().includes(search.toLowerCase())
  );

  const handleCreate = (data: CreateBucketForm) => {
    const newBucket: Bucket = {
      name: data.name,
      createdAt: new Date(),
      versioning: data.versioning,
      encryption: data.encryption,
      objectCount: 0,
      totalSize: 0,
      region: data.region,
    };
    setBuckets((prev) => [newBucket, ...prev]);
    toast.success(`Bucket "${data.name}" created`);
  };

  const handleDelete = (name: string) => {
    if (confirm(`Delete bucket "${name}"? This action cannot be undone.`)) {
      setBuckets((prev) => prev.filter((b) => b.name !== name));
      toast.success(`Bucket "${name}" deleted`);
    }
  };

  const totalSize = buckets.reduce((sum, b) => sum + b.totalSize, 0);
  const totalObjects = buckets.reduce((sum, b) => sum + b.objectCount, 0);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Buckets</h1>
          <p className="text-slate-400">Manage storage buckets</p>
        </div>
        <button onClick={() => setCreateModalOpen(true)} className="btn-primary flex items-center">
          <PlusIcon className="h-4 w-4 mr-2" />
          Create Bucket
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card p-4">
          <p className="text-sm text-slate-400">Total Buckets</p>
          <p className="text-2xl font-bold text-white">{buckets.length}</p>
        </div>
        <div className="card p-4">
          <p className="text-sm text-slate-400">Total Objects</p>
          <p className="text-2xl font-bold text-white">{formatNumber(totalObjects)}</p>
        </div>
        <div className="card p-4">
          <p className="text-sm text-slate-400">Total Size</p>
          <p className="text-2xl font-bold text-white">{formatBytes(totalSize)}</p>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
        <input
          type="text"
          placeholder="Search buckets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="input pl-10"
        />
      </div>

      {/* Bucket Table */}
      <div className="card overflow-hidden">
        <table className="min-w-full divide-y divide-slate-700">
          <thead className="bg-slate-800">
            <tr>
              <th className="table-header">Name</th>
              <th className="table-header">Objects</th>
              <th className="table-header">Size</th>
              <th className="table-header">Region</th>
              <th className="table-header">Features</th>
              <th className="table-header">Created</th>
              <th className="table-header text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700">
            {filteredBuckets.map((bucket) => (
              <tr key={bucket.name} className="hover:bg-slate-800/50">
                <td className="table-cell">
                  <Link
                    to={`/buckets/${bucket.name}`}
                    className="flex items-center space-x-3 text-white hover:text-primary-400"
                  >
                    <ArchiveBoxIcon className="h-5 w-5 text-slate-400" />
                    <span className="font-medium">{bucket.name}</span>
                  </Link>
                </td>
                <td className="table-cell">{formatNumber(bucket.objectCount)}</td>
                <td className="table-cell">{formatBytes(bucket.totalSize)}</td>
                <td className="table-cell">
                  <span className="px-2 py-1 text-xs rounded bg-slate-700 text-slate-300">
                    {bucket.region}
                  </span>
                </td>
                <td className="table-cell">
                  <div className="flex space-x-2">
                    {bucket.versioning && (
                      <span className="px-2 py-0.5 text-xs rounded bg-blue-500/20 text-blue-400">
                        <ArrowPathIcon className="h-3 w-3 inline mr-1" />
                        Versioning
                      </span>
                    )}
                    {bucket.encryption && (
                      <span className="px-2 py-0.5 text-xs rounded bg-green-500/20 text-green-400">
                        <LockClosedIcon className="h-3 w-3 inline mr-1" />
                        Encrypted
                      </span>
                    )}
                  </div>
                </td>
                <td className="table-cell text-slate-400">
                  {bucket.createdAt.toLocaleDateString()}
                </td>
                <td className="table-cell text-right">
                  <button
                    onClick={() => handleDelete(bucket.name)}
                    className="p-1.5 text-slate-400 hover:text-red-400 hover:bg-red-500/10 rounded"
                    title="Delete bucket"
                  >
                    <TrashIcon className="h-4 w-4" />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {filteredBuckets.length === 0 && (
          <div className="text-center py-12">
            <ArchiveBoxIcon className="h-12 w-12 text-slate-600 mx-auto mb-4" />
            <p className="text-slate-400">No buckets found</p>
          </div>
        )}
      </div>

      <CreateBucketModal
        isOpen={createModalOpen}
        onClose={() => setCreateModalOpen(false)}
        onCreate={handleCreate}
      />
    </div>
  );
}
