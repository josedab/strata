/**
 * Bucket Detail Page - View bucket contents and settings
 */

import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { clsx } from 'clsx';
import {
  ArrowLeftIcon,
  DocumentIcon,
  FolderIcon,
  TrashIcon,
  ArrowDownTrayIcon,
  ArrowUpTrayIcon,
  MagnifyingGlassIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';
import type { StorageObject } from '@/types';

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

const mockObjects: StorageObject[] = [
  {
    key: 'images/',
    bucket: 'production-assets',
    size: 0,
    contentType: 'application/x-directory',
    etag: '',
    lastModified: new Date('2024-06-01'),
    storageClass: 'STANDARD',
  },
  {
    key: 'documents/',
    bucket: 'production-assets',
    size: 0,
    contentType: 'application/x-directory',
    etag: '',
    lastModified: new Date('2024-05-15'),
    storageClass: 'STANDARD',
  },
  {
    key: 'logo.png',
    bucket: 'production-assets',
    size: 45892,
    contentType: 'image/png',
    etag: '"abc123"',
    lastModified: new Date('2024-06-10'),
    storageClass: 'STANDARD',
  },
  {
    key: 'config.json',
    bucket: 'production-assets',
    size: 1256,
    contentType: 'application/json',
    etag: '"def456"',
    lastModified: new Date('2024-06-12'),
    storageClass: 'STANDARD',
  },
  {
    key: 'backup-2024-06.tar.gz',
    bucket: 'production-assets',
    size: 892456123,
    contentType: 'application/gzip',
    etag: '"ghi789"',
    lastModified: new Date('2024-06-15'),
    storageClass: 'GLACIER',
  },
  {
    key: 'README.md',
    bucket: 'production-assets',
    size: 4521,
    contentType: 'text/markdown',
    etag: '"jkl012"',
    lastModified: new Date('2024-06-08'),
    storageClass: 'STANDARD',
  },
];

function isDirectory(obj: StorageObject): boolean {
  return obj.contentType === 'application/x-directory' || obj.key.endsWith('/');
}

function getFileName(key: string): string {
  const parts = key.split('/').filter(Boolean);
  return parts[parts.length - 1] ?? key;
}

export default function BucketDetail() {
  const { name } = useParams<{ name: string }>();
  const [objects, setObjects] = useState<StorageObject[]>(mockObjects);
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [currentPath, setCurrentPath] = useState('');

  const filteredObjects = objects
    .filter((obj) => {
      if (currentPath) {
        return obj.key.startsWith(currentPath) && obj.key !== currentPath;
      }
      return !obj.key.includes('/') || obj.key.endsWith('/');
    })
    .filter((obj) => obj.key.toLowerCase().includes(search.toLowerCase()));

  const handleDelete = (keys: string[]) => {
    if (confirm(`Delete ${keys.length} object(s)?`)) {
      setObjects((prev) => prev.filter((o) => !keys.includes(o.key)));
      setSelected(new Set());
      toast.success(`Deleted ${keys.length} object(s)`);
    }
  };

  const handleNavigate = (key: string) => {
    setCurrentPath(key);
    setSearch('');
  };

  const toggleSelect = (key: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const selectAll = () => {
    if (selected.size === filteredObjects.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filteredObjects.map((o) => o.key)));
    }
  };

  const pathParts = currentPath.split('/').filter(Boolean);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center space-x-4">
        <Link
          to="/buckets"
          className="p-2 text-slate-400 hover:text-white hover:bg-slate-700 rounded-lg"
        >
          <ArrowLeftIcon className="h-5 w-5" />
        </Link>
        <div>
          <h1 className="text-2xl font-bold text-white">{name}</h1>
          <nav className="flex items-center space-x-1 text-sm text-slate-400">
            <button
              onClick={() => setCurrentPath('')}
              className="hover:text-white"
            >
              {name}
            </button>
            {pathParts.map((part, index) => (
              <span key={index} className="flex items-center">
                <span className="mx-1">/</span>
                <button
                  onClick={() =>
                    setCurrentPath(pathParts.slice(0, index + 1).join('/') + '/')
                  }
                  className="hover:text-white"
                >
                  {part}
                </button>
              </span>
            ))}
          </nav>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center justify-between">
        <div className="relative flex-1 max-w-md">
          <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
          <input
            type="text"
            placeholder="Search objects..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="input pl-10"
          />
        </div>
        <div className="flex items-center space-x-2">
          {selected.size > 0 && (
            <button
              onClick={() => handleDelete(Array.from(selected))}
              className="btn-danger flex items-center"
            >
              <TrashIcon className="h-4 w-4 mr-2" />
              Delete ({selected.size})
            </button>
          )}
          <button className="btn-secondary flex items-center">
            <ArrowUpTrayIcon className="h-4 w-4 mr-2" />
            Upload
          </button>
        </div>
      </div>

      {/* Objects Table */}
      <div className="card overflow-hidden">
        <table className="min-w-full divide-y divide-slate-700">
          <thead className="bg-slate-800">
            <tr>
              <th className="table-header w-12">
                <input
                  type="checkbox"
                  checked={selected.size === filteredObjects.length && filteredObjects.length > 0}
                  onChange={selectAll}
                  className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
                />
              </th>
              <th className="table-header">Name</th>
              <th className="table-header">Size</th>
              <th className="table-header">Type</th>
              <th className="table-header">Storage Class</th>
              <th className="table-header">Last Modified</th>
              <th className="table-header text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700">
            {filteredObjects.map((obj) => {
              const isDir = isDirectory(obj);
              const fileName = getFileName(obj.key);

              return (
                <tr key={obj.key} className="hover:bg-slate-800/50">
                  <td className="table-cell">
                    <input
                      type="checkbox"
                      checked={selected.has(obj.key)}
                      onChange={() => toggleSelect(obj.key)}
                      className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
                    />
                  </td>
                  <td className="table-cell">
                    {isDir ? (
                      <button
                        onClick={() => handleNavigate(obj.key)}
                        className="flex items-center space-x-3 text-white hover:text-primary-400"
                      >
                        <FolderIcon className="h-5 w-5 text-yellow-400" />
                        <span className="font-medium">{fileName}</span>
                      </button>
                    ) : (
                      <div className="flex items-center space-x-3">
                        <DocumentIcon className="h-5 w-5 text-slate-400" />
                        <span className="text-white">{fileName}</span>
                      </div>
                    )}
                  </td>
                  <td className="table-cell">
                    {isDir ? '-' : formatBytes(obj.size)}
                  </td>
                  <td className="table-cell text-slate-400">
                    {isDir ? 'Folder' : obj.contentType}
                  </td>
                  <td className="table-cell">
                    <span
                      className={clsx(
                        'px-2 py-0.5 text-xs rounded',
                        obj.storageClass === 'GLACIER'
                          ? 'bg-blue-500/20 text-blue-400'
                          : obj.storageClass === 'STANDARD_IA'
                          ? 'bg-yellow-500/20 text-yellow-400'
                          : 'bg-green-500/20 text-green-400'
                      )}
                    >
                      {obj.storageClass}
                    </span>
                  </td>
                  <td className="table-cell text-slate-400">
                    {obj.lastModified.toLocaleDateString()}
                  </td>
                  <td className="table-cell text-right">
                    {!isDir && (
                      <div className="flex items-center justify-end space-x-1">
                        <button
                          className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-700 rounded"
                          title="Download"
                        >
                          <ArrowDownTrayIcon className="h-4 w-4" />
                        </button>
                        <button
                          onClick={() => handleDelete([obj.key])}
                          className="p-1.5 text-slate-400 hover:text-red-400 hover:bg-red-500/10 rounded"
                          title="Delete"
                        >
                          <TrashIcon className="h-4 w-4" />
                        </button>
                      </div>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {filteredObjects.length === 0 && (
          <div className="text-center py-12">
            <FolderIcon className="h-12 w-12 text-slate-600 mx-auto mb-4" />
            <p className="text-slate-400">No objects found</p>
          </div>
        )}
      </div>
    </div>
  );
}
