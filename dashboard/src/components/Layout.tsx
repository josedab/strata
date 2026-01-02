/**
 * Main Layout Component with Sidebar Navigation
 */

import { Link, useLocation } from 'react-router-dom';
import { clsx } from 'clsx';
import {
  HomeIcon,
  ServerStackIcon,
  ArchiveBoxIcon,
  BellAlertIcon,
  Cog6ToothIcon,
  Bars3Icon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { useUIStore, useClusterStore } from '@/store';
import type { NavItem } from '@/types';

const navigation: NavItem[] = [
  { name: 'Dashboard', path: '/', icon: HomeIcon },
  { name: 'Nodes', path: '/nodes', icon: ServerStackIcon },
  { name: 'Buckets', path: '/buckets', icon: ArchiveBoxIcon },
  { name: 'Alerts', path: '/alerts', icon: BellAlertIcon },
  { name: 'Settings', path: '/settings', icon: Cog6ToothIcon },
];

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const location = useLocation();
  const { sidebarOpen, toggleSidebar } = useUIStore();
  const alerts = useClusterStore((state) => state.alerts);
  const activeAlerts = alerts.filter((a) => a.status === 'active').length;

  return (
    <div className="min-h-screen bg-slate-900">
      {/* Mobile sidebar toggle */}
      <div className="lg:hidden fixed top-0 left-0 right-0 z-40 bg-slate-800 border-b border-slate-700 px-4 py-3">
        <button
          onClick={toggleSidebar}
          className="text-slate-400 hover:text-white"
        >
          {sidebarOpen ? (
            <XMarkIcon className="h-6 w-6" />
          ) : (
            <Bars3Icon className="h-6 w-6" />
          )}
        </button>
      </div>

      {/* Sidebar */}
      <aside
        className={clsx(
          'fixed inset-y-0 left-0 z-30 w-64 bg-slate-800 border-r border-slate-700',
          'transform transition-transform duration-200 ease-in-out',
          'lg:translate-x-0',
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        )}
      >
        {/* Logo */}
        <div className="h-16 flex items-center px-6 border-b border-slate-700">
          <Link to="/" className="flex items-center space-x-3">
            <div className="h-8 w-8 bg-gradient-to-br from-primary-500 to-strata-accent rounded-lg flex items-center justify-center">
              <span className="text-white font-bold text-lg">S</span>
            </div>
            <span className="text-xl font-semibold text-white">Strata</span>
          </Link>
        </div>

        {/* Navigation */}
        <nav className="mt-6 px-3">
          <ul className="space-y-1">
            {navigation.map((item) => {
              const isActive = location.pathname === item.path;
              const Icon = item.icon;
              const badge = item.name === 'Alerts' ? activeAlerts : undefined;

              return (
                <li key={item.name}>
                  <Link
                    to={item.path}
                    className={clsx(
                      'flex items-center px-3 py-2.5 rounded-lg text-sm font-medium',
                      'transition-colors duration-150',
                      isActive
                        ? 'bg-primary-600/20 text-primary-400'
                        : 'text-slate-400 hover:bg-slate-700/50 hover:text-white'
                    )}
                  >
                    <Icon className="h-5 w-5 mr-3 flex-shrink-0" />
                    <span className="flex-1">{item.name}</span>
                    {badge !== undefined && badge > 0 && (
                      <span className="ml-2 px-2 py-0.5 text-xs rounded-full bg-red-500 text-white">
                        {badge}
                      </span>
                    )}
                  </Link>
                </li>
              );
            })}
          </ul>
        </nav>

        {/* Cluster Status */}
        <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-slate-700">
          <div className="flex items-center space-x-3">
            <div className="h-2.5 w-2.5 rounded-full bg-green-500 animate-pulse" />
            <div>
              <p className="text-sm font-medium text-white">Cluster Online</p>
              <p className="text-xs text-slate-400">5 nodes healthy</p>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main
        className={clsx(
          'transition-all duration-200 ease-in-out',
          'lg:ml-64',
          'pt-16 lg:pt-0'
        )}
      >
        <div className="p-6">{children}</div>
      </main>

      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-20 lg:hidden"
          onClick={toggleSidebar}
        />
      )}
    </div>
  );
}
