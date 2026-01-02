/**
 * Main Application Component
 */

import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuthStore } from '@/store';
import Layout from '@/components/Layout';
import Dashboard from '@/pages/Dashboard';
import Nodes from '@/pages/Nodes';
import Buckets from '@/pages/Buckets';
import BucketDetail from '@/pages/BucketDetail';
import Alerts from '@/pages/Alerts';
import Settings from '@/pages/Settings';
import Login from '@/pages/Login';

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated);

  // For development, allow access without auth
  const isDev = import.meta.env.DEV;

  if (!isAuthenticated && !isDev) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route
        path="/*"
        element={
          <ProtectedRoute>
            <Layout>
              <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/nodes" element={<Nodes />} />
                <Route path="/buckets" element={<Buckets />} />
                <Route path="/buckets/:name" element={<BucketDetail />} />
                <Route path="/alerts" element={<Alerts />} />
                <Route path="/settings" element={<Settings />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </Layout>
          </ProtectedRoute>
        }
      />
    </Routes>
  );
}
