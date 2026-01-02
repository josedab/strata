/**
 * Global state store using Zustand
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { User, Session, Alert, ClusterHealth, ClusterStats } from '@/types';

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  login: (session: Session) => void;
  logout: () => void;
}

interface ClusterState {
  health: ClusterHealth | null;
  stats: ClusterStats | null;
  alerts: Alert[];
  setHealth: (health: ClusterHealth) => void;
  setStats: (stats: ClusterStats) => void;
  setAlerts: (alerts: Alert[]) => void;
  acknowledgeAlert: (id: string) => void;
}

interface UIState {
  sidebarOpen: boolean;
  theme: 'dark' | 'light';
  toggleSidebar: () => void;
  setTheme: (theme: 'dark' | 'light') => void;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      user: null,
      token: null,
      isAuthenticated: false,
      login: (session: Session) =>
        set({
          user: session.user,
          token: session.token,
          isAuthenticated: true,
        }),
      logout: () =>
        set({
          user: null,
          token: null,
          isAuthenticated: false,
        }),
    }),
    {
      name: 'strata-auth',
      partialize: (state) => ({ token: state.token }),
    }
  )
);

export const useClusterStore = create<ClusterState>((set) => ({
  health: null,
  stats: null,
  alerts: [],
  setHealth: (health) => set({ health }),
  setStats: (stats) => set({ stats }),
  setAlerts: (alerts) => set({ alerts }),
  acknowledgeAlert: (id) =>
    set((state) => ({
      alerts: state.alerts.map((alert) =>
        alert.id === id
          ? { ...alert, status: 'acknowledged' as const, acknowledgedAt: new Date() }
          : alert
      ),
    })),
}));

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      sidebarOpen: true,
      theme: 'dark',
      toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setTheme: (theme) => set({ theme }),
    }),
    {
      name: 'strata-ui',
    }
  )
);
