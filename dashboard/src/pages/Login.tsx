/**
 * Login Page
 */

import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useForm } from 'react-hook-form';
import { useAuthStore } from '@/store';
import toast from 'react-hot-toast';

interface LoginForm {
  username: string;
  password: string;
}

export default function Login() {
  const navigate = useNavigate();
  const login = useAuthStore((state) => state.login);
  const [isLoading, setIsLoading] = useState(false);

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<LoginForm>();

  const onSubmit = async (data: LoginForm) => {
    setIsLoading(true);

    try {
      // Simulate API call
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Mock successful login
      if (data.username === 'admin' && data.password === 'admin') {
        login({
          user: {
            id: '1',
            username: 'admin',
            email: 'admin@strata.io',
            role: 'admin',
            createdAt: new Date(),
          },
          token: 'mock-jwt-token',
          expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
        });
        toast.success('Welcome back!');
        navigate('/');
      } else {
        toast.error('Invalid credentials');
      }
    } catch {
      toast.error('Login failed');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-slate-900 px-4">
      <div className="w-full max-w-md">
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center h-16 w-16 bg-gradient-to-br from-primary-500 to-strata-accent rounded-2xl mb-4">
            <span className="text-white font-bold text-3xl">S</span>
          </div>
          <h1 className="text-2xl font-bold text-white">Strata Dashboard</h1>
          <p className="text-slate-400 mt-2">Sign in to manage your cluster</p>
        </div>

        {/* Login Form */}
        <div className="card p-6">
          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            <div>
              <label className="label">Username</label>
              <input
                {...register('username', { required: 'Username is required' })}
                className="input"
                placeholder="Enter your username"
                autoComplete="username"
              />
              {errors.username && (
                <p className="mt-1 text-sm text-red-400">{errors.username.message}</p>
              )}
            </div>

            <div>
              <label className="label">Password</label>
              <input
                {...register('password', { required: 'Password is required' })}
                type="password"
                className="input"
                placeholder="Enter your password"
                autoComplete="current-password"
              />
              {errors.password && (
                <p className="mt-1 text-sm text-red-400">{errors.password.message}</p>
              )}
            </div>

            <div className="flex items-center justify-between">
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  className="h-4 w-4 rounded border-slate-600 bg-slate-700 text-primary-500 focus:ring-primary-500"
                />
                <span className="text-sm text-slate-400">Remember me</span>
              </label>
              <a href="#" className="text-sm text-primary-400 hover:text-primary-300">
                Forgot password?
              </a>
            </div>

            <button
              type="submit"
              disabled={isLoading}
              className="w-full btn-primary flex items-center justify-center"
            >
              {isLoading ? (
                <div className="h-5 w-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              ) : (
                'Sign In'
              )}
            </button>
          </form>

          {/* Demo credentials hint */}
          <div className="mt-6 p-3 bg-slate-700/50 rounded-lg">
            <p className="text-xs text-slate-400 text-center">
              Demo credentials: <code className="text-primary-400">admin</code> /{' '}
              <code className="text-primary-400">admin</code>
            </p>
          </div>
        </div>

        {/* Footer */}
        <p className="text-center text-sm text-slate-500 mt-8">
          Strata Distributed Filesystem &copy; 2024
        </p>
      </div>
    </div>
  );
}
