/**
 * Authentication helpers and state management.
 */

import { signal } from '@preact/signals';
import { api } from './api';

interface User {
  email: string;
  display_name: string | null;
  is_admin: boolean;
  email_verified: boolean;
}

// Reactive state for auth
export const currentUser = signal<User | null>(null);
export const isLoading = signal(true);

/**
 * Initialize auth state from stored tokens.
 */
export async function initAuth(): Promise<void> {
  isLoading.value = true;

  if (api.isAuthenticated()) {
    try {
      const user = await api.getCurrentUser();
      currentUser.value = user;
    } catch {
      // Token invalid, clear it
      api.clearTokens();
      currentUser.value = null;
    }
  }

  isLoading.value = false;
}

/**
 * Login with OTP.
 */
export async function loginWithOTP(email: string, code: string): Promise<{
  user: User;
  needsPasskey: boolean;
}> {
  const response = await api.verifyLogin(email, code);
  currentUser.value = response.user;
  return {
    user: response.user,
    needsPasskey: response.needs_passkey,
  };
}

/**
 * Logout and clear state.
 */
export async function logout(): Promise<void> {
  await api.logout();
  currentUser.value = null;
}

/**
 * Check if current user is admin.
 */
export function isAdmin(): boolean {
  return currentUser.value?.is_admin ?? false;
}

/**
 * Redirect to login if not authenticated.
 * Returns true if authenticated, false if redirecting.
 */
export function requireAuth(): boolean {
  if (typeof window === 'undefined') return true;

  if (!api.isAuthenticated()) {
    // Use replace to avoid back-button loops
    window.location.replace('/login');
    return false;
  }
  return true;
}

/**
 * Redirect to dashboard if already authenticated.
 * Returns true if not authenticated, false if redirecting.
 */
export function redirectIfAuthenticated(): boolean {
  if (typeof window === 'undefined') return true;

  if (api.isAuthenticated()) {
    window.location.replace('/dashboard');
    return false;
  }
  return true;
}
