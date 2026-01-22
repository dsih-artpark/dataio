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

// Promise to track in-flight auth initialization (prevents race conditions)
let initAuthPromise: Promise<void> | null = null;
let initAuthCompleted = false;

/**
 * Initialize auth state from stored tokens.
 * This function is idempotent - multiple calls will share the same promise.
 */
export async function initAuth(): Promise<void> {
  // If already completed and we have a user, return immediately
  if (initAuthCompleted && currentUser.value) {
    return;
  }

  // If already in progress, return the existing promise
  if (initAuthPromise) {
    return initAuthPromise;
  }

  initAuthPromise = _doInitAuth();
  try {
    await initAuthPromise;
  } finally {
    initAuthPromise = null;
  }
}

async function _doInitAuth(): Promise<void> {
  isLoading.value = true;

  if (api.isAuthenticated()) {
    try {
      const user = await api.getCurrentUser();
      currentUser.value = user;
      initAuthCompleted = true;
    } catch {
      // Token invalid, clear it
      api.clearTokens();
      currentUser.value = null;
      initAuthCompleted = false;
    }
  } else {
    currentUser.value = null;
    initAuthCompleted = false;
  }

  isLoading.value = false;
}

/**
 * Reset auth state (used after logout or token clear).
 */
export function resetAuthState(): void {
  initAuthCompleted = false;
  initAuthPromise = null;
  currentUser.value = null;
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
  resetAuthState();
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
 * Redirect to datasets if already authenticated.
 * Returns true if not authenticated, false if redirecting.
 */
export function redirectIfAuthenticated(): boolean {
  if (typeof window === 'undefined') return true;

  if (api.isAuthenticated()) {
    window.location.replace('/datasets');
    return false;
  }
  return true;
}
