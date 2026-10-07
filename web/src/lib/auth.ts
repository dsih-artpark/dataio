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
// Start as true because session restoration may happen from the refresh cookie.
export const isLoading = signal(typeof window !== 'undefined');

// Promise to track in-flight auth initialization (prevents race conditions)
let initAuthPromise: Promise<void> | null = null;
let initAuthCompleted = false;

const DEFAULT_POST_LOGIN_PATH = '/datasets';

function normalizeRedirectPath(target: string | null | undefined): string {
  if (!target) return DEFAULT_POST_LOGIN_PATH;
  if (!target.startsWith('/')) return DEFAULT_POST_LOGIN_PATH;
  if (target.startsWith('//')) return DEFAULT_POST_LOGIN_PATH;
  return target;
}

export function getCurrentPathWithSearch(): string {
  if (typeof window === 'undefined') return DEFAULT_POST_LOGIN_PATH;
  return `${window.location.pathname}${window.location.search}${window.location.hash}`;
}

export function setPostLoginRedirect(target: string): void {
  if (typeof window === 'undefined') return;
  sessionStorage.setItem('post_login_redirect', normalizeRedirectPath(target));
}

export function getPostLoginRedirect(): string {
  if (typeof window === 'undefined') return DEFAULT_POST_LOGIN_PATH;
  const fromQuery = new URLSearchParams(window.location.search).get('next');
  const fromStorage = sessionStorage.getItem('post_login_redirect');
  return normalizeRedirectPath(fromQuery || fromStorage);
}

export function consumePostLoginRedirect(): string {
  const target = getPostLoginRedirect();
  if (typeof window !== 'undefined') {
    sessionStorage.removeItem('post_login_redirect');
  }
  return target;
}

export function redirectToPath(target: string, replace = true): void {
  if (typeof window === 'undefined') return;
  const normalized = normalizeRedirectPath(target);
  if (replace) {
    window.location.replace(normalized);
  } else {
    window.location.href = normalized;
  }
}

export function redirectToLogin(target?: string): void {
  if (typeof window === 'undefined') return;
  const destination = normalizeRedirectPath(target || getCurrentPathWithSearch());
  setPostLoginRedirect(destination);
  const params = new URLSearchParams({ next: destination });
  window.location.replace(`/login?${params.toString()}`);
}

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

  if (!api.isAuthenticated()) {
    await api.restoreSession();
  }

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
  verificationStatus?: string;
  verificationMessage?: string | null;
}> {
  const response = await api.verifyLogin(email, code);
  currentUser.value = response.access_token ? response.user : null;
  return {
    user: response.user,
    needsPasskey: response.needs_passkey,
    verificationStatus: response.verification_status,
    verificationMessage: response.verification_message,
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
    return false;
  }
  return true;
}

/**
 * Redirect to datasets if already authenticated.
 * Returns true if not authenticated, false if redirecting.
 * Waits for auth initialization to complete before checking.
 */
export async function redirectIfAuthenticated(): Promise<boolean> {
  if (typeof window === 'undefined') return true;

  const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ''));
  if (
    hashParams.get('oauth') === 'success' ||
    hashParams.get('needs_passkey') === 'true' ||
    hashParams.get('verification_status') === 'pending'
  ) {
    return true;
  }

  try {
    await initAuth();
    if (currentUser.value) {
      redirectToPath(consumePostLoginRedirect());
      return false;
    }
  } catch {
    return true;
  }
  return true;
}

export async function requireAuthenticatedPage(options?: {
  adminOnly?: boolean;
  fallbackPath?: string;
}): Promise<boolean> {
  if (typeof window === 'undefined') return true;

  try {
    await initAuth();
  } catch {
    redirectToLogin();
    return false;
  }

  if (!currentUser.value) {
    redirectToLogin();
    return false;
  }

  if (options?.adminOnly && !isAdmin()) {
    redirectToPath(options.fallbackPath || DEFAULT_POST_LOGIN_PATH);
    return false;
  }

  return true;
}
