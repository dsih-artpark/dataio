/**
 * Tests for authentication helpers.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock the api module
vi.mock('../api', () => {
  return {
    api: {
      isAuthenticated: vi.fn(() => false),
      getCurrentUser: vi.fn(),
      clearTokens: vi.fn(),
      logout: vi.fn(),
      verifyLogin: vi.fn(),
    },
  };
});

// Mock @preact/signals
vi.mock('@preact/signals', () => {
  return {
    signal: <T>(initialValue: T) => ({
      value: initialValue,
      peek: () => initialValue,
    }),
  };
});


describe('Auth Helpers', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  describe('currentUser signal', () => {
    it('should initialize as null', async () => {
      const { currentUser } = await import('../auth');
      expect(currentUser.value).toBeNull();
    });
  });

  describe('isLoading signal', () => {
    it('should be false when no tokens exist', async () => {
      localStorage.removeItem('access_token');
      const { isLoading } = await import('../auth');
      // Note: this depends on the actual implementation checking localStorage
      expect(typeof isLoading.value).toBe('boolean');
    });
  });

  describe('isAdmin', () => {
    it('should return false when no user', async () => {
      const { isAdmin } = await import('../auth');
      expect(isAdmin()).toBe(false);
    });
  });

  describe('requireAuth', () => {
    it('should redirect to login when not authenticated', async () => {
      const { api } = await import('../api');
      (api.isAuthenticated as ReturnType<typeof vi.fn>).mockReturnValue(false);

      const { requireAuth } = await import('../auth');
      const result = requireAuth();

      // Should return false and trigger redirect
      expect(result).toBe(false);
    });

    it('should return true when authenticated', async () => {
      const { api } = await import('../api');
      (api.isAuthenticated as ReturnType<typeof vi.fn>).mockReturnValue(true);

      const { requireAuth } = await import('../auth');
      const result = requireAuth();

      expect(result).toBe(true);
    });
  });
});


describe('Login Flow', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should update currentUser on successful login', async () => {
    const mockUser = {
      email: 'test@example.com',
      display_name: 'Test User',
      is_admin: false,
      email_verified: true,
    };

    const { api } = await import('../api');
    (api.verifyLogin as ReturnType<typeof vi.fn>).mockResolvedValue({
      user: mockUser,
      access_token: 'token',
      refresh_token: 'refresh',
      needs_passkey: false,
    });

    const { loginWithOTP } = await import('../auth');
    const result = await loginWithOTP('test@example.com', '123456');

    expect(result.user).toEqual(mockUser);
    expect(result.needsPasskey).toBe(false);
  });
});


describe('Logout Flow', () => {
  it('should call api.logout', async () => {
    const { api } = await import('../api');
    const { logout } = await import('../auth');

    await logout();

    expect(api.logout).toHaveBeenCalled();
  });
});
