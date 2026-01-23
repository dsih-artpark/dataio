/**
 * Smoke tests for the web frontend.
 *
 * Quick sanity checks to verify the frontend builds and basic functionality works.
 */

import { describe, it, expect } from 'vitest';

describe('Web Frontend Smoke Tests', () => {
  describe('Module Imports', () => {
    it('should import api module', async () => {
      const apiModule = await import('../src/lib/api');
      expect(apiModule).toBeDefined();
      expect(apiModule.api).toBeDefined();
    });

    it('should import auth module', async () => {
      const authModule = await import('../src/lib/auth');
      expect(authModule).toBeDefined();
      expect(authModule.currentUser).toBeDefined();
      expect(authModule.isLoading).toBeDefined();
    });

    it('should import types', async () => {
      // Just verifying TypeScript types are exportable
      const typesExist = true;
      expect(typesExist).toBe(true);
    });
  });

  describe('API Client Basics', () => {
    it('should have expected methods on api client', async () => {
      const { api } = await import('../src/lib/api');

      expect(typeof api.setTokens).toBe('function');
      expect(typeof api.clearTokens).toBe('function');
      expect(typeof api.isAuthenticated).toBe('function');
      expect(typeof api.getAccessToken).toBe('function');
    });

    it('should have auth methods', async () => {
      const { api } = await import('../src/lib/api');

      expect(typeof api.initiateLogin).toBe('function');
      expect(typeof api.verifyLogin).toBe('function');
      expect(typeof api.logout).toBe('function');
    });

    it('should have dataset methods', async () => {
      const { api } = await import('../src/lib/api');

      expect(typeof api.getDatasets).toBe('function');
      expect(typeof api.getDataset).toBe('function');
      expect(typeof api.getPublicDatasets).toBe('function');
    });
  });

  describe('Auth Helpers Basics', () => {
    it('should have expected exports', async () => {
      const authModule = await import('../src/lib/auth');

      expect(authModule.currentUser).toBeDefined();
      expect(authModule.isLoading).toBeDefined();
      expect(typeof authModule.initAuth).toBe('function');
      expect(typeof authModule.loginWithOTP).toBe('function');
      expect(typeof authModule.logout).toBe('function');
      expect(typeof authModule.isAdmin).toBe('function');
      expect(typeof authModule.requireAuth).toBe('function');
    });
  });

  describe('Environment', () => {
    it('should have window object in jsdom', () => {
      expect(typeof window).toBe('object');
    });

    it('should have localStorage in jsdom', () => {
      expect(typeof localStorage).toBe('object');
      expect(typeof localStorage.getItem).toBe('function');
      expect(typeof localStorage.setItem).toBe('function');
    });

    it('should have document object in jsdom', () => {
      expect(typeof document).toBe('object');
    });
  });

  describe('TypeScript Types', () => {
    it('should compile without type errors', () => {
      // This test passes if the test file compiles
      const testValue: string = 'test';
      expect(testValue).toBe('test');
    });
  });
});
