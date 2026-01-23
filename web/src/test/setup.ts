/**
 * Vitest setup file - runs before each test file.
 */

import { afterEach, vi } from 'vitest';

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: (key: string) => store[key] || null,
    setItem: (key: string, value: string) => {
      store[key] = value;
    },
    removeItem: (key: string) => {
      delete store[key];
    },
    clear: () => {
      store = {};
    },
  };
})();

Object.defineProperty(global, 'localStorage', {
  value: localStorageMock,
});

// Mock window.location
const locationMock = {
  href: 'http://localhost:3000',
  origin: 'http://localhost:3000',
  pathname: '/',
  search: '',
  hash: '',
  replace: vi.fn(),
  assign: vi.fn(),
  reload: vi.fn(),
};

Object.defineProperty(global, 'location', {
  value: locationMock,
  writable: true,
});

// Mock fetch
global.fetch = vi.fn();

// Reset mocks after each test
afterEach(() => {
  vi.clearAllMocks();
  localStorageMock.clear();
  locationMock.replace.mockClear();
  locationMock.assign.mockClear();
});

// Mock import.meta.env
vi.stubGlobal('import.meta', {
  env: {
    PUBLIC_API_URL: 'http://localhost:8000/api/v1',
    PUBLIC_WEBAUTHN_RP_ID: 'localhost',
  },
});
