/**
 * API client for communicating with the DataIO backend.
 */

import type {
  DatasetDetail,
  DatasetsResponse,
  CollectionsResponse,
  DataOwnersResponse,
  DatasetDownloadUrls,
} from './types';

const API_URL = import.meta.env.PUBLIC_API_URL || 'http://localhost:8000/api/v1';

interface ApiError {
  detail: string;
}

class ApiClient {
  private accessToken: string | null = null;
  private refreshToken: string | null = null;

  constructor() {
    // Load tokens from localStorage on init
    if (typeof window !== 'undefined') {
      this.accessToken = localStorage.getItem('access_token');
      this.refreshToken = localStorage.getItem('refresh_token');
    }
  }

  setTokens(accessToken: string, refreshToken: string) {
    this.accessToken = accessToken;
    this.refreshToken = refreshToken;
    if (typeof window !== 'undefined') {
      localStorage.setItem('access_token', accessToken);
      localStorage.setItem('refresh_token', refreshToken);
    }
  }

  clearTokens() {
    this.accessToken = null;
    this.refreshToken = null;
    if (typeof window !== 'undefined') {
      localStorage.removeItem('access_token');
      localStorage.removeItem('refresh_token');
    }
  }

  getAccessToken(): string | null {
    return this.accessToken;
  }

  getRefreshToken(): string | null {
    return this.refreshToken;
  }

  isAuthenticated(): boolean {
    return !!this.accessToken;
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {},
    requireAuth = true
  ): Promise<T> {
    const headers: HeadersInit = {
      'Content-Type': 'application/json',
      ...options.headers,
    };

    if (requireAuth && this.accessToken) {
      (headers as Record<string, string>)['Authorization'] = `Bearer ${this.accessToken}`;
    }

    const response = await fetch(`${API_URL}/web${endpoint}`, {
      ...options,
      headers,
    });

    // Handle 401 - try to refresh token
    if (response.status === 401 && requireAuth && this.refreshToken) {
      const refreshed = await this.refreshAccessToken();
      if (refreshed) {
        // Retry request with new token
        (headers as Record<string, string>)['Authorization'] = `Bearer ${this.accessToken}`;
        const retryResponse = await fetch(`${API_URL}/web${endpoint}`, {
          ...options,
          headers,
        });
        if (!retryResponse.ok) {
          const error = await retryResponse.json() as ApiError;
          throw new Error(error.detail || 'Request failed');
        }
        return retryResponse.json();
      } else {
        // Refresh failed, clear tokens and redirect to login
        this.clearTokens();
        if (typeof window !== 'undefined') {
          window.location.replace('/login');
        }
        throw new Error('Session expired');
      }
    }

    if (!response.ok) {
      const error = await response.json() as ApiError;
      throw new Error(error.detail || 'Request failed');
    }

    return response.json();
  }

  private async refreshAccessToken(): Promise<boolean> {
    if (!this.refreshToken) return false;

    try {
      const response = await fetch(`${API_URL}/web/auth/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: this.refreshToken }),
      });

      if (!response.ok) return false;

      const data = await response.json();
      this.setTokens(data.access_token, data.refresh_token);
      return true;
    } catch {
      return false;
    }
  }

  // Auth endpoints
  async initiateLogin(email: string) {
    return this.request<{ sent: boolean; message: string }>(
      '/auth/login/initiate',
      { method: 'POST', body: JSON.stringify({ email }) },
      false
    );
  }

  async verifyLogin(email: string, code: string) {
    const data = await this.request<{
      access_token: string;
      refresh_token: string;
      user: {
        email: string;
        display_name: string | null;
        is_admin: boolean;
        email_verified: boolean;
      };
      needs_passkey: boolean;
    }>(
      '/auth/login/verify',
      { method: 'POST', body: JSON.stringify({ email, code }) },
      false
    );
    this.setTokens(data.access_token, data.refresh_token);
    return data;
  }

  async logout() {
    if (this.refreshToken) {
      try {
        await this.request(
          '/auth/logout',
          { method: 'POST', body: JSON.stringify({ refresh_token: this.refreshToken }) },
          false
        );
      } catch {
        // Ignore logout errors
      }
    }
    this.clearTokens();
  }

  // Passkey endpoints
  async getPasskeyRegistrationOptions() {
    return this.request<{ options: string }>('/auth/passkey/register/options', {
      method: 'POST',
    });
  }

  async verifyPasskeyRegistration(credential: object, deviceName?: string) {
    return this.request<{ success: boolean; passkey: object }>(
      '/auth/passkey/register/verify',
      {
        method: 'POST',
        body: JSON.stringify({ credential, device_name: deviceName }),
      }
    );
  }

  async getPasskeyLoginOptions(email: string) {
    return this.request<{ options: string }>(
      '/auth/passkey/login/options',
      { method: 'POST', body: JSON.stringify({ email }) },
      false
    );
  }

  async verifyPasskeyLogin(email: string, credential: object) {
    const data = await this.request<{
      access_token: string;
      refresh_token: string;
      user: object;
    }>(
      '/auth/passkey/login/verify',
      { method: 'POST', body: JSON.stringify({ email, credential }) },
      false
    );
    this.setTokens(data.access_token, data.refresh_token);
    return data;
  }

  async listPasskeys() {
    return this.request<{ passkeys: object[] }>('/passkeys');
  }

  async deletePasskey(passkeyId: string) {
    return this.request<{ deleted: boolean }>(`/passkeys/${passkeyId}`, {
      method: 'DELETE',
    });
  }

  // Profile endpoints
  async getCurrentUser() {
    return this.request<{
      email: string;
      display_name: string | null;
      is_admin: boolean;
      email_verified: boolean;
      last_login: string | null;
      created_at: string | null;
    }>('/me');
  }

  async updateProfile(displayName?: string) {
    return this.request('/me', {
      method: 'PUT',
      body: JSON.stringify({ display_name: displayName }),
    });
  }

  // API Key endpoints
  async listApiKeys() {
    return this.request<{
      api_keys: {
        id: string;
        name: string;
        key_prefix: string;
        created_at: string;
        last_used_at: string | null;
        expires_at: string | null;
      }[];
    }>('/api-keys');
  }

  async createApiKey(name: string, expiresAt?: string) {
    return this.request<{
      id: string;
      name: string;
      key: string;
      key_prefix: string;
      created_at: string;
      expires_at: string | null;
      warning: string;
    }>('/api-keys', {
      method: 'POST',
      body: JSON.stringify({ name, expires_at: expiresAt }),
    });
  }

  async revokeApiKey(keyId: string) {
    return this.request<{ revoked: boolean }>(`/api-keys/${keyId}`, {
      method: 'DELETE',
    });
  }

  // Dataset endpoints (authenticated)
  async getDatasets(params?: {
    search?: string;
    collection_id?: number;
    data_owner_id?: number;
    limit?: number;
    offset?: number;
  }): Promise<DatasetsResponse> {
    const searchParams = new URLSearchParams();
    if (params?.search) searchParams.set('search', params.search);
    if (params?.collection_id) searchParams.set('collection_id', String(params.collection_id));
    if (params?.data_owner_id) searchParams.set('data_owner_id', String(params.data_owner_id));
    if (params?.limit) searchParams.set('limit', String(params.limit));
    if (params?.offset) searchParams.set('offset', String(params.offset));

    const query = searchParams.toString();
    return this.request<DatasetsResponse>(`/datasets${query ? `?${query}` : ''}`);
  }

  async getDataset(datasetId: string): Promise<DatasetDetail> {
    return this.request<DatasetDetail>(`/datasets/${datasetId}`);
  }

  async getDatasetDownloadUrls(datasetId: string): Promise<DatasetDownloadUrls> {
    return this.request<DatasetDownloadUrls>(`/datasets/${datasetId}/download-urls`);
  }

  async getCollections(): Promise<CollectionsResponse> {
    return this.request<CollectionsResponse>('/collections');
  }

  async getDataOwners(): Promise<DataOwnersResponse> {
    return this.request<DataOwnersResponse>('/data-owners');
  }

  // Public dataset endpoints (no authentication required)
  async getPublicDatasets(params?: {
    search?: string;
    collection_id?: number;
    data_owner_id?: number;
    limit?: number;
    offset?: number;
  }): Promise<DatasetsResponse> {
    const searchParams = new URLSearchParams();
    if (params?.search) searchParams.set('search', params.search);
    if (params?.collection_id) searchParams.set('collection_id', String(params.collection_id));
    if (params?.data_owner_id) searchParams.set('data_owner_id', String(params.data_owner_id));
    if (params?.limit) searchParams.set('limit', String(params.limit));
    if (params?.offset) searchParams.set('offset', String(params.offset));

    const query = searchParams.toString();
    return this.request<DatasetsResponse>(`/public/datasets${query ? `?${query}` : ''}`, {}, false);
  }

  async getPublicDataset(datasetId: string): Promise<DatasetDetail> {
    return this.request<DatasetDetail>(`/public/datasets/${datasetId}`, {}, false);
  }

  async getPublicCollections(): Promise<CollectionsResponse> {
    return this.request<CollectionsResponse>('/public/collections', {}, false);
  }

  async getPublicDataOwners(): Promise<DataOwnersResponse> {
    return this.request<DataOwnersResponse>('/public/data-owners', {}, false);
  }

  // Admin endpoints
  async adminListUsers(params?: { search?: string; include_groups?: boolean; limit?: number; offset?: number }) {
    const searchParams = new URLSearchParams();
    if (params?.search) searchParams.set('search', params.search);
    if (params?.include_groups) searchParams.set('include_groups', 'true');
    if (params?.limit) searchParams.set('limit', String(params.limit));
    if (params?.offset) searchParams.set('offset', String(params.offset));

    const query = searchParams.toString();
    return this.request<{
      users: object[];
      total: number;
      limit: number;
      offset: number;
    }>(`/admin/users${query ? `?${query}` : ''}`);
  }

  async adminGetUser(email: string) {
    return this.request<object>(`/admin/users/${encodeURIComponent(email)}`);
  }

  async adminInviteUser(data: {
    email: string;
    display_name?: string;
    is_admin?: boolean;
    groups?: string[];
  }) {
    return this.request<{ invited: boolean; email: string }>('/admin/users', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  async adminUpdateUser(email: string, data: { display_name?: string; is_admin?: boolean }) {
    return this.request<object>(`/admin/users/${encodeURIComponent(email)}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  async adminListGroups(params?: { search?: string; limit?: number; offset?: number }) {
    const searchParams = new URLSearchParams();
    if (params?.search) searchParams.set('search', params.search);
    if (params?.limit) searchParams.set('limit', String(params.limit));
    if (params?.offset) searchParams.set('offset', String(params.offset));

    const query = searchParams.toString();
    return this.request<{
      groups: object[];
      total: number;
      limit: number;
      offset: number;
    }>(`/admin/groups${query ? `?${query}` : ''}`);
  }

  async adminGetGroup(groupEmail: string) {
    return this.request<object>(`/admin/groups/${encodeURIComponent(groupEmail)}`);
  }

  async adminCreateGroup(data: { email: string; display_name?: string }) {
    return this.request<object>('/admin/groups', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  async adminAddGroupMember(groupEmail: string, userEmail: string) {
    return this.request<{ added: boolean }>(`/admin/groups/${encodeURIComponent(groupEmail)}/members`, {
      method: 'POST',
      body: JSON.stringify({ user_email: userEmail }),
    });
  }

  async adminRemoveGroupMember(groupEmail: string, userEmail: string) {
    return this.request<{ removed: boolean }>(
      `/admin/groups/${encodeURIComponent(groupEmail)}/members/${encodeURIComponent(userEmail)}`,
      { method: 'DELETE' }
    );
  }
}

export const api = new ApiClient();
export default api;
