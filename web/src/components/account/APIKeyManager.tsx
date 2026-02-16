import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';

interface APIKey {
  id: string;
  name: string;
  key_prefix: string;
  created_at: string;
  last_used_at: string | null;
  expires_at: string | null;
}

export default function APIKeyManager() {
  const [apiKeys, setApiKeys] = useState<APIKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [newKeyName, setNewKeyName] = useState('');
  const [creating, setCreating] = useState(false);
  const [newKey, setNewKey] = useState<string | null>(null);

  useEffect(() => {
    fetchApiKeys();
  }, []);

  const fetchApiKeys = async () => {
    setLoading(true);
    try {
      const response = await api.listApiKeys();
      setApiKeys(response.api_keys);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load API keys');
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async (e: Event) => {
    e.preventDefault();
    if (!newKeyName.trim()) return;

    setCreating(true);
    setError('');

    try {
      const response = await api.createApiKey(newKeyName.trim());
      setNewKey(response.key);
      setNewKeyName('');
      setShowCreate(false);
      fetchApiKeys();
    } catch (err) {
      console.error('API key creation error:', err);
      setError(err instanceof Error ? err.message : 'Failed to create API key');
    } finally {
      setCreating(false);
    }
  };

  const handleRevoke = async (keyId: string, keyName: string) => {
    if (!confirm(`Are you sure you want to revoke the API key "${keyName}"?`)) return;

    try {
      await api.revokeApiKey(keyId);
      setApiKeys(apiKeys.filter((k) => k.id !== keyId));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to revoke API key');
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const formatDate = (date: string | null) => {
    if (!date) return 'Never';
    return new Date(date).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  if (loading) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
          <p class="mt-4 text-gray-500">Loading API keys...</p>
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-6">
      {error && (
        <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
          {error}
        </div>
      )}

      {/* New Key Modal */}
      {newKey && (
        <div class="card border-green-200 bg-green-50">
          <div class="card-body">
            <div class="flex items-start gap-3">
              <div class="w-10 h-10 bg-green-100 rounded-full flex items-center justify-center flex-shrink-0">
                <svg class="w-5 h-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <div class="flex-1">
                <h3 class="font-semibold text-green-800">API Key Created</h3>
                <p class="text-sm text-green-700 mt-1">
                  Copy your API key now. You won't be able to see it again!
                </p>
                <div class="mt-3 flex items-center gap-2">
                  <code class="flex-1 bg-white px-3 py-2 rounded border border-green-200 text-sm font-mono">
                    {newKey}
                  </code>
                  <button
                    onClick={() => copyToClipboard(newKey)}
                    class="btn-secondary text-sm"
                  >
                    Copy
                  </button>
                </div>
                <button
                  onClick={() => setNewKey(null)}
                  class="text-sm text-green-700 hover:text-green-800 mt-3"
                >
                  Dismiss
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Create Form */}
      <div class="card">
        <div class="card-header flex items-center justify-between">
          <div>
            <h2 class="text-lg font-semibold text-gray-900">Your API Keys</h2>
            <p class="text-sm text-gray-500 mt-1">
              Use API keys to authenticate with the DataIO API
            </p>
          </div>
          {!showCreate && (
            <button
              onClick={() => setShowCreate(true)}
              class="btn-primary text-sm"
            >
              Create API Key
            </button>
          )}
        </div>

        {showCreate && (
          <div class="px-6 py-4 bg-gray-50 border-b border-gray-200">
            <form onSubmit={handleCreate} class="flex gap-3">
              <input
                type="text"
                value={newKeyName}
                onInput={(e) => setNewKeyName((e.target as HTMLInputElement).value)}
                placeholder="Key name (e.g., Production API)"
                class="input flex-1"
                required
              />
              <button
                type="submit"
                disabled={creating}
                class="btn-primary"
              >
                {creating ? 'Creating...' : 'Create'}
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowCreate(false);
                  setNewKeyName('');
                }}
                class="btn-secondary"
              >
                Cancel
              </button>
            </form>
          </div>
        )}

        <div class="card-body">
          {apiKeys.length === 0 ? (
            <div class="text-center py-8">
              <div class="w-12 h-12 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-3">
                <svg class="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
                </svg>
              </div>
              <p class="text-gray-500">No API keys yet</p>
              <button
                onClick={() => setShowCreate(true)}
                class="btn-primary mt-4"
              >
                Create your first API key
              </button>
            </div>
          ) : (
            <table class="min-w-full divide-y divide-gray-200">
              <thead>
                <tr>
                  <th class="text-left text-xs font-medium text-gray-500 uppercase tracking-wider pb-3">
                    Name
                  </th>
                  <th class="text-left text-xs font-medium text-gray-500 uppercase tracking-wider pb-3">
                    Key
                  </th>
                  <th class="text-left text-xs font-medium text-gray-500 uppercase tracking-wider pb-3">
                    Created
                  </th>
                  <th class="text-left text-xs font-medium text-gray-500 uppercase tracking-wider pb-3">
                    Last Used
                  </th>
                  <th class="text-right text-xs font-medium text-gray-500 uppercase tracking-wider pb-3">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-200">
                {apiKeys.map((key) => (
                  <tr key={key.id}>
                    <td class="py-3 font-medium text-gray-900">{key.name}</td>
                    <td class="py-3 text-gray-500 font-mono text-sm">{key.key_prefix}</td>
                    <td class="py-3 text-gray-500 text-sm">{formatDate(key.created_at)}</td>
                    <td class="py-3 text-gray-500 text-sm">{formatDate(key.last_used_at)}</td>
                    <td class="py-3 text-right">
                      <button
                        onClick={() => handleRevoke(key.id, key.name)}
                        class="text-red-600 hover:text-red-700 text-sm"
                      >
                        Revoke
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Usage Instructions */}
      <div class="card">
        <div class="card-header">
          <h2 class="text-lg font-semibold text-gray-900">How to use</h2>
        </div>
        <div class="card-body">
          <p class="text-gray-600 mb-4">
            Include your API key in the <code class="bg-gray-100 px-1.5 py-0.5 rounded text-sm">X-API-Key</code> header:
          </p>
          <pre class="bg-gray-900 text-gray-100 rounded-lg p-4 text-sm overflow-x-auto">
{`curl -H "X-API-Key: YOUR_API_KEY" \\
  https://api.dataio.artpark.ai/api/v1/datasets`}
          </pre>
        </div>
      </div>
    </div>
  );
}
