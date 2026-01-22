import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { currentUser } from '../../lib/auth';
import PasskeyPrompt from '../auth/PasskeyPrompt';
import { isWebAuthnSupported } from '../../lib/webauthn';
import DeleteAccountModal from './DeleteAccountModal';

interface Passkey {
  id: string;
  device_name: string;
  created_at: string;
  last_used_at: string | null;
}

interface APIKey {
  id: string;
  name: string;
  key_prefix: string;
  created_at: string;
  last_used_at: string | null;
  expires_at: string | null;
}

export default function UnifiedAccountSettings() {
  // Profile state
  const [displayName, setDisplayName] = useState('');
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState('');
  const [error, setError] = useState('');

  // Passkeys state
  const [passkeys, setPasskeys] = useState<Passkey[]>([]);
  const [passkeysError, setPasskeysError] = useState('');
  const [showPasskeySetup, setShowPasskeySetup] = useState(false);

  // API Keys state
  const [apiKeys, setApiKeys] = useState<APIKey[]>([]);
  const [apiKeysLoading, setApiKeysLoading] = useState(true);
  const [apiKeysError, setApiKeysError] = useState('');
  const [showCreateKey, setShowCreateKey] = useState(false);
  const [newKeyName, setNewKeyName] = useState('');
  const [creatingKey, setCreatingKey] = useState(false);
  const [newKey, setNewKey] = useState<string | null>(null);

  // Delete modal state
  const [showDeleteModal, setShowDeleteModal] = useState(false);

  const user = currentUser.value;

  useEffect(() => {
    if (user) {
      setDisplayName(user.display_name || '');
    }
    fetchPasskeys();
    fetchApiKeys();

    const params = new URLSearchParams(window.location.search);
    if (params.get('setup-passkey') === 'true') {
      setShowPasskeySetup(true);
    }
  }, [user]);

  const fetchPasskeys = async () => {
    try {
      setPasskeysError('');
      const response = await api.listPasskeys();
      setPasskeys(response.passkeys as Passkey[]);
    } catch (err) {
      setPasskeysError(err instanceof Error ? err.message : 'Failed to load passkeys');
    }
  };

  const fetchApiKeys = async () => {
    setApiKeysLoading(true);
    try {
      const response = await api.listApiKeys();
      setApiKeys(response.api_keys);
    } catch (err) {
      setApiKeysError(err instanceof Error ? err.message : 'Failed to load API keys');
    } finally {
      setApiKeysLoading(false);
    }
  };

  const handleUpdateProfile = async (e: Event) => {
    e.preventDefault();
    setError('');
    setSuccess('');
    setLoading(true);

    try {
      await api.updateProfile(displayName || undefined);
      setSuccess('Profile updated successfully');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update profile');
    } finally {
      setLoading(false);
    }
  };

  const handleDeletePasskey = async (passkeyId: string) => {
    if (!confirm('Are you sure you want to remove this passkey?')) return;

    try {
      await api.deletePasskey(passkeyId);
      setPasskeys(passkeys.filter((p) => p.id !== passkeyId));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to remove passkey');
    }
  };

  const handlePasskeySetupComplete = () => {
    setShowPasskeySetup(false);
    fetchPasskeys();
    window.history.replaceState({}, '', '/account');
  };

  const handleCreateApiKey = async (e: Event) => {
    e.preventDefault();
    if (!newKeyName.trim()) return;

    setCreatingKey(true);
    setApiKeysError('');

    try {
      const response = await api.createApiKey(newKeyName.trim());
      setNewKey(response.key);
      setNewKeyName('');
      setShowCreateKey(false);
      fetchApiKeys();
    } catch (err) {
      setApiKeysError(err instanceof Error ? err.message : 'Failed to create API key');
    } finally {
      setCreatingKey(false);
    }
  };

  const handleRevokeApiKey = async (keyId: string, keyName: string) => {
    if (!confirm(`Are you sure you want to revoke the API key "${keyName}"?`)) return;

    try {
      await api.revokeApiKey(keyId);
      setApiKeys(apiKeys.filter((k) => k.id !== keyId));
    } catch (err) {
      setApiKeysError(err instanceof Error ? err.message : 'Failed to revoke API key');
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

  if (showPasskeySetup) {
    return (
      <div class="card max-w-lg mx-auto">
        <div class="card-body">
          <PasskeyPrompt
            onComplete={handlePasskeySetupComplete}
            onSkip={() => {
              setShowPasskeySetup(false);
              window.history.replaceState({}, '', '/account');
            }}
          />
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-8">
      {/* Success/Error Messages */}
      {(error || success) && (
        <div
          class={`px-4 py-3 rounded-lg text-sm ${
            error ? 'bg-red-50 text-red-700 border border-red-200' : 'bg-green-50 text-green-700 border border-green-200'
          }`}
        >
          {error || success}
        </div>
      )}

      {/* New API Key Alert */}
      {newKey && (
        <div class="bg-green-50 border border-green-200 rounded-lg p-4">
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
                <code class="flex-1 bg-white px-3 py-2 rounded border border-green-200 text-sm font-mono break-all">
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
      )}

      {/* Two Column Layout */}
      <div class="grid lg:grid-cols-2 gap-6">
        {/* Left Column - Profile & Account Info */}
        <div class="space-y-6">
          {/* Profile */}
          <div class="card">
            <div class="card-header">
              <h2 class="text-lg font-semibold text-gray-900">Profile</h2>
            </div>
            <div class="card-body">
              <form onSubmit={handleUpdateProfile} class="space-y-4">
                <div>
                  <label for="email" class="label">Email</label>
                  <input
                    type="email"
                    id="email"
                    value={user?.email || ''}
                    class="input bg-gray-50"
                    disabled
                  />
                </div>

                <div>
                  <label for="display-name" class="label">Display Name</label>
                  <input
                    type="text"
                    id="display-name"
                    value={displayName}
                    onInput={(e) => setDisplayName((e.target as HTMLInputElement).value)}
                    class="input"
                    placeholder="Your name"
                  />
                </div>

                <div class="flex justify-end">
                  <button type="submit" disabled={loading} class="btn-primary">
                    {loading ? 'Saving...' : 'Save Changes'}
                  </button>
                </div>
              </form>
            </div>
          </div>

          {/* Account Info */}
          <div class="card">
            <div class="card-header">
              <h2 class="text-lg font-semibold text-gray-900">Account</h2>
            </div>
            <div class="card-body">
              <dl class="space-y-3 text-sm">
                <div class="flex justify-between">
                  <dt class="text-gray-500">Email verified</dt>
                  <dd>
                    {user?.email_verified ? (
                      <span class="text-green-600 font-medium">Yes</span>
                    ) : (
                      <span class="text-yellow-600 font-medium">No</span>
                    )}
                  </dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-gray-500">Account type</dt>
                  <dd class="font-medium text-gray-900">
                    {user?.is_admin ? 'Administrator' : 'User'}
                  </dd>
                </div>
              </dl>
            </div>
          </div>

          {/* Passkeys */}
          <div class="card">
            <div class="card-header flex items-center justify-between">
              <div>
                <h2 class="text-lg font-semibold text-gray-900">Passkeys</h2>
                <p class="text-sm text-gray-500 mt-0.5">Sign in without typing a code</p>
              </div>
              {isWebAuthnSupported() && passkeys.length > 0 && (
                <button
                  onClick={() => setShowPasskeySetup(true)}
                  class="btn-secondary text-sm"
                >
                  Add
                </button>
              )}
            </div>
            <div class="card-body">
              {passkeysError && (
                <div class="mb-4 px-4 py-3 rounded-lg text-sm bg-red-50 text-red-700 border border-red-200">
                  {passkeysError}
                </div>
              )}
              {passkeys.length === 0 && !passkeysError ? (
                <div class="text-center py-4">
                  <p class="text-gray-500 text-sm">No passkeys registered</p>
                  {isWebAuthnSupported() && (
                    <button
                      onClick={() => setShowPasskeySetup(true)}
                      class="btn-primary mt-3 text-sm"
                    >
                      Set up passkey
                    </button>
                  )}
                </div>
              ) : (
                <ul class="divide-y divide-gray-200">
                  {passkeys.map((passkey) => (
                    <li key={passkey.id} class="py-3 flex items-center justify-between">
                      <div>
                        <p class="font-medium text-gray-900 text-sm">{passkey.device_name}</p>
                        <p class="text-xs text-gray-500">
                          Added {formatDate(passkey.created_at)}
                        </p>
                      </div>
                      <button
                        onClick={() => handleDeletePasskey(passkey.id)}
                        class="text-red-600 hover:text-red-700 text-sm"
                      >
                        Remove
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </div>

        {/* Right Column - API Keys & Danger Zone */}
        <div class="space-y-6">
          {/* API Keys */}
          <div class="card">
            <div class="card-header flex items-center justify-between">
              <div>
                <h2 class="text-lg font-semibold text-gray-900">API Keys</h2>
                <p class="text-sm text-gray-500 mt-0.5">Programmatic access to the platform</p>
              </div>
              {!showCreateKey && apiKeys.length > 0 && (
                <button
                  onClick={() => setShowCreateKey(true)}
                  class="btn-primary text-sm"
                >
                  Create
                </button>
              )}
            </div>

            {showCreateKey && (
              <div class="px-6 py-4 bg-gray-50 border-b border-gray-200">
                <form onSubmit={handleCreateApiKey} class="flex gap-2">
                  <input
                    type="text"
                    value={newKeyName}
                    onInput={(e) => setNewKeyName((e.target as HTMLInputElement).value)}
                    placeholder="Key name"
                    class="input flex-1 text-sm"
                    required
                  />
                  <button type="submit" disabled={creatingKey} class="btn-primary text-sm">
                    {creatingKey ? '...' : 'Create'}
                  </button>
                  <button
                    type="button"
                    onClick={() => { setShowCreateKey(false); setNewKeyName(''); }}
                    class="btn-secondary text-sm"
                  >
                    Cancel
                  </button>
                </form>
              </div>
            )}

            <div class="card-body">
              {apiKeysError && (
                <div class="mb-4 px-4 py-3 rounded-lg text-sm bg-red-50 text-red-700 border border-red-200">
                  {apiKeysError}
                </div>
              )}

              {apiKeysLoading ? (
                <div class="text-center py-6">
                  <div class="animate-spin w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full mx-auto" />
                </div>
              ) : apiKeys.length === 0 ? (
                <div class="text-center py-4">
                  <p class="text-gray-500 text-sm">No API keys yet</p>
                  <button
                    onClick={() => setShowCreateKey(true)}
                    class="btn-primary mt-3 text-sm"
                  >
                    Create API key
                  </button>
                </div>
              ) : (
                <ul class="divide-y divide-gray-200">
                  {apiKeys.map((key) => (
                    <li key={key.id} class="py-3">
                      <div class="flex items-center justify-between">
                        <div>
                          <p class="font-medium text-gray-900 text-sm">{key.name}</p>
                          <p class="text-xs text-gray-500 font-mono">{key.key_prefix}</p>
                        </div>
                        <button
                          onClick={() => handleRevokeApiKey(key.id, key.name)}
                          class="text-red-600 hover:text-red-700 text-sm"
                        >
                          Revoke
                        </button>
                      </div>
                      <p class="text-xs text-gray-400 mt-1">
                        Created {formatDate(key.created_at)} · Last used {formatDate(key.last_used_at)}
                      </p>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>

          {/* API Usage */}
          <div class="card">
            <div class="card-header">
              <h2 class="text-lg font-semibold text-gray-900">API Usage</h2>
            </div>
            <div class="card-body">
              <p class="text-sm text-gray-600 mb-3">
                Include your API key in the <code class="bg-gray-100 px-1.5 py-0.5 rounded text-xs">X-API-Key</code> header:
              </p>
              <pre class="bg-gray-900 text-gray-100 rounded-lg p-3 text-xs overflow-x-auto">
{`curl -H "X-API-Key: YOUR_KEY" \\
  https://data.artpark.ai/api/v1/datasets`}
              </pre>
            </div>
          </div>

          {/* Danger Zone */}
          <div class="card border-red-200">
            <div class="card-header bg-red-50 border-b border-red-200">
              <h2 class="text-lg font-semibold text-red-800">Danger Zone</h2>
            </div>
            <div class="card-body">
              <div class="flex items-center justify-between">
                <div>
                  <p class="font-medium text-gray-900 text-sm">Delete Account</p>
                  <p class="text-xs text-gray-500">
                    Permanently delete your account and all data.
                  </p>
                </div>
                <button
                  onClick={() => setShowDeleteModal(true)}
                  class="inline-flex items-center px-3 py-1.5 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700"
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Delete Account Modal */}
      {showDeleteModal && (
        <DeleteAccountModal onClose={() => setShowDeleteModal(false)} />
      )}
    </div>
  );
}
