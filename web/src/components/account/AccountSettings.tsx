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

export default function AccountSettings() {
  const [displayName, setDisplayName] = useState('');
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState('');
  const [error, setError] = useState('');
  const [passkeys, setPasskeys] = useState<Passkey[]>([]);
  const [passkeysError, setPasskeysError] = useState('');
  const [showPasskeySetup, setShowPasskeySetup] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);

  const user = currentUser.value;

  useEffect(() => {
    if (user) {
      setDisplayName(user.display_name || '');
    }
    fetchPasskeys();

    // Check if we should show passkey setup
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
    // Remove query param
    window.history.replaceState({}, '', '/account');
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
      <div class="card max-w-lg">
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
    <div class="space-y-6">
      {/* Profile */}
      <div class="card">
        <div class="card-header">
          <h2 class="text-lg font-semibold text-gray-900">Profile</h2>
        </div>
        <div class="card-body">
          {(error || success) && (
            <div
              class={`mb-4 px-4 py-3 rounded-lg text-sm ${
                error ? 'bg-red-50 text-red-700 border border-red-200' : 'bg-green-50 text-green-700 border border-green-200'
              }`}
            >
              {error || success}
            </div>
          )}

          <form onSubmit={handleUpdateProfile} class="space-y-4">
            <div>
              <label for="email" class="label">
                Email
              </label>
              <input
                type="email"
                id="email"
                value={user?.email || ''}
                class="input bg-gray-50"
                disabled
              />
            </div>

            <div>
              <label for="display-name" class="label">
                Display Name
              </label>
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

      {/* Passkeys */}
      <div class="card">
        <div class="card-header flex items-center justify-between">
          <div>
            <h2 class="text-lg font-semibold text-gray-900">Passkeys</h2>
            <p class="text-sm text-gray-500 mt-1">
              Passkeys let you sign in without typing a code
            </p>
          </div>
          {isWebAuthnSupported() && (
            <button
              onClick={() => setShowPasskeySetup(true)}
              class="btn-secondary text-sm"
            >
              Add Passkey
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
            <div class="text-center py-6">
              <div class="w-12 h-12 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-3">
                <svg class="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                </svg>
              </div>
              <p class="text-gray-500">No passkeys registered</p>
              {isWebAuthnSupported() && (
                <button
                  onClick={() => setShowPasskeySetup(true)}
                  class="btn-primary mt-4"
                >
                  Set up your first passkey
                </button>
              )}
            </div>
          ) : (
            <ul class="divide-y divide-gray-200">
              {passkeys.map((passkey) => (
                <li key={passkey.id} class="py-4 flex items-center justify-between">
                  <div>
                    <p class="font-medium text-gray-900">{passkey.device_name}</p>
                    <p class="text-sm text-gray-500">
                      Added {formatDate(passkey.created_at)} · Last used{' '}
                      {formatDate(passkey.last_used_at)}
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

      {/* Account Info */}
      <div class="card">
        <div class="card-header">
          <h2 class="text-lg font-semibold text-gray-900">Account Information</h2>
        </div>
        <div class="card-body">
          <dl class="space-y-3">
            <div class="flex justify-between">
              <dt class="text-gray-500">Email verified</dt>
              <dd class="text-gray-900">
                {user?.email_verified ? (
                  <span class="text-green-600">Yes</span>
                ) : (
                  <span class="text-yellow-600">No</span>
                )}
              </dd>
            </div>
            <div class="flex justify-between">
              <dt class="text-gray-500">Account type</dt>
              <dd class="text-gray-900">
                {user?.is_admin ? 'Administrator' : 'User'}
              </dd>
            </div>
          </dl>
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
              <p class="font-medium text-gray-900">Delete Account</p>
              <p class="text-sm text-gray-500">
                Permanently delete your account and all associated data.
              </p>
            </div>
            <button
              onClick={() => setShowDeleteModal(true)}
              class="inline-flex items-center px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700"
            >
              Delete Account
            </button>
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
