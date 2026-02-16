import { useState, useEffect } from 'preact/hooks';
import { api } from '../../lib/api';
import { currentUser } from '../../lib/auth';

type InviteState = 'loading' | 'success' | 'error';

export default function AcceptInviteHandler() {
  const [state, setState] = useState<InviteState>('loading');
  const [error, setError] = useState('');
  const [needsPasskey, setNeedsPasskey] = useState(false);

  useEffect(() => {
    const acceptInvite = async () => {
      // Get token from URL params
      const params = new URLSearchParams(window.location.search);
      const token = params.get('token');

      if (!token) {
        setError('Invalid invitation link. Please contact your administrator.');
        setState('error');
        return;
      }

      try {
        const response = await api.acceptInvitation(token);
        currentUser.value = response.user;
        setNeedsPasskey(response.needs_passkey);
        setState('success');

        // Redirect after brief delay
        setTimeout(() => {
          if (response.needs_passkey) {
            window.location.href = '/settings?setup=passkey';
          } else {
            window.location.href = '/datasets';
          }
        }, 3000);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to accept invitation. The link may have expired.');
        setState('error');
      }
    };

    acceptInvite();
  }, []);

  if (state === 'loading') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <h2 class="text-2xl font-bold text-gray-900">Accepting invitation...</h2>
          <p class="mt-2 text-gray-600">Please wait while we set up your account.</p>
        </div>
      </div>
    );
  }

  if (state === 'success') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-8 h-8 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <h2 class="text-2xl font-bold text-gray-900">Welcome to DataIO!</h2>
          <p class="mt-2 text-gray-600">
            Your account has been activated successfully.
          </p>
          {needsPasskey ? (
            <p class="mt-1 text-sm text-gray-500">
              Redirecting to set up your passkey...
            </p>
          ) : (
            <p class="mt-1 text-sm text-gray-500">
              Redirecting to datasets...
            </p>
          )}
        </div>

        {needsPasskey && (
          <div class="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <p class="text-sm text-blue-800">
              <strong>Recommended:</strong> Set up a passkey for secure, passwordless access.
            </p>
          </div>
        )}
      </div>
    );
  }

  // Error state
  return (
    <div class="space-y-6">
      <div class="text-center">
        <div class="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
          <svg class="w-8 h-8 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </div>
        <h2 class="text-2xl font-bold text-gray-900">Invitation Failed</h2>
        <p class="mt-2 text-gray-600">{error}</p>
      </div>

      <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
        <p class="text-sm text-yellow-800">
          <strong>Common reasons:</strong>
        </p>
        <ul class="mt-2 text-sm text-yellow-700 list-disc list-inside space-y-1">
          <li>The invitation link has expired (48 hours)</li>
          <li>The invitation was revoked by an administrator</li>
          <li>You've already accepted this invitation</li>
        </ul>
      </div>

      <div class="space-y-3">
        <a href="/login" class="btn-primary w-full block text-center">
          Sign In
        </a>
        <a href="/" class="btn-secondary w-full block text-center">
          Return Home
        </a>
      </div>
    </div>
  );
}
