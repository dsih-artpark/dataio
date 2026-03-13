import { useState, useEffect } from 'preact/hooks';
import { api } from '../../lib/api';
import { currentUser } from '../../lib/auth';

type VerifyState = 'loading' | 'success' | 'pending' | 'error';

export default function VerifyEmailHandler() {
  const [state, setState] = useState<VerifyState>('loading');
  const [error, setError] = useState('');
  const [pendingMessage, setPendingMessage] = useState('');

  useEffect(() => {
    const verifyEmail = async () => {
      // Get token and email from URL params
      const params = new URLSearchParams(window.location.search);
      const token = params.get('token');
      const email = params.get('email');

      if (!token || !email) {
        setError('Invalid verification link. Please try registering again.');
        setState('error');
        return;
      }

      try {
        const response = await api.verifyRegistration(email, undefined, token);
        currentUser.value = response.access_token ? response.user : null;

        if (response.verification_status === 'pending') {
          setPendingMessage(response.verification_message || 'Your account is pending admin approval.');
          setState('pending');
        } else {
          setState('success');
          // Redirect after brief delay
          setTimeout(() => {
            window.location.href = '/datasets';
          }, 2000);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Verification failed. The link may have expired.');
        setState('error');
      }
    };

    verifyEmail();
  }, []);

  if (state === 'loading') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <h2 class="text-2xl font-bold text-gray-900">Verifying your email...</h2>
          <p class="mt-2 text-gray-600">Please wait while we verify your registration.</p>
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
          <h2 class="text-2xl font-bold text-gray-900">Email Verified!</h2>
          <p class="mt-2 text-gray-600">
            Your account has been created successfully.
          </p>
          <p class="mt-1 text-sm text-gray-500">
            Redirecting to datasets...
          </p>
        </div>
      </div>
    );
  }

  if (state === 'pending') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 bg-yellow-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-8 h-8 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h2 class="text-2xl font-bold text-gray-900">Registration Pending</h2>
          <p class="mt-2 text-gray-600">{pendingMessage}</p>
        </div>

        <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <p class="text-sm text-yellow-800">
            <strong>What happens next?</strong>
          </p>
          <ul class="mt-2 text-sm text-yellow-700 list-disc list-inside space-y-1">
            <li>An administrator will review your registration</li>
            <li>You'll receive an email once your account is verified</li>
            <li>Until then, you can browse public datasets</li>
          </ul>
        </div>

        <div class="space-y-3">
          <a href="/datasets" class="btn-primary w-full block text-center">
            Browse Datasets
          </a>
          <a href="/" class="btn-secondary w-full block text-center">
            Return Home
          </a>
        </div>
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
        <h2 class="text-2xl font-bold text-gray-900">Verification Failed</h2>
        <p class="mt-2 text-gray-600">{error}</p>
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
