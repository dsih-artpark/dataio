import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { loginWithOTP } from '../../lib/auth';
import { isWebAuthnSupported, authenticateWithPasskey } from '../../lib/webauthn';
import OTPInput from './OTPInput';

type LoginStep = 'email' | 'otp' | 'passkey-prompt' | 'pending';

export default function LoginForm() {
  const [step, setStep] = useState<LoginStep>('email');
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [pendingMessage, setPendingMessage] = useState('');

  useEffect(() => {
    const hash = new URLSearchParams(window.location.hash.replace(/^#/, ''));
    const oauthSuccess = hash.get('oauth') === 'success';
    const needsPasskey = hash.get('needs_passkey') === 'true';
    const verificationStatus = hash.get('verification_status');
    const verificationMessage = hash.get('verification_message');

    if (verificationStatus === 'pending') {
      setPendingMessage(
        verificationMessage || 'Your account is pending administrator approval.'
      );
      setStep('pending');
      window.history.replaceState({}, document.title, '/login');
      return;
    }

    if (oauthSuccess) {
      setLoading(true);
      api.restoreSession()
        .then((restored) => {
          window.history.replaceState({}, document.title, '/login');
          if (!restored) {
            throw new Error('Unable to complete sign-in');
          }
          if (needsPasskey && isWebAuthnSupported()) {
            setStep('passkey-prompt');
          } else {
            window.location.href = '/datasets';
          }
        })
        .catch((err) => {
          setError(err instanceof Error ? err.message : 'Unable to complete sign-in');
          setLoading(false);
        });
    }
  }, []);

  const handleEmailSubmit = async (e: Event) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      await api.initiateLogin(email);
      setStep('otp');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to send code');
    } finally {
      setLoading(false);
    }
  };

  const handleOTPComplete = async (code: string) => {
    setError('');
    setLoading(true);

    try {
      const response = await loginWithOTP(email, code);
      if (response.verificationStatus === 'pending') {
        setPendingMessage(
          response.verificationMessage || 'Your account is pending administrator approval.'
        );
        setStep('pending');
        setLoading(false);
        return;
      }

      if (response.needsPasskey && isWebAuthnSupported()) {
        setStep('passkey-prompt');
      } else {
        window.location.href = '/datasets';
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Invalid code');
      setLoading(false);
    }
  };

  const handlePasskeyLogin = async () => {
    setError('');
    setLoading(true);

    try {
      await authenticateWithPasskey(email || undefined);
      window.location.href = '/datasets';
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Passkey login failed');
      setLoading(false);
    }
  };

  const handleSetupPasskey = () => {
    window.location.href = '/account?setup-passkey=true';
  };

  if (step === 'pending') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <h2 class="text-2xl font-bold text-gray-900">Approval Pending</h2>
          <p class="mt-2 text-gray-600">{pendingMessage}</p>
        </div>
        <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <p class="text-sm text-yellow-800">
            We created your account and sent it for review. You will receive an email once an administrator approves access.
          </p>
        </div>
        <div class="space-y-3">
          <a href="/datasets" class="btn-primary w-full block text-center">
            Browse Public Datasets
          </a>
          <button onClick={() => setStep('email')} class="btn-secondary w-full">
            Try Another Email
          </button>
        </div>
      </div>
    );
  }

  if (step === 'passkey-prompt') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <h2 class="text-2xl font-bold text-gray-900">Set up a Passkey</h2>
          <p class="mt-2 text-gray-600">
            You are signed in. Add a passkey now for faster and more secure access next time.
          </p>
        </div>

        <div class="space-y-3">
          <button onClick={handleSetupPasskey} class="btn-primary w-full">
            Set up Passkey
          </button>
          <a href="/datasets" class="btn-secondary w-full block text-center">
            Continue to Datasets
          </a>
        </div>
      </div>
    );
  }

  if (step === 'otp') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <h2 class="text-2xl font-bold text-gray-900">Enter your code</h2>
          <p class="mt-2 text-gray-600">
            We sent a 6-digit code to <span class="font-medium">{email}</span>
          </p>
        </div>

        {error && (
          <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
            {error}
          </div>
        )}

        <OTPInput length={6} onComplete={handleOTPComplete} disabled={loading} />

        <div class="text-center">
          <button onClick={() => setStep('email')} class="text-sm text-primary-600 hover:text-primary-700">
            Use a different email
          </button>
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-6">
      <div class="text-center">
        <h2 class="text-2xl font-bold text-gray-900">Sign in or create your account</h2>
        <p class="mt-2 text-gray-600">
          Use your work email, passkey, Google, or GitHub. We will create the account automatically if it does not exist.
        </p>
      </div>

      {error && (
        <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
          {error}
        </div>
      )}

      <div class="space-y-3">
        <button
          onClick={() => api.startOAuth('google')}
          disabled={loading}
          class="btn-secondary w-full"
        >
          Continue with Google
        </button>
        <button
          onClick={() => api.startOAuth('github')}
          disabled={loading}
          class="btn-secondary w-full"
        >
          Continue with GitHub
        </button>
        {isWebAuthnSupported() && (
          <button
            onClick={handlePasskeyLogin}
            disabled={loading}
            class="btn-secondary w-full"
          >
            Sign in with Passkey
          </button>
        )}
      </div>

      <div class="relative">
        <div class="absolute inset-0 flex items-center">
          <div class="w-full border-t border-gray-200" />
        </div>
        <div class="relative flex justify-center text-xs uppercase">
          <span class="bg-white px-2 text-gray-500">or continue with email</span>
        </div>
      </div>

      <form onSubmit={handleEmailSubmit} class="space-y-4">
        <div>
          <label for="email" class="label">
            Email address
          </label>
          <input
            type="email"
            id="email"
            value={email}
            onInput={(e) => setEmail((e.target as HTMLInputElement).value)}
            class="input"
            placeholder="you@institution.edu"
            required
            disabled={loading}
          />
        </div>

        <button type="submit" disabled={loading || !email} class="btn-primary w-full">
          {loading ? 'Sending...' : 'Continue'}
        </button>
      </form>
    </div>
  );
}
