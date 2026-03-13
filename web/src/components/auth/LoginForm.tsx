import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { loginWithOTP } from '../../lib/auth';
import { isWebAuthnSupported, authenticateWithPasskey } from '../../lib/webauthn';
import OTPInput from './OTPInput';

type LoginStep = 'email' | 'otp' | 'passkey-prompt' | 'pending';

function GoogleIcon() {
  return (
    <svg viewBox="0 0 24 24" class="h-5 w-5" aria-hidden="true">
      <path fill="#4285F4" d="M21.6 12.23c0-.68-.06-1.33-.17-1.95H12v3.69h5.39a4.61 4.61 0 0 1-2 3.03v2.5h3.24c1.9-1.75 2.97-4.34 2.97-7.27Z" />
      <path fill="#34A853" d="M12 22c2.7 0 4.97-.9 6.63-2.43l-3.24-2.5c-.9.6-2.05.96-3.39.96-2.61 0-4.82-1.77-5.61-4.14H3.05v2.57A10 10 0 0 0 12 22Z" />
      <path fill="#FBBC05" d="M6.39 13.89A5.97 5.97 0 0 1 6.08 12c0-.66.11-1.3.31-1.89V7.54H3.05A10 10 0 0 0 2 12c0 1.61.38 3.14 1.05 4.46l3.34-2.57Z" />
      <path fill="#EA4335" d="M12 5.97c1.47 0 2.8.5 3.85 1.49l2.89-2.89C16.96 2.92 14.7 2 12 2a10 10 0 0 0-8.95 5.54l3.34 2.57C7.18 7.74 9.39 5.97 12 5.97Z" />
    </svg>
  );
}

function GitHubIcon() {
  return (
    <svg viewBox="0 0 24 24" class="h-5 w-5 fill-current" aria-hidden="true">
      <path d="M12 .5a12 12 0 0 0-3.8 23.4c.6.11.82-.26.82-.58v-2.03c-3.34.73-4.04-1.42-4.04-1.42-.55-1.37-1.33-1.74-1.33-1.74-1.09-.74.08-.73.08-.73 1.2.09 1.84 1.23 1.84 1.23 1.08 1.82 2.82 1.29 3.5.99.11-.77.42-1.29.76-1.59-2.67-.3-5.47-1.31-5.47-5.86 0-1.3.47-2.37 1.24-3.2-.12-.3-.54-1.53.12-3.19 0 0 1.01-.32 3.3 1.22a11.5 11.5 0 0 1 6 0c2.28-1.54 3.29-1.22 3.29-1.22.66 1.66.24 2.89.12 3.19.77.83 1.24 1.9 1.24 3.2 0 4.56-2.81 5.55-5.49 5.85.43.37.82 1.1.82 2.23v3.3c0 .32.22.7.83.58A12 12 0 0 0 12 .5Z" />
    </svg>
  );
}

function PasskeyIcon() {
  return (
    <svg viewBox="0 0 24 24" class="h-5 w-5" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="1.8">
      <path strokeLinecap="round" strokeLinejoin="round" d="M14.5 10.5a3.5 3.5 0 1 0-2.8 3.43V17h2v1.5h2V20h2v-2.35l1.15-1.15A6.5 6.5 0 1 0 14.5 10.5Z" />
    </svg>
  );
}

export default function LoginForm() {
  const [step, setStep] = useState<LoginStep>('email');
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [pendingMessage, setPendingMessage] = useState('');
  const [authProviders, setAuthProviders] = useState({
    google: false,
    github: false,
    passkey: isWebAuthnSupported(),
  });

  useEffect(() => {
    api.getAuthProviders()
      .then((providers) => setAuthProviders({
        ...providers,
        passkey: providers.passkey && isWebAuthnSupported(),
      }))
      .catch(() => {
        setAuthProviders({
          google: false,
          github: false,
          passkey: isWebAuthnSupported(),
        });
      });

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
      const fallbackMessage = email
        ? 'Passkey login failed. Please try again.'
        : 'Passkey login failed. If this passkey was created earlier, enter your email and try again.';
      setError(err instanceof Error ? err.message : fallbackMessage);
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
        {authProviders.google && (
          <button
            onClick={() => api.startOAuth('google')}
            disabled={loading}
            class="btn-secondary w-full flex items-center justify-center gap-3"
          >
            <GoogleIcon />
            Continue with Google
          </button>
        )}
        {authProviders.github && (
          <button
            onClick={() => api.startOAuth('github')}
            disabled={loading}
            class="btn-secondary w-full flex items-center justify-center gap-3"
          >
            <GitHubIcon />
            Continue with GitHub
          </button>
        )}
        {authProviders.passkey && (
          <button
            onClick={handlePasskeyLogin}
            disabled={loading}
            class="btn-secondary w-full flex items-center justify-center gap-3"
          >
            <PasskeyIcon />
            Sign in with Passkey
          </button>
        )}
      </div>

      {(authProviders.google || authProviders.github || authProviders.passkey) && (
        <div class="relative">
          <div class="absolute inset-0 flex items-center">
            <div class="w-full border-t border-gray-200" />
          </div>
          <div class="relative flex justify-center text-xs uppercase">
            <span class="bg-white px-2 text-gray-500">or continue with email</span>
          </div>
        </div>
      )}

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
            placeholder="name@institute.ac.in"
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
