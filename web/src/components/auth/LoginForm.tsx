import { useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { loginWithOTP } from '../../lib/auth';
import { isWebAuthnSupported, authenticateWithPasskey } from '../../lib/webauthn';
import OTPInput from './OTPInput';

type LoginStep = 'email' | 'otp' | 'passkey-prompt';

export default function LoginForm() {
  const [step, setStep] = useState<LoginStep>('email');
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

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
      const { needsPasskey } = await loginWithOTP(email, code);
      if (needsPasskey && isWebAuthnSupported()) {
        setStep('passkey-prompt');
      } else {
        window.location.href = '/dashboard';
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
      await authenticateWithPasskey(email);
      window.location.href = '/dashboard';
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Passkey login failed');
      setLoading(false);
    }
  };

  const handleSkipPasskey = () => {
    window.location.href = '/dashboard';
  };

  const handleSetupPasskey = () => {
    window.location.href = '/account?setup-passkey=true';
  };

  if (step === 'passkey-prompt') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 bg-primary-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-8 h-8 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
            </svg>
          </div>
          <h2 class="text-2xl font-bold text-gray-900">Set up a Passkey</h2>
          <p class="mt-2 text-gray-600">
            Passkeys let you sign in quickly and securely without typing a code.
          </p>
        </div>

        <div class="space-y-3">
          <button
            onClick={handleSetupPasskey}
            class="btn-primary w-full"
          >
            Set up Passkey
          </button>
          <button
            onClick={handleSkipPasskey}
            class="btn-secondary w-full"
          >
            Skip for now
          </button>
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

        <OTPInput
          length={6}
          onComplete={handleOTPComplete}
          disabled={loading}
        />

        <div class="text-center">
          <button
            onClick={() => setStep('email')}
            class="text-sm text-primary-600 hover:text-primary-700"
          >
            Use a different email
          </button>
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-6">
      <div class="text-center">
        <h2 class="text-2xl font-bold text-gray-900">Sign in</h2>
        <p class="mt-2 text-gray-600">
          Enter your email to receive a login code
        </p>
      </div>

      {error && (
        <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
          {error}
        </div>
      )}

      <div class="space-y-4">
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
            placeholder="you@example.com"
            required
            disabled={loading}
          />
        </div>

        {/* Show both options after email is entered - NO auto-submit */}
        <button
          onClick={handleEmailSubmit}
          disabled={loading || !email}
          class="btn-primary w-full"
        >
          {loading ? 'Sending...' : 'Continue with Email Code'}
        </button>

        {isWebAuthnSupported() && (
          <button
            onClick={() => {
              if (email) {
                handlePasskeyLogin();
              } else {
                setError('Please enter your email first');
              }
            }}
            disabled={loading || !email}
            class="btn-secondary w-full"
          >
            <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
            </svg>
            Sign in with Passkey
          </button>
        )}
      </div>

      <div class="text-center pt-4 border-t border-gray-200">
        <p class="text-sm text-gray-600">
          Don't have an account?{' '}
          <a href="/register" class="text-primary-600 hover:text-primary-700 font-medium">
            Register
          </a>
        </p>
      </div>
    </div>
  );
}
