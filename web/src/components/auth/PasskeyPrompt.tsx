import { useState } from 'preact/hooks';
import { isWebAuthnSupported, registerPasskey } from '../../lib/webauthn';

interface PasskeyPromptProps {
  onComplete?: () => void;
  onSkip?: () => void;
}

export default function PasskeyPrompt({ onComplete, onSkip }: PasskeyPromptProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [deviceName, setDeviceName] = useState('');

  if (!isWebAuthnSupported()) {
    return (
      <div class="text-center py-8">
        <div class="w-16 h-16 bg-yellow-100 rounded-full flex items-center justify-center mx-auto mb-4">
          <svg class="w-8 h-8 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <h3 class="text-lg font-semibold text-gray-900">Passkeys not supported</h3>
        <p class="mt-2 text-gray-600">
          Your browser doesn't support passkeys. You can still use email codes to log in.
        </p>
        {onSkip && (
          <button onClick={onSkip} class="btn-secondary mt-4">
            Continue
          </button>
        )}
      </div>
    );
  }

  const handleSetup = async () => {
    setError('');
    setLoading(true);

    try {
      await registerPasskey(deviceName || undefined);
      onComplete?.();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to set up passkey');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div class="space-y-6">
      <div class="text-center">
        <div class="w-16 h-16 bg-primary-100 rounded-full flex items-center justify-center mx-auto mb-4">
          <svg class="w-8 h-8 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
          </svg>
        </div>
        <h3 class="text-lg font-semibold text-gray-900">Set up a Passkey</h3>
        <p class="mt-2 text-gray-600">
          Passkeys use your device's biometrics or PIN to sign you in securely
          without typing a code.
        </p>
      </div>

      {error && (
        <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
          {error}
        </div>
      )}

      <div>
        <label for="device-name" class="label">
          Device name (optional)
        </label>
        <input
          type="text"
          id="device-name"
          value={deviceName}
          onInput={(e) => setDeviceName((e.target as HTMLInputElement).value)}
          class="input"
          placeholder="My MacBook"
          disabled={loading}
        />
        <p class="mt-1 text-xs text-gray-500">
          Give this passkey a name to help you identify it later.
        </p>
      </div>

      <div class="space-y-3">
        <button
          onClick={handleSetup}
          disabled={loading}
          class="btn-primary w-full"
        >
          {loading ? 'Setting up...' : 'Set up Passkey'}
        </button>

        {onSkip && (
          <button
            onClick={onSkip}
            disabled={loading}
            class="btn-secondary w-full"
          >
            Skip for now
          </button>
        )}
      </div>
    </div>
  );
}
