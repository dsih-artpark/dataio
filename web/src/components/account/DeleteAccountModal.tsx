import { useState } from 'preact/hooks';
import { api } from '../../lib/api';
import OTPInput from '../auth/OTPInput';

interface DeleteAccountModalProps {
  onClose: () => void;
}

type DeleteStep = 'confirm' | 'otp' | 'deleting';

export default function DeleteAccountModal({ onClose }: DeleteAccountModalProps) {
  const [step, setStep] = useState<DeleteStep>('confirm');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSendCode = async () => {
    setError('');
    setLoading(true);

    try {
      await api.initiateAccountDeletion();
      setStep('otp');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to send verification code');
    } finally {
      setLoading(false);
    }
  };

  const handleOTPComplete = async (code: string) => {
    setError('');
    setLoading(true);
    setStep('deleting');

    try {
      await api.verifyAccountDeletion(code);
      // Clear tokens and redirect
      api.clearTokens();
      window.location.href = '/?deleted=true';
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Invalid verification code');
      setStep('otp');
      setLoading(false);
    }
  };

  if (step === 'deleting') {
    return (
      <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
        <div class="bg-white rounded-lg shadow-xl w-full max-w-md mx-4 p-6">
          <div class="text-center">
            <div class="w-16 h-16 border-4 border-red-600 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
            <h3 class="text-lg font-semibold text-gray-900">Deleting your account...</h3>
            <p class="mt-2 text-gray-600">This may take a moment.</p>
          </div>
        </div>
      </div>
    );
  }

  if (step === 'otp') {
    return (
      <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
        <div class="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
          <div class="px-6 py-4 border-b border-gray-200">
            <h3 class="text-lg font-semibold text-gray-900">Confirm Deletion</h3>
          </div>
          <div class="p-6 space-y-4">
            <p class="text-gray-600">
              Enter the 6-digit code we sent to your email to confirm account deletion.
            </p>

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

            <div class="flex gap-3 justify-end pt-4">
              <button
                type="button"
                onClick={onClose}
                class="btn-secondary"
                disabled={loading}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Confirm step
  return (
    <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div class="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
        <div class="px-6 py-4 border-b border-red-200 bg-red-50">
          <h3 class="text-lg font-semibold text-red-800">Delete Account</h3>
        </div>
        <div class="p-6 space-y-4">
          <div class="bg-red-50 border border-red-200 rounded-lg p-4">
            <p class="text-red-800 font-medium">This action is permanent and cannot be undone.</p>
            <p class="text-red-700 text-sm mt-2">Deleting your account will:</p>
            <ul class="mt-2 text-sm text-red-700 list-disc list-inside space-y-1">
              <li>Remove your profile and all account data</li>
              <li>Revoke all your API keys</li>
              <li>Delete all your passkeys</li>
              <li>Remove your access to all datasets</li>
            </ul>
          </div>

          {error && (
            <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
              {error}
            </div>
          )}

          <div class="flex gap-3 justify-end pt-2">
            <button
              type="button"
              onClick={onClose}
              class="btn-secondary"
              disabled={loading}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSendCode}
              disabled={loading}
              class="inline-flex items-center px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700 disabled:opacity-50"
            >
              {loading ? (
                <>
                  <span class="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-2" />
                  Sending...
                </>
              ) : (
                'Send Verification Code'
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
