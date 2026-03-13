import { useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { currentUser } from '../../lib/auth';
import OTPInput from './OTPInput';

type RegisterStep = 'email' | 'otp' | 'success' | 'pending';

export default function RegisterForm() {
  const [step, setStep] = useState<RegisterStep>('email');
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [verificationStatus, setVerificationStatus] = useState<string>('');
  const [pendingMessage, setPendingMessage] = useState<string>('');

  const handleEmailSubmit = async (e: Event) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const response = await api.initiateRegistration(email);
      setVerificationStatus(response.verification_status);
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

    try {
      const response = await api.verifyRegistration(email, code);
      if (response.access_token) {
        currentUser.value = response.user;
      } else {
        currentUser.value = null;
      }

      if (response.verification_status === 'pending') {
        setPendingMessage(response.verification_message || 'Your account is pending admin approval.');
        setStep('pending');
      } else {
        setStep('success');
        // Redirect after brief delay to show success message
        setTimeout(() => {
          window.location.href = '/datasets';
        }, 2000);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Invalid code');
      setLoading(false);
    }
  };

  if (step === 'success') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-8 h-8 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <h2 class="text-2xl font-bold text-gray-900">Registration Complete!</h2>
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

  if (step === 'pending') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <div class="w-16 h-16 bg-yellow-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-8 h-8 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h2 class="text-2xl font-bold text-gray-900">Registration Pending</h2>
          <p class="mt-2 text-gray-600">
            {pendingMessage}
          </p>
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

  if (step === 'otp') {
    return (
      <div class="space-y-6">
        <div class="text-center">
          <h2 class="text-2xl font-bold text-gray-900">Verify your email</h2>
          <p class="mt-2 text-gray-600">
            We sent a 6-digit code to <span class="font-medium">{email}</span>
          </p>
        </div>

        {verificationStatus === 'pending' && (
          <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-3">
            <p class="text-sm text-yellow-800">
              <strong>Note:</strong> Personal email addresses require admin approval after registration.
            </p>
          </div>
        )}

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

        <div class="text-center space-y-2">
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
        <h2 class="text-2xl font-bold text-gray-900">Create your account</h2>
        <p class="mt-2 text-gray-600">
          Enter your email to get started
        </p>
      </div>

      <div class="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <p class="text-sm text-blue-800">
          <strong>Beta Access:</strong> During beta, instant verification is available for institutional emails
          (e.g., <code class="bg-blue-100 px-1 rounded">.edu</code>, <code class="bg-blue-100 px-1 rounded">.ac.in</code>).
          Personal emails require admin approval.
        </p>
      </div>

      {error && (
        <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
          {error}
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

        <button
          type="submit"
          disabled={loading || !email}
          class="btn-primary w-full"
        >
          {loading ? 'Sending...' : 'Continue'}
        </button>
      </form>

      <div class="text-center pt-4 border-t border-gray-200">
        <p class="text-sm text-gray-600">
          Already have an account?{' '}
          <a href="/login" class="text-primary-600 hover:text-primary-700 font-medium">
            Sign in
          </a>
        </p>
      </div>
    </div>
  );
}
