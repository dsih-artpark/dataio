/**
 * WebAuthn/Passkey helpers using @simplewebauthn/browser.
 */

import {
  startRegistration,
  startAuthentication,
  browserSupportsWebAuthn,
} from '@simplewebauthn/browser';
import { api } from './api';

/**
 * Check if the browser supports WebAuthn/Passkeys.
 */
export function isWebAuthnSupported(): boolean {
  return browserSupportsWebAuthn();
}

/**
 * Register a new passkey for the current user.
 */
export async function registerPasskey(deviceName?: string): Promise<{
  success: boolean;
  passkey: object;
}> {
  // Get registration options from server
  const { options } = await api.getPasskeyRegistrationOptions();
  const parsedOptions = JSON.parse(options);

  // Start the browser's registration ceremony
  const credential = await startRegistration(parsedOptions);

  // Verify with server
  return api.verifyPasskeyRegistration(credential, deviceName);
}

/**
 * Authenticate with a passkey.
 */
export async function authenticateWithPasskey(email: string): Promise<{
  access_token: string;
  refresh_token: string;
  user: object;
}> {
  // Get authentication options from server
  const { options } = await api.getPasskeyLoginOptions(email);
  const parsedOptions = JSON.parse(options);

  // Start the browser's authentication ceremony
  const credential = await startAuthentication(parsedOptions);

  // Verify with server
  return api.verifyPasskeyLogin(email, credential);
}
