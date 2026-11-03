// Open the floating data assistant (AssistantLauncher) from anywhere on the
// public site, optionally with a question typed in.
export const OPEN_ASSISTANT_EVENT = 'dataio:open-assistant';

export function openAssistant(message?: string): void {
  window.dispatchEvent(new CustomEvent(OPEN_ASSISTANT_EVENT, { detail: { message } }));
}
