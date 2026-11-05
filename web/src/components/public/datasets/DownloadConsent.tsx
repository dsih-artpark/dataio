// Download for the public dataset card and page, with the signed-out pop-up:
// a visitor who isn't signed in gives their email and accepts the Terms of Use
// and the dataset's licence once; the API then hands out a visitor ID that is
// kept in the dataio_visitor cookie so later downloads go straight through.
// Signed-in users never see the pop-up (their account email is recorded).
import { useEffect, useRef, useState } from 'preact/hooks';
import type { VNode } from 'preact';
import { createPortal } from 'preact/compat';
import { currentUser, initAuth } from '../../../lib/auth';
import { ConsentRequiredError, downloadDatasetZip } from '../../../lib/download';

const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

interface DownloadTarget {
  ds_id: string;
  title: string;
  licence: string | null;
  readme_md?: string | null;
}

/**
 * `start(open)`: open = the dataset is Open download (anyone may download);
 * false = a signed-in user's own download grant. Render `dialog` somewhere in
 * the component; it is null unless the pop-up is showing.
 */
export function useDatasetDownload(dataset: DownloadTarget | null) {
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [asking, setAsking] = useState(false);
  const returnFocus = useRef<HTMLElement | null>(null);

  const run = async (mode: 'open' | 'signed-in', email?: string) => {
    if (!dataset) return false;
    setDownloading(true);
    setError(null);
    try {
      await downloadDatasetZip(dataset.ds_id, dataset.title, mode, dataset.readme_md, email);
      return true;
    } catch (err) {
      if (err instanceof ConsentRequiredError) {
        returnFocus.current = document.activeElement as HTMLElement | null;
        setAsking(true);
        return false;
      }
      setError(err instanceof Error ? err.message : 'Download failed');
      return false;
    } finally {
      setDownloading(false);
    }
  };

  const start = async (open: boolean) => {
    if (!open) return void run('signed-in');
    await initAuth().catch(() => undefined);
    return void run(currentUser.value ? 'signed-in' : 'open');
  };

  const close = () => {
    setAsking(false);
    returnFocus.current?.focus();
  };

  const dialog = asking && dataset ? (
    <ConsentDialog
      dataset={dataset}
      busy={downloading}
      error={error}
      onCancel={close}
      onSubmit={async (email) => {
        if (await run('open', email)) close();
      }}
    />
  ) : null;

  // While the pop-up is open its own message area shows errors.
  return { downloading, error: asking ? null : error, start, dialog };
}

interface ConsentDialogProps {
  dataset: DownloadTarget;
  busy: boolean;
  error: string | null;
  onCancel: () => void;
  onSubmit: (email: string) => void;
}

function ConsentDialog({ dataset, busy, error, onCancel, onSubmit }: ConsentDialogProps): VNode {
  const [email, setEmail] = useState('');
  const [agreed, setAgreed] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const panel = useRef<HTMLDivElement>(null);
  const titleId = `download-consent-${dataset.ds_id}`;

  useEffect(() => {
    panel.current?.querySelector<HTMLInputElement>('input[type="email"]')?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onCancel();
      if (event.key !== 'Tab' || !panel.current) return;
      // Keep keyboard focus inside the pop-up.
      const items = panel.current.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), input');
      const first = items[0];
      const last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, []);

  const submit = (event: Event) => {
    event.preventDefault();
    if (!EMAIL.test(email.trim())) return setProblem('Enter a valid email address, like name@organisation.org');
    if (!agreed) return setProblem('Please accept the Terms of Use and the dataset’s licence to download.');
    setProblem(null);
    onSubmit(email.trim());
  };

  const message = problem ?? error;
  const link = 'font-semibold text-primary-700 hover:text-primary-900 underline';

  // In <body>, so no card or island around the button can stack it under the
  // header or the assistant button.
  return createPortal(
    <div class="fixed inset-0 z-40 flex items-center justify-center p-4 bg-gray-900/50" onClick={(e) => e.target === e.currentTarget && onCancel()}>
      <div
        ref={panel}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        class="w-full max-w-[480px] max-h-[90vh] overflow-y-auto bg-white rounded-2xl shadow-xl p-6 sm:p-7"
      >
        <form onSubmit={submit} noValidate class="flex flex-col gap-4">
          <div class="flex flex-col gap-1.5">
            <h2 id={titleId} class="m-0 text-xl font-bold text-gray-900">
              Before you download
            </h2>
            <p class="m-0 text-[15px] leading-relaxed text-gray-600">
              <span class="font-semibold text-gray-800">{dataset.title}</span> is free to download. Tell us your email so we
              can let you know about corrections or updates to this data. No account needed.
            </p>
          </div>

          <label class="flex flex-col gap-1.5 text-sm font-semibold text-gray-700">
            Email *
            <input
              type="email"
              autoComplete="email"
              required
              maxLength={254}
              value={email}
              onInput={(e) => setEmail((e.target as HTMLInputElement).value)}
              class="w-full min-h-[44px] px-2.5 py-2 rounded-lg border border-gray-300 text-[15px] font-normal text-gray-900 bg-white focus:outline-none focus:ring-2 focus:ring-primary-500"
            />
          </label>

          <label class="flex items-start gap-2.5 text-sm leading-normal text-gray-700 cursor-pointer">
            <input
              type="checkbox"
              required
              checked={agreed}
              onChange={(e) => setAgreed((e.target as HTMLInputElement).checked)}
              class="w-[18px] h-[18px] mt-0.5 shrink-0 accent-primary-600"
            />
            <span>
              I agree to the{' '}
              <a href="/terms" target="_blank" rel="noopener" class={link}>
                Terms of Use
              </a>{' '}
              and to this dataset’s licence{dataset.licence ? ` (${dataset.licence})` : ''}, and I have read the{' '}
              <a href="/privacy" target="_blank" rel="noopener" class={link}>
                Privacy Policy
              </a>
              . *
            </span>
          </label>

          <p class="m-0 text-[13px] leading-normal text-gray-600">
            We’ll remember this on this device with a cookie for 12 months, so you won’t be asked again.
          </p>

          {message && (
            <p role="alert" class="m-0 text-sm text-red-700">
              {message}
            </p>
          )}

          <div class="flex flex-wrap justify-end gap-3">
            <button
              type="button"
              onClick={onCancel}
              class="min-h-[44px] px-4 rounded-[10px] border border-gray-300 bg-white text-gray-700 text-[15px] font-semibold hover:border-gray-400 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={busy}
              class="min-h-[44px] px-5 rounded-[10px] bg-primary-600 text-white text-[15px] font-semibold hover:bg-primary-700 disabled:opacity-60 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
            >
              {busy ? 'Preparing download…' : 'Download'}
            </button>
          </div>
        </form>
      </div>
    </div>,
    document.body,
  );
}
