import { useEffect, useRef, useState } from 'preact/hooks';
import DataChat from '../chat/DataChat';
import { OPEN_ASSISTANT_EVENT } from '../../lib/assistant';

const SUGGESTIONS = [
  'Dengue cases by district in Karnataka, 2020–2024',
  'Which states have the most cattle?',
  'Rainfall and dengue data for the same districts',
];

/**
 * "Ask the assistant" button, bottom right of every public page, opening a
 * chat panel. Signed-out visitors use the public assistant (Open datasets,
 * limited per day); signed-in users get answers for their own access.
 */
export default function AssistantLauncher() {
  const [open, setOpen] = useState(false);
  // Remounts the chat with a question passed from elsewhere on the page.
  const [seed, setSeed] = useState<{ key: number; message?: string }>({ key: 0 });
  const buttonRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const onOpen = (event: Event) => {
      const message = (event as CustomEvent<{ message?: string }>).detail?.message;
      if (message) setSeed((s) => ({ key: s.key + 1, message }));
      setOpen(true);
    };
    window.addEventListener(OPEN_ASSISTANT_EVENT, onOpen);
    return () => window.removeEventListener(OPEN_ASSISTANT_EVENT, onOpen);
  }, []);

  useEffect(() => {
    if (!open) return;
    panelRef.current?.querySelector<HTMLInputElement>('input[type="text"]')?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [open]);

  const close = () => {
    setOpen(false);
    buttonRef.current?.focus();
  };

  return (
    <>
      <div
        ref={panelRef}
        role="dialog"
        aria-label="Data assistant"
        hidden={!open}
        class="fixed z-30 [&[hidden]]:hidden left-4 right-4 bottom-[84px] sm:left-auto sm:right-6 sm:bottom-[92px] sm:w-[380px] h-[min(560px,70vh)] flex flex-col bg-white border border-gray-200 rounded-2xl shadow-[0_12px_32px_rgba(17,24,39,0.18)] overflow-hidden"
      >
        <div class="flex items-center justify-between gap-3 px-4 py-3.5 bg-primary-600 text-white">
          <div class="flex flex-col gap-0.5">
            <strong class="text-base">Data assistant</strong>
            <span class="text-[13px] text-primary-100">Ask in plain words. Answers cite their datasets.</span>
          </div>
          <button
            type="button"
            aria-label="Close assistant"
            onClick={close}
            class="w-11 h-11 shrink-0 rounded-lg flex items-center justify-center hover:bg-primary-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-white"
          >
            <svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
              <path d="M6 6l12 12M18 6L6 18" />
            </svg>
          </button>
        </div>
        <div class="flex-1 min-h-0 [&>div]:h-full [&>div]:rounded-none [&>div]:border-0 [&>div]:shadow-none">
          {open && <DataChat key={seed.key} initialMessage={seed.message} suggestions={SUGGESTIONS} hideHeader autoFocus={false} />}
        </div>
      </div>
      <button
        ref={buttonRef}
        type="button"
        aria-label="Ask the data assistant"
        aria-expanded={open}
        onClick={() => (open ? close() : setOpen(true))}
        class="fixed z-[31] right-4 bottom-4 sm:right-6 sm:bottom-6 flex items-center justify-center gap-2 min-h-[52px] min-w-[52px] px-[18px] rounded-full bg-primary-600 text-white text-[15px] font-semibold shadow-[0_6px_18px_rgba(74,77,230,0.35)] hover:bg-primary-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
      >
        <svg aria-hidden="true" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.6A8 8 0 1 1 21 12z" />
          <path d="M8.5 11h.01M12 11h.01M15.5 11h.01" />
        </svg>
        <span class="hidden sm:inline">{open ? 'Close assistant' : 'Ask the assistant'}</span>
      </button>
    </>
  );
}
