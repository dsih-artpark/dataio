import { useEffect, useState } from 'preact/hooks';

/**
 * "Back to top" pill, bottom centre. Hidden until the reader reaches the end
 * of a long page, so it stays clear of the assistant button (bottom right).
 */
export default function BackToTop() {
  const [atEnd, setAtEnd] = useState(false);

  useEffect(() => {
    const update = () => {
      const el = document.scrollingElement || document.documentElement;
      const longPage = el.scrollHeight > el.clientHeight * 1.5;
      setAtEnd(longPage && el.scrollTop + el.clientHeight >= el.scrollHeight - 120);
    };
    update();
    window.addEventListener('scroll', update, { passive: true });
    window.addEventListener('resize', update);
    return () => {
      window.removeEventListener('scroll', update);
      window.removeEventListener('resize', update);
    };
  }, []);

  return (
    <a
      href="#top"
      aria-hidden={!atEnd}
      tabIndex={atEnd ? 0 : -1}
      onClick={(event) => {
        event.preventDefault();
        const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        window.scrollTo({ top: 0, behavior: reduceMotion ? 'auto' : 'smooth' });
        document.getElementById('top')?.focus({ preventScroll: true });
      }}
      class={`fixed inset-x-0 bottom-6 z-20 mx-auto w-fit flex items-center gap-2 min-h-[44px] px-[18px] rounded-full bg-gray-900 text-white text-sm font-semibold shadow-lg transition duration-300 motion-reduce:transition-none focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2 ${
        atEnd ? 'translate-y-0 opacity-100 visible' : 'translate-y-6 opacity-0 invisible'
      }`}
    >
      <svg
        width="16"
        height="16"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2"
        stroke-linecap="round"
        stroke-linejoin="round"
        aria-hidden="true"
      >
        <path d="M12 19V5" />
        <path d="M5 12l7-7 7 7" />
      </svg>
      Back to top
    </a>
  );
}
