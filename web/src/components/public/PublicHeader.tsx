import { useEffect, useState } from 'preact/hooks';
import { currentUser, initAuth, isLoading as authLoading } from '../../lib/auth';
import UserMenu from '../common/UserMenu';
import { PLATFORM_NAME } from '../../lib/site';

export type PublicNavKey = 'datasets' | 'audiences' | 'approach' | 'developers' | 'partner';

const NAV_LINKS: { key: PublicNavKey; label: string; href: string }[] = [
  { key: 'datasets', label: 'Datasets', href: '/datasets' },
  { key: 'audiences', label: 'Who it’s for', href: '/#audiences' },
  { key: 'approach', label: 'Our approach', href: '/#approach' },
  { key: 'developers', label: 'Developers', href: '/#developers' },
  { key: 'partner', label: 'Partner with us', href: '/#partner' },
];

interface PublicHeaderProps {
  /** Highlights this link as the current section. */
  current?: PublicNavKey;
}

export default function PublicHeader({ current }: PublicHeaderProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const user = currentUser.value;
  // The server can't know the session, so the account slot renders a
  // placeholder until hydrated; this keeps server and first client render equal.
  const [hydrated, setHydrated] = useState(false);
  const loading = !hydrated || authLoading.value;

  useEffect(() => {
    setHydrated(true);
    initAuth().catch(() => {
      authLoading.value = false;
    });
  }, []);

  const linkClass = (key: PublicNavKey) =>
    key === current ? 'text-primary-700 font-semibold' : 'text-gray-700 hover:text-gray-900';

  const account = loading ? (
    <div class="w-8 h-8 rounded-full bg-gray-100 animate-pulse" aria-hidden="true" />
  ) : user ? (
    <UserMenu user={user} />
  ) : (
    <a
      href="/login"
      class="inline-flex items-center min-h-[44px] px-4 rounded-lg border border-gray-300 text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
    >
      Sign in
    </a>
  );

  return (
    <header class="bg-white border-b border-gray-200">
      <nav aria-label="Main" class="max-w-page mx-auto px-4 sm:px-6 py-3 flex items-center gap-4 lg:gap-8">
        <a href="/" class="flex items-center gap-2.5 text-gray-900 min-w-0">
          <span class="font-bold text-base leading-tight">{PLATFORM_NAME}</span>
        </a>

        <ul class="hidden lg:flex flex-1 flex-wrap items-center gap-x-6 gap-y-2 text-[15px]">
          {NAV_LINKS.map((link) => (
            <li key={link.key}>
              <a
                href={link.href}
                class={linkClass(link.key)}
                aria-current={link.key === current ? 'page' : undefined}
              >
                {link.label}
              </a>
            </li>
          ))}
        </ul>

        <div class="ml-auto flex items-center gap-2">
          <div class="hidden sm:block">{account}</div>
          <button
            type="button"
            class="lg:hidden inline-flex items-center justify-center w-11 h-11 rounded-lg text-gray-700 hover:bg-gray-100 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
            aria-expanded={menuOpen}
            aria-controls="public-nav-menu"
            aria-label={menuOpen ? 'Close menu' : 'Open menu'}
            onClick={() => setMenuOpen(!menuOpen)}
          >
            <svg
              class="w-6 h-6"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              {menuOpen ? (
                <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 12h16M4 18h16" />
              )}
            </svg>
          </button>
        </div>
      </nav>

      {menuOpen && (
        <div id="public-nav-menu" class="lg:hidden border-t border-gray-200">
          <ul class="max-w-page mx-auto px-4 sm:px-6 py-2 flex flex-col">
            {NAV_LINKS.map((link) => (
              <li key={link.key}>
                <a
                  href={link.href}
                  class={`flex items-center min-h-[44px] text-base ${linkClass(link.key)}`}
                  aria-current={link.key === current ? 'page' : undefined}
                  onClick={() => setMenuOpen(false)}
                >
                  {link.label}
                </a>
              </li>
            ))}
            <li class="sm:hidden py-2">{account}</li>
          </ul>
        </div>
      )}
    </header>
  );
}
