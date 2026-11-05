import { useEffect } from 'preact/hooks';
import { currentUser, initAuth, isLoading as authLoading } from '../../lib/auth';
import UserMenu from './UserMenu';
import { PLATFORM_NAME } from '../../lib/site';

interface UnifiedNavProps {
  variant?: 'default' | 'transparent';
}

export default function UnifiedNav({ variant = 'default' }: UnifiedNavProps) {
  // Use signal values directly for reactivity
  const user = currentUser.value;
  const isLoading = authLoading.value;
  const isAuthenticated = !!user;

  useEffect(() => {
    initAuth().catch(() => {
      authLoading.value = false;
    });
  }, []);

  const bgClass = variant === 'transparent'
    ? 'bg-white/80 backdrop-blur-md'
    : 'bg-white';

  return (
    <header class={`${bgClass} border-b border-gray-200 sticky top-0 z-50`}>
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between items-center h-16">
          {/* Logo */}
          <div class="flex items-center space-x-2">
            <a href="/" class="flex items-center space-x-2">
              <span class="text-base font-bold leading-tight text-gray-900">{PLATFORM_NAME}</span>
            </a>
          </div>

          {/* Navigation */}
          <nav class="flex items-center gap-4 sm:gap-6">
            <a href="/datasets" class="text-gray-600 hover:text-gray-900 text-sm font-medium">
              Datasets
            </a>

            {/* Resources dropdown - always show */}
            <div class="relative hidden sm:block group">
              <button class="text-gray-600 hover:text-gray-900 text-sm font-medium flex items-center gap-1">
                Resources
                <svg class="w-4 h-4 transition-transform group-hover:rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              <div class="absolute right-0 mt-2 w-48 bg-white rounded-lg shadow-lg border border-gray-200 py-1 opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-150 z-50">
                <a href="https://dataio.artpark.ai/docs" target="_blank" rel="noopener noreferrer" class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                  <svg class="w-4 h-4 mr-2.5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                  </svg>
                  dataio docs
                </a>
                <a href="https://dataio.artpark.ai/api/v1/" target="_blank" rel="noopener noreferrer" class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                  <svg class="w-4 h-4 mr-2.5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
                  </svg>
                  API Docs
                </a>
                <hr class="my-1 border-gray-100" />
                <a href="https://github.com/dsih-artpark/dataio" target="_blank" rel="noopener noreferrer" class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                  <svg class="w-4 h-4 mr-2.5 text-gray-500" fill="currentColor" viewBox="0 0 24 24">
                    <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" />
                  </svg>
                  GitHub
                </a>
                <a href="https://pypi.org/project/dataio-artpark/" target="_blank" rel="noopener noreferrer" class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                  <img src="/pypi_logo.svg.png" alt="PyPI" class="w-4 h-4 mr-2.5" />
                  PyPI
                </a>
              </div>
            </div>

            {/* Auth section */}
            {isLoading ? (
              <div class="w-8 h-8 rounded-full bg-gray-100 animate-pulse" />
            ) : isAuthenticated && user ? (
              <UserMenu user={user} />
            ) : (
              <div class="flex items-center gap-2">
                <a href="/login" class="btn-primary text-sm px-4 py-2">
                  Sign In
                </a>
              </div>
            )}
          </nav>
        </div>
      </div>
    </header>
  );
}
