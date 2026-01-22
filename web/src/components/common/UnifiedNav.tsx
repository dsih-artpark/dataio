import { useState, useEffect } from 'preact/hooks';
import { currentUser, logout, initAuth } from '../../lib/auth';
import { api } from '../../lib/api';

interface UnifiedNavProps {
  variant?: 'default' | 'transparent';
  showResources?: boolean;
}

export default function UnifiedNav({ variant = 'default', showResources = false }: UnifiedNavProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const user = currentUser.value;

  useEffect(() => {
    const checkAuth = async () => {
      if (api.isAuthenticated()) {
        try {
          await initAuth();
          setIsAuthenticated(true);
        } catch {
          setIsAuthenticated(false);
        }
      } else {
        setIsAuthenticated(false);
      }
      setIsLoading(false);
    };
    checkAuth();
  }, []);

  const handleLogout = async () => {
    await logout();
    window.location.replace('/');
  };

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
              <div class="w-8 h-8 bg-primary-600 rounded-lg flex items-center justify-center">
                <span class="text-white font-bold text-sm">AP</span>
              </div>
              <span class="text-lg font-bold text-gray-900 hidden sm:block">ARTPARK Data Platform</span>
              <span class="text-lg font-bold text-gray-900 sm:hidden">DataIO</span>
            </a>
          </div>

          {/* Navigation */}
          <nav class="flex items-center gap-4 sm:gap-6">
            <a href="/datasets" class="text-gray-600 hover:text-gray-900 text-sm font-medium">
              Datasets
            </a>

            {/* Resources dropdown - optional */}
            {showResources && (
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
                    Documentation
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
                </div>
              </div>
            )}

            {/* Auth section */}
            {isLoading ? (
              <div class="w-8 h-8 rounded-full bg-gray-100 animate-pulse" />
            ) : isAuthenticated && user ? (
              <div class="relative">
                <button
                  onClick={() => setIsOpen(!isOpen)}
                  class="flex items-center space-x-2 text-gray-700 hover:text-gray-900 focus:outline-none"
                >
                  <div class="w-8 h-8 bg-primary-100 rounded-full flex items-center justify-center">
                    <span class="text-primary-700 font-medium text-sm">
                      {user.email?.[0]?.toUpperCase() || 'U'}
                    </span>
                  </div>
                  <span class="hidden sm:block text-sm font-medium max-w-[120px] truncate">
                    {user.display_name || user.email?.split('@')[0] || 'User'}
                  </span>
                  <svg
                    class={`w-4 h-4 transition-transform ${isOpen ? 'rotate-180' : ''}`}
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                  </svg>
                </button>

                {isOpen && (
                  <>
                    {/* Backdrop */}
                    <div
                      class="fixed inset-0 z-40"
                      onClick={() => setIsOpen(false)}
                    />
                    {/* Dropdown */}
                    <div class="absolute right-0 mt-2 w-56 bg-white rounded-xl shadow-lg border border-gray-200 py-2 z-50">
                      <div class="px-4 py-2 border-b border-gray-100">
                        <p class="text-sm font-medium text-gray-900 truncate">
                          {user.display_name || 'User'}
                        </p>
                        <p class="text-xs text-gray-500 truncate">{user.email}</p>
                      </div>
                      <div class="py-1">
                        <a
                          href="/account"
                          class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50"
                        >
                          <svg class="w-4 h-4 mr-3 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                          </svg>
                          Account Settings
                        </a>
                        {user.is_admin && (
                          <a
                            href="/admin"
                            class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50"
                          >
                            <svg class="w-4 h-4 mr-3 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                            </svg>
                            Admin
                          </a>
                        )}
                      </div>
                      <div class="border-t border-gray-100 pt-1">
                        <button
                          onClick={handleLogout}
                          class="flex items-center w-full px-4 py-2 text-sm text-red-600 hover:bg-red-50"
                        >
                          <svg class="w-4 h-4 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
                          </svg>
                          Sign out
                        </button>
                      </div>
                    </div>
                  </>
                )}
              </div>
            ) : (
              <div class="flex items-center gap-2">
                <a href="/login" class="text-gray-600 hover:text-gray-900 text-sm font-medium px-3 py-2">
                  Sign In
                </a>
                <a href="/register" class="btn-primary text-sm px-4 py-2">
                  Register
                </a>
              </div>
            )}
          </nav>
        </div>
      </div>
    </header>
  );
}
