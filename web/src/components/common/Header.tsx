import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { currentUser, logout } from '../../lib/auth';

export default function Header() {
  const [isOpen, setIsOpen] = useState(false);
  const user = currentUser.value;

  const handleLogout = async () => {
    await logout();
    window.location.replace('/login');
  };

  return (
    <header class="bg-white border-b border-gray-200 sticky top-0 z-30">
      <div class="px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16">
          {/* Logo */}
          <div class="flex items-center">
            <a href="/dashboard" class="flex items-center space-x-2">
              <div class="w-8 h-8 bg-primary-600 rounded-lg flex items-center justify-center">
                <span class="text-white font-bold">D</span>
              </div>
              <span class="text-xl font-bold text-gray-900 hidden sm:block">DataIO</span>
            </a>
          </div>

          {/* User menu */}
          <div class="flex items-center">
            <div class="relative">
              <button
                onClick={() => setIsOpen(!isOpen)}
                class="flex items-center space-x-3 text-gray-700 hover:text-gray-900 focus:outline-none"
              >
                <div class="w-8 h-8 bg-primary-100 rounded-full flex items-center justify-center">
                  <span class="text-primary-700 font-medium text-sm">
                    {user?.email?.[0]?.toUpperCase() || 'U'}
                  </span>
                </div>
                <span class="hidden sm:block text-sm font-medium">
                  {user?.display_name || user?.email || 'User'}
                </span>
                <svg
                  class={`w-4 h-4 transition-transform ${isOpen ? 'rotate-180' : ''}`}
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M19 9l-7 7-7-7"
                  />
                </svg>
              </button>

              {isOpen && (
                <div class="absolute right-0 mt-2 w-48 bg-white rounded-lg shadow-lg border border-gray-200 py-1">
                  <a
                    href="/account"
                    class="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-50"
                  >
                    Account Settings
                  </a>
                  <a
                    href="/account/api-keys"
                    class="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-50"
                  >
                    API Keys
                  </a>
                  <hr class="my-1 border-gray-200" />
                  <button
                    onClick={handleLogout}
                    class="block w-full text-left px-4 py-2 text-sm text-red-600 hover:bg-red-50"
                  >
                    Sign out
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
