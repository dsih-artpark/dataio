import type { JSX } from 'preact';
import { currentUser } from '../../lib/auth';

interface NavItem {
  name: string;
  href: string;
  icon: string;
  adminOnly?: boolean;
}

const navItems: NavItem[] = [
  { name: 'Datasets', href: '/datasets', icon: 'database' },
  { name: 'Account', href: '/account', icon: 'user' },
  { name: 'Users', href: '/admin/users', icon: 'users', adminOnly: true },
  { name: 'Groups', href: '/admin/groups', icon: 'users-cog', adminOnly: true },
];

const icons: Record<string, JSX.Element> = {
  database: (
    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="2"
        d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"
      />
    </svg>
  ),
  user: (
    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="2"
        d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"
      />
    </svg>
  ),
  users: (
    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="2"
        d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"
      />
    </svg>
  ),
  'users-cog': (
    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="2"
        d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"
      />
    </svg>
  ),
};

export default function Sidebar() {
  const user = currentUser.value;
  const isAdmin = user?.is_admin ?? false;
  const currentPath = typeof window !== 'undefined' ? window.location.pathname : '';

  const filteredItems = navItems.filter((item) => !item.adminOnly || isAdmin);

  return (
    <aside class="hidden lg:flex lg:flex-shrink-0">
      <div class="flex flex-col w-64 fixed inset-y-0 pt-16">
        <nav class="flex-1 flex flex-col bg-white border-r border-gray-200 pt-5 pb-4 overflow-y-auto">
          <div class="flex-1 px-3 space-y-1">
            {filteredItems.map((item) => {
              const isActive = currentPath === item.href || currentPath.startsWith(item.href + '/');
              return (
                <a
                  key={item.name}
                  href={item.href}
                  class={`group flex items-center px-3 py-2 text-sm font-medium rounded-lg transition-colors ${
                    isActive
                      ? 'bg-primary-50 text-primary-700'
                      : 'text-gray-700 hover:bg-gray-50 hover:text-gray-900'
                  }`}
                >
                  <span class={`mr-3 ${isActive ? 'text-primary-600' : 'text-gray-400 group-hover:text-gray-500'}`}>
                    {icons[item.icon]}
                  </span>
                  {item.name}
                </a>
              );
            })}
          </div>

          {/* Admin section divider */}
          {isAdmin && (
            <div class="px-3 mt-6">
              <h3 class="px-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Administration
              </h3>
            </div>
          )}
        </nav>
      </div>
    </aside>
  );
}
