import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';

interface User {
  email: string;
  display_name: string | null;
  is_admin: boolean;
  is_group: boolean;
  email_verified: boolean;
  last_login: string | null;
  created_at: string | null;
}

export default function UserList() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 20;

  // Invite modal state
  const [showInvite, setShowInvite] = useState(false);
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteDisplayName, setInviteDisplayName] = useState('');
  const [inviteIsAdmin, setInviteIsAdmin] = useState(false);
  const [inviting, setInviting] = useState(false);

  const fetchUsers = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await api.adminListUsers({
        search: search || undefined,
        limit,
        offset,
      });
      setUsers(response.users as User[]);
      setTotal(response.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load users');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, [offset]);

  const handleSearch = (e: Event) => {
    e.preventDefault();
    setOffset(0);
    fetchUsers();
  };

  const handleInvite = async (e: Event) => {
    e.preventDefault();
    if (!inviteEmail.trim()) return;

    setInviting(true);
    setError('');

    try {
      await api.adminInviteUser({
        email: inviteEmail.trim(),
        display_name: inviteDisplayName.trim() || undefined,
        is_admin: inviteIsAdmin,
      });
      setShowInvite(false);
      setInviteEmail('');
      setInviteDisplayName('');
      setInviteIsAdmin(false);
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to invite user');
    } finally {
      setInviting(false);
    }
  };

  const handleToggleAdmin = async (email: string, currentIsAdmin: boolean) => {
    if (!confirm(`Are you sure you want to ${currentIsAdmin ? 'remove' : 'grant'} admin privileges for ${email}?`)) {
      return;
    }

    try {
      await api.adminUpdateUser(email, { is_admin: !currentIsAdmin });
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update user');
    }
  };

  const formatDate = (date: string | null) => {
    if (!date) return 'Never';
    return new Date(date).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  if (loading && users.length === 0) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
          <p class="mt-4 text-gray-500">Loading users...</p>
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-4">
      {error && (
        <div class="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
          {error}
        </div>
      )}

      {/* Search and Actions */}
      <div class="flex gap-3 items-center">
        <form onSubmit={handleSearch} class="flex gap-3 flex-1">
          <input
            type="text"
            value={search}
            onInput={(e) => setSearch((e.target as HTMLInputElement).value)}
            placeholder="Search by email..."
            class="input flex-1"
          />
          <button type="submit" class="btn-secondary">
            Search
          </button>
        </form>
        <button
          onClick={() => setShowInvite(true)}
          class="btn-primary"
        >
          Invite User
        </button>
      </div>

      {/* Invite Modal */}
      {showInvite && (
        <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div class="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
            <div class="px-6 py-4 border-b border-gray-200">
              <h3 class="text-lg font-semibold text-gray-900">Invite User</h3>
            </div>
            <form onSubmit={handleInvite} class="p-6 space-y-4">
              <div>
                <label class="label">Email Address</label>
                <input
                  type="email"
                  value={inviteEmail}
                  onInput={(e) => setInviteEmail((e.target as HTMLInputElement).value)}
                  class="input"
                  required
                  placeholder="user@example.com"
                />
              </div>
              <div>
                <label class="label">Display Name (optional)</label>
                <input
                  type="text"
                  value={inviteDisplayName}
                  onInput={(e) => setInviteDisplayName((e.target as HTMLInputElement).value)}
                  class="input"
                  placeholder="John Doe"
                />
              </div>
              <div class="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="inviteIsAdmin"
                  checked={inviteIsAdmin}
                  onChange={(e) => setInviteIsAdmin((e.target as HTMLInputElement).checked)}
                  class="w-4 h-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                />
                <label for="inviteIsAdmin" class="text-sm text-gray-700">
                  Grant admin privileges
                </label>
              </div>
              <div class="flex gap-3 justify-end">
                <button
                  type="button"
                  onClick={() => setShowInvite(false)}
                  class="btn-secondary"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={inviting}
                  class="btn-primary"
                >
                  {inviting ? 'Inviting...' : 'Send Invitation'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Users Table */}
      <div class="card overflow-hidden">
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  User
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Last Login
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Created
                </th>
                <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              {users.length === 0 ? (
                <tr>
                  <td colSpan={5} class="px-6 py-12 text-center text-gray-500">
                    No users found
                  </td>
                </tr>
              ) : (
                users.filter(u => !u.is_group).map((user) => (
                  <tr key={user.email} class="hover:bg-gray-50">
                    <td class="px-6 py-4">
                      <div>
                        <div class="font-medium text-gray-900">
                          {user.display_name || user.email}
                        </div>
                        {user.display_name && (
                          <div class="text-sm text-gray-500">{user.email}</div>
                        )}
                      </div>
                    </td>
                    <td class="px-6 py-4">
                      <div class="flex gap-2">
                        {user.is_admin && (
                          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                            Admin
                          </span>
                        )}
                        {user.email_verified ? (
                          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            Verified
                          </span>
                        ) : (
                          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                            Pending
                          </span>
                        )}
                      </div>
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {formatDate(user.last_login)}
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {formatDate(user.created_at)}
                    </td>
                    <td class="px-6 py-4 text-right">
                      <button
                        onClick={() => handleToggleAdmin(user.email, user.is_admin)}
                        class={`text-sm ${user.is_admin ? 'text-red-600 hover:text-red-700' : 'text-primary-600 hover:text-primary-700'}`}
                      >
                        {user.is_admin ? 'Remove Admin' : 'Make Admin'}
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {total > limit && (
          <div class="px-6 py-3 flex items-center justify-between border-t border-gray-200">
            <div class="text-sm text-gray-500">
              Showing {offset + 1} to {Math.min(offset + limit, total)} of {total} users
            </div>
            <div class="flex gap-2">
              <button
                onClick={() => setOffset(Math.max(0, offset - limit))}
                disabled={offset === 0}
                class="btn-secondary text-sm disabled:opacity-50"
              >
                Previous
              </button>
              <button
                onClick={() => setOffset(offset + limit)}
                disabled={offset + limit >= total}
                class="btn-secondary text-sm disabled:opacity-50"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
