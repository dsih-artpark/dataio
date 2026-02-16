import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';

interface PendingUser {
  email: string;
  display_name: string | null;
  registered_at: string | null;
  verification_status: string;
}

export default function PendingUsers() {
  const [users, setUsers] = useState<PendingUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 20;

  const fetchPendingUsers = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await api.adminListPendingUsers({ limit, offset });
      setUsers(response.users);
      setTotal(response.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load pending users');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPendingUsers();
  }, [offset]);

  const handleVerify = async (email: string) => {
    if (!confirm(`Verify and approve ${email}? They will gain full platform access.`)) {
      return;
    }

    setActionLoading(email);
    setError('');

    try {
      await api.adminVerifyUser(email);
      // Remove from list
      setUsers(users.filter(u => u.email !== email));
      setTotal(total - 1);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to verify user');
    } finally {
      setActionLoading(null);
    }
  };

  const handleReject = async (email: string) => {
    if (!confirm(`Reject ${email}? They will not be able to access the platform.`)) {
      return;
    }

    setActionLoading(email);
    setError('');

    try {
      await api.adminRejectUser(email);
      // Remove from list
      setUsers(users.filter(u => u.email !== email));
      setTotal(total - 1);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to reject user');
    } finally {
      setActionLoading(null);
    }
  };

  const formatDate = (date: string | null) => {
    if (!date) return 'Unknown';
    return new Date(date).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  if (loading && users.length === 0) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
          <p class="mt-4 text-gray-500">Loading pending users...</p>
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

      {users.length === 0 ? (
        <div class="card">
          <div class="card-body text-center py-12">
            <div class="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <svg class="w-8 h-8 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <h3 class="text-lg font-medium text-gray-900">No pending registrations</h3>
            <p class="mt-1 text-gray-500">All user registrations have been processed.</p>
          </div>
        </div>
      ) : (
        <div class="card overflow-hidden">
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    User
                  </th>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Registered
                  </th>
                  <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                {users.map((user) => (
                  <tr key={user.email} class="hover:bg-gray-50">
                    <td class="px-6 py-4">
                      <div>
                        <div class="font-medium text-gray-900">
                          {user.display_name || user.email}
                        </div>
                        {user.display_name && (
                          <div class="text-sm text-gray-500">{user.email}</div>
                        )}
                        {!user.display_name && (
                          <div class="text-sm text-yellow-600">
                            Personal email - requires approval
                          </div>
                        )}
                      </div>
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {formatDate(user.registered_at)}
                    </td>
                    <td class="px-6 py-4 text-right">
                      <div class="flex items-center justify-end gap-2">
                        <button
                          onClick={() => handleVerify(user.email)}
                          disabled={actionLoading === user.email}
                          class="inline-flex items-center px-3 py-1.5 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700 disabled:opacity-50"
                        >
                          {actionLoading === user.email ? (
                            <span class="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-1.5" />
                          ) : (
                            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                            </svg>
                          )}
                          Approve
                        </button>
                        <button
                          onClick={() => handleReject(user.email)}
                          disabled={actionLoading === user.email}
                          class="inline-flex items-center px-3 py-1.5 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700 disabled:opacity-50"
                        >
                          <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                          </svg>
                          Reject
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {total > limit && (
            <div class="px-6 py-3 flex items-center justify-between border-t border-gray-200">
              <div class="text-sm text-gray-500">
                Showing {offset + 1} to {Math.min(offset + limit, total)} of {total} pending users
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
      )}
    </div>
  );
}
