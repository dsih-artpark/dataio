import { useEffect, useState, useRef } from 'preact/hooks';
import { api } from '../../lib/api';

interface User {
  email: string;
  display_name: string | null;
  is_admin: boolean;
  is_group: boolean;
  email_verified: boolean;
  last_login: string | null;
  created_at: string | null;
  suspended_at?: string | null;
}

interface UserDetail extends User {
  suspended_by?: string | null;
  groups: string[];
  permissions: {
    resource_type: string;
    resource_id: string;
    permission: string;
  }[];
}

interface Dataset {
  ds_id: string;
  title: string;
  access_level: string | null;
}

export default function UserList() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [successMsg, setSuccessMsg] = useState('');
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

  // CSV upload state
  const [showCsvUpload, setShowCsvUpload] = useState(false);
  const [csvData, setCsvData] = useState<{ email: string; display_name?: string }[]>([]);
  const [uploadingCsv, setUploadingCsv] = useState(false);
  const [csvResults, setCsvResults] = useState<{ success: string[]; failed: { email: string; error: string }[] } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Action loading state
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  // User detail modal state
  const [selectedUser, setSelectedUser] = useState<UserDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);

  // Dataset permission state
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [datasetSearch, setDatasetSearch] = useState('');
  const [loadingDatasets, setLoadingDatasets] = useState(false);
  const [settingPermission, setSettingPermission] = useState(false);

  // Group management state
  const [allGroups, setAllGroups] = useState<{ email: string; display_name: string | null }[]>([]);
  const [loadingGroups, setLoadingGroups] = useState(false);
  const [managingGroup, setManagingGroup] = useState(false);

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
      setSuccessMsg('Invitation sent successfully');
      setTimeout(() => setSuccessMsg(''), 3000);
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to invite user');
    } finally {
      setInviting(false);
    }
  };

  const handleCsvFileSelect = (e: Event) => {
    const file = (e.target as HTMLInputElement).files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (event) => {
      const text = event.target?.result as string;
      const lines = text.split('\n').filter(line => line.trim());
      const headers = lines[0].toLowerCase().split(',').map(h => h.trim());

      const emailIdx = headers.indexOf('email');
      const nameIdx = headers.indexOf('display_name') !== -1
        ? headers.indexOf('display_name')
        : headers.indexOf('name');

      if (emailIdx === -1) {
        setError('CSV must have an "email" column');
        return;
      }

      const parsed = lines.slice(1).map(line => {
        const values = line.split(',').map(v => v.trim().replace(/^["']|["']$/g, ''));
        return {
          email: values[emailIdx] || '',
          display_name: nameIdx !== -1 ? values[nameIdx] : undefined,
        };
      }).filter(u => u.email);

      setCsvData(parsed);
      setShowCsvUpload(true);
    };
    reader.readAsText(file);
  };

  const handleCsvUpload = async () => {
    if (csvData.length === 0) return;

    setUploadingCsv(true);
    setError('');
    setCsvResults(null);

    try {
      const results = await api.adminBulkInviteUsers(csvData);
      setCsvResults(results);
      if (results.success.length > 0) {
        fetchUsers();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to bulk invite users');
    } finally {
      setUploadingCsv(false);
    }
  };

  const handleToggleAdmin = async (email: string, currentIsAdmin: boolean) => {
    if (!confirm(`Are you sure you want to ${currentIsAdmin ? 'remove' : 'grant'} admin privileges for ${email}?`)) {
      return;
    }

    setActionLoading(email);
    try {
      await api.adminUpdateUser(email, { is_admin: !currentIsAdmin });
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update user');
    } finally {
      setActionLoading(null);
    }
  };

  const handleSuspend = async (email: string) => {
    if (!confirm(`Are you sure you want to suspend ${email}? They will not be able to access the platform.`)) {
      return;
    }

    setActionLoading(email);
    try {
      await api.adminSuspendUser(email);
      setSuccessMsg(`User ${email} has been suspended`);
      setTimeout(() => setSuccessMsg(''), 3000);
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to suspend user');
    } finally {
      setActionLoading(null);
    }
  };

  const handleUnsuspend = async (email: string) => {
    setActionLoading(email);
    try {
      await api.adminUnsuspendUser(email);
      setSuccessMsg(`User ${email} has been unsuspended`);
      setTimeout(() => setSuccessMsg(''), 3000);
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to unsuspend user');
    } finally {
      setActionLoading(null);
    }
  };

  const handleDelete = async (email: string) => {
    if (!confirm(`Are you sure you want to PERMANENTLY DELETE ${email}? This cannot be undone.`)) {
      return;
    }
    if (!confirm(`Please confirm again: Delete ${email} permanently?`)) {
      return;
    }

    setActionLoading(email);
    try {
      await api.adminDeleteUser(email);
      setSuccessMsg(`User ${email} has been deleted`);
      setTimeout(() => setSuccessMsg(''), 3000);
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete user');
    } finally {
      setActionLoading(null);
    }
  };

  const fetchUserDetail = async (email: string) => {
    setLoadingDetail(true);
    try {
      const detail = await api.adminGetUser(email);
      setSelectedUser(detail as UserDetail);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load user details');
    } finally {
      setLoadingDetail(false);
    }
  };

  const fetchDatasets = async (search?: string) => {
    setLoadingDatasets(true);
    try {
      const response = await api.adminListDatasets({ search, limit: 20 });
      setDatasets(response.datasets);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load datasets');
    } finally {
      setLoadingDatasets(false);
    }
  };

  const handleSetPermission = async (datasetId: string, permission: string) => {
    if (!selectedUser) return;

    setSettingPermission(true);
    try {
      await api.adminSetUserPermission(selectedUser.email, datasetId, permission);
      // Refresh user detail to show updated permissions
      fetchUserDetail(selectedUser.email);
      setSuccessMsg(`Permission updated for dataset ${datasetId}`);
      setTimeout(() => setSuccessMsg(''), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to set permission');
    } finally {
      setSettingPermission(false);
    }
  };

  const handleRemovePermission = async (datasetId: string) => {
    if (!selectedUser) return;
    if (!confirm(`Remove access to dataset ${datasetId}?`)) return;

    setSettingPermission(true);
    try {
      await api.adminSetUserPermission(selectedUser.email, datasetId, 'NONE');
      fetchUserDetail(selectedUser.email);
      setSuccessMsg('Permission removed');
      setTimeout(() => setSuccessMsg(''), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to remove permission');
    } finally {
      setSettingPermission(false);
    }
  };

  const handleSearchDatasets = (e: Event) => {
    e.preventDefault();
    fetchDatasets(datasetSearch || undefined);
  };

  const fetchAllGroups = async () => {
    setLoadingGroups(true);
    try {
      const response = await api.adminListGroups({ limit: 100 });
      setAllGroups(response.groups as { email: string; display_name: string | null }[]);
    } catch (err) {
      console.error('Failed to fetch groups:', err);
    } finally {
      setLoadingGroups(false);
    }
  };

  const handleAddToGroup = async (groupEmail: string) => {
    if (!selectedUser) return;

    setManagingGroup(true);
    try {
      await api.adminAddGroupMember(groupEmail, selectedUser.email);
      fetchUserDetail(selectedUser.email);
      setSuccessMsg(`Added to group ${groupEmail}`);
      setTimeout(() => setSuccessMsg(''), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to add to group');
    } finally {
      setManagingGroup(false);
    }
  };

  const handleRemoveFromGroup = async (groupEmail: string) => {
    if (!selectedUser) return;
    if (!confirm(`Remove ${selectedUser.email} from group ${groupEmail}?`)) return;

    setManagingGroup(true);
    try {
      await api.adminRemoveGroupMember(groupEmail, selectedUser.email);
      fetchUserDetail(selectedUser.email);
      setSuccessMsg(`Removed from group ${groupEmail}`);
      setTimeout(() => setSuccessMsg(''), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to remove from group');
    } finally {
      setManagingGroup(false);
    }
  };

  const openUserDetail = (user: User) => {
    fetchUserDetail(user.email);
    fetchDatasets();
    fetchAllGroups();
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
          <button onClick={() => setError('')} class="float-right text-red-500 hover:text-red-700">&times;</button>
        </div>
      )}

      {successMsg && (
        <div class="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded-lg">
          {successMsg}
        </div>
      )}

      {/* Search and Actions */}
      <div class="flex flex-wrap gap-3 items-center">
        <form onSubmit={handleSearch} class="flex gap-2 flex-1 min-w-[200px]">
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
        <div class="flex gap-2">
          <input
            type="file"
            accept=".csv"
            ref={fileInputRef}
            onChange={handleCsvFileSelect}
            class="hidden"
          />
          <button
            onClick={() => fileInputRef.current?.click()}
            class="btn-secondary"
          >
            Upload CSV
          </button>
          <button
            onClick={() => setShowInvite(true)}
            class="btn-primary"
          >
            Invite User
          </button>
        </div>
      </div>

      {/* CSV Upload Modal */}
      {showCsvUpload && (
        <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div class="bg-white rounded-lg shadow-xl w-full max-w-lg mx-4 max-h-[90vh] overflow-auto">
            <div class="px-6 py-4 border-b border-gray-200">
              <h3 class="text-lg font-semibold text-gray-900">Bulk Invite Users</h3>
            </div>
            <div class="p-6 space-y-4">
              {csvResults ? (
                <div class="space-y-4">
                  <div class="bg-green-50 border border-green-200 rounded-lg p-4">
                    <p class="font-medium text-green-800">{csvResults.success.length} users invited successfully</p>
                  </div>
                  {csvResults.failed.length > 0 && (
                    <div class="bg-red-50 border border-red-200 rounded-lg p-4">
                      <p class="font-medium text-red-800 mb-2">{csvResults.failed.length} users failed:</p>
                      <ul class="text-sm text-red-700 space-y-1">
                        {csvResults.failed.map((f, i) => (
                          <li key={i}>{f.email}: {f.error}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                  <button
                    onClick={() => {
                      setShowCsvUpload(false);
                      setCsvData([]);
                      setCsvResults(null);
                      if (fileInputRef.current) fileInputRef.current.value = '';
                    }}
                    class="btn-primary w-full"
                  >
                    Done
                  </button>
                </div>
              ) : (
                <>
                  <p class="text-gray-600">
                    Found {csvData.length} users to invite:
                  </p>
                  <div class="max-h-60 overflow-auto border border-gray-200 rounded-lg">
                    <table class="min-w-full divide-y divide-gray-200 text-sm">
                      <thead class="bg-gray-50 sticky top-0">
                        <tr>
                          <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
                          <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-gray-200">
                        {csvData.map((u, i) => (
                          <tr key={i}>
                            <td class="px-3 py-2 text-gray-900">{u.email}</td>
                            <td class="px-3 py-2 text-gray-500">{u.display_name || '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div class="flex gap-3 justify-end">
                    <button
                      onClick={() => {
                        setShowCsvUpload(false);
                        setCsvData([]);
                        if (fileInputRef.current) fileInputRef.current.value = '';
                      }}
                      class="btn-secondary"
                    >
                      Cancel
                    </button>
                    <button
                      onClick={handleCsvUpload}
                      disabled={uploadingCsv}
                      class="btn-primary"
                    >
                      {uploadingCsv ? 'Inviting...' : `Invite ${csvData.length} Users`}
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}

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

      {/* User Detail Modal */}
      {selectedUser && (
        <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div class="bg-white rounded-lg shadow-xl w-full max-w-3xl mx-4 max-h-[90vh] overflow-hidden flex flex-col">
            <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
              <div>
                <h3 class="text-lg font-semibold text-gray-900">
                  {selectedUser.display_name || selectedUser.email}
                </h3>
                {selectedUser.display_name && (
                  <p class="text-sm text-gray-500">{selectedUser.email}</p>
                )}
              </div>
              <button
                onClick={() => setSelectedUser(null)}
                class="text-gray-400 hover:text-gray-500"
              >
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            <div class="p-6 overflow-y-auto flex-1">
              {loadingDetail ? (
                <div class="text-center py-8">
                  <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
                </div>
              ) : (
                <div class="space-y-6">
                  {/* User Info */}
                  <div class="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <span class="text-gray-500">Status:</span>
                      <div class="flex flex-wrap gap-1 mt-1">
                        {selectedUser.suspended_at && (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                            Suspended
                          </span>
                        )}
                        {selectedUser.is_admin && (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                            Admin
                          </span>
                        )}
                        {selectedUser.email_verified ? (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            Verified
                          </span>
                        ) : (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                            Pending
                          </span>
                        )}
                      </div>
                    </div>
                    <div>
                      <span class="text-gray-500">Groups:</span>
                      <div class="flex flex-wrap gap-1 mt-1">
                        {selectedUser.groups?.length > 0 ? (
                          selectedUser.groups.map((g) => (
                            <span key={g} class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                              {g}
                              <button
                                onClick={() => handleRemoveFromGroup(g)}
                                disabled={managingGroup}
                                class="hover:text-blue-600"
                                title="Remove from group"
                              >
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                                </svg>
                              </button>
                            </span>
                          ))
                        ) : (
                          <span class="text-gray-400">No groups</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <span class="text-gray-500">Last Login:</span>
                      <p class="text-gray-900">{formatDate(selectedUser.last_login)}</p>
                    </div>
                    <div>
                      <span class="text-gray-500">Created:</span>
                      <p class="text-gray-900">{formatDate(selectedUser.created_at)}</p>
                    </div>
                  </div>

                  {/* Group Membership */}
                  <div>
                    <h4 class="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                      Add to Group
                    </h4>
                    {loadingGroups ? (
                      <div class="text-center py-2">
                        <div class="animate-spin w-5 h-5 border-2 border-primary-600 border-t-transparent rounded-full mx-auto" />
                      </div>
                    ) : allGroups.length === 0 ? (
                      <p class="text-gray-400 text-sm">No groups available</p>
                    ) : (
                      <div class="flex flex-wrap gap-2">
                        {allGroups
                          .filter((g) => !selectedUser.groups?.includes(g.email))
                          .map((g) => (
                            <button
                              key={g.email}
                              onClick={() => handleAddToGroup(g.email)}
                              disabled={managingGroup}
                              class="inline-flex items-center gap-1 px-3 py-1.5 rounded-lg text-sm font-medium bg-gray-100 text-gray-700 hover:bg-gray-200 disabled:opacity-50"
                            >
                              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
                              </svg>
                              {g.display_name || g.email}
                            </button>
                          ))}
                        {allGroups.filter((g) => !selectedUser.groups?.includes(g.email)).length === 0 && (
                          <p class="text-gray-400 text-sm">User is in all available groups</p>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Current Permissions */}
                  <div>
                    <h4 class="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                      Current Dataset Permissions
                    </h4>
                    {selectedUser.permissions?.filter(p => p.resource_type === 'DATASET').length === 0 ? (
                      <p class="text-gray-400 text-sm">No direct dataset permissions</p>
                    ) : (
                      <div class="space-y-2">
                        {selectedUser.permissions?.filter(p => p.resource_type === 'DATASET').map((p) => (
                          <div key={p.resource_id} class="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                            <div>
                              <span class="font-medium text-gray-900">{p.resource_id}</span>
                              <span class={`ml-2 inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                                p.permission === 'DOWNLOAD' ? 'bg-green-100 text-green-800' : 'bg-blue-100 text-blue-800'
                              }`}>
                                {p.permission}
                              </span>
                            </div>
                            <button
                              onClick={() => handleRemovePermission(p.resource_id)}
                              disabled={settingPermission}
                              class="text-red-600 hover:text-red-700 text-sm"
                            >
                              Remove
                            </button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* Add Permission */}
                  <div>
                    <h4 class="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                      Add Dataset Access
                    </h4>
                    <form onSubmit={handleSearchDatasets} class="flex gap-2 mb-3">
                      <input
                        type="text"
                        value={datasetSearch}
                        onInput={(e) => setDatasetSearch((e.target as HTMLInputElement).value)}
                        placeholder="Search datasets..."
                        class="input flex-1"
                      />
                      <button type="submit" class="btn-secondary">
                        Search
                      </button>
                    </form>
                    {loadingDatasets ? (
                      <div class="text-center py-4">
                        <div class="animate-spin w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full mx-auto" />
                      </div>
                    ) : datasets.length === 0 ? (
                      <p class="text-gray-400 text-sm">No datasets found</p>
                    ) : (
                      <div class="max-h-48 overflow-y-auto border border-gray-200 rounded-lg">
                        <table class="min-w-full divide-y divide-gray-200 text-sm">
                          <thead class="bg-gray-50 sticky top-0">
                            <tr>
                              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Dataset</th>
                              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Default Access</th>
                              <th class="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">Grant</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-gray-200">
                            {datasets.map((d) => {
                              const existingPerm = selectedUser.permissions?.find(
                                p => p.resource_type === 'DATASET' && p.resource_id === d.ds_id
                              );
                              return (
                                <tr key={d.ds_id}>
                                  <td class="px-3 py-2">
                                    <div class="font-medium text-gray-900">{d.ds_id}</div>
                                    <div class="text-gray-500 truncate max-w-xs">{d.title}</div>
                                  </td>
                                  <td class="px-3 py-2 text-gray-500">
                                    {d.access_level || 'NONE'}
                                  </td>
                                  <td class="px-3 py-2 text-right">
                                    {existingPerm ? (
                                      <span class="text-gray-400 text-xs">Has {existingPerm.permission}</span>
                                    ) : (
                                      <div class="flex justify-end gap-1">
                                        <button
                                          onClick={() => handleSetPermission(d.ds_id, 'VIEW')}
                                          disabled={settingPermission}
                                          class="text-xs px-2 py-1 rounded bg-blue-50 text-blue-600 hover:bg-blue-100"
                                        >
                                          View
                                        </button>
                                        <button
                                          onClick={() => handleSetPermission(d.ds_id, 'DOWNLOAD')}
                                          disabled={settingPermission}
                                          class="text-xs px-2 py-1 rounded bg-green-50 text-green-600 hover:bg-green-100"
                                        >
                                          Download
                                        </button>
                                      </div>
                                    )}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
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
                <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              {users.length === 0 ? (
                <tr>
                  <td colSpan={4} class="px-6 py-12 text-center text-gray-500">
                    No users found
                  </td>
                </tr>
              ) : (
                users.filter(u => !u.is_group).map((user) => (
                  <tr key={user.email} class={`hover:bg-gray-50 ${user.suspended_at ? 'bg-red-50' : ''}`}>
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
                      <div class="flex flex-wrap gap-1">
                        {user.suspended_at && (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                            Suspended
                          </span>
                        )}
                        {user.is_admin && (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                            Admin
                          </span>
                        )}
                        {user.email_verified ? (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            Verified
                          </span>
                        ) : (
                          <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                            Pending
                          </span>
                        )}
                      </div>
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {formatDate(user.last_login)}
                    </td>
                    <td class="px-6 py-4 text-right">
                      <div class="flex justify-end gap-2">
                        {actionLoading === user.email ? (
                          <span class="text-sm text-gray-400">Loading...</span>
                        ) : (
                          <>
                            <button
                              onClick={() => openUserDetail(user)}
                              class="text-xs px-2 py-1 rounded text-primary-600 hover:bg-primary-50"
                            >
                              Manage
                            </button>
                            <button
                              onClick={() => handleToggleAdmin(user.email, user.is_admin)}
                              class={`text-xs px-2 py-1 rounded ${user.is_admin ? 'text-red-600 hover:bg-red-50' : 'text-primary-600 hover:bg-primary-50'}`}
                            >
                              {user.is_admin ? 'Remove Admin' : 'Make Admin'}
                            </button>
                            {user.suspended_at ? (
                              <button
                                onClick={() => handleUnsuspend(user.email)}
                                class="text-xs px-2 py-1 rounded text-green-600 hover:bg-green-50"
                              >
                                Unsuspend
                              </button>
                            ) : (
                              <button
                                onClick={() => handleSuspend(user.email)}
                                class="text-xs px-2 py-1 rounded text-yellow-600 hover:bg-yellow-50"
                              >
                                Suspend
                              </button>
                            )}
                            <button
                              onClick={() => handleDelete(user.email)}
                              class="text-xs px-2 py-1 rounded text-red-600 hover:bg-red-50"
                            >
                              Delete
                            </button>
                          </>
                        )}
                      </div>
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
