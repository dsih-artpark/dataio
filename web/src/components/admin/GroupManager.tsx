import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';

interface Group {
  email: string;
  display_name: string | null;
  member_count: number;
  created_at: string | null;
}

interface GroupDetail extends Group {
  members: {
    email: string;
    display_name: string | null;
    is_admin: boolean;
  }[];
}

export default function GroupManager() {
  const [groups, setGroups] = useState<Group[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 20;

  // Create group modal state
  const [showCreate, setShowCreate] = useState(false);
  const [newGroupEmail, setNewGroupEmail] = useState('');
  const [newGroupName, setNewGroupName] = useState('');
  const [creating, setCreating] = useState(false);

  // Group detail modal state
  const [selectedGroup, setSelectedGroup] = useState<GroupDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [addMemberEmail, setAddMemberEmail] = useState('');
  const [addingMember, setAddingMember] = useState(false);

  const fetchGroups = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await api.adminListGroups({
        search: search || undefined,
        limit,
        offset,
      });
      setGroups(response.groups as Group[]);
      setTotal(response.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load groups');
    } finally {
      setLoading(false);
    }
  };

  const fetchGroupDetail = async (groupEmail: string) => {
    setLoadingDetail(true);
    try {
      const detail = await api.adminGetGroup(groupEmail);
      setSelectedGroup(detail as GroupDetail);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load group details');
    } finally {
      setLoadingDetail(false);
    }
  };

  useEffect(() => {
    fetchGroups();
  }, [offset]);

  const handleSearch = (e: Event) => {
    e.preventDefault();
    setOffset(0);
    fetchGroups();
  };

  const handleCreate = async (e: Event) => {
    e.preventDefault();
    if (!newGroupEmail.trim()) return;

    setCreating(true);
    setError('');

    try {
      await api.adminCreateGroup({
        email: newGroupEmail.trim(),
        display_name: newGroupName.trim() || undefined,
      });
      setShowCreate(false);
      setNewGroupEmail('');
      setNewGroupName('');
      fetchGroups();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create group');
    } finally {
      setCreating(false);
    }
  };

  const handleAddMember = async (e: Event) => {
    e.preventDefault();
    if (!selectedGroup || !addMemberEmail.trim()) return;

    setAddingMember(true);
    setError('');

    try {
      await api.adminAddGroupMember(selectedGroup.email, addMemberEmail.trim());
      setAddMemberEmail('');
      fetchGroupDetail(selectedGroup.email);
      fetchGroups();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to add member');
    } finally {
      setAddingMember(false);
    }
  };

  const handleRemoveMember = async (userEmail: string) => {
    if (!selectedGroup) return;
    if (!confirm(`Remove ${userEmail} from this group?`)) return;

    try {
      await api.adminRemoveGroupMember(selectedGroup.email, userEmail);
      fetchGroupDetail(selectedGroup.email);
      fetchGroups();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to remove member');
    }
  };

  const formatDate = (date: string | null) => {
    if (!date) return 'Unknown';
    return new Date(date).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  if (loading && groups.length === 0) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
          <p class="mt-4 text-gray-500">Loading groups...</p>
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
            placeholder="Search groups..."
            class="input flex-1"
          />
          <button type="submit" class="btn-secondary">
            Search
          </button>
        </form>
        <button
          onClick={() => setShowCreate(true)}
          class="btn-primary"
        >
          Create Group
        </button>
      </div>

      {/* Create Group Modal */}
      {showCreate && (
        <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div class="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
            <div class="px-6 py-4 border-b border-gray-200">
              <h3 class="text-lg font-semibold text-gray-900">Create Group</h3>
            </div>
            <form onSubmit={handleCreate} class="p-6 space-y-4">
              <div>
                <label class="label">Group Email</label>
                <input
                  type="email"
                  value={newGroupEmail}
                  onInput={(e) => setNewGroupEmail((e.target as HTMLInputElement).value)}
                  class="input"
                  required
                  placeholder="group@example.com"
                />
                <p class="text-xs text-gray-500 mt-1">
                  Used as the group identifier
                </p>
              </div>
              <div>
                <label class="label">Display Name (optional)</label>
                <input
                  type="text"
                  value={newGroupName}
                  onInput={(e) => setNewGroupName((e.target as HTMLInputElement).value)}
                  class="input"
                  placeholder="Research Team"
                />
              </div>
              <div class="flex gap-3 justify-end">
                <button
                  type="button"
                  onClick={() => setShowCreate(false)}
                  class="btn-secondary"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={creating}
                  class="btn-primary"
                >
                  {creating ? 'Creating...' : 'Create Group'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Group Detail Modal */}
      {selectedGroup && (
        <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div class="bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[80vh] overflow-hidden flex flex-col">
            <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
              <div>
                <h3 class="text-lg font-semibold text-gray-900">
                  {selectedGroup.display_name || selectedGroup.email}
                </h3>
                {selectedGroup.display_name && (
                  <p class="text-sm text-gray-500">{selectedGroup.email}</p>
                )}
              </div>
              <button
                onClick={() => setSelectedGroup(null)}
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
                  {/* Add Member Form */}
                  <form onSubmit={handleAddMember} class="flex gap-3">
                    <input
                      type="email"
                      value={addMemberEmail}
                      onInput={(e) => setAddMemberEmail((e.target as HTMLInputElement).value)}
                      placeholder="Add member by email..."
                      class="input flex-1"
                      required
                    />
                    <button
                      type="submit"
                      disabled={addingMember}
                      class="btn-primary"
                    >
                      {addingMember ? 'Adding...' : 'Add Member'}
                    </button>
                  </form>

                  {/* Members List */}
                  <div>
                    <h4 class="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                      Members ({selectedGroup.members?.length || 0})
                    </h4>
                    {selectedGroup.members?.length === 0 ? (
                      <p class="text-gray-500 text-center py-4">No members yet</p>
                    ) : (
                      <div class="space-y-2">
                        {selectedGroup.members?.map((member) => (
                          <div
                            key={member.email}
                            class="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
                          >
                            <div>
                              <div class="font-medium text-gray-900">
                                {member.display_name || member.email}
                              </div>
                              {member.display_name && (
                                <div class="text-sm text-gray-500">{member.email}</div>
                              )}
                            </div>
                            <div class="flex items-center gap-3">
                              {member.is_admin && (
                                <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800">
                                  Admin
                                </span>
                              )}
                              <button
                                onClick={() => handleRemoveMember(member.email)}
                                class="text-red-600 hover:text-red-700 text-sm"
                              >
                                Remove
                              </button>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Groups Table */}
      <div class="card overflow-hidden">
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Group
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Members
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
              {groups.length === 0 ? (
                <tr>
                  <td colSpan={4} class="px-6 py-12 text-center text-gray-500">
                    No groups found
                  </td>
                </tr>
              ) : (
                groups.map((group) => (
                  <tr key={group.email} class="hover:bg-gray-50">
                    <td class="px-6 py-4">
                      <div>
                        <div class="font-medium text-gray-900">
                          {group.display_name || group.email}
                        </div>
                        {group.display_name && (
                          <div class="text-sm text-gray-500">{group.email}</div>
                        )}
                      </div>
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {group.member_count || 0} members
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {formatDate(group.created_at)}
                    </td>
                    <td class="px-6 py-4 text-right">
                      <button
                        onClick={() => fetchGroupDetail(group.email)}
                        class="text-primary-600 hover:text-primary-700 text-sm"
                      >
                        Manage
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
              Showing {offset + 1} to {Math.min(offset + limit, total)} of {total} groups
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
