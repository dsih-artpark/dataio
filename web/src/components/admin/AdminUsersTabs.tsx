import { useState, useEffect } from 'preact/hooks';
import { api } from '../../lib/api';
import UserList from './UserList';
import PendingUsers from './PendingUsers';

type Tab = 'all' | 'pending';

export default function AdminUsersTabs() {
  const [activeTab, setActiveTab] = useState<Tab>('all');
  const [pendingCount, setPendingCount] = useState(0);

  // Fetch pending count for badge
  useEffect(() => {
    const fetchPendingCount = async () => {
      try {
        const response = await api.adminListPendingUsers({ limit: 1, offset: 0 });
        setPendingCount(response.total);
      } catch {
        // Ignore errors
      }
    };
    fetchPendingCount();
  }, []);

  return (
    <div class="space-y-4">
      {/* Tabs */}
      <div class="border-b border-gray-200">
        <nav class="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveTab('all')}
            class={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'all'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            All Users
          </button>
          <button
            onClick={() => setActiveTab('pending')}
            class={`py-4 px-1 border-b-2 font-medium text-sm flex items-center gap-2 ${
              activeTab === 'pending'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Pending Verification
            {pendingCount > 0 && (
              <span class="inline-flex items-center justify-center px-2 py-0.5 text-xs font-medium rounded-full bg-yellow-100 text-yellow-800">
                {pendingCount}
              </span>
            )}
          </button>
        </nav>
      </div>

      {/* Tab Content */}
      <div>
        {activeTab === 'all' ? <UserList /> : <PendingUsers />}
      </div>
    </div>
  );
}
