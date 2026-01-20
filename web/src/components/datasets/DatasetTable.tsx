import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';

interface Dataset {
  ds_id: string;
  title: string;
  description: string;
  collection_name?: string;
  data_owner_name?: string;
  access_level: string;
  temporal_coverage_start_date?: string;
  temporal_coverage_end_date?: string;
}

export default function DatasetTable() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 20;

  const fetchDatasets = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await api.getDatasets({
        search: search || undefined,
        limit,
        offset,
      });
      setDatasets(response.datasets as Dataset[]);
      setTotal(response.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load datasets');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDatasets();
  }, [offset]);

  const handleSearch = (e: Event) => {
    e.preventDefault();
    setOffset(0);
    fetchDatasets();
  };

  const formatDate = (date?: string) => {
    if (!date) return '-';
    return new Date(date).toLocaleDateString();
  };

  if (loading && datasets.length === 0) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
          <p class="mt-4 text-gray-500">Loading datasets...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p class="text-red-600">{error}</p>
          <button onClick={fetchDatasets} class="btn-secondary mt-4">
            Try again
          </button>
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-4">
      {/* Search */}
      <form onSubmit={handleSearch} class="flex gap-3">
        <div class="flex-1">
          <input
            type="text"
            value={search}
            onInput={(e) => setSearch((e.target as HTMLInputElement).value)}
            placeholder="Search datasets..."
            class="input"
          />
        </div>
        <button type="submit" class="btn-primary">
          Search
        </button>
      </form>

      {/* Table */}
      <div class="card overflow-hidden">
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Dataset
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Collection
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Data Owner
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Temporal Coverage
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Access
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              {datasets.length === 0 ? (
                <tr>
                  <td colSpan={5} class="px-6 py-12 text-center text-gray-500">
                    No datasets found
                  </td>
                </tr>
              ) : (
                datasets.map((dataset) => (
                  <tr key={dataset.ds_id} class="hover:bg-gray-50">
                    <td class="px-6 py-4">
                      <a
                        href={`/dashboard/datasets/${dataset.ds_id}`}
                        class="text-primary-600 hover:text-primary-700 font-medium"
                      >
                        {dataset.title}
                      </a>
                      <p class="text-sm text-gray-500 truncate max-w-xs">
                        {dataset.description || 'No description'}
                      </p>
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {dataset.collection_name || '-'}
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {dataset.data_owner_name || '-'}
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-500">
                      {formatDate(dataset.temporal_coverage_start_date)} -{' '}
                      {formatDate(dataset.temporal_coverage_end_date)}
                    </td>
                    <td class="px-6 py-4">
                      <span
                        class={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                          dataset.access_level === 'DOWNLOAD'
                            ? 'bg-green-100 text-green-800'
                            : dataset.access_level === 'VIEW'
                            ? 'bg-blue-100 text-blue-800'
                            : 'bg-gray-100 text-gray-800'
                        }`}
                      >
                        {dataset.access_level}
                      </span>
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
              Showing {offset + 1} to {Math.min(offset + limit, total)} of {total} datasets
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
