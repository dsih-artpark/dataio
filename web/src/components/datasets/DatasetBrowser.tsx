import { useEffect, useState, useCallback } from 'preact/hooks';
import { api } from '../../lib/api';
import { initAuth } from '../../lib/auth';
import type { Dataset, DatasetDetail, Collection, FilterState, AccessLevel } from '../../lib/types';
import DatasetListItem from './DatasetListItem';
import DatasetDetailPanel from './DatasetDetailPanel';

export default function DatasetBrowser() {
  // Data state
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [collections, setCollections] = useState<Collection[]>([]);
  const [selectedDataset, setSelectedDataset] = useState<DatasetDetail | null>(null);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 50;

  // Auth state
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [authChecked, setAuthChecked] = useState(false);

  // UI state
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState('');
  const [detailError, setDetailError] = useState<string | null>(null);
  const [filtersOpen, setFiltersOpen] = useState(false);

  // Filter state
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    collections: [],
    accessLevels: [],
  });

  // Debounce search
  const [debouncedSearch, setDebouncedSearch] = useState('');

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(filters.search);
    }, 300);
    return () => clearTimeout(timer);
  }, [filters.search]);

  // Check auth status on mount - gracefully handle failures since this page supports anonymous access
  useEffect(() => {
    const checkAuth = async () => {
      if (api.isAuthenticated()) {
        try {
          await initAuth();
          setIsAuthenticated(true);
        } catch {
          // Don't redirect on failure - this page supports anonymous access
          // Just fall back to unauthenticated mode
          setIsAuthenticated(false);
        }
      } else {
        setIsAuthenticated(false);
      }
      setAuthChecked(true);
    };
    checkAuth();
  }, []);

  // Fetch collections on mount (after auth check)
  useEffect(() => {
    if (!authChecked) return;

    const fetchCollections = async () => {
      try {
        const response = isAuthenticated
          ? await api.getCollections()
          : await api.getPublicCollections();
        setCollections(response.collections as Collection[]);
      } catch (err) {
        console.error('Failed to fetch collections:', err);
      }
    };
    fetchCollections();
  }, [authChecked, isAuthenticated]);

  // Serialize filters for dependency comparison (arrays don't compare by value)
  const collectionsKey = JSON.stringify(filters.collections);
  const accessLevelsKey = JSON.stringify(filters.accessLevels);

  // Fetch datasets when filters or pagination change
  const fetchDatasets = useCallback(async () => {
    if (!authChecked) return;

    setLoading(true);
    setError('');

    try {
      const params: {
        search?: string;
        collection_id?: number;
        limit: number;
        offset: number;
      } = {
        limit,
        offset,
      };

      if (debouncedSearch) {
        params.search = debouncedSearch;
      }

      // Parse collections from serialized key (for use in this callback)
      const selectedCollections: number[] = JSON.parse(collectionsKey);
      const selectedAccessLevels: AccessLevel[] = JSON.parse(accessLevelsKey);

      // API only supports single collection_id
      // If exactly one collection selected, use server-side filtering
      // Otherwise, filter client-side (either 0 or multiple collections)
      if (selectedCollections.length === 1) {
        params.collection_id = selectedCollections[0];
      }

      const response = isAuthenticated
        ? await api.getDatasets(params)
        : await api.getPublicDatasets(params);

      let filteredDatasets = response.datasets as Dataset[];

      // Client-side filtering for collections (when multiple are selected)
      // Note: If 1 collection is selected, server already filtered it
      if (selectedCollections.length > 1) {
        filteredDatasets = filteredDatasets.filter((d) =>
          d.collection_id && selectedCollections.includes(d.collection_id)
        );
      }

      // Client-side filtering for access levels
      if (selectedAccessLevels.length > 0) {
        filteredDatasets = filteredDatasets.filter((d) =>
          d.access_level && selectedAccessLevels.includes(d.access_level)
        );
      }

      setDatasets(filteredDatasets);
      setTotal(response.total);

      // Auto-select first dataset on desktop if none selected
      if (filteredDatasets.length > 0 && !selectedDataset && window.innerWidth >= 768) {
        fetchDatasetDetail(filteredDatasets[0].ds_id);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load datasets');
    } finally {
      setLoading(false);
    }
  }, [authChecked, isAuthenticated, debouncedSearch, collectionsKey, accessLevelsKey, offset]);

  useEffect(() => {
    fetchDatasets();
  }, [fetchDatasets]);

  // Fetch dataset detail
  const fetchDatasetDetail = async (datasetId: string) => {
    setDetailLoading(true);
    setDetailError(null);

    try {
      const response = isAuthenticated
        ? await api.getDataset(datasetId)
        : await api.getPublicDataset(datasetId);
      setSelectedDataset(response as DatasetDetail);
    } catch (err) {
      setDetailError(err instanceof Error ? err.message : 'Failed to load dataset details');
      setSelectedDataset(null);
    } finally {
      setDetailLoading(false);
    }
  };

  const handleDatasetSelect = (dataset: Dataset) => {
    if (selectedDataset?.ds_id !== dataset.ds_id) {
      fetchDatasetDetail(dataset.ds_id);
    }
  };

  const handleFilterChange = (newFilters: FilterState) => {
    setFilters(newFilters);
    setOffset(0);
    setSelectedDataset(null);
  };

  const handleCollectionToggle = (collectionId: number) => {
    const newCollections = filters.collections.includes(collectionId)
      ? filters.collections.filter((id) => id !== collectionId)
      : [...filters.collections, collectionId];
    handleFilterChange({ ...filters, collections: newCollections });
  };

  const handleAccessToggle = (level: AccessLevel) => {
    const newLevels = filters.accessLevels.includes(level)
      ? filters.accessLevels.filter((l) => l !== level)
      : [...filters.accessLevels, level];
    handleFilterChange({ ...filters, accessLevels: newLevels });
  };

  const clearFilters = () => {
    handleFilterChange({ search: '', collections: [], accessLevels: [] });
  };

  const hasActiveFilters = filters.collections.length > 0 || filters.accessLevels.length > 0;
  const activeFilterCount = filters.collections.length + filters.accessLevels.length;

  // Group collections by category for dropdown
  const collectionsByCategory = collections.reduce(
    (acc, col) => {
      const category = col.category_name || 'Other';
      if (!acc[category]) acc[category] = [];
      acc[category].push(col);
      return acc;
    },
    {} as Record<string, Collection[]>
  );

  const accessLevelOptions: { value: AccessLevel; label: string }[] = [
    { value: 'DOWNLOAD', label: 'Open Download' },
    { value: 'VIEW', label: 'View Only' },
    { value: 'NONE', label: 'Restricted' },
  ];

  return (
    <div class="h-[calc(100vh-8rem)] flex flex-col">
      {/* Top Filter Bar */}
      <div class="flex-shrink-0 mb-4">
        <div class="flex flex-wrap items-center gap-3">
          {/* Search */}
          <div class="flex-1 min-w-[200px] max-w-md">
            <div class="relative">
              <svg
                class="absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                value={filters.search}
                onInput={(e) => handleFilterChange({ ...filters, search: (e.target as HTMLInputElement).value })}
                placeholder="Search datasets..."
                class="w-full pl-10 pr-4 py-2.5 bg-white border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent shadow-sm"
              />
            </div>
          </div>

          {/* Collection Filter Dropdown */}
          <div class="relative">
            <button
              onClick={() => setFiltersOpen(!filtersOpen)}
              class={`flex items-center gap-2 px-4 py-2.5 bg-white border rounded-xl text-sm font-medium transition-colors shadow-sm ${
                hasActiveFilters
                  ? 'border-primary-300 text-primary-700 bg-primary-50'
                  : 'border-gray-200 text-gray-700 hover:bg-gray-50'
              }`}
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
              </svg>
              Filters
              {activeFilterCount > 0 && (
                <span class="w-5 h-5 bg-primary-600 text-white text-xs rounded-full flex items-center justify-center">
                  {activeFilterCount}
                </span>
              )}
              <svg class={`w-4 h-4 transition-transform ${filtersOpen ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {/* Filter Dropdown */}
            {filtersOpen && (
              <>
                <div class="fixed inset-0 z-30" onClick={() => setFiltersOpen(false)} />
                <div class="absolute left-0 mt-2 w-80 bg-white rounded-xl shadow-lg border border-gray-200 z-40 overflow-hidden">
                  <div class="p-4 border-b border-gray-100">
                    <div class="flex items-center justify-between">
                      <span class="font-semibold text-gray-900">Filters</span>
                      {hasActiveFilters && (
                        <button
                          onClick={clearFilters}
                          class="text-xs text-primary-600 hover:text-primary-700 font-medium"
                        >
                          Clear all
                        </button>
                      )}
                    </div>
                  </div>

                  <div class="max-h-80 overflow-y-auto">
                    {/* Access Level */}
                    <div class="p-4 border-b border-gray-100">
                      <h4 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Access Level</h4>
                      <div class="space-y-2">
                        {accessLevelOptions.map((option) => (
                          <label key={option.value} class="flex items-center gap-3 cursor-pointer group">
                            <input
                              type="checkbox"
                              checked={filters.accessLevels.includes(option.value)}
                              onChange={() => handleAccessToggle(option.value)}
                              class="w-4 h-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                            />
                            <span class="text-sm text-gray-700 group-hover:text-gray-900">
                              {option.label}
                            </span>
                          </label>
                        ))}
                      </div>
                    </div>

                    {/* Collections */}
                    <div class="p-4">
                      <h4 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Collection</h4>
                      <div class="space-y-4">
                        {Object.entries(collectionsByCategory).map(([category, cols]) => (
                          <div key={category}>
                            <p class="text-xs font-medium text-gray-400 mb-2">{category}</p>
                            <div class="space-y-2">
                              {cols.map((col) => (
                                <label key={col.id} class="flex items-center gap-3 cursor-pointer group">
                                  <input
                                    type="checkbox"
                                    checked={filters.collections.includes(col.id)}
                                    onChange={() => handleCollectionToggle(col.id)}
                                    class="w-4 h-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                                  />
                                  <span class="text-sm text-gray-700 group-hover:text-gray-900 truncate">
                                    {col.collection_name}
                                  </span>
                                </label>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div class="p-3 border-t border-gray-100 bg-gray-50">
                    <button
                      onClick={() => setFiltersOpen(false)}
                      class="w-full py-2.5 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700 transition-colors"
                    >
                      Apply Filters
                    </button>
                  </div>
                </div>
              </>
            )}
          </div>

          {/* Active filter pills */}
          {hasActiveFilters && (
            <div class="flex flex-wrap gap-2">
              {filters.accessLevels.map((level) => (
                <span
                  key={level}
                  class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-primary-100 text-primary-700 rounded-full text-xs font-medium"
                >
                  {level === 'DOWNLOAD' ? 'Open' : level === 'VIEW' ? 'View Only' : 'Restricted'}
                  <button
                    onClick={() => handleAccessToggle(level)}
                    class="hover:bg-primary-200 rounded-full p-0.5"
                  >
                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </span>
              ))}
              {filters.collections.map((colId) => {
                const col = collections.find((c) => c.id === colId);
                return col ? (
                  <span
                    key={colId}
                    class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 text-gray-700 rounded-full text-xs font-medium"
                  >
                    {col.collection_name}
                    <button
                      onClick={() => handleCollectionToggle(colId)}
                      class="hover:bg-gray-200 rounded-full p-0.5"
                    >
                      <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </span>
                ) : null;
              })}
            </div>
          )}

          {/* Results count & auth status */}
          <div class="ml-auto flex items-center gap-4">
            <p class="text-sm text-gray-500">
              {loading ? (
                'Loading...'
              ) : (
                <>
                  <span class="font-semibold text-gray-700">{datasets.length}</span>
                  {' of '}
                  <span class="font-semibold text-gray-700">{total}</span>
                  {' datasets'}
                </>
              )}
            </p>
            {isAuthenticated && (
              <span class="text-xs text-green-600 font-medium flex items-center gap-1.5">
                <span class="w-2 h-2 bg-green-500 rounded-full" />
                Signed in
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Two-Panel Layout */}
      <div class="flex-1 flex overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
        {/* Dataset List - 38% on large screens */}
        <div class="w-full md:w-[42%] lg:w-[38%] xl:w-[35%] border-r border-gray-200 flex flex-col min-w-0">
          <div class="flex-1 overflow-y-auto">
            {loading && datasets.length === 0 ? (
              <div class="p-8 text-center">
                <div class="w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto" />
                <p class="mt-3 text-sm text-gray-500">Loading datasets...</p>
              </div>
            ) : error ? (
              <div class="p-8 text-center">
                <div class="w-12 h-12 mx-auto mb-3 rounded-full bg-red-100 flex items-center justify-center">
                  <svg class="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <p class="text-sm text-red-600">{error}</p>
                <button onClick={fetchDatasets} class="mt-3 text-sm text-primary-600 hover:text-primary-700 font-medium">
                  Try again
                </button>
              </div>
            ) : datasets.length === 0 ? (
              <div class="p-8 text-center">
                <div class="w-12 h-12 mx-auto mb-3 rounded-full bg-gray-100 flex items-center justify-center">
                  <svg class="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
                  </svg>
                </div>
                <p class="text-sm text-gray-600 font-medium">No datasets found</p>
                <p class="text-xs text-gray-500 mt-1">Try adjusting your filters</p>
              </div>
            ) : (
              <>
                {datasets.map((dataset) => (
                  <DatasetListItem
                    key={dataset.ds_id}
                    dataset={dataset}
                    isSelected={selectedDataset?.ds_id === dataset.ds_id}
                    onClick={() => handleDatasetSelect(dataset)}
                  />
                ))}

                {/* Load more */}
                {datasets.length < total && (
                  <div class="p-4 border-t border-gray-100">
                    <button
                      onClick={() => setOffset((prev) => prev + limit)}
                      disabled={loading}
                      class="w-full py-2.5 text-sm text-primary-600 hover:text-primary-700 font-medium disabled:opacity-50"
                    >
                      {loading ? 'Loading...' : 'Load more'}
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </div>

        {/* Detail Panel - 62% on large screens */}
        <div class="hidden md:flex flex-1 flex-col min-w-0 bg-gray-50/30">
          <div class="flex-1 overflow-y-auto p-6">
            <DatasetDetailPanel
              dataset={selectedDataset}
              loading={detailLoading}
              error={detailError}
              isAuthenticated={isAuthenticated}
            />
          </div>
        </div>
      </div>

      {/* Mobile: Login prompt */}
      {!isAuthenticated && (
        <div class="mt-4 bg-blue-50 border border-blue-200 rounded-xl p-4 md:hidden">
          <div class="flex items-start gap-3">
            <svg class="w-5 h-5 text-blue-500 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
            <div>
              <p class="text-sm text-blue-800 font-medium">Want to download datasets?</p>
              <p class="text-sm text-blue-700 mt-0.5">
                <a href="/login" class="underline hover:text-blue-600">Sign in</a> to download and access additional content.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
