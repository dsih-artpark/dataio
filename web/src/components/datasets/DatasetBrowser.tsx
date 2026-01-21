import { useEffect, useState, useCallback } from 'preact/hooks';
import { api } from '../../lib/api';
import { initAuth } from '../../lib/auth';
import type { Dataset, DatasetDetail, Collection, FilterState } from '../../lib/types';
import FilterPanel from './FilterPanel';
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
  const [filterCollapsed, setFilterCollapsed] = useState(false);
  const [mobileFilterOpen, setMobileFilterOpen] = useState(false);
  const [expandedMobileItem, setExpandedMobileItem] = useState<string | null>(null);

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

  // Check auth status on mount
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

      // API only supports single collection_id, so we use first selected
      if (filters.collections.length === 1) {
        params.collection_id = filters.collections[0];
      }

      const response = isAuthenticated
        ? await api.getDatasets(params)
        : await api.getPublicDatasets(params);

      let filteredDatasets = response.datasets as Dataset[];

      // Client-side filtering for multiple collections (if more than one selected)
      if (filters.collections.length > 1) {
        filteredDatasets = filteredDatasets.filter((d) =>
          filters.collections.includes(d.collection_id)
        );
      }

      // Client-side filtering for access levels
      if (filters.accessLevels.length > 0) {
        filteredDatasets = filteredDatasets.filter((d) =>
          filters.accessLevels.includes(d.access_level)
        );
      }

      setDatasets(filteredDatasets);
      setTotal(response.total);

      // Auto-select first dataset on desktop if none selected
      if (filteredDatasets.length > 0 && !selectedDataset && window.innerWidth >= 1024) {
        fetchDatasetDetail(filteredDatasets[0].ds_id);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load datasets');
    } finally {
      setLoading(false);
    }
  }, [authChecked, isAuthenticated, debouncedSearch, filters.collections, filters.accessLevels, offset]);

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
    setOffset(0); // Reset pagination on filter change
    setSelectedDataset(null); // Clear selection
  };

  const handleMobileItemToggle = (datasetId: string) => {
    setExpandedMobileItem((prev) => (prev === datasetId ? null : datasetId));
  };

  // Check if on mobile
  const isMobile = typeof window !== 'undefined' && window.innerWidth < 1024;

  return (
    <div class="h-[calc(100vh-12rem)] lg:h-[calc(100vh-10rem)] flex flex-col">
      {/* Mobile header with filter button */}
      <div class="lg:hidden flex items-center gap-3 mb-4">
        <button
          onClick={() => setMobileFilterOpen(true)}
          class="flex items-center gap-2 px-3 py-2 border border-gray-200 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
          </svg>
          Filters
          {(filters.collections.length > 0 || filters.accessLevels.length > 0) && (
            <span class="w-5 h-5 bg-primary-600 text-white text-xs rounded-full flex items-center justify-center">
              {filters.collections.length + filters.accessLevels.length}
            </span>
          )}
        </button>
        <div class="flex-1">
          <input
            type="text"
            value={filters.search}
            onInput={(e) => handleFilterChange({ ...filters, search: (e.target as HTMLInputElement).value })}
            placeholder="Search datasets..."
            class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
          />
        </div>
      </div>

      {/* Main content area */}
      <div class="flex-1 flex overflow-hidden rounded-lg border border-gray-200 bg-white">
        {/* Filter panel - desktop */}
        <div class={`hidden lg:block ${filterCollapsed ? 'w-12' : 'w-64'} flex-shrink-0 transition-all duration-200`}>
          <FilterPanel
            collections={collections}
            filters={filters}
            onFilterChange={handleFilterChange}
            isCollapsed={filterCollapsed}
            onToggleCollapse={() => setFilterCollapsed(!filterCollapsed)}
          />
        </div>

        {/* Dataset list */}
        <div class="flex-1 lg:w-80 lg:flex-none border-r border-gray-200 flex flex-col min-w-0">
          {/* Results header */}
          <div class="px-4 py-3 border-b border-gray-200 bg-gray-50 flex-shrink-0">
            <div class="flex items-center justify-between">
              <p class="text-sm text-gray-600">
                {loading ? (
                  'Loading...'
                ) : (
                  <>
                    <span class="font-medium text-gray-900">{datasets.length}</span>
                    {' '}of{' '}
                    <span class="font-medium text-gray-900">{total}</span>
                    {' '}datasets
                  </>
                )}
              </p>
              {isAuthenticated && (
                <span class="text-xs text-green-600 font-medium flex items-center gap-1">
                  <span class="w-1.5 h-1.5 bg-green-500 rounded-full"></span>
                  Signed in
                </span>
              )}
            </div>
          </div>

          {/* List content */}
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
                    isMobile={isMobile}
                    isExpanded={expandedMobileItem === dataset.ds_id}
                    onToggleExpand={() => handleMobileItemToggle(dataset.ds_id)}
                  />
                ))}

                {/* Load more */}
                {datasets.length < total && (
                  <div class="p-4 border-t border-gray-100">
                    <button
                      onClick={() => setOffset((prev) => prev + limit)}
                      disabled={loading}
                      class="w-full py-2 text-sm text-primary-600 hover:text-primary-700 font-medium disabled:opacity-50"
                    >
                      {loading ? 'Loading...' : 'Load more'}
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </div>

        {/* Detail panel - desktop only */}
        <div class="hidden lg:block flex-1 p-4 bg-gray-50 min-w-0">
          <DatasetDetailPanel
            dataset={selectedDataset}
            loading={detailLoading}
            error={detailError}
            isAuthenticated={isAuthenticated}
          />
        </div>
      </div>

      {/* Mobile filter drawer */}
      {mobileFilterOpen && (
        <div class="lg:hidden fixed inset-0 z-50">
          {/* Backdrop */}
          <div
            class="absolute inset-0 bg-black/50"
            onClick={() => setMobileFilterOpen(false)}
          />
          {/* Drawer */}
          <div class="absolute left-0 top-0 bottom-0 w-80 max-w-[85vw] bg-white shadow-xl animate-slide-in">
            <FilterPanel
              collections={collections}
              filters={filters}
              onFilterChange={handleFilterChange}
              isCollapsed={false}
              onToggleCollapse={() => {}}
              isMobile={true}
              onClose={() => setMobileFilterOpen(false)}
            />
          </div>
        </div>
      )}

      {/* Login prompt - shown at bottom on mobile only when not authenticated */}
      {!isAuthenticated && (
        <div class="mt-4 bg-blue-50 border border-blue-200 rounded-lg p-4 lg:hidden">
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
