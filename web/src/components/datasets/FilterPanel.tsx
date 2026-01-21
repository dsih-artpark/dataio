import { useState } from 'preact/hooks';
import type { Collection, AccessLevel, FilterState } from '../../lib/types';

interface FilterPanelProps {
  collections: Collection[];
  filters: FilterState;
  onFilterChange: (filters: FilterState) => void;
  isCollapsed: boolean;
  onToggleCollapse: () => void;
  isMobile?: boolean;
  onClose?: () => void;
}

export default function FilterPanel({
  collections,
  filters,
  onFilterChange,
  isCollapsed,
  onToggleCollapse,
  isMobile = false,
  onClose,
}: FilterPanelProps) {
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    collection: true,
    access: true,
  });

  const toggleSection = (section: string) => {
    setExpandedSections((prev) => ({ ...prev, [section]: !prev[section] }));
  };

  const handleSearchChange = (value: string) => {
    onFilterChange({ ...filters, search: value });
  };

  const handleCollectionToggle = (collectionId: number) => {
    const newCollections = filters.collections.includes(collectionId)
      ? filters.collections.filter((id) => id !== collectionId)
      : [...filters.collections, collectionId];
    onFilterChange({ ...filters, collections: newCollections });
  };

  const handleAccessToggle = (level: AccessLevel) => {
    const newLevels = filters.accessLevels.includes(level)
      ? filters.accessLevels.filter((l) => l !== level)
      : [...filters.accessLevels, level];
    onFilterChange({ ...filters, accessLevels: newLevels });
  };

  const clearFilters = () => {
    onFilterChange({ search: '', collections: [], accessLevels: [] });
  };

  const hasActiveFilters =
    filters.search || filters.collections.length > 0 || filters.accessLevels.length > 0;

  // Group collections by category
  const collectionsByCategory = collections.reduce(
    (acc, col) => {
      const category = col.category_name || 'Other';
      if (!acc[category]) acc[category] = [];
      acc[category].push(col);
      return acc;
    },
    {} as Record<string, Collection[]>
  );

  const accessLevelOptions: { value: AccessLevel; label: string; color: string }[] = [
    { value: 'DOWNLOAD', label: 'Open Download', color: 'text-green-600' },
    { value: 'VIEW', label: 'View Only', color: 'text-blue-600' },
    { value: 'NONE', label: 'Restricted', color: 'text-gray-600' },
  ];

  if (isCollapsed && !isMobile) {
    return (
      <div class="w-12 border-r border-gray-200 bg-gray-50 flex flex-col items-center py-4">
        <button
          onClick={onToggleCollapse}
          class="p-2 rounded-lg hover:bg-gray-200 transition-colors"
          title="Expand filters"
        >
          <svg class="w-5 h-5 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
          </svg>
        </button>
        {hasActiveFilters && (
          <div class="mt-2 w-2 h-2 bg-primary-600 rounded-full" title="Filters active" />
        )}
      </div>
    );
  }

  return (
    <div class={`${isMobile ? 'h-full' : 'w-64 border-r border-gray-200'} bg-white flex flex-col`}>
      {/* Header */}
      <div class="flex items-center justify-between p-4 border-b border-gray-200">
        <h2 class="font-semibold text-gray-900">Filters</h2>
        <div class="flex items-center gap-2">
          {hasActiveFilters && (
            <button
              onClick={clearFilters}
              class="text-xs text-primary-600 hover:text-primary-700 font-medium"
            >
              Clear all
            </button>
          )}
          {isMobile ? (
            <button onClick={onClose} class="p-1 hover:bg-gray-100 rounded">
              <svg class="w-5 h-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          ) : (
            <button
              onClick={onToggleCollapse}
              class="p-1 hover:bg-gray-100 rounded"
              title="Collapse filters"
            >
              <svg class="w-5 h-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 19l-7-7 7-7m8 14l-7-7 7-7" />
              </svg>
            </button>
          )}
        </div>
      </div>

      {/* Search */}
      <div class="p-4 border-b border-gray-100">
        <div class="relative">
          <svg
            class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            value={filters.search}
            onInput={(e) => handleSearchChange((e.target as HTMLInputElement).value)}
            placeholder="Search datasets..."
            class="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          />
        </div>
      </div>

      {/* Filter sections */}
      <div class="flex-1 overflow-y-auto">
        {/* Access Level */}
        <div class="border-b border-gray-100">
          <button
            onClick={() => toggleSection('access')}
            class="w-full flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
          >
            <span class="font-medium text-gray-700 text-sm">Access Level</span>
            <svg
              class={`w-4 h-4 text-gray-400 transition-transform ${expandedSections.access ? 'rotate-180' : ''}`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          {expandedSections.access && (
            <div class="px-4 pb-4 space-y-2">
              {accessLevelOptions.map((option) => (
                <label key={option.value} class="flex items-center gap-2 cursor-pointer group">
                  <input
                    type="checkbox"
                    checked={filters.accessLevels.includes(option.value)}
                    onChange={() => handleAccessToggle(option.value)}
                    class="w-4 h-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  />
                  <span class={`text-sm group-hover:text-gray-900 ${option.color}`}>
                    {option.label}
                  </span>
                  {filters.accessLevels.includes(option.value) && (
                    <span class="ml-auto w-1.5 h-1.5 bg-primary-600 rounded-full" />
                  )}
                </label>
              ))}
            </div>
          )}
        </div>

        {/* Collections by Category */}
        <div class="border-b border-gray-100">
          <button
            onClick={() => toggleSection('collection')}
            class="w-full flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
          >
            <span class="font-medium text-gray-700 text-sm">Collection</span>
            <svg
              class={`w-4 h-4 text-gray-400 transition-transform ${expandedSections.collection ? 'rotate-180' : ''}`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          {expandedSections.collection && (
            <div class="px-4 pb-4 space-y-4">
              {Object.entries(collectionsByCategory).map(([category, cols]) => (
                <div key={category}>
                  <p class="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                    {category}
                  </p>
                  <div class="space-y-2">
                    {cols.map((col) => (
                      <label key={col.id} class="flex items-center gap-2 cursor-pointer group">
                        <input
                          type="checkbox"
                          checked={filters.collections.includes(col.id)}
                          onChange={() => handleCollectionToggle(col.id)}
                          class="w-4 h-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                        />
                        <span class="text-sm text-gray-600 group-hover:text-gray-900 truncate">
                          {col.collection_name}
                        </span>
                        {filters.collections.includes(col.id) && (
                          <span class="ml-auto w-1.5 h-1.5 bg-primary-600 rounded-full flex-shrink-0" />
                        )}
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Mobile apply button */}
      {isMobile && (
        <div class="p-4 border-t border-gray-200 bg-gray-50">
          <button onClick={onClose} class="w-full btn-primary py-2.5">
            Apply Filters
          </button>
        </div>
      )}
    </div>
  );
}
