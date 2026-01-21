import type { Dataset } from '../../lib/types';

interface DatasetListItemProps {
  dataset: Dataset;
  isSelected: boolean;
  onClick: () => void;
  isMobile?: boolean;
  isExpanded?: boolean;
  onToggleExpand?: () => void;
}

export default function DatasetListItem({
  dataset,
  isSelected,
  onClick,
  isMobile = false,
  isExpanded = false,
  onToggleExpand,
}: DatasetListItemProps) {
  const formatDateRange = () => {
    if (!dataset.temporal_coverage_start_date && !dataset.temporal_coverage_end_date) {
      return null;
    }
    const start = dataset.temporal_coverage_start_date
      ? new Date(dataset.temporal_coverage_start_date).getFullYear()
      : '...';
    const end = dataset.temporal_coverage_end_date
      ? new Date(dataset.temporal_coverage_end_date).getFullYear()
      : 'Present';
    return `${start}–${end}`;
  };

  const getAccessBadge = () => {
    switch (dataset.access_level) {
      case 'DOWNLOAD':
        return { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' };
      case 'VIEW':
        return { bg: 'bg-blue-100', text: 'text-blue-700', dot: 'bg-blue-500' };
      default:
        return { bg: 'bg-gray-100', text: 'text-gray-600', dot: 'bg-gray-400' };
    }
  };

  const badge = getAccessBadge();
  const dateRange = formatDateRange();

  if (isMobile) {
    return (
      <div
        class={`border-b border-gray-100 ${isExpanded ? 'bg-gray-50' : 'bg-white'}`}
      >
        <button
          onClick={onToggleExpand}
          class="w-full text-left p-4 hover:bg-gray-50 transition-colors"
        >
          <div class="flex items-start gap-3">
            <div class={`w-2 h-2 rounded-full mt-2 flex-shrink-0 ${badge.dot}`} />
            <div class="flex-1 min-w-0">
              <h3 class="font-medium text-gray-900 truncate">{dataset.title}</h3>
              <div class="flex items-center gap-2 mt-1 text-sm text-gray-500">
                <span class="truncate">{dataset.collection_name}</span>
                {dateRange && (
                  <>
                    <span class="text-gray-300">·</span>
                    <span class="flex-shrink-0">{dateRange}</span>
                  </>
                )}
              </div>
            </div>
            <svg
              class={`w-5 h-5 text-gray-400 transition-transform flex-shrink-0 ${isExpanded ? 'rotate-180' : ''}`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </button>

        {/* Expanded content for mobile */}
        {isExpanded && (
          <div class="px-4 pb-4">
            <div class="ml-5 space-y-3">
              {dataset.description && (
                <p class="text-sm text-gray-600">{dataset.description}</p>
              )}
              <div class="flex flex-wrap gap-2 text-xs">
                <span class={`px-2 py-1 rounded-full ${badge.bg} ${badge.text}`}>
                  {dataset.access_level === 'DOWNLOAD' ? 'Open' : dataset.access_level === 'VIEW' ? 'View Only' : 'Restricted'}
                </span>
                <span class="px-2 py-1 rounded-full bg-gray-100 text-gray-600">
                  {dataset.data_owner_name}
                </span>
              </div>
              <a
                href={`/datasets/detail?id=${dataset.ds_id}`}
                class="inline-flex items-center text-sm text-primary-600 hover:text-primary-700 font-medium"
              >
                View details
                <svg class="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </a>
            </div>
          </div>
        )}
      </div>
    );
  }

  // Desktop compact list item
  return (
    <button
      onClick={onClick}
      class={`w-full text-left px-5 py-4 border-b border-gray-100 transition-all duration-150 ${
        isSelected
          ? 'bg-primary-50 border-l-3 border-l-primary-600'
          : 'hover:bg-gray-50/80 border-l-3 border-l-transparent'
      }`}
    >
      <div class="flex items-start gap-3">
        <div class={`w-2.5 h-2.5 rounded-full mt-1.5 flex-shrink-0 ${badge.dot}`} />
        <div class="flex-1 min-w-0">
          <h3
            class={`font-medium leading-snug ${isSelected ? 'text-primary-900' : 'text-gray-900'}`}
          >
            {dataset.title}
          </h3>
          <div class="flex items-center gap-2 mt-1 text-sm text-gray-500">
            <span class="truncate">{dataset.collection_name}</span>
            {dateRange && (
              <>
                <span class="text-gray-300">·</span>
                <span class="flex-shrink-0">{dateRange}</span>
              </>
            )}
          </div>
        </div>
        {isSelected && (
          <svg class="w-5 h-5 text-primary-600 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
        )}
      </div>
    </button>
  );
}
