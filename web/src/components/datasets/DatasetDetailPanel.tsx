import { useState } from 'preact/hooks';
import type { DatasetDetail } from '../../lib/types';
import CodeSnippets from './CodeSnippets';

interface DatasetDetailPanelProps {
  dataset: DatasetDetail | null;
  loading: boolean;
  error: string | null;
  isAuthenticated?: boolean;
}

type TabId = 'info' | 'schema' | 'code';

export default function DatasetDetailPanel({
  dataset,
  loading,
  error,
  isAuthenticated = false,
}: DatasetDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<TabId>('info');

  const tabs: { id: TabId; label: string; icon: string }[] = [
    { id: 'info', label: 'Info', icon: 'M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z' },
    { id: 'schema', label: 'Schema', icon: 'M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4' },
    { id: 'code', label: 'Code', icon: 'M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4' },
  ];

  const formatDate = (date?: string) => {
    if (!date) return '—';
    return new Date(date).toLocaleDateString('en-IN', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  const getAccessBadge = (level: string) => {
    switch (level) {
      case 'DOWNLOAD':
        return { bg: 'bg-green-100', text: 'text-green-700', label: 'Open Download' };
      case 'VIEW':
        return { bg: 'bg-blue-100', text: 'text-blue-700', label: 'View Only' };
      default:
        return { bg: 'bg-gray-100', text: 'text-gray-600', label: 'Restricted' };
    }
  };

  // Empty state
  if (!dataset && !loading && !error) {
    return (
      <div class="h-full flex items-center justify-center bg-gray-50 rounded-lg border border-gray-200">
        <div class="text-center p-8">
          <div class="w-16 h-16 mx-auto mb-4 rounded-full bg-gray-100 flex items-center justify-center">
            <svg class="w-8 h-8 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
            </svg>
          </div>
          <h3 class="text-gray-900 font-medium mb-1">Select a dataset</h3>
          <p class="text-sm text-gray-500">
            Choose a dataset from the list to view its details
          </p>
        </div>
      </div>
    );
  }

  // Loading state
  if (loading) {
    return (
      <div class="h-full flex items-center justify-center bg-white rounded-lg border border-gray-200">
        <div class="text-center">
          <div class="w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto" />
          <p class="mt-3 text-sm text-gray-500">Loading dataset details...</p>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div class="h-full flex items-center justify-center bg-white rounded-lg border border-gray-200">
        <div class="text-center p-8">
          <div class="w-12 h-12 mx-auto mb-4 rounded-full bg-red-100 flex items-center justify-center">
            <svg class="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p class="text-red-600 text-sm">{error}</p>
        </div>
      </div>
    );
  }

  if (!dataset) return null;

  const accessBadge = getAccessBadge(dataset.access_level);
  const canDownload = isAuthenticated && dataset.can_download;

  return (
    <div class="h-full flex flex-col bg-white rounded-lg border border-gray-200 overflow-hidden">
      {/* Header */}
      <div class="p-4 border-b border-gray-200">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <h2 class="text-lg font-semibold text-gray-900 truncate">{dataset.title}</h2>
            <p class="text-xs text-gray-500 mt-0.5 font-mono">{dataset.ds_id}</p>
          </div>
          <span class={`flex-shrink-0 px-2.5 py-1 rounded-full text-xs font-medium ${accessBadge.bg} ${accessBadge.text}`}>
            {accessBadge.label}
          </span>
        </div>
      </div>

      {/* Tabs */}
      <div class="flex border-b border-gray-200 px-2">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            class={`flex items-center gap-1.5 px-3 py-2.5 text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? 'text-primary-600 border-b-2 border-primary-600 -mb-px'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={tab.icon} />
            </svg>
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div class="flex-1 overflow-y-auto p-4">
        {activeTab === 'info' && (
          <div class="space-y-5">
            {/* Description */}
            {dataset.description && (
              <div>
                <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Description</h3>
                <p class="text-sm text-gray-700 leading-relaxed">{dataset.description}</p>
              </div>
            )}

            {/* Metadata grid */}
            <div>
              <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Details</h3>
              <dl class="grid grid-cols-2 gap-3">
                <div class="bg-gray-50 rounded-lg p-3">
                  <dt class="text-xs text-gray-500">Collection</dt>
                  <dd class="text-sm font-medium text-gray-900 mt-0.5">
                    {dataset.collection?.name || '—'}
                  </dd>
                  {dataset.collection?.category && (
                    <dd class="text-xs text-gray-500">{dataset.collection.category}</dd>
                  )}
                </div>
                <div class="bg-gray-50 rounded-lg p-3">
                  <dt class="text-xs text-gray-500">Data Owner</dt>
                  <dd class="text-sm font-medium text-gray-900 mt-0.5">
                    {dataset.data_owner?.name || '—'}
                  </dd>
                </div>
                <div class="bg-gray-50 rounded-lg p-3">
                  <dt class="text-xs text-gray-500">Temporal Coverage</dt>
                  <dd class="text-sm font-medium text-gray-900 mt-0.5">
                    {formatDate(dataset.temporal_coverage_start_date)} – {formatDate(dataset.temporal_coverage_end_date)}
                  </dd>
                  {dataset.temporal_resolution && dataset.temporal_resolution !== 'NONE' && (
                    <dd class="text-xs text-gray-500">{dataset.temporal_resolution.toLowerCase()} resolution</dd>
                  )}
                </div>
                <div class="bg-gray-50 rounded-lg p-3">
                  <dt class="text-xs text-gray-500">Spatial Coverage</dt>
                  <dd class="text-sm font-medium text-gray-900 mt-0.5">
                    {dataset.spatial_coverage_region_id || '—'}
                  </dd>
                  {dataset.spatial_resolution && (
                    <dd class="text-xs text-gray-500">{dataset.spatial_resolution.toLowerCase()} level</dd>
                  )}
                </div>
              </dl>
            </div>

            {/* Tags */}
            {dataset.tags && dataset.tags.length > 0 && (
              <div>
                <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Tags</h3>
                <div class="flex flex-wrap gap-1.5">
                  {dataset.tags.map((tag) => (
                    <span
                      key={tag}
                      class="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs"
                    >
                      {tag}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Login prompt for unauthenticated users */}
            {!isAuthenticated && (
              <div class="bg-blue-50 border border-blue-200 rounded-lg p-3">
                <div class="flex items-start gap-2">
                  <svg class="w-4 h-4 text-blue-500 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                  </svg>
                  <div>
                    <p class="text-sm text-blue-800 font-medium">Sign in for more</p>
                    <p class="text-xs text-blue-700 mt-0.5">
                      <a href="/login" class="underline hover:text-blue-600">Sign in</a> to view file details and download datasets.
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'schema' && (
          <div class="space-y-4">
            {dataset.raw_datasets && dataset.raw_datasets.length > 0 ? (
              <>
                <div>
                  <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                    Available Files ({dataset.raw_datasets.length})
                  </h3>
                  <div class="space-y-2">
                    {dataset.raw_datasets.map((rd) => (
                      <div
                        key={rd.id}
                        class="flex items-center gap-3 p-3 bg-gray-50 rounded-lg"
                      >
                        <div class="w-8 h-8 rounded bg-gray-200 flex items-center justify-center flex-shrink-0">
                          <svg class="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                          </svg>
                        </div>
                        <div class="min-w-0 flex-1">
                          <p class="text-sm font-medium text-gray-900 truncate">{rd.title}</p>
                          <p class="text-xs text-gray-500 font-mono">{rd.rds_id}</p>
                        </div>
                        {canDownload && rd.source && (
                          <a
                            href={rd.source}
                            target="_blank"
                            rel="noopener noreferrer"
                            class="flex-shrink-0 p-2 text-primary-600 hover:bg-primary-50 rounded-lg transition-colors"
                            title="Download"
                          >
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                            </svg>
                          </a>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </>
            ) : (
              <div class="text-center py-8">
                <div class="w-12 h-12 mx-auto mb-3 rounded-full bg-gray-100 flex items-center justify-center">
                  <svg class="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                  </svg>
                </div>
                {isAuthenticated ? (
                  <>
                    <p class="text-sm text-gray-600 font-medium">No files available</p>
                    <p class="text-xs text-gray-500 mt-1">
                      This dataset doesn't have downloadable files yet
                    </p>
                  </>
                ) : (
                  <>
                    <p class="text-sm text-gray-600 font-medium">Sign in to view files</p>
                    <p class="text-xs text-gray-500 mt-1">
                      <a href="/login" class="text-primary-600 hover:text-primary-700 underline">Sign in</a> to view file details and download data
                    </p>
                  </>
                )}
              </div>
            )}

            {/* Data dictionary info */}
            {dataset.additional_metadata && Object.keys(dataset.additional_metadata).length > 0 && (
              <div>
                <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                  Additional Metadata
                </h3>
                <div class="bg-gray-50 rounded-lg p-3">
                  <pre class="text-xs text-gray-600 overflow-x-auto">
                    {JSON.stringify(dataset.additional_metadata, null, 2)}
                  </pre>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'code' && (
          <div class="h-full min-h-[300px]">
            <CodeSnippets datasetId={dataset.ds_id} />
          </div>
        )}
      </div>

      {/* Footer */}
      <div class="p-4 border-t border-gray-200 bg-gray-50">
        <a
          href={`/datasets/detail?id=${dataset.ds_id}`}
          class="block w-full text-center py-2 px-4 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700 transition-colors"
        >
          View Full Details
        </a>
      </div>
    </div>
  );
}
