import { useState, useMemo, useEffect } from 'preact/hooks';
import type { DatasetDetail, MetadataJson, TableMetadata } from '../../lib/types';
import CodeSnippets from './CodeSnippets';
import { marked } from 'marked';
import JSZip from 'jszip';
import api from '../../lib/api';

interface DatasetDetailPanelProps {
  dataset: DatasetDetail | null;
  loading: boolean;
  error: string | null;
  isAuthenticated?: boolean;
}

type TabId = 'about' | 'metadata' | 'readme' | 'code';

export default function DatasetDetailPanel({
  dataset,
  loading,
  error,
  isAuthenticated = false,
}: DatasetDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<TabId>('about');
  const [activeTableTab, setActiveTableTab] = useState<string | null>(null);
  const [metadataFormat, setMetadataFormat] = useState<'json' | 'yaml'>('json');
  const [downloading, setDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  // Parse the metadata JSON
  const parsedMetadata = useMemo<MetadataJson | null>(() => {
    if (!dataset?.data_dictionary_json) return null;
    try {
      return JSON.parse(dataset.data_dictionary_json) as MetadataJson;
    } catch {
      return null;
    }
  }, [dataset?.data_dictionary_json]);

  // Get table names from metadata
  const tableNames = useMemo(() => {
    if (!parsedMetadata?.tables) return [];
    return Object.keys(parsedMetadata.tables);
  }, [parsedMetadata]);

  // Set default active table when metadata loads
  useEffect(() => {
    if (tableNames.length > 0 && !activeTableTab) {
      setActiveTableTab(tableNames[0]);
    }
  }, [tableNames, activeTableTab]);

  // Parse README markdown
  const renderedReadme = useMemo(() => {
    if (!dataset?.readme_md) return null;
    try {
      return marked.parse(dataset.readme_md) as string;
    } catch {
      return null;
    }
  }, [dataset?.readme_md]);

  // Get active table metadata
  const activeTableMetadata = useMemo<TableMetadata | null>(() => {
    if (!parsedMetadata?.tables || !activeTableTab) return null;
    return parsedMetadata.tables[activeTableTab] || null;
  }, [parsedMetadata, activeTableTab]);

  // Build tabs dynamically based on available content
  const tabs = useMemo(() => {
    const result: { id: TabId; label: string; icon: string }[] = [
      { id: 'about', label: 'About', icon: 'M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z' },
    ];

    if (parsedMetadata?.tables && Object.keys(parsedMetadata.tables).length > 0) {
      result.push({ id: 'metadata', label: 'Data Dictionary', icon: 'M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253' });
    }

    if (dataset?.readme_md) {
      result.push({ id: 'readme', label: 'README', icon: 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z' });
    }

    result.push({ id: 'code', label: 'Code Snippets', icon: 'M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4' });

    return result;
  }, [parsedMetadata, dataset?.readme_md]);

  const formatDateRange = (startDate?: string, endDate?: string) => {
    if (!startDate && !endDate) return '—';

    const startYear = startDate ? new Date(startDate).getFullYear() : null;
    const endYear = endDate ? new Date(endDate).getFullYear() : null;

    if (startYear && endYear) {
      if (startYear === endYear) {
        return String(startYear);
      }
      return `${startYear} – ${endYear}`;
    }

    if (startYear) return `${startYear} – present`;
    if (endYear) return `Until ${endYear}`;

    return '—';
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

  // Convert JSON to YAML (simple implementation)
  const jsonToYaml = (obj: unknown, indent = 0): string => {
    const spaces = '  '.repeat(indent);

    if (obj === null || obj === undefined) {
      return 'null';
    }

    if (typeof obj === 'string') {
      if (obj.includes('\n') || obj.includes(':') || obj.includes('#')) {
        return `"${obj.replace(/"/g, '\\"')}"`;
      }
      return obj;
    }

    if (typeof obj === 'number' || typeof obj === 'boolean') {
      return String(obj);
    }

    if (Array.isArray(obj)) {
      if (obj.length === 0) return '[]';
      return obj.map(item => `${spaces}- ${jsonToYaml(item, indent + 1).trimStart()}`).join('\n');
    }

    if (typeof obj === 'object') {
      const entries = Object.entries(obj as Record<string, unknown>);
      if (entries.length === 0) return '{}';
      return entries.map(([key, value]) => {
        const valueStr = jsonToYaml(value, indent + 1);
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          return `${spaces}${key}:\n${valueStr}`;
        }
        return `${spaces}${key}: ${valueStr}`;
      }).join('\n');
    }

    return String(obj);
  };

  // Generate zip filename: {0080:last_four_dataset_id_digits}-{ds_name}.zip, all lower
  const getZipFilename = () => {
    if (!dataset) return 'dataset.zip';
    const lastFour = dataset.ds_id.slice(-4).padStart(4, '0');
    const safeName = dataset.title
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
      .substring(0, 50);
    return `${lastFour}-${safeName}.zip`;
  };

  // Get folder name for zip contents (same as zip filename without .zip)
  const getFolderName = () => {
    if (!dataset) return 'dataset';
    const lastFour = dataset.ds_id.slice(-4).padStart(4, '0');
    const safeName = dataset.title
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
      .substring(0, 50);
    return `${lastFour}-${safeName}`;
  };

  // Download dataset as zip
  const downloadDataset = async () => {
    if (!dataset || !isAuthenticated) return;

    setDownloading(true);
    setDownloadError(null);

    try {
      // Get download URLs from API
      const downloadData = await api.getDatasetDownloadUrls(dataset.ds_id);

      const zip = new JSZip();
      const folderName = getFolderName();
      const folder = zip.folder(folderName);

      if (!folder) {
        throw new Error('Failed to create zip folder');
      }

      // Add README.md if available
      if (downloadData.readme_md) {
        folder.file('README.md', downloadData.readme_md);
      }

      // Add metadata file (json or yaml based on user preference)
      if (downloadData.data_dictionary_json) {
        if (metadataFormat === 'json') {
          // Pretty print the JSON
          try {
            const parsed = JSON.parse(downloadData.data_dictionary_json);
            folder.file('metadata.json', JSON.stringify(parsed, null, 2));
          } catch {
            folder.file('metadata.json', downloadData.data_dictionary_json);
          }
        } else {
          try {
            const parsed = JSON.parse(downloadData.data_dictionary_json);
            folder.file('metadata.yaml', jsonToYaml(parsed));
          } catch {
            // Fall back to JSON if YAML conversion fails
            folder.file('metadata.json', downloadData.data_dictionary_json);
          }
        }
      }

      // Check if we have tables to download
      if (!downloadData.tables || downloadData.tables.length === 0) {
        throw new Error('No data files available for download. Please try again later or contact support.');
      }

      // Download and add each table file
      const tableResults = await Promise.allSettled(
        downloadData.tables.map(async (table) => {
          const response = await fetch(table.download_url);
          if (!response.ok) {
            throw new Error(`Failed to download ${table.table_name}: HTTP ${response.status}`);
          }
          const blob = await response.blob();

          // Determine file extension from URL or content type
          let extension = '.csv';
          const contentType = response.headers.get('content-type');
          if (contentType?.includes('parquet')) {
            extension = '.parquet';
          } else if (table.download_url.includes('.parquet')) {
            extension = '.parquet';
          }

          const filename = `${table.table_name}${extension}`;
          folder.file(filename, blob);
          return { table: table.table_name, success: true };
        })
      );

      // Check for failed downloads and report to user
      const failures = tableResults.filter(r => r.status === 'rejected') as PromiseRejectedResult[];
      if (failures.length > 0) {
        const failureReasons = failures.map(f => f.reason?.message || 'Unknown error').join(', ');
        console.error('Some table downloads failed:', failureReasons);
        // If all downloads failed, throw an error
        if (failures.length === downloadData.tables.length) {
          throw new Error(`Failed to download data files: ${failureReasons}. This may be a CORS issue or expired links.`);
        }
        // If some succeeded, continue but log the warning
      }

      // Generate and download the zip
      const zipBlob = await zip.generateAsync({ type: 'blob' });
      const url = URL.createObjectURL(zipBlob);
      const a = document.createElement('a');
      a.href = url;
      a.download = getZipFilename();
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Download failed:', err);
      setDownloadError(err instanceof Error ? err.message : 'Download failed');
    } finally {
      setDownloading(false);
    }
  };

  // Download metadata only
  const downloadMetadataOnly = () => {
    if (!dataset || !dataset.data_dictionary_json) return;

    let content: string;
    let filename: string;
    let mimeType: string;

    const safeTitle = dataset.title.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);

    if (metadataFormat === 'json') {
      try {
        const parsed = JSON.parse(dataset.data_dictionary_json);
        content = JSON.stringify(parsed, null, 2);
      } catch {
        content = dataset.data_dictionary_json;
      }
      filename = `${dataset.ds_id}_${safeTitle}_metadata.json`;
      mimeType = 'application/json';
    } else {
      try {
        const parsed = JSON.parse(dataset.data_dictionary_json);
        content = jsonToYaml(parsed);
        filename = `${dataset.ds_id}_${safeTitle}_metadata.yaml`;
        mimeType = 'text/yaml';
      } catch {
        return;
      }
    }

    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
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
    <div class="h-full flex flex-col bg-white rounded-xl border border-gray-200 overflow-hidden shadow-sm">
      {/* Header with download button */}
      <div class="px-6 py-5 border-b border-gray-200 bg-white">
        <div class="flex items-start justify-between gap-4">
          <div class="min-w-0 flex-1">
            <div class="flex items-center gap-3 mb-1">
              <h2 class="text-xl font-semibold text-gray-900 leading-tight">{dataset.title}</h2>
              <span class={`flex-shrink-0 px-2.5 py-1 rounded-full text-xs font-semibold ${accessBadge.bg} ${accessBadge.text}`}>
                {accessBadge.label}
              </span>
            </div>
            <p class="text-sm text-gray-500 font-mono">{dataset.ds_id}</p>
          </div>
          {/* Download button in header */}
          <div class="flex-shrink-0">
            {canDownload ? (
              <button
                onClick={downloadDataset}
                disabled={downloading}
                class="flex items-center gap-2 py-2.5 px-5 bg-primary-600 text-white rounded-lg text-sm font-semibold hover:bg-primary-700 transition-colors shadow-sm disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {downloading ? (
                  <>
                    <div class="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                    Downloading...
                  </>
                ) : (
                  <>
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                    </svg>
                    Download
                  </>
                )}
              </button>
            ) : isAuthenticated ? (
              <span class="inline-flex items-center px-4 py-2.5 bg-gray-100 text-gray-500 rounded-lg text-sm">
                Download unavailable
              </span>
            ) : (
              <a
                href="/login"
                class="inline-flex items-center gap-2 py-2.5 px-5 bg-primary-600 text-white rounded-lg text-sm font-semibold hover:bg-primary-700 transition-colors shadow-sm"
              >
                Sign in to download
              </a>
            )}
          </div>
        </div>
        {/* Download error message */}
        {downloadError && (
          <div class="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600">
            {downloadError}
          </div>
        )}
      </div>

      {/* Tabs */}
      <div class="flex border-b border-gray-200 px-4 bg-gray-50/50">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            class={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? 'text-primary-600 border-b-2 border-primary-600 -mb-px bg-white'
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
      <div class="flex-1 overflow-y-auto p-6">
        {activeTab === 'about' && (
          <div class="space-y-6">
            {/* Description */}
            {dataset.description && (
              <div>
                <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Description</h3>
                <p class="text-sm text-gray-700 leading-relaxed">{dataset.description}</p>
              </div>
            )}

            {/* Metadata grid */}
            <div>
              <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Details</h3>
              <dl class="grid grid-cols-2 gap-4">
                <div class="bg-gray-50 rounded-xl p-4">
                  <dt class="text-xs text-gray-500 font-medium">Collection</dt>
                  <dd class="text-sm font-semibold text-gray-900 mt-1">
                    {dataset.collection?.name || '—'}
                  </dd>
                  {dataset.collection?.category && (
                    <dd class="text-xs text-gray-500 mt-0.5">{dataset.collection.category}</dd>
                  )}
                </div>
                <div class="bg-gray-50 rounded-xl p-4">
                  <dt class="text-xs text-gray-500 font-medium">Data Owner</dt>
                  <dd class="text-sm font-semibold text-gray-900 mt-1">
                    {dataset.data_owner?.name || '—'}
                  </dd>
                </div>
                <div class="bg-gray-50 rounded-xl p-4">
                  <dt class="text-xs text-gray-500 font-medium">Temporal Coverage</dt>
                  <dd class="text-sm font-semibold text-gray-900 mt-1">
                    {formatDateRange(dataset.temporal_coverage_start_date, dataset.temporal_coverage_end_date)}
                  </dd>
                  {dataset.temporal_resolution && dataset.temporal_resolution !== 'NONE' && (
                    <dd class="text-xs text-gray-500 mt-0.5">{dataset.temporal_resolution.toLowerCase()} resolution</dd>
                  )}
                </div>
                <div class="bg-gray-50 rounded-xl p-4">
                  <dt class="text-xs text-gray-500 font-medium">Spatial Coverage</dt>
                  <dd class="text-sm font-semibold text-gray-900 mt-1">
                    {dataset.spatial_coverage_region_id || '—'}
                  </dd>
                  {dataset.spatial_resolution && (
                    <dd class="text-xs text-gray-500 mt-0.5">{dataset.spatial_resolution.toLowerCase()} level</dd>
                  )}
                </div>
              </dl>
            </div>

            {/* Available Tables from metadata */}
            {tableNames.length > 0 && (
              <div>
                <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">
                  Available Tables ({tableNames.length})
                </h3>
                <div class="flex flex-wrap gap-2">
                  {tableNames.map((tableName) => (
                    <span
                      key={tableName}
                      class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 text-gray-700 rounded-lg text-sm font-medium"
                    >
                      <svg class="w-3.5 h-3.5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
                      </svg>
                      {tableName}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Tags */}
            {dataset.tags && dataset.tags.length > 0 && (
              <div>
                <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Tags</h3>
                <div class="flex flex-wrap gap-2">
                  {dataset.tags.map((tag) => (
                    <span
                      key={tag}
                      class="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm font-medium"
                    >
                      {tag}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Login prompt for unauthenticated users */}
            {!isAuthenticated && (
              <div class="bg-blue-50 border border-blue-200 rounded-xl p-4">
                <div class="flex items-start gap-3">
                  <svg class="w-5 h-5 text-blue-500 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                  </svg>
                  <div>
                    <p class="text-sm text-blue-800 font-semibold">Sign in for more</p>
                    <p class="text-sm text-blue-700 mt-1">
                      <a href="/login" class="underline hover:text-blue-600">Sign in</a> to view file details and download datasets.
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'metadata' && parsedMetadata?.tables && (
          <div class="space-y-4">
            {/* Download button and format toggle */}
            <div class="flex items-center justify-between">
              <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider">Data Dictionary</h3>
              <div class="flex items-center gap-2">
                <div class="flex items-center bg-gray-100 rounded-lg p-1">
                  <button
                    onClick={() => setMetadataFormat('json')}
                    class={`px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
                      metadataFormat === 'json'
                        ? 'bg-white text-gray-900 shadow-sm'
                        : 'text-gray-500 hover:text-gray-700'
                    }`}
                  >
                    JSON
                  </button>
                  <button
                    onClick={() => setMetadataFormat('yaml')}
                    class={`px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
                      metadataFormat === 'yaml'
                        ? 'bg-white text-gray-900 shadow-sm'
                        : 'text-gray-500 hover:text-gray-700'
                    }`}
                  >
                    YAML
                  </button>
                </div>
                <button
                  onClick={downloadMetadataOnly}
                  class="flex items-center gap-1.5 px-3 py-1.5 bg-primary-600 text-white rounded-lg text-xs font-medium hover:bg-primary-700 transition-colors"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                  </svg>
                  Download
                </button>
              </div>
            </div>

            {/* Table tabs */}
            {tableNames.length > 1 && (
              <div class="flex gap-1 p-1 bg-gray-100 rounded-lg overflow-x-auto">
                {tableNames.map((tableName) => (
                  <button
                    key={tableName}
                    onClick={() => setActiveTableTab(tableName)}
                    class={`flex-shrink-0 px-3 py-2 text-xs font-medium rounded-md transition-colors whitespace-nowrap ${
                      activeTableTab === tableName
                        ? 'bg-white text-gray-900 shadow-sm'
                        : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                    }`}
                  >
                    {tableName}
                  </button>
                ))}
              </div>
            )}

            {/* Active table metadata */}
            {activeTableMetadata && (
              <div class="space-y-4">
                {/* Table info */}
                <div class="bg-gray-50 rounded-xl p-4 space-y-2">
                  <div class="flex items-center gap-2">
                    <svg class="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
                    </svg>
                    <span class="font-semibold text-gray-900">{activeTableMetadata.table_name}</span>
                  </div>
                  {activeTableMetadata.description && (
                    <p class="text-sm text-gray-600">{activeTableMetadata.description}</p>
                  )}
                  {activeTableMetadata.source && (
                    <p class="text-xs text-gray-500">
                      <span class="font-medium">Source:</span> {activeTableMetadata.source}
                    </p>
                  )}
                </div>

                {/* Data dictionary table */}
                {activeTableMetadata.data_dictionary && Object.keys(activeTableMetadata.data_dictionary).length > 0 && (
                  <div class="border border-gray-200 rounded-xl overflow-hidden">
                    <div class="bg-gray-50 px-4 py-2 border-b border-gray-200">
                      <span class="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                        Fields ({Object.keys(activeTableMetadata.data_dictionary).length})
                      </span>
                    </div>
                    <div class="overflow-x-auto">
                      <table class="w-full text-sm">
                        <thead class="bg-gray-50 border-b border-gray-200">
                          <tr>
                            <th class="px-4 py-2.5 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Field</th>
                            <th class="px-4 py-2.5 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Description</th>
                            <th class="px-4 py-2.5 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Comments</th>
                          </tr>
                        </thead>
                        <tbody class="divide-y divide-gray-100">
                          {Object.entries(activeTableMetadata.data_dictionary).map(([fieldName, fieldInfo]) => (
                            <tr key={fieldName} class="hover:bg-gray-50">
                              <td class="px-4 py-3">
                                <code class="text-xs bg-gray-100 px-1.5 py-0.5 rounded font-mono text-gray-800">{fieldName}</code>
                              </td>
                              <td class="px-4 py-3 text-gray-700">
                                {fieldInfo.description || <span class="text-gray-400 italic">—</span>}
                              </td>
                              <td class="px-4 py-3 text-gray-500 text-xs">
                                {fieldInfo.comments || <span class="text-gray-400 italic">—</span>}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {activeTab === 'readme' && renderedReadme && (
          <div class="prose prose-sm prose-gray max-w-none">
            <div
              class="readme-content"
              dangerouslySetInnerHTML={{ __html: renderedReadme }}
            />
          </div>
        )}

        {activeTab === 'code' && (
          <div class="h-full min-h-[300px]">
            <CodeSnippets datasetId={dataset.ds_id} />
          </div>
        )}
      </div>

    </div>
  );
}
