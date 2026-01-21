import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';

interface Dataset {
  ds_id: string;
  title: string;
  description: string;
  collection?: {
    id: number;
    name: string;
    category: string;
  };
  data_owner?: {
    id: number;
    name: string;
  };
  spatial_coverage_region_id?: string;
  spatial_resolution?: string;
  temporal_coverage_start_date?: string;
  temporal_coverage_end_date?: string;
  temporal_resolution?: string;
  access_level: string;
  can_download: boolean;
  tags?: string[];
  additional_metadata?: Record<string, unknown>;
}

interface Props {
  datasetId: string;
}

export default function PublicDatasetDetail({ datasetId }: Props) {
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchDataset = async () => {
      setLoading(true);
      setError('');

      try {
        const response = await api.getPublicDataset(datasetId);
        setDataset(response as Dataset);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load dataset');
      } finally {
        setLoading(false);
      }
    };

    fetchDataset();
  }, [datasetId]);

  const formatDate = (date?: string) => {
    if (!date) return '-';
    return new Date(date).toLocaleDateString();
  };

  if (loading) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="animate-spin w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto" />
          <p class="mt-4 text-gray-500">Loading dataset...</p>
        </div>
      </div>
    );
  }

  if (error || !dataset) {
    return (
      <div class="card">
        <div class="card-body text-center py-12">
          <div class="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p class="text-red-600">{error || 'Dataset not found'}</p>
          <a href="/datasets" class="btn-secondary mt-4 inline-block">
            Back to datasets
          </a>
        </div>
      </div>
    );
  }

  return (
    <div class="space-y-6">
      {/* Header */}
      <div class="flex items-start justify-between">
        <div>
          <div class="flex items-center gap-3">
            <h1 class="text-2xl font-bold text-gray-900">{dataset.title}</h1>
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
          </div>
          <p class="mt-1 text-sm text-gray-500">ID: {dataset.ds_id}</p>
        </div>
        <a href="/datasets" class="btn-secondary">
          Back to datasets
        </a>
      </div>

      {/* Description */}
      <div class="card">
        <div class="card-body">
          <h2 class="text-lg font-medium text-gray-900 mb-2">Description</h2>
          <p class="text-gray-600">{dataset.description || 'No description available.'}</p>
        </div>
      </div>

      {/* Metadata */}
      <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div class="card">
          <div class="card-body">
            <h2 class="text-lg font-medium text-gray-900 mb-4">Details</h2>
            <dl class="space-y-3">
              <div>
                <dt class="text-sm font-medium text-gray-500">Collection</dt>
                <dd class="mt-1 text-sm text-gray-900">
                  {dataset.collection ? `${dataset.collection.name} (${dataset.collection.category})` : '-'}
                </dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500">Data Owner</dt>
                <dd class="mt-1 text-sm text-gray-900">{dataset.data_owner?.name || '-'}</dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500">Spatial Coverage</dt>
                <dd class="mt-1 text-sm text-gray-900">{dataset.spatial_coverage_region_id || '-'}</dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500">Spatial Resolution</dt>
                <dd class="mt-1 text-sm text-gray-900">{dataset.spatial_resolution || '-'}</dd>
              </div>
            </dl>
          </div>
        </div>

        <div class="card">
          <div class="card-body">
            <h2 class="text-lg font-medium text-gray-900 mb-4">Temporal Information</h2>
            <dl class="space-y-3">
              <div>
                <dt class="text-sm font-medium text-gray-500">Coverage Period</dt>
                <dd class="mt-1 text-sm text-gray-900">
                  {formatDate(dataset.temporal_coverage_start_date)} - {formatDate(dataset.temporal_coverage_end_date)}
                </dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500">Temporal Resolution</dt>
                <dd class="mt-1 text-sm text-gray-900">{dataset.temporal_resolution || '-'}</dd>
              </div>
            </dl>
          </div>
        </div>
      </div>

      {/* Tags */}
      {dataset.tags && dataset.tags.length > 0 && (
        <div class="card">
          <div class="card-body">
            <h2 class="text-lg font-medium text-gray-900 mb-2">Tags</h2>
            <div class="flex flex-wrap gap-2">
              {dataset.tags.map((tag) => (
                <span
                  key={tag}
                  class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800"
                >
                  {tag}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Login prompt for download */}
      <div class="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div class="flex items-start">
          <div class="flex-shrink-0">
            <svg class="h-5 w-5 text-blue-400" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
            </svg>
          </div>
          <div class="ml-3">
            <h3 class="text-sm font-medium text-blue-800">
              Want to download this dataset?
            </h3>
            <p class="mt-1 text-sm text-blue-700">
              <a href="/login" class="font-medium underline hover:text-blue-600">
                Sign in
              </a>{' '}
              to download this dataset. Registration is required to access download links.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
