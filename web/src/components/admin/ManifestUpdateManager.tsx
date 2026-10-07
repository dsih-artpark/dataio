import { useEffect, useState } from 'preact/hooks';

import { ApiRequestError, api } from '../../lib/api';
import type { AdminDatasetSummary, AdminManifestRecord, ValidationFinding } from '../../lib/types';

const STANDARDISED_BUCKET = 'STANDARDISED';

function severityClasses(severity: string) {
  if (severity === 'error') return 'bg-red-50 text-red-700 ring-red-200';
  if (severity === 'warning') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-slate-50 text-slate-700 ring-slate-200';
}

function findingLabel(finding: ValidationFinding) {
  const parts = [
    finding.table ? `table ${finding.table}` : null,
    typeof finding.row === 'number' ? `row ${finding.row}` : null,
    finding.field ? `field ${finding.field}` : null,
    finding.path ? `path ${finding.path}` : null,
  ].filter(Boolean);

  return parts.join(' • ');
}

export default function ManifestUpdateManager() {
  const [datasets, setDatasets] = useState<AdminDatasetSummary[]>([]);
  const [datasetSearch, setDatasetSearch] = useState('');
  const [selectedDatasetId, setSelectedDatasetId] = useState('');
  const [loadingDatasets, setLoadingDatasets] = useState(true);
  const [loadingManifest, setLoadingManifest] = useState(false);
  const [uploadingManifest, setUploadingManifest] = useState(false);
  const [manifestRecord, setManifestRecord] = useState<AdminManifestRecord | null>(null);
  const [manifestFile, setManifestFile] = useState<File | null>(null);
  const [manifestError, setManifestError] = useState('');
  const [manifestSuccess, setManifestSuccess] = useState('');
  const [manifestUploadFindings, setManifestUploadFindings] = useState<ValidationFinding[]>([]);

  const fetchDatasets = async (search?: string) => {
    setLoadingDatasets(true);
    try {
      const response = await api.adminListDatasets({ search, limit: 50, offset: 0 });
      setDatasets(response.datasets);
      if (!selectedDatasetId && response.datasets.length > 0) {
        setSelectedDatasetId(response.datasets[0].ds_id);
      }
    } catch (err) {
      setManifestError(err instanceof Error ? err.message : 'Failed to load datasets');
    } finally {
      setLoadingDatasets(false);
    }
  };

  const loadManifest = async () => {
    if (!selectedDatasetId) return;

    setLoadingManifest(true);
    setManifestError('');
    setManifestSuccess('');
    try {
      const response = await api.adminGetManifest(selectedDatasetId, STANDARDISED_BUCKET);
      setManifestRecord(response);
    } catch (err) {
      setManifestRecord(null);
      setManifestError(err instanceof Error ? err.message : 'Failed to load manifest');
    } finally {
      setLoadingManifest(false);
    }
  };

  useEffect(() => {
    fetchDatasets();
  }, []);

  useEffect(() => {
    if (selectedDatasetId) {
      loadManifest();
    }
  }, [selectedDatasetId]);

  const handleManifestSearch = async (e: Event) => {
    e.preventDefault();
    await fetchDatasets(datasetSearch || undefined);
  };

  const handleManifestUpload = async (e: Event) => {
    e.preventDefault();
    if (!selectedDatasetId || !manifestFile) return;

    setUploadingManifest(true);
    setManifestError('');
    setManifestSuccess('');
    setManifestUploadFindings([]);

    try {
      await api.adminUploadManifest(selectedDatasetId, STANDARDISED_BUCKET, manifestFile);
      setManifestFile(null);
      setManifestSuccess('Manifest uploaded and cached successfully for the standardised dataset.');
      await loadManifest();
    } catch (err) {
      if (err instanceof ApiRequestError) {
        setManifestError(err.message);
        const detailData = err.detailData as { findings?: ValidationFinding[] } | undefined;
        setManifestUploadFindings(detailData?.findings ?? []);
      } else {
        setManifestError(err instanceof Error ? err.message : 'Failed to upload manifest');
      }
    } finally {
      setUploadingManifest(false);
    }
  };

  return (
    <section class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
      <div class="flex items-start justify-between gap-4">
        <div>
          <h2 class="text-lg font-semibold text-slate-900">Canonical Manifest</h2>
          <p class="mt-1 text-sm text-slate-600">
            Review and replace the canonical <code class="mx-1 rounded bg-slate-100 px-1.5 py-0.5 text-xs">manifest.yaml</code>{' '}
            for the <strong>standardised</strong> version of a dataset.
          </p>
          <p class="mt-2 text-sm text-slate-500">
            Promotion is blocked unless the stored standardised data also validates against the uploaded manifest.
          </p>
        </div>
        <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
          Standardised only
        </span>
      </div>

      <form class="mt-6 grid gap-4 md:grid-cols-[1fr_180px]" onSubmit={handleManifestSearch}>
        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Search datasets</span>
          <input
            value={datasetSearch}
            onInput={(e) => setDatasetSearch((e.currentTarget as HTMLInputElement).value)}
            placeholder="Dataset ID or title"
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-slate-500 focus:outline-none focus:ring-2 focus:ring-slate-200"
          />
        </label>

        <button
          type="submit"
          class="mt-7 rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800"
        >
          Search
        </button>
      </form>

      <div class="mt-4 grid gap-4 md:grid-cols-[1fr_auto]">
        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Dataset</span>
          <select
            value={selectedDatasetId}
            onChange={(e) => setSelectedDatasetId((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-slate-500 focus:outline-none focus:ring-2 focus:ring-slate-200"
            disabled={loadingDatasets || datasets.length === 0}
          >
            {datasets.length === 0 ? (
              <option value="">No datasets found</option>
            ) : (
              datasets.map((dataset) => (
                <option key={dataset.ds_id} value={dataset.ds_id}>
                  {dataset.ds_id} - {dataset.title}
                </option>
              ))
            )}
          </select>
        </label>

        <button
          type="button"
          onClick={() => loadManifest()}
          disabled={!selectedDatasetId || loadingManifest}
          class="mt-7 rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {loadingManifest ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      {manifestError ? (
        <div class="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {manifestError}
        </div>
      ) : null}
      {manifestSuccess ? (
        <div class="mt-4 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
          {manifestSuccess}
        </div>
      ) : null}

      <div class="mt-6 rounded-2xl border border-slate-200 bg-slate-50 p-4">
        <div class="flex flex-wrap items-center gap-3 text-sm text-slate-600">
          <span class="rounded-full bg-white px-3 py-1 ring-1 ring-slate-200">
            {manifestRecord?.has_manifest ? 'Manifest present' : 'No manifest stored'}
          </span>
          <span class="rounded-full bg-white px-3 py-1 ring-1 ring-slate-200">Bucket: STANDARDISED</span>
          {manifestRecord?.manifest_updated_at ? (
            <span>Updated {new Date(manifestRecord.manifest_updated_at).toLocaleString()}</span>
          ) : null}
          {manifestRecord?.manifest_updated_by ? (
            <span>by {manifestRecord.manifest_updated_by}</span>
          ) : null}
        </div>

        <pre class="mt-4 max-h-[28rem] overflow-auto rounded-xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
          {manifestRecord?.manifest_yaml || '# No canonical manifest is currently stored for this standardised dataset.'}
        </pre>
      </div>

      <form class="mt-6 rounded-2xl border border-slate-200 p-4" onSubmit={handleManifestUpload}>
        <div class="flex flex-wrap items-end gap-4">
          <label class="block flex-1">
            <span class="mb-2 block text-sm font-medium text-slate-700">Upload replacement manifest</span>
            <input
              type="file"
              accept=".yaml,.yml"
              onChange={(e) => {
                const input = e.currentTarget as HTMLInputElement;
                setManifestFile(input.files?.[0] ?? null);
              }}
              class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-900 file:px-3 file:py-2 file:text-sm file:font-medium file:text-white"
            />
          </label>
          <button
            type="button"
            onClick={() => setManifestFile(null)}
            disabled={!manifestFile}
            class="rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
          >
            Remove file
          </button>
          <button
            type="submit"
            disabled={!selectedDatasetId || !manifestFile || uploadingManifest}
            class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {uploadingManifest ? 'Uploading...' : 'Update Manifest'}
          </button>
        </div>
        {manifestFile ? (
          <p class="mt-3 text-xs text-slate-500">
            Selected file: <span class="font-medium text-slate-700">{manifestFile.name}</span>
          </p>
        ) : null}

        {manifestUploadFindings.length > 0 ? (
          <div class="mt-4 space-y-3">
            <h3 class="text-sm font-semibold text-slate-900">Manifest findings</h3>
            {manifestUploadFindings.map((finding, index) => (
              <div
                key={`${finding.code}-${index}`}
                class={`rounded-xl px-4 py-3 text-sm ring-1 ${severityClasses(finding.severity)}`}
              >
                <div class="flex flex-wrap items-center gap-2">
                  <span class="font-semibold uppercase">{finding.severity}</span>
                  <span class="font-mono text-xs">{finding.code}</span>
                </div>
                <p class="mt-2">{finding.message}</p>
                {findingLabel(finding) ? <p class="mt-1 text-xs opacity-80">{findingLabel(finding)}</p> : null}
              </div>
            ))}
          </div>
        ) : null}
      </form>
    </section>
  );
}
