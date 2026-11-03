import { useEffect, useMemo, useState } from 'preact/hooks';
import { marked } from 'marked';
import { api, ApiRequestError } from '../../../lib/api';
import { currentUser, initAuth } from '../../../lib/auth';
import {
  ACCESS_DESCRIPTIONS,
  LICENCE_NOT_SPECIFIED,
  NO_FILES_MESSAGE,
  NO_TABLES,
  SPATIAL_RESOLUTION_LABELS,
  TEMPORAL_RESOLUTION_LABELS,
  formatMonthYear,
  formatYears,
  labelFor,
  requestAccessPath,
  tableCountLabel,
  type CatalogueDatasetDetail,
  type CataloguePreview,
} from '../../../lib/catalogue';
import CodeSnippets from '../../datasets/CodeSnippets';
import AccessBadge from '../AccessBadge';
import MetaChip from '../MetaChip';
import { useDatasetDownload } from './DownloadConsent';

const card = 'bg-white border border-gray-200 rounded-2xl p-5 sm:p-7 flex flex-col gap-3.5';
const h2 = 'm-0 text-[22px] font-bold text-gray-900';
const th = 'px-3.5 py-2.5 font-semibold text-gray-700 text-left whitespace-nowrap';
const td = 'px-3.5 py-2.5 text-gray-700 align-top';

function datasetIdFromAddress(): string | null {
  const params = new URLSearchParams(window.location.search);
  return params.get('id') || params.get('dataset');
}

/** One dataset: title block with access panel, preview, tables and columns, API. */
export default function DatasetPage() {
  const [dsId, setDsId] = useState<string | null>(null);
  const [dataset, setDataset] = useState<CatalogueDatasetDetail | null>(null);
  const [preview, setPreview] = useState<CataloguePreview | null>(null);
  const [error, setError] = useState<'missing' | 'not-found' | 'failed' | null>(null);
  // A signed-in user may have been granted download on a View only / Restricted dataset.
  const [userCanDownload, setUserCanDownload] = useState(false);
  const { downloading, error: downloadError, start: startDownload, dialog: downloadDialog } = useDatasetDownload(dataset);

  useEffect(() => {
    const id = datasetIdFromAddress();
    setDsId(id);
    if (!id) {
      setError('missing');
      return;
    }
    api
      .getCatalogueDataset(id)
      .then((detail) => {
        setDataset(detail);
        document.title = `${detail.title} | Data and AI Commons for One Health`;
      })
      .catch((err) => setError(err instanceof ApiRequestError && err.statusCode === 404 ? 'not-found' : 'failed'));
    api
      .getCataloguePreview(id)
      .then(setPreview)
      .catch(() => setPreview({ mode: 'unavailable', table: null, columns: [], rows: [] }));
    // Only ask for the user's own access once we know someone is signed in:
    // the signed-in endpoint sends signed-out visitors to the login page.
    initAuth()
      .then(() => {
        if (!currentUser.value) return;
        return api.getDataset(id).then((own) => setUserCanDownload(!!own.can_download));
      })
      .catch(() => undefined);
  }, []);

  const readmeHtml = useMemo(
    () => (dataset?.readme_md ? (marked.parse(dataset.readme_md) as string) : null),
    [dataset?.readme_md],
  );

  if (error) {
    return (
      <div class={card}>
        <h1 class="m-0 text-2xl font-bold">
          {error === 'not-found' ? 'Dataset not found' : error === 'missing' ? 'No dataset chosen' : 'Couldn’t load this dataset'}
        </h1>
        <p class="m-0 text-[15px] text-gray-600">
          {error === 'not-found'
            ? `There’s no dataset with the ID “${dsId}”. It may have been renamed or removed.`
            : error === 'missing'
              ? 'Pick a dataset from the catalogue.'
              : 'Please try again in a moment.'}
        </p>
        <a href="/datasets" class="self-start font-semibold text-primary-700 hover:text-primary-900">
          Browse all datasets →
        </a>
      </div>
    );
  }

  if (!dataset) {
    return (
      <div class="flex flex-col gap-7" aria-busy="true" aria-label="Loading dataset">
        <div class="h-[220px] rounded-2xl bg-white border border-gray-200 animate-pulse" />
        <div class="h-[260px] rounded-2xl bg-white border border-gray-200 animate-pulse" />
      </div>
    );
  }

  const access = dataset.access_level;
  const canDownload = access === 'DOWNLOAD' || userCanDownload;
  const hasFiles = dataset.table_count > 0;
  const updated = formatMonthYear(dataset.updated_at);

  return (
    <div class="flex flex-col gap-7">
      {/* TITLE + ACCESS */}
      <section class={`${card} !flex-row flex-wrap !gap-x-8 !gap-y-5 justify-between`}>
        <div class="flex-[1_1_480px] min-w-0 flex flex-col gap-2.5">
          <span class="text-[13px] text-gray-600">
            {dataset.ds_id}
            {dataset.collection_name && ` · ${dataset.collection_name}`}
          </span>
          <h1 class="m-0 text-[26px] sm:text-[32px] leading-tight font-bold text-gray-900">{dataset.title}</h1>
          {dataset.description && <p class="m-0 text-base leading-relaxed text-gray-600">{dataset.description}</p>}
          <div class="flex flex-wrap gap-2">
            <MetaChip label="Where" value={dataset.spatial_coverage_region_name} />
            <MetaChip label="Detail" value={labelFor(SPATIAL_RESOLUTION_LABELS, dataset.spatial_resolution)} />
            <MetaChip label="When" value={formatYears(dataset.temporal_coverage_start_date, dataset.temporal_coverage_end_date)} />
            <MetaChip label="How often" value={labelFor(TEMPORAL_RESOLUTION_LABELS, dataset.temporal_resolution)} />
            <MetaChip label="Source" value={dataset.data_owner_name} />
            <MetaChip label="Licence" value={dataset.licence} fallback={LICENCE_NOT_SPECIFIED} />
            <MetaChip label="Tables" value={tableCountLabel(dataset.table_count)} fallback={NO_TABLES} />
          </div>
          {(updated || dataset.tags.length > 0) && (
            <p class="m-0 text-sm text-gray-600">
              {updated && `Updated ${updated}`}
              {updated && dataset.tags.length > 0 && ' · '}
              {dataset.tags.length > 0 && `Tags: ${dataset.tags.join(', ')}`}
            </p>
          )}
        </div>
        <aside aria-label="Access" class="flex-[0_1_300px] flex flex-col gap-3">
          <AccessBadge level={access} />
          {canDownload && !hasFiles ? (
            <p class="m-0 text-sm leading-[1.55] text-gray-600">{NO_FILES_MESSAGE}</p>
          ) : canDownload ? (
            <>
              <p class="m-0 text-sm leading-[1.55] text-gray-600">
                {access === 'DOWNLOAD'
                  ? ACCESS_DESCRIPTIONS.DOWNLOAD
                  : 'You have been given access to download this dataset.'}
              </p>
              <button
                type="button"
                onClick={() => startDownload(access === 'DOWNLOAD')}
                disabled={downloading}
                class="min-h-[44px] px-[18px] rounded-[10px] bg-primary-600 text-white font-semibold text-[15px] hover:bg-primary-700 disabled:opacity-60 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
              >
                {downloading ? 'Preparing download…' : 'Download (zip)'}
              </button>
              <a
                href="#api"
                class="text-center min-h-[44px] inline-flex items-center justify-center px-[18px] rounded-[10px] border border-gray-300 bg-white text-gray-700 font-semibold text-[15px] hover:border-gray-400"
              >
                Use the API
              </a>
              {downloadError && (
                <p role="alert" class="m-0 text-sm text-red-700">
                  {downloadError}
                </p>
              )}
              {downloadDialog}
            </>
          ) : (
            <>
              <p class="m-0 text-sm leading-[1.55] text-gray-600">
                {access === 'VIEW'
                  ? 'You can see the tables, columns and a preview with sample values. Ask us for access to download.'
                  : ACCESS_DESCRIPTIONS.NONE}
              </p>
              <a
                href={requestAccessPath(dataset.ds_id)}
                class="text-center min-h-[44px] inline-flex items-center justify-center px-[18px] rounded-[10px] bg-primary-600 text-white font-semibold text-[15px] hover:bg-primary-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
              >
                Request access
              </a>
              {!currentUser.value && (
                <p class="m-0 text-[13px] text-gray-600">
                  Already have access? <a href="/login" class="font-semibold text-primary-700">Sign in</a>
                </p>
              )}
            </>
          )}
        </aside>
      </section>

      {/* PREVIEW */}
      {preview && preview.mode !== 'none' && (
        <section aria-labelledby="preview-heading" class={card}>
          <div class="flex flex-wrap justify-between items-center gap-2.5">
            <h2 id="preview-heading" class={h2}>
              Preview
            </h2>
            {preview.mode === 'synthetic' && (
              <span class="px-2.5 py-1 rounded-full bg-amber-100 text-amber-800 text-[13px] font-semibold">Synthetic preview only</span>
            )}
          </div>
          {preview.mode === 'unavailable' ? (
            <p class="m-0 text-sm text-gray-600">A preview isn’t available for this dataset.</p>
          ) : (
            <>
              <p class="m-0 text-sm leading-[1.55] text-gray-600">
                {preview.mode === 'synthetic'
                  ? 'Place names, codes and dates are real. The numbers are random and don’t reflect the real data’s totals, averages or trends; other text is hidden.'
                  : `The first ${preview.rows.length} rows of “${preview.table}”.`}
              </p>
              <div class="overflow-x-auto border border-gray-200 rounded-[10px]" tabIndex={0} role="region" aria-label="Preview table">
                <table class="w-full border-collapse text-sm min-w-[520px]">
                  <thead>
                    <tr class="bg-gray-100">
                      {preview.columns.map((column) => (
                        <th key={column} scope="col" class={th}>
                          {column}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, index) => (
                      <tr key={index} class="border-t border-gray-100">
                        {row.map((cell, cellIndex) => (
                          <td key={cellIndex} class={`${td} whitespace-nowrap`}>
                            {cell}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </section>
      )}

      {/* TABLES + COLUMNS */}
      <section aria-labelledby="columns-heading" class={card}>
        <h2 id="columns-heading" class={h2}>
          Tables and columns
        </h2>
        <p class="m-0 text-sm leading-[1.55] text-gray-600">Visible to everyone, whatever the access level.</p>
        {dataset.tables.length === 0 && <p class="m-0 text-sm text-gray-600">No data dictionary has been published for this dataset yet.</p>}
        {dataset.tables.map((table) => (
          <div key={table.name} class="flex flex-col gap-2">
            {dataset.tables.length > 1 && (
              <h3 class="m-0 text-base font-bold text-gray-900">
                {table.name}
                {table.description && <span class="font-normal text-gray-600"> · {table.description}</span>}
              </h3>
            )}
            <div class="overflow-x-auto border border-gray-200 rounded-[10px]" tabIndex={0} role="region" aria-label={`Columns of ${table.name}`}>
              <table class="w-full border-collapse text-sm min-w-[520px]">
                <thead>
                  <tr class="bg-gray-100">
                    <th scope="col" class={th}>Column</th>
                    <th scope="col" class={th}>Description</th>
                    <th scope="col" class={th}>Type</th>
                    <th scope="col" class={th}>Unit</th>
                  </tr>
                </thead>
                <tbody>
                  {table.columns.map((column) => (
                    <tr key={column.name} class="border-t border-gray-100">
                      <td class={`${td} font-mono text-[13px] whitespace-nowrap`}>{column.name}</td>
                      <td class={td}>{column.description || <span class="text-gray-400">—</span>}</td>
                      <td class={`${td} whitespace-nowrap`}>{column.type || <span class="text-gray-400">—</span>}</td>
                      <td class={`${td} whitespace-nowrap`}>{column.unit || <span class="text-gray-400">—</span>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </section>

      {/* DOCUMENTATION (README) */}
      {readmeHtml && (
        <section aria-labelledby="readme-heading" class={card}>
          <h2 id="readme-heading" class={h2}>
            About this dataset
          </h2>
          <div class="prose prose-sm prose-gray max-w-none">
            <div class="readme-content" dangerouslySetInnerHTML={{ __html: readmeHtml }} />
          </div>
        </section>
      )}

      {/* API */}
      <section id="api" aria-labelledby="api-heading" class={`${card} scroll-mt-4`}>
        <h2 id="api-heading" class={h2}>
          Use it in your code
        </h2>
        <p class="m-0 text-sm leading-[1.55] text-gray-600">
          {canDownload && !hasFiles
            ? `${NO_FILES_MESSAGE} Once they are, you can fetch them with the Python SDK, command-line tool or REST API.`
            : canDownload
            ? 'With the Python SDK, command-line tool or REST API. You need a free API key from your account page.'
            : 'The API gives the same access as the website: request access first to download this dataset.'}
        </p>
        <CodeSnippets datasetId={dataset.ds_id} />
      </section>
    </div>
  );
}
