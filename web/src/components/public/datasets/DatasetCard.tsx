import {
  LICENCE_NOT_SPECIFIED,
  NO_TABLES,
  SPATIAL_RESOLUTION_LABELS,
  tableCountLabel,
  datasetPath,
  formatMonthYear,
  formatYears,
  labelFor,
  requestAccessPath,
  type CatalogueDataset,
} from '../../../lib/catalogue';
import AccessBadge from '../AccessBadge';
import MetaChip from '../MetaChip';
import { useDatasetDownload } from './DownloadConsent';

interface DatasetCardProps {
  dataset: CatalogueDataset;
  /** The search, to explain matches found only through a tag. */
  search?: string;
}

const FORMAT = { tabular: 'CSV', geojson: 'GeoJSON' } as const;

/** A tag that matched the search when the title and description didn't. */
function matchedTag(dataset: CatalogueDataset, search?: string): string | null {
  if (!search) return null;
  const words = search.toLowerCase().split(/\s+/).filter((word) => word.length > 2);
  const text = `${dataset.title} ${dataset.description ?? ''}`.toLowerCase();
  if (!words.length || words.every((word) => text.includes(word))) return null;
  return dataset.tags.find((tag) => words.some((word) => tag.toLowerCase().includes(word))) ?? null;
}

/** Result card on /datasets: title, access, chips and Download / API / Details. */
export default function DatasetCard({ dataset, search }: DatasetCardProps) {
  const { downloading, error, start, dialog } = useDatasetDownload(dataset);
  const href = datasetPath(dataset.ds_id);
  const open = dataset.access_level === 'DOWNLOAD';
  const tag = matchedTag(dataset, search);
  const updated = formatMonthYear(dataset.updated_at);

  return (
    <article class="bg-white border border-gray-200 rounded-[14px] px-[22px] py-5 flex flex-col gap-3">
      <div class="flex flex-wrap justify-between items-start gap-x-4 gap-y-2">
        <div class="flex-[1_1_380px] min-w-0 flex flex-col gap-1">
          <h2 class="m-0 text-lg font-bold leading-snug">
            <a href={href} class="text-gray-900 no-underline hover:text-primary-700 hover:underline">
              {dataset.title}
            </a>
          </h2>
          <span class="text-[13px] text-gray-600">
            {dataset.ds_id}
            {dataset.collection_name && ` · ${dataset.collection_name}`}
          </span>
        </div>
        <AccessBadge level={dataset.access_level} />
      </div>

      {dataset.description && <p class="m-0 text-[15px] leading-[1.55] text-gray-700 line-clamp-3">{dataset.description}</p>}
      {tag && <p class="m-0 text-[13px] text-gray-600">Related to your search through the tag “{tag}”</p>}

      <div class="flex flex-wrap gap-2">
        <MetaChip label="Where" value={dataset.spatial_coverage_region_name} />
        <MetaChip label="Detail" value={labelFor(SPATIAL_RESOLUTION_LABELS, dataset.spatial_resolution)} />
        <MetaChip label="When" value={formatYears(dataset.temporal_coverage_start_date, dataset.temporal_coverage_end_date)} />
        <MetaChip label="Source" value={dataset.data_owner_name} />
        <MetaChip label="Format" value={FORMAT[dataset.dataset_kind] ?? null} />
        <MetaChip label="Licence" value={dataset.licence} fallback={LICENCE_NOT_SPECIFIED} />
        <MetaChip label="Tables" value={tableCountLabel(dataset.table_count)} fallback={NO_TABLES} />
      </div>

      <div class="flex flex-wrap gap-x-4 gap-y-2 items-center text-sm text-gray-600 border-t border-gray-100 pt-3">
        {dataset.has_data_dictionary && <span>Data dictionary included</span>}
        {updated && <span>Updated {updated}</span>}
        <span class="flex flex-wrap gap-2 ml-auto">
          {open && dataset.table_count === 0 ? (
            <span class="inline-flex items-center min-h-[40px] text-sm text-gray-600">Files not yet available</span>
          ) : open ? (
            <button
              type="button"
              onClick={() => start(true)}
              disabled={downloading}
              class="inline-flex items-center gap-1.5 min-h-[40px] px-3 rounded-lg bg-primary-600 text-white font-semibold text-sm hover:bg-primary-700 disabled:opacity-60 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
            >
              <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 3v12" />
                <path d="M7 10l5 5 5-5" />
                <path d="M5 21h14" />
              </svg>
              {downloading ? 'Preparing…' : 'Download'}
            </button>
          ) : (
            <a
              href={requestAccessPath(dataset.ds_id)}
              class="inline-flex items-center min-h-[40px] px-3 rounded-lg border border-primary-200 bg-primary-50 text-primary-900 font-semibold text-sm hover:border-primary-400 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
            >
              Request access
            </a>
          )}
          <a
            href={`${href}#api`}
            class="inline-flex items-center min-h-[40px] px-3 rounded-lg border border-gray-300 bg-white text-gray-700 font-semibold text-sm hover:border-gray-400 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
          >
            API
          </a>
          <a href={href} class="inline-flex items-center min-h-[40px] px-1 font-semibold text-sm text-primary-700 hover:text-primary-900">
            Details <span aria-hidden="true">&nbsp;→</span>
            <span class="sr-only"> for {dataset.title}</span>
          </a>
        </span>
      </div>
      {error && (
        <p role="alert" class="m-0 text-sm text-red-700">
          {error}
        </p>
      )}
      {dialog}
    </article>
  );
}
