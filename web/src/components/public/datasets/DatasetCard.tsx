import type { VNode } from 'preact';
import { datasetPath, formatYears, type CatalogueDataset } from '../../../lib/catalogue';
import type { DatasetView } from '../../../lib/datasetView';
import AccessBadge from '../AccessBadge';

interface DatasetCardProps {
  dataset: CatalogueDataset;
  /** The search, to explain matches found only through a tag. */
  search?: string;
  /** Tile (grid, the default) or card (one wide row). */
  view?: DatasetView;
}

/** A tag that matched the search when the title and description didn't. */
function matchedTag(dataset: CatalogueDataset, search?: string): string | null {
  if (!search) return null;
  const words = search.toLowerCase().split(/\s+/).filter((word) => word.length > 2);
  const text = `${dataset.title} ${dataset.description ?? ''}`.toLowerCase();
  if (!words.length || words.every((word) => text.includes(word))) return null;
  return dataset.tags.find((tag) => words.some((word) => tag.toLowerCase().includes(word))) ?? null;
}

function DatasetIcon(): VNode {
  return (
    <span class="shrink-0 inline-flex items-center justify-center w-10 h-10 rounded-[10px] bg-primary-50 text-primary-700" aria-hidden="true">
      <svg class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <ellipse cx="12" cy="5" rx="8" ry="3" />
        <path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
        <path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" />
      </svg>
    </span>
  );
}

const viewLink =
  'inline-flex justify-center items-center gap-1.5 min-h-[40px] px-3 rounded-lg border border-gray-300 bg-white text-gray-900 font-semibold text-sm no-underline hover:border-primary-400 hover:text-primary-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500';

/**
 * Result on /datasets. As a tile (laid out like Rukshat's prototype): icon and
 * access, ID, title, description, then collection / source / coverage and one
 * "View dataset" link. As a card: the same parts in one wide row. Downloading
 * and the API live on the dataset page.
 */
export default function DatasetCard({ dataset, search, view = 'tile' }: DatasetCardProps) {
  const href = datasetPath(dataset.ds_id);
  // Restricted: only the ID, title and access are shown.
  const restricted = dataset.access_level === 'NONE';
  const tag = matchedTag(dataset, search);
  const details = restricted
    ? []
    : [
        { label: 'Collection', value: dataset.collection_name },
        { label: 'Source', value: dataset.data_owner_name },
        { label: 'Coverage', value: formatYears(dataset.temporal_coverage_start_date, dataset.temporal_coverage_end_date) },
      ].filter((detail) => detail.value);

  const viewButton = (
    <a href={href} aria-label={`View dataset: ${dataset.title}`} class={viewLink}>
      View dataset <span aria-hidden="true">↗</span>
    </a>
  );

  if (view === 'card') {
    return (
      <article class="bg-white border border-gray-200 rounded-[14px] p-5 flex flex-col sm:flex-row sm:items-start gap-4">
        <DatasetIcon />
        <div class="flex-1 min-w-0 flex flex-col gap-1.5">
          <span class="text-[13px] font-semibold tracking-[0.02em] text-primary-700">{dataset.ds_id}</span>
          <h2 class="m-0 text-lg font-bold leading-snug">
            <a href={href} class="text-gray-900 no-underline hover:text-primary-700 hover:underline">
              {dataset.title}
            </a>
          </h2>
          {dataset.description && <p class="m-0 text-[15px] leading-[1.55] text-gray-700 line-clamp-2">{dataset.description}</p>}
          {tag && <p class="m-0 text-[13px] text-gray-600">Related to your search through the tag “{tag}”</p>}
          {details.length > 0 && (
            <dl class="m-0 pt-1 flex flex-wrap gap-x-5 gap-y-1 text-sm">
              {details.map((detail) => (
                <div key={detail.label} class="flex gap-1.5 min-w-0">
                  <dt class="text-gray-500">{detail.label}</dt>
                  <dd class="m-0 text-gray-900">{detail.value}</dd>
                </div>
              ))}
            </dl>
          )}
        </div>
        <div class="shrink-0 flex sm:flex-col items-center sm:items-end justify-between gap-3">
          <AccessBadge level={dataset.access_level} />
          {viewButton}
        </div>
      </article>
    );
  }

  return (
    <article class="h-full bg-white border border-gray-200 rounded-[14px] p-[22px] flex flex-col gap-3">
      <div class="flex justify-between items-start gap-3">
        <DatasetIcon />
        <AccessBadge level={dataset.access_level} />
      </div>

      <div class="flex flex-col gap-1">
        <span class="text-[13px] font-semibold tracking-[0.02em] text-primary-700">{dataset.ds_id}</span>
        <h2 class="m-0 text-lg font-bold leading-snug">
          <a href={href} class="text-gray-900 no-underline hover:text-primary-700 hover:underline">
            {dataset.title}
          </a>
        </h2>
      </div>

      {dataset.description && <p class="m-0 text-[15px] leading-[1.55] text-gray-700 line-clamp-3">{dataset.description}</p>}
      {tag && <p class="m-0 text-[13px] text-gray-600">Related to your search through the tag “{tag}”</p>}

      {details.length > 0 && (
        <dl class="m-0 pt-3 border-t border-gray-100 flex flex-col gap-2.5">
          {details.map((detail) => (
            <div key={detail.label} class="flex flex-col">
              <dt class="text-xs text-gray-500">{detail.label}</dt>
              <dd class="m-0 text-sm text-gray-900">{detail.value}</dd>
            </div>
          ))}
        </dl>
      )}

      <div class="mt-auto flex flex-col">{viewButton}</div>
    </article>
  );
}
