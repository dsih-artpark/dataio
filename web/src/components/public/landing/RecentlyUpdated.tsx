import { useEffect, useState } from 'preact/hooks';
import { api } from '../../../lib/api';
import {
  SPATIAL_RESOLUTION_LABELS,
  TEMPORAL_RESOLUTION_LABELS,
  datasetPath,
  formatMonthYear,
  formatYears,
  labelFor,
  type CatalogueDataset,
} from '../../../lib/catalogue';
import AccessBadge from '../AccessBadge';

const COUNT = 3;

/** "Yearly · State · 1997–2019 · DAHD · Updated Mar 2026" */
function metaLine(dataset: CatalogueDataset): string {
  const updated = formatMonthYear(dataset.updated_at);
  return [
    labelFor(TEMPORAL_RESOLUTION_LABELS, dataset.temporal_resolution),
    labelFor(SPATIAL_RESOLUTION_LABELS, dataset.spatial_resolution),
    formatYears(dataset.temporal_coverage_start_date, dataset.temporal_coverage_end_date),
    dataset.data_owner_name,
    updated && `Updated ${updated}`,
  ]
    .filter(Boolean)
    .join(' · ');
}

/** The three most recently updated datasets, as compact lines. */
export default function RecentlyUpdated() {
  const [datasets, setDatasets] = useState<CatalogueDataset[] | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    api
      .searchCatalogue({ sort: 'updated', limit: COUNT })
      .then((response) => setDatasets(response.datasets))
      .catch(() => setFailed(true));
  }, []);

  if (failed) {
    return (
      <p class="m-0 text-[15px] text-gray-600">
        Couldn’t load the latest updates. <a href="/datasets" class="font-semibold text-primary-700">Browse all datasets →</a>
      </p>
    );
  }

  if (!datasets) {
    return (
      <div class="flex flex-col gap-3" aria-hidden="true">
        {Array.from({ length: COUNT }, (_, index) => (
          <div key={index} class="h-[76px] rounded-xl bg-gray-100 animate-pulse" />
        ))}
      </div>
    );
  }

  return (
    <ul class="m-0 p-0 list-none flex flex-col gap-3">
      {datasets.map((dataset) => (
        <li key={dataset.ds_id}>
          <a
            href={datasetPath(dataset.ds_id)}
            class="flex flex-wrap items-center gap-x-6 gap-y-3 px-[22px] py-[18px] rounded-xl bg-white border border-gray-200 text-gray-900 no-underline hover:border-primary-300 hover:shadow-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
          >
            <span class="flex-[1_1_360px] min-w-0 flex flex-col gap-1">
              <span class="text-[17px] font-semibold">{dataset.title}</span>
              <span class="text-sm text-gray-600">{metaLine(dataset)}</span>
            </span>
            <AccessBadge level={dataset.access_level} />
          </a>
        </li>
      ))}
    </ul>
  );
}
