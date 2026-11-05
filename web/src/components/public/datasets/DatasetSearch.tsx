import { useEffect, useRef, useState } from 'preact/hooks';
import { api } from '../../../lib/api';
import {
  ACCESS_LABELS,
  DATASET_KIND_LABELS,
  SORT_LABELS,
  SPATIAL_RESOLUTION_LABELS,
  TEMPORAL_RESOLUTION_LABELS,
  catalogueQuery,
  parseCatalogueQuery,
  type CatalogueAccessLevel,
  type CatalogueDataset,
  type CatalogueFilters,
  type CatalogueSort,
  type CatalogueStats,
  type DatasetKind,
} from '../../../lib/catalogue';
import { STATE_TILES, THEMES, loadCatalogueStats, themeCounts } from '../../../lib/landing';
import { CONTACT_EMAIL } from '../../../lib/site';
import { openAssistant } from '../../../lib/assistant';
import DatasetCard from './DatasetCard';

const PAGE_SIZE = 20;
const INDIA = 'country_IN';
const SOURCES_SHOWN = 5;

const LEVELS: { value: string; label: string }[] = [
  { value: 'STATE', label: 'State' },
  { value: 'DISTRICT', label: 'District' },
  { value: 'SUBDISTRICT', label: 'Sub-district / taluk' },
  { value: 'VILLAGE', label: 'Village' },
  { value: 'LAT/LONG', label: 'Point / station' },
  { value: 'OTHER', label: 'Other' },
];
const FREQUENCIES = ['DATE', 'WEEK', 'MONTH', 'YEAR', 'NONE'];
const ACCESS_LEVELS: CatalogueAccessLevel[] = ['DOWNLOAD', 'VIEW', 'NONE'];
const KINDS: DatasetKind[] = ['tabular', 'geojson'];

const REQUEST_DATASET_HREF = `mailto:${CONTACT_EMAIL}?subject=${encodeURIComponent('Dataset request')}&body=${encodeURIComponent(
  'Hi,\r\n\r\nI could not find a dataset I need.\r\n\r\nWhat data:\r\nWhich place (state, district or other):\r\nWhich years:\r\nWhat you will use it for:\r\nYour organisation:\r\n',
)}`;

const legend = 'text-[13px] font-bold uppercase tracking-[0.06em] text-gray-700 mb-2.5';
const fieldset = 'border-0 m-0 p-0 min-w-0 flex flex-col gap-2.5';
const divider = 'pt-[18px] border-t border-gray-200';
const checkboxRow = 'flex items-center gap-2.5 text-[15px] text-gray-900 min-h-[32px] cursor-pointer';
const checkbox = 'w-[18px] h-[18px] shrink-0 accent-primary-600 cursor-pointer';
const input =
  'w-full min-h-[44px] px-2.5 border border-gray-300 rounded-lg text-[15px] bg-white focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500';

function toggle<T>(list: T[] | undefined, value: T): T[] {
  const current = list ?? [];
  return current.includes(value) ? current.filter((item) => item !== value) : [...current, value];
}

function readFilters(): CatalogueFilters {
  if (typeof window === 'undefined') return {};
  const filters = parseCatalogueQuery(new URLSearchParams(window.location.search));
  delete filters.limit;
  return filters;
}

/** Dataset discovery: filter sidebar + results, all state in the page address. */
export default function DatasetSearch() {
  const [filters, setFilters] = useState<CatalogueFilters>({});
  const [ready, setReady] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [results, setResults] = useState<{ datasets: CatalogueDataset[]; total: number } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState<CatalogueStats | null>(null);
  const [owners, setOwners] = useState<{ id: number; name: string }[]>([]);
  const [sourceQuery, setSourceQuery] = useState('');
  const [showAllSources, setShowAllSources] = useState(false);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const [yearFrom, setYearFrom] = useState('');
  const [yearTo, setYearTo] = useState('');
  const resultsRef = useRef<HTMLDivElement>(null);
  const requestId = useRef(0);

  // Start from the address, and follow back/forward.
  useEffect(() => {
    const apply = () => {
      const next = readFilters();
      setFilters(next);
      setSearchText(next.search ?? '');
      setYearFrom(next.year_from != null ? String(next.year_from) : '');
      setYearTo(next.year_to != null ? String(next.year_to) : '');
    };
    apply();
    setReady(true);
    window.addEventListener('popstate', apply);
    loadCatalogueStats().then(setStats).catch(() => undefined);
    api
      .getPublicDataOwners()
      .then((response) => setOwners([...response.data_owners].sort((a, b) => a.name.localeCompare(b.name))))
      .catch(() => undefined);
    return () => window.removeEventListener('popstate', apply);
  }, []);

  // Fetch results whenever the filters change.
  useEffect(() => {
    if (!ready) return;
    const id = ++requestId.current;
    setLoading(true);
    setError(null);
    api
      .searchCatalogue({ ...filters, limit: PAGE_SIZE })
      .then((response) => {
        if (id !== requestId.current) return;
        setResults({ datasets: response.datasets, total: response.total });
      })
      .catch((err) => {
        if (id !== requestId.current) return;
        setError(err instanceof Error ? err.message : 'Couldn’t load datasets');
      })
      .finally(() => {
        if (id === requestId.current) setLoading(false);
      });
  }, [filters, ready]);

  const update = (patch: Partial<CatalogueFilters>, options: { keepPage?: boolean } = {}) => {
    const next: CatalogueFilters = { ...filters, ...patch };
    if (!options.keepPage) delete next.offset;
    for (const key of Object.keys(next) as (keyof CatalogueFilters)[]) {
      const value = next[key];
      if (value == null || value === '' || (Array.isArray(value) && !value.length)) delete next[key];
    }
    setFilters(next);
    const query = catalogueQuery(next).toString();
    window.history.pushState(null, '', query ? `?${query}` : window.location.pathname);
  };

  const clearAll = () => {
    setSearchText('');
    setYearFrom('');
    setYearTo('');
    update({
      category_id: undefined,
      region_id: undefined,
      spatial_resolution: undefined,
      temporal_resolution: undefined,
      year_from: undefined,
      year_to: undefined,
      access_level: undefined,
      data_owner_id: undefined,
      dataset_kind: undefined,
      licence: undefined,
      collection_id: undefined,
    });
  };

  const applyYears = () => {
    const from = /^\d{4}$/.test(yearFrom) ? Number(yearFrom) : undefined;
    const to = /^\d{4}$/.test(yearTo) ? Number(yearTo) : undefined;
    if (from !== filters.year_from || to !== filters.year_to) update({ year_from: from, year_to: to });
  };

  const counts = themeCounts(stats);
  const themeOptions = [
    ...THEMES.filter((theme) => (counts.get(theme.id)?.count ?? 0) > 0 || filters.category_id?.includes(theme.id)).map(
      (theme) => ({ id: theme.id, label: theme.label, count: counts.get(theme.id)?.count }),
    ),
    ...[...counts.entries()]
      .filter(([id]) => !THEMES.some((theme) => theme.id === id))
      .map(([id, value]) => ({ id, label: value.name, count: value.count })),
  ];
  const themeLabel = (id: string) => themeOptions.find((theme) => theme.id === id)?.label ?? id;
  // Places can still come from links (e.g. a state tile on the home page); they show as removable chips.
  const regionLabel = (id: string) =>
    id === INDIA
      ? 'All India'
      : (stats?.coverage.states.find((state) => state.region_id === id)?.region_name ??
        STATE_TILES.find((tile) => tile.regionId === id)?.name ??
        id);
  const ownerLabel = (id: number) => owners.find((owner) => owner.id === id)?.name ?? `Source ${id}`;

  const sourceMatches = owners.filter((owner) => owner.name.toLowerCase().includes(sourceQuery.trim().toLowerCase()));
  const selectedOwners = owners.filter((owner) => filters.data_owner_id?.includes(owner.id));
  const visibleOwners =
    showAllSources || sourceQuery.trim()
      ? sourceMatches
      : [...selectedOwners, ...sourceMatches.filter((owner) => !selectedOwners.includes(owner))].slice(
          0,
          Math.max(SOURCES_SHOWN, selectedOwners.length),
        );

  // Removable chips for everything that narrows the results.
  const chips: { key: string; label: string; remove: () => void }[] = [];
  if (filters.search) chips.push({ key: 'search', label: `“${filters.search}”`, remove: () => (setSearchText(''), update({ search: undefined })) });
  filters.category_id?.forEach((id) =>
    chips.push({ key: `t-${id}`, label: themeLabel(id), remove: () => update({ category_id: toggle(filters.category_id, id) }) }),
  );
  filters.region_id?.forEach((id) =>
    chips.push({ key: `r-${id}`, label: regionLabel(id), remove: () => update({ region_id: toggle(filters.region_id, id) }) }),
  );
  filters.spatial_resolution?.forEach((value) =>
    chips.push({
      key: `l-${value}`,
      label: `${LEVELS.find((level) => level.value === value)?.label ?? SPATIAL_RESOLUTION_LABELS[value] ?? value} level`,
      remove: () => update({ spatial_resolution: toggle(filters.spatial_resolution, value) }),
    }),
  );
  if (filters.year_from != null || filters.year_to != null)
    chips.push({
      key: 'years',
      label: `${filters.year_from ?? '…'}–${filters.year_to ?? '…'}`,
      remove: () => (setYearFrom(''), setYearTo(''), update({ year_from: undefined, year_to: undefined })),
    });
  filters.temporal_resolution?.forEach((value) =>
    chips.push({
      key: `f-${value}`,
      label: TEMPORAL_RESOLUTION_LABELS[value] ?? value,
      remove: () => update({ temporal_resolution: toggle(filters.temporal_resolution, value) }),
    }),
  );
  filters.access_level?.forEach((value) =>
    chips.push({ key: `a-${value}`, label: ACCESS_LABELS[value], remove: () => update({ access_level: toggle(filters.access_level, value) }) }),
  );
  filters.data_owner_id?.forEach((id) =>
    chips.push({ key: `o-${id}`, label: ownerLabel(id), remove: () => update({ data_owner_id: toggle(filters.data_owner_id, id) }) }),
  );
  filters.dataset_kind?.forEach((value) =>
    chips.push({ key: `k-${value}`, label: DATASET_KIND_LABELS[value], remove: () => update({ dataset_kind: toggle(filters.dataset_kind, value) }) }),
  );
  filters.licence?.forEach((value) =>
    chips.push({ key: `lic-${value}`, label: `Licence: ${value}`, remove: () => update({ licence: toggle(filters.licence, value) }) }),
  );

  const offset = filters.offset ?? 0;
  const total = results?.total ?? 0;
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const page = Math.floor(offset / PAGE_SIZE) + 1;
  const goToPage = (target: number) => {
    update({ offset: (target - 1) * PAGE_SIZE }, { keepPage: true });
    resultsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      window.prompt('Copy this link:', window.location.href);
    }
  };

  const activeFilterCount = chips.filter((chip) => chip.key !== 'search').length;

  return (
    <>
      {/* PAGE HEADER + SEARCH */}
      <section class="bg-white border-b border-gray-200">
        <div class="max-w-[1320px] mx-auto px-4 sm:px-6 pt-8 pb-6 flex flex-col gap-4">
          <h1 class="m-0 text-[30px] font-bold text-gray-900">Datasets</h1>
          <form
            role="search"
            class="flex flex-wrap gap-2 max-w-[860px] bg-white border border-gray-300 rounded-xl p-1.5 focus-within:ring-2 focus-within:ring-primary-500 focus-within:border-primary-500"
            onSubmit={(event) => {
              event.preventDefault();
              update({ search: searchText.trim() || undefined });
            }}
          >
            <label for="ds-search" class="sr-only">
              Search datasets
            </label>
            <input
              id="ds-search"
              type="search"
              value={searchText}
              onInput={(event) => setSearchText((event.target as HTMLInputElement).value)}
              placeholder="Search by topic, place or source"
              class="flex-[1_1_260px] min-w-0 border-0 bg-transparent px-3 py-2.5 text-base text-gray-900 focus:outline-none focus:ring-0"
            />
            <button
              type="submit"
              class="flex-[1_1_auto] sm:flex-none min-h-[44px] px-5 rounded-lg bg-primary-600 text-white text-[15px] font-semibold hover:bg-primary-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
            >
              Search
            </button>
          </form>
          <p class="m-0 text-sm text-gray-600">
            Searches titles, descriptions, column names, sources and tags. To ask a question in plain words, use the{' '}
            <button type="button" onClick={() => openAssistant()} class="p-0 border-0 bg-transparent font-semibold text-primary-700 hover:text-primary-900 underline-offset-2 hover:underline">
              data assistant
            </button>
            .
          </p>
        </div>
      </section>

      <div class="max-w-[1320px] mx-auto px-4 sm:px-6 py-6 flex flex-wrap gap-6 items-start">
        {/* FILTERS */}
        <button
          type="button"
          class="lg:hidden w-full min-h-[44px] flex items-center justify-between px-4 rounded-xl border border-gray-300 bg-white text-[15px] font-semibold text-gray-900"
          aria-expanded={filtersOpen}
          aria-controls="filters-panel"
          onClick={() => setFiltersOpen(!filtersOpen)}
        >
          <span>
            Filters{activeFilterCount ? ` (${activeFilterCount})` : ''}
          </span>
          <span aria-hidden="true">{filtersOpen ? '▲' : '▼'}</span>
        </button>
        <aside
          id="filters-panel"
          aria-label="Filters"
          class={`${filtersOpen ? 'flex' : 'hidden'} lg:flex flex-[1_1_280px] lg:max-w-[320px] min-w-0 bg-white border border-gray-200 rounded-[14px] p-5 flex-col gap-[22px]`}
        >
          <div class="flex justify-between items-center">
            <h2 class="m-0 text-base font-bold">Filters</h2>
            <button
              type="button"
              onClick={clearAll}
              class="min-h-[44px] px-0 border-0 bg-transparent text-sm font-semibold text-primary-700 hover:text-primary-900 disabled:text-gray-400"
              disabled={!activeFilterCount}
            >
              Clear all
            </button>
          </div>

          <fieldset class={fieldset}>
            <legend class={legend}>Theme</legend>
            {themeOptions.map((theme) => (
              <label key={theme.id} class={checkboxRow}>
                <input
                  type="checkbox"
                  class={checkbox}
                  checked={filters.category_id?.includes(theme.id) ?? false}
                  onChange={() => update({ category_id: toggle(filters.category_id, theme.id) })}
                />
                <span class="flex-1">{theme.label}</span>
                {theme.count != null && <span class="text-[13px] text-gray-600">{theme.count}</span>}
              </label>
            ))}
          </fieldset>

          <div class={divider}>
          <fieldset class={fieldset}>
            <legend class={legend}>Geography</legend>
            <span class="text-sm text-gray-700">Level of detail</span>
            <label class={checkboxRow}>
              <input
                type="checkbox"
                class={checkbox}
                checked={filters.region_id?.includes(INDIA) ?? false}
                onChange={() => update({ region_id: toggle(filters.region_id, INDIA) })}
              />
              <span class="flex-1">All India</span>
              {stats && <span class="text-[13px] text-gray-600">{stats.coverage.national}</span>}
            </label>
            {LEVELS.map((level) => (
              <label key={level.value} class={checkboxRow}>
                <input
                  type="checkbox"
                  class={checkbox}
                  checked={filters.spatial_resolution?.includes(level.value) ?? false}
                  onChange={() => update({ spatial_resolution: toggle(filters.spatial_resolution, level.value) })}
                />
                <span>{level.label}</span>
              </label>
            ))}
          </fieldset>
          </div>

          <div class={divider}>
          <fieldset class={fieldset}>
            <legend class={legend}>Time period</legend>
            <form
              class="flex gap-2.5"
              onSubmit={(event) => {
                event.preventDefault();
                applyYears();
              }}
            >
              <label class="flex-1 min-w-0 flex flex-col gap-1 text-sm text-gray-700">
                From
                <input type="number" inputMode="numeric" min={1800} max={2200} placeholder={stats?.earliest_year ? String(stats.earliest_year) : 'Year'} value={yearFrom} onInput={(e) => setYearFrom((e.target as HTMLInputElement).value)} onBlur={applyYears} class={input} />
              </label>
              <label class="flex-1 min-w-0 flex flex-col gap-1 text-sm text-gray-700">
                To
                <input type="number" inputMode="numeric" min={1800} max={2200} placeholder={stats?.latest_year ? String(stats.latest_year) : 'Year'} value={yearTo} onInput={(e) => setYearTo((e.target as HTMLInputElement).value)} onBlur={applyYears} class={input} />
              </label>
              <button type="submit" class="sr-only">
                Apply years
              </button>
            </form>
            <span class="mt-1.5 text-sm text-gray-700">How often it’s recorded</span>
            <div class="flex flex-wrap gap-2">
              {FREQUENCIES.map((value) => {
                const on = filters.temporal_resolution?.includes(value) ?? false;
                return (
                  <button
                    key={value}
                    type="button"
                    aria-pressed={on}
                    onClick={() => update({ temporal_resolution: toggle(filters.temporal_resolution, value) })}
                    class={`min-h-[36px] px-3 rounded-full border text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 ${
                      on ? 'border-primary-600 bg-primary-100 text-primary-900 font-semibold' : 'border-gray-300 bg-white text-gray-700 hover:border-gray-400'
                    }`}
                  >
                    {TEMPORAL_RESOLUTION_LABELS[value]}
                  </button>
                );
              })}
            </div>
          </fieldset>
          </div>

          <div class={divider}>
          <fieldset class={fieldset}>
            <legend class={legend}>Access</legend>
            {ACCESS_LEVELS.map((level) => (
              <label key={level} class={checkboxRow}>
                <input
                  type="checkbox"
                  class={checkbox}
                  checked={filters.access_level?.includes(level) ?? false}
                  onChange={() => update({ access_level: toggle(filters.access_level, level) })}
                />
                <span class="flex-1">{ACCESS_LABELS[level]}</span>
                {stats && <span class="text-[13px] text-gray-600">{stats.by_access_level[level] ?? 0}</span>}
              </label>
            ))}
            <a href="/#access" class="text-sm font-semibold text-primary-700 hover:text-primary-900">
              What do these mean?
            </a>
          </fieldset>
          </div>

          <div class={divider}>
          <fieldset class={fieldset}>
            <legend class={legend}>Source</legend>
            <span class="-mt-1 text-[13px] text-gray-600">Ministry, department or organisation that published the data</span>
            <label for="f-source" class="sr-only">
              Find a source
            </label>
            <input
              id="f-source"
              type="search"
              placeholder="Type to find a source"
              value={sourceQuery}
              onInput={(event) => setSourceQuery((event.target as HTMLInputElement).value)}
              class={input}
            />
            <div class={showAllSources ? 'max-h-80 overflow-y-auto flex flex-col gap-2.5 pr-1' : 'flex flex-col gap-2.5'}>
              {visibleOwners.map((owner) => (
                <label key={owner.id} class="flex items-start gap-2.5 text-[15px] text-gray-900 cursor-pointer">
                  <input
                    type="checkbox"
                    class={`${checkbox} mt-0.5`}
                    checked={filters.data_owner_id?.includes(owner.id) ?? false}
                    onChange={() => update({ data_owner_id: toggle(filters.data_owner_id, owner.id) })}
                  />
                  <span>{owner.name}</span>
                </label>
              ))}
              {sourceQuery.trim() && !sourceMatches.length && <p class="m-0 text-sm text-gray-600">No source matches.</p>}
            </div>
            {!sourceQuery.trim() && owners.length > SOURCES_SHOWN && (
              <button
                type="button"
                onClick={() => setShowAllSources(!showAllSources)}
                class="self-start p-0 border-0 bg-transparent text-sm font-semibold text-primary-700 hover:text-primary-900"
              >
                {showAllSources ? 'Show fewer sources' : `Show all ${owners.length} sources`}
              </button>
            )}
          </fieldset>
          </div>

          <div class={divider}>
          <fieldset class={fieldset}>
            <legend class={legend}>Data type</legend>
            {KINDS.map((kind) => (
              <label key={kind} class={checkboxRow}>
                <input
                  type="checkbox"
                  class={checkbox}
                  checked={filters.dataset_kind?.includes(kind) ?? false}
                  onChange={() => update({ dataset_kind: toggle(filters.dataset_kind, kind) })}
                />
                <span class="flex-1">{DATASET_KIND_LABELS[kind]}</span>
                {stats && <span class="text-[13px] text-gray-600">{stats.by_dataset_kind[kind] ?? 0}</span>}
              </label>
            ))}
          </fieldset>
          </div>
        </aside>

        {/* RESULTS */}
        <div id="results" ref={resultsRef} class="flex-[999_1_560px] min-w-0 flex flex-col gap-4 scroll-mt-4">
          {chips.length > 0 && (
            <div class="flex flex-wrap items-center gap-2">
              <span class="mr-1 text-sm text-gray-700">Active:</span>
              {chips.map((chip) => (
                <button
                  key={chip.key}
                  type="button"
                  aria-label={`Remove filter: ${chip.label}`}
                  onClick={chip.remove}
                  class="inline-flex items-center gap-1.5 min-h-[36px] px-3 rounded-full border border-primary-200 bg-primary-50 text-sm text-primary-900 hover:border-primary-400 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
                >
                  {chip.label} <span aria-hidden="true">×</span>
                </button>
              ))}
            </div>
          )}

          <div class="flex flex-wrap justify-between items-center gap-3">
            <p class="m-0 text-[15px] text-gray-700" aria-live="polite">
              {loading && !results ? (
                'Loading datasets…'
              ) : (
                <>
                  <strong>{total.toLocaleString('en-IN')}</strong> {total === 1 ? 'dataset matches' : 'datasets match'}
                </>
              )}
            </p>
            <div class="flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={copyLink}
                class="inline-flex items-center gap-1.5 min-h-[40px] px-3 border border-gray-300 rounded-lg bg-white text-sm text-gray-700 hover:border-gray-400 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
              >
                <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7" />
                  <path d="M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7" />
                </svg>
                <span aria-live="polite">{copied ? 'Link copied' : 'Copy link to this search'}</span>
              </button>
              <label class="flex items-center gap-2 text-sm text-gray-700">
                Sort by
                <select
                  value={filters.sort ?? 'relevance'}
                  onChange={(event) => update({ sort: (event.target as HTMLSelectElement).value as CatalogueSort })}
                  class="min-h-[40px] px-2.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-primary-500"
                >
                  {(Object.keys(SORT_LABELS) as CatalogueSort[]).map((sort) => (
                    <option key={sort} value={sort}>
                      {SORT_LABELS[sort]}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </div>

          {error && (
            <div role="alert" class="p-4 rounded-xl border border-red-200 bg-red-50 text-[15px] text-red-800">
              {error}{' '}
              <button type="button" class="p-0 border-0 bg-transparent font-semibold underline" onClick={() => setFilters({ ...filters })}>
                Try again
              </button>
            </div>
          )}

          <div class={`flex flex-col gap-4 ${loading && results ? 'opacity-60' : ''}`} aria-busy={loading}>
            {!results && loading
              ? Array.from({ length: 4 }, (_, index) => <div key={index} class="h-[220px] rounded-[14px] bg-white border border-gray-200 animate-pulse" />)
              : results?.datasets.map((dataset) => (
                  <DatasetCard key={dataset.ds_id} dataset={dataset} search={filters.search} />
                ))}
          </div>

          {results && !results.datasets.length && !loading && (
            <div class="p-6 rounded-[14px] bg-white border border-gray-200 text-[15px] text-gray-700">
              <h2 class="m-0 mb-1 text-[17px] font-bold text-gray-900">No datasets match</h2>
              <p class="m-0">
                Try fewer filters or other words.{' '}
                {activeFilterCount > 0 && (
                  <button type="button" onClick={clearAll} class="p-0 border-0 bg-transparent font-semibold text-primary-700 hover:text-primary-900">
                    Clear all filters
                  </button>
                )}
              </p>
            </div>
          )}

          {pages > 1 && (
            <nav aria-label="Pagination" class="flex flex-wrap justify-center items-center gap-1.5 mt-2">
              <button
                type="button"
                disabled={page <= 1}
                onClick={() => goToPage(page - 1)}
                class="min-h-[44px] px-3.5 rounded-lg text-sm font-semibold text-primary-700 disabled:text-gray-400 disabled:font-normal hover:bg-primary-50"
              >
                ← Previous
              </button>
              {pageNumbers(page, pages).map((item, index) =>
                item === '…' ? (
                  <span key={`gap-${index}`} class="px-1 text-gray-500">
                    …
                  </span>
                ) : (
                  <button
                    key={item}
                    type="button"
                    aria-current={item === page ? 'page' : undefined}
                    onClick={() => goToPage(item)}
                    class={`min-w-[44px] min-h-[44px] rounded-lg text-sm ${
                      item === page ? 'bg-primary-600 text-white font-semibold' : 'border border-gray-300 bg-white text-gray-700 hover:border-gray-400'
                    }`}
                  >
                    {item}
                  </button>
                ),
              )}
              <button
                type="button"
                disabled={page >= pages}
                onClick={() => goToPage(page + 1)}
                class="min-h-[44px] px-3.5 rounded-lg text-sm font-semibold text-primary-700 disabled:text-gray-400 disabled:font-normal hover:bg-primary-50"
              >
                Next →
              </button>
            </nav>
          )}

          <div class="mt-2 p-6 border border-dashed border-primary-300 rounded-[14px] bg-primary-50 flex flex-wrap gap-4 items-center">
            <div class="flex-[1_1_360px] flex flex-col gap-1.5">
              <h2 class="m-0 text-[17px] font-bold text-gray-900">Can’t find what you need?</h2>
              <p class="m-0 text-[15px] leading-relaxed text-gray-700">
                Describe it in plain words and the data assistant will suggest datasets, or tell us what’s missing.
              </p>
            </div>
            <div class="flex flex-wrap gap-2.5">
              <button
                type="button"
                onClick={() => openAssistant()}
                class="min-h-[44px] px-[18px] rounded-[10px] bg-white border border-primary-200 text-primary-900 font-semibold text-sm hover:border-primary-400 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
              >
                Describe what you need
              </button>
              <a
                href={REQUEST_DATASET_HREF}
                class="inline-flex items-center min-h-[44px] px-[18px] rounded-[10px] bg-primary-600 text-white font-semibold text-sm hover:bg-primary-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
              >
                Request a dataset
              </a>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

/** 1 … 4 5 6 … 12 */
function pageNumbers(page: number, pages: number): (number | '…')[] {
  const wanted = new Set([1, pages, page - 1, page, page + 1].filter((n) => n >= 1 && n <= pages));
  const sorted = [...wanted].sort((a, b) => a - b);
  const out: (number | '…')[] = [];
  sorted.forEach((n, index) => {
    if (index > 0 && n - (sorted[index - 1] as number) > 1) out.push('…');
    out.push(n);
  });
  return out;
}
