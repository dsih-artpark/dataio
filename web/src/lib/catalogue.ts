// Public catalogue: types for /public/datasets, /public/datasets/{id} and
// /public/stats, plus the plain-language labels the public pages use.

export type CatalogueAccessLevel = 'DOWNLOAD' | 'VIEW' | 'NONE';
export type DatasetKind = 'tabular' | 'geojson';
export type CatalogueSort = 'relevance' | 'updated' | 'newest' | 'span' | 'title';

export interface CatalogueDataset {
  ds_id: string;
  title: string;
  description: string | null;
  collection_id: number | null;
  collection_name: string | null;
  category_id: string | null;
  category_name: string | null;
  data_owner_id: number | null;
  data_owner_name: string | null;
  spatial_coverage_region_id: string | null;
  spatial_coverage_region_name: string | null;
  spatial_resolution: string | null;
  temporal_coverage_start_date: string | null;
  temporal_coverage_end_date: string | null;
  temporal_resolution: string | null;
  access_level: CatalogueAccessLevel;
  licence: string | null;
  dataset_kind: DatasetKind;
  tags: string[];
  has_data_dictionary: boolean;
  /** Tables the catalogue describes; 0 means no data files yet. */
  table_count: number;
  updated_at: string | null;
}

export interface CatalogueColumn {
  name: string;
  description: string | null;
  type: string | null;
  unit: string | null;
}

export interface CatalogueTable {
  name: string;
  description: string | null;
  columns: CatalogueColumn[];
}

export interface CatalogueDatasetDetail extends CatalogueDataset {
  tables: CatalogueTable[];
  // Left out (null) for Restricted datasets.
  readme_md: string | null;
  has_manifest: boolean;
  manifest_updated_at: string | null;
}

export interface CatalogueSearchResponse {
  datasets: CatalogueDataset[];
  total: number;
  limit: number;
  offset: number;
  sort: CatalogueSort;
}

export interface CatalogueStats {
  total_datasets: number;
  total_sources: number;
  earliest_year: number | null;
  latest_year: number | null;
  by_access_level: Record<CatalogueAccessLevel, number>;
  by_dataset_kind: Record<DatasetKind, number>;
  themes: { category_id: string; category_name: string; count: number }[];
  coverage: {
    national: number;
    unknown: number;
    states: { region_id: string; region_name: string; count: number }[];
  };
}

export interface CataloguePreview {
  /** real = Open download; synthetic = View only; none = Restricted; unavailable = no CSV table. */
  mode: 'real' | 'synthetic' | 'none' | 'unavailable';
  table: string | null;
  columns: string[];
  rows: string[][];
}

export interface OpenDownloadUrls {
  dataset_id: string;
  title: string;
  tables: { table_name: string; download_url: string; metadata: Record<string, unknown> }[];
  /** Random ID for the dataio_visitor cookie, so the next download doesn't ask again. */
  visitor_id: string;
}

/** Signed-out download: email + terms the first time, then only the visitor ID. */
export interface OpenDownloadConsent {
  email?: string;
  visitor_id?: string;
  accept_terms?: boolean;
}

export interface RegionOption {
  region_id: string;
  region_name: string;
}

export const ORGANISATION_TYPES = [
  'NGO / non-profit',
  'University or research institute',
  'Government',
  'Philanthropy / foundation',
  'Private company',
  'Other',
] as const;

export interface AccessRequestInput {
  dataset_id: string;
  full_name: string;
  email: string;
  organisation: string;
  organisation_type: string;
  organisation_type_other?: string | null;
  purpose: string;
  consent: boolean;
}

export type AccessRequestStatus = 'PENDING' | 'APPROVED' | 'REJECTED';

export interface AccessRequestRecord {
  id: string;
  dataset_id: string;
  dataset_title: string | null;
  full_name: string;
  email: string;
  organisation: string;
  organisation_type: string;
  organisation_type_other: string | null;
  purpose: string;
  status: AccessRequestStatus;
  created_at: string | null;
  reviewed_at: string | null;
  reviewed_by: string | null;
  review_note: string | null;
}

/** Filters for /public/datasets. The same keys are used in the /datasets page address. */
export interface CatalogueFilters {
  search?: string;
  category_id?: string[];
  collection_id?: number[];
  /** States, districts or country_IN; a dataset in any of them matches. */
  region_id?: string[];
  spatial_resolution?: string[];
  temporal_resolution?: string[];
  year_from?: number;
  year_to?: number;
  access_level?: CatalogueAccessLevel[];
  data_owner_id?: number[];
  dataset_kind?: DatasetKind[];
  licence?: string[];
  sort?: CatalogueSort;
  limit?: number;
  offset?: number;
}

const LIST_KEYS = [
  'category_id',
  'collection_id',
  'region_id',
  'spatial_resolution',
  'temporal_resolution',
  'access_level',
  'data_owner_id',
  'dataset_kind',
  'licence',
] as const;
const NUMBER_LIST_KEYS = new Set(['collection_id', 'data_owner_id']);

/** Filters → query string. Lists are comma-separated, empty values left out. */
export function catalogueQuery(filters: CatalogueFilters): URLSearchParams {
  const params = new URLSearchParams();
  if (filters.search?.trim()) params.set('search', filters.search.trim());
  for (const key of LIST_KEYS) {
    const values = filters[key] as (string | number)[] | undefined;
    if (values && values.length) params.set(key, values.join(','));
  }
  if (filters.year_from != null) params.set('year_from', String(filters.year_from));
  if (filters.year_to != null) params.set('year_to', String(filters.year_to));
  if (filters.sort && filters.sort !== 'relevance') params.set('sort', filters.sort);
  if (filters.limit != null) params.set('limit', String(filters.limit));
  if (filters.offset) params.set('offset', String(filters.offset));
  return params;
}

/** Query string → filters, ignoring anything malformed. Inverse of catalogueQuery. */
export function parseCatalogueQuery(params: URLSearchParams): CatalogueFilters {
  const filters: CatalogueFilters = {};
  const search = params.get('search');
  if (search) filters.search = search;
  for (const key of LIST_KEYS) {
    const raw = params.get(key);
    if (!raw) continue;
    const parts = raw
      .split(',')
      .map((part) => part.trim())
      .filter(Boolean);
    const values = NUMBER_LIST_KEYS.has(key) ? parts.map(Number).filter((n) => Number.isInteger(n)) : parts;
    if (values.length) (filters as Record<string, unknown>)[key] = values;
  }
  for (const key of ['year_from', 'year_to', 'limit', 'offset'] as const) {
    const raw = params.get(key);
    if (raw && /^\d+$/.test(raw)) filters[key] = Number(raw);
  }
  const sort = params.get('sort');
  if (sort && SORT_LABELS[sort as CatalogueSort]) filters.sort = sort as CatalogueSort;
  return filters;
}

// ------------------------------------------------------------------ labels

export const ACCESS_LABELS: Record<CatalogueAccessLevel, string> = {
  DOWNLOAD: 'Open download',
  VIEW: 'View only',
  NONE: 'Restricted',
};

export const ACCESS_DESCRIPTIONS: Record<CatalogueAccessLevel, string> = {
  DOWNLOAD: 'Download the full dataset straight away. No sign-in needed.',
  VIEW: 'See the tables, columns and documentation, with a preview of sample values. Ask us for access to download.',
  NONE: 'Sensitive data. Listed so you know it exists; shared under agreement.',
};

export const SORT_LABELS: Record<CatalogueSort, string> = {
  relevance: 'Most relevant',
  updated: 'Recently updated',
  newest: 'Newest data',
  span: 'Longest time span',
  title: 'A–Z',
};

export const DATASET_KIND_LABELS: Record<DatasetKind, string> = {
  tabular: 'Tables',
  geojson: 'Map boundaries',
};

/** Level of detail (spatial resolution). */
export const SPATIAL_RESOLUTION_LABELS: Record<string, string> = {
  COUNTRY: 'Country',
  STATE: 'State',
  UT: 'Union territory',
  DISTRICT: 'District',
  SUBDISTRICT: 'Sub-district',
  MUNICIPALITY: 'Municipality',
  VILLAGE: 'Village',
  WARD: 'Ward',
  PRABHAG: 'Prabhag',
  ULB: 'Urban local body',
  'LAT/LONG': 'Point locations',
  OTHER: 'Other',
};

/** How often it's recorded (temporal resolution). */
export const TEMPORAL_RESOLUTION_LABELS: Record<string, string> = {
  YEAR: 'Yearly',
  MONTH: 'Monthly',
  WEEK: 'Weekly',
  DATE: 'Daily',
  HOUR: 'Hourly',
  MINUTE: 'Every minute',
  SECOND: 'Every second',
  NONE: 'One-time',
};

export const LICENCE_NOT_SPECIFIED = 'Not yet specified';
export const NO_TABLES = 'Not yet available';
export const NO_FILES_MESSAGE = 'The data files for this dataset haven’t been published yet.';

/** Value for the "Tables" chip: the count, or null when there are none (chip shows NO_TABLES). */
export function tableCountLabel(count: number): string | null {
  return count > 0 ? String(count) : null;
}

export function labelFor(labels: Record<string, string>, value: string | null | undefined): string | null {
  if (!value) return null;
  return labels[value] ?? value.charAt(0) + value.slice(1).toLowerCase();
}

function yearOf(isoDate: string | null | undefined): number | null {
  if (!isoDate) return null;
  const year = Number(isoDate.slice(0, 4));
  return Number.isFinite(year) ? year : null;
}

/** "2015–2023", "2019", "From 2015", or null when no dates are recorded. */
export function formatYears(start: string | null | undefined, end: string | null | undefined): string | null {
  const from = yearOf(start);
  const to = yearOf(end);
  if (from && to) return from === to ? String(from) : `${from}–${to}`;
  if (from) return `From ${from}`;
  if (to) return `Until ${to}`;
  return null;
}

/** "Mar 2026" from an ISO timestamp. */
export function formatMonthYear(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return null;
  return date.toLocaleDateString('en-IN', { month: 'short', year: 'numeric' });
}

/**
 * Address of a dataset's page. The site is static files, so the ID goes in the
 * query string; a clean /datasets/<id> needs a rewrite rule on the web server.
 */
export function datasetPath(dsId: string): string {
  return `/dataset?id=${encodeURIComponent(dsId)}`;
}

export function requestAccessPath(dsId: string): string {
  return `/request-access?dataset=${encodeURIComponent(dsId)}`;
}
