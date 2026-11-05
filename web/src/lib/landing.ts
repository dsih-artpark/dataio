// Landing page content that is not in the catalogue: theme wording and
// colours, the state tile map layout, popular searches. Live numbers come
// from /public/stats via loadCatalogueStats().
import { api } from './api';
import { catalogueQuery, type CatalogueFilters, type CatalogueStats } from './catalogue';

let statsPromise: Promise<CatalogueStats> | null = null;

/** /public/stats, fetched once per page load and shared by every island. */
export function loadCatalogueStats(): Promise<CatalogueStats> {
  if (!statsPromise) {
    statsPromise = api.getCatalogueStats().catch((error) => {
      statsPromise = null;
      throw error;
    });
  }
  return statsPromise;
}

/** /datasets address for a set of filters. */
export function datasetsHref(filters: CatalogueFilters = {}): string {
  const query = catalogueQuery(filters).toString();
  return query ? `/datasets?${query}` : '/datasets';
}

export interface ThemeInfo {
  /** collections.category_id */
  id: string;
  label: string;
  description: string;
  /** Tailwind classes for the hero chip. */
  chip: string;
}

/** Plain-language names for the catalogue's categories, in wireframe order. */
export const THEMES: ThemeInfo[] = [
  {
    id: 'EP',
    label: 'Disease surveillance',
    description: 'Case counts and outbreak data: dengue, TB, foot-and-mouth disease and more.',
    chip: 'bg-red-100 text-red-700 border-red-200',
  },
  {
    id: 'CS',
    label: 'Census & surveys',
    description: 'Population and livestock censuses, national sample surveys, animal husbandry statistics.',
    chip: 'bg-blue-100 text-blue-700 border-blue-200',
  },
  {
    id: 'GS',
    label: 'Geospatial',
    description: 'State, district and village boundaries and other map layers.',
    chip: 'bg-orange-100 text-orange-700 border-orange-200',
  },
  {
    id: 'MW',
    label: 'Weather & climate',
    description: 'Station records and reanalysis data such as ERA5.',
    chip: 'bg-cyan-100 text-cyan-700 border-cyan-200',
  },
  {
    id: 'ET',
    label: 'Entomological data',
    description: 'Surveys of mosquitoes and other insects that spread disease.',
    chip: 'bg-green-100 text-green-700 border-green-200',
  },
  {
    id: 'LU',
    label: 'Land use & land cover',
    description: 'Land use and land cover maps.',
    chip: 'bg-lime-100 text-lime-800 border-lime-200',
  },
  {
    id: 'ST',
    label: 'Research studies',
    description: 'Data from published studies and research projects.',
    chip: 'bg-purple-100 text-purple-700 border-purple-200',
  },
];

/** Shown as a placeholder until the theme has datasets (decision 5 in the redesign plan). */
export const COMING_SOON_THEME = {
  label: 'Environmental surveillance',
  description: 'Wastewater and other environmental monitoring data.',
  chip: 'bg-indigo-100 text-indigo-700 border-indigo-200',
};

export const POPULAR_SEARCHES = [
  'livestock census by district',
  'rainfall data for Odisha',
  'dengue cases since 2015',
];

export interface StateTile {
  code: string;
  name: string;
  /** regions.region_id */
  regionId: string;
  col: number;
  row: number;
}

/** One square per state, not to scale. Some union territories are left out. */
export const STATE_TILES: StateTile[] = [
  { code: 'JK', name: 'Jammu & Kashmir', regionId: 'ut_1', col: 4, row: 1 },
  { code: 'LA', name: 'Ladakh', regionId: 'ut_37', col: 5, row: 1 },
  { code: 'PB', name: 'Punjab', regionId: 'state_3', col: 3, row: 2 },
  { code: 'HP', name: 'Himachal Pradesh', regionId: 'state_2', col: 4, row: 2 },
  { code: 'UK', name: 'Uttarakhand', regionId: 'state_5', col: 5, row: 2 },
  { code: 'RJ', name: 'Rajasthan', regionId: 'state_8', col: 2, row: 3 },
  { code: 'HR', name: 'Haryana', regionId: 'state_6', col: 3, row: 3 },
  { code: 'DL', name: 'Delhi', regionId: 'ut_7', col: 4, row: 3 },
  { code: 'UP', name: 'Uttar Pradesh', regionId: 'state_9', col: 5, row: 3 },
  { code: 'BR', name: 'Bihar', regionId: 'state_10', col: 6, row: 3 },
  { code: 'SK', name: 'Sikkim', regionId: 'state_11', col: 7, row: 3 },
  { code: 'AR', name: 'Arunachal Pradesh', regionId: 'state_12', col: 9, row: 3 },
  { code: 'GJ', name: 'Gujarat', regionId: 'state_24', col: 1, row: 4 },
  { code: 'MP', name: 'Madhya Pradesh', regionId: 'state_23', col: 3, row: 4 },
  { code: 'CG', name: 'Chhattisgarh', regionId: 'state_22', col: 4, row: 4 },
  { code: 'JH', name: 'Jharkhand', regionId: 'state_20', col: 5, row: 4 },
  { code: 'WB', name: 'West Bengal', regionId: 'state_19', col: 6, row: 4 },
  { code: 'AS', name: 'Assam', regionId: 'state_18', col: 8, row: 4 },
  { code: 'NL', name: 'Nagaland', regionId: 'state_13', col: 9, row: 4 },
  { code: 'MH', name: 'Maharashtra', regionId: 'state_27', col: 2, row: 5 },
  { code: 'TG', name: 'Telangana', regionId: 'state_36', col: 4, row: 5 },
  { code: 'OD', name: 'Odisha', regionId: 'state_21', col: 5, row: 5 },
  { code: 'ML', name: 'Meghalaya', regionId: 'state_17', col: 7, row: 5 },
  { code: 'TR', name: 'Tripura', regionId: 'state_16', col: 8, row: 5 },
  { code: 'MN', name: 'Manipur', regionId: 'state_14', col: 9, row: 5 },
  { code: 'GA', name: 'Goa', regionId: 'state_30', col: 2, row: 6 },
  { code: 'KA', name: 'Karnataka', regionId: 'state_29', col: 3, row: 6 },
  { code: 'AP', name: 'Andhra Pradesh', regionId: 'state_28', col: 4, row: 6 },
  { code: 'MZ', name: 'Mizoram', regionId: 'state_15', col: 9, row: 6 },
  { code: 'KL', name: 'Kerala', regionId: 'state_32', col: 3, row: 7 },
  { code: 'TN', name: 'Tamil Nadu', regionId: 'state_33', col: 4, row: 7 },
  { code: 'PY', name: 'Puducherry', regionId: 'ut_34', col: 5, row: 7 },
];

/**
 * Theme counts keyed by category_id. The stats endpoint groups by id and
 * name, so a collection with a blank or different name for the same id
 * shows up as a second row; those are added together here.
 */
export function themeCounts(stats: CatalogueStats | null): Map<string, { name: string; count: number }> {
  const counts = new Map<string, { name: string; count: number }>();
  for (const theme of stats?.themes ?? []) {
    if (!theme.category_id) continue;
    const existing = counts.get(theme.category_id);
    const name = theme.category_name?.trim() || existing?.name || theme.category_id;
    counts.set(theme.category_id, { name, count: (existing?.count ?? 0) + theme.count });
  }
  return counts;
}
