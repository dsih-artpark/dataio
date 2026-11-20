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
  /** Theme tile (Rukshat's prototype): soft tint start colour, e.g. 'from-red-50'. */
  tile: string;
  /** Icon square and code badge on the tile. */
  accent: string;
  /** Short coloured line at the bottom of the tile. */
  bar: string;
  /** Inner SVG markup of the tile icon (24x24, stroke). */
  icon: string;
}

/** Plain-language names for the catalogue's categories, in wireframe order. */
export const THEMES: ThemeInfo[] = [
  {
    id: 'EP',
    label: 'Disease surveillance',
    description: 'Case counts and outbreak data: dengue, TB, foot-and-mouth disease and more.',
    chip: 'bg-red-100 text-red-700 border-red-200',
    tile: 'from-red-50',
    accent: 'bg-red-100 text-red-700',
    bar: 'bg-red-300',
    icon: '<path d="M3 12h4l2-5 4 10 2-5h6"/>',
  },
  {
    id: 'CS',
    label: 'Census & surveys',
    description: 'Population and livestock censuses, national sample surveys, animal husbandry statistics.',
    chip: 'bg-blue-100 text-blue-700 border-blue-200',
    tile: 'from-blue-50',
    accent: 'bg-blue-100 text-blue-700',
    bar: 'bg-blue-300',
    icon: '<circle cx="9" cy="8" r="3"/><path d="M3 20a6 6 0 0 1 12 0"/><circle cx="17" cy="9" r="2.5"/><path d="M15.5 14.5A5 5 0 0 1 21 19"/>',
  },
  {
    id: 'GS',
    label: 'Geospatial',
    description: 'State, district and village boundaries and other map layers.',
    chip: 'bg-orange-100 text-orange-700 border-orange-200',
    tile: 'from-orange-50',
    accent: 'bg-orange-100 text-orange-700',
    bar: 'bg-orange-300',
    icon: '<path d="M9 4 3 6v14l6-2 6 2 6-2V4l-6 2-6-2z"/><path d="M9 4v14"/><path d="M15 6v14"/>',
  },
  {
    id: 'MW',
    label: 'Weather & climate',
    description: 'Station records and reanalysis data such as ERA5.',
    chip: 'bg-cyan-100 text-cyan-700 border-cyan-200',
    tile: 'from-cyan-50',
    accent: 'bg-cyan-100 text-cyan-700',
    bar: 'bg-cyan-300',
    icon: '<path d="M7 18a4 4 0 0 1-.5-8A6 6 0 0 1 18 9a4.5 4.5 0 0 1-.5 9z"/>',
  },
  {
    id: 'ET',
    label: 'Entomological data',
    description: 'Surveys of mosquitoes and other insects that spread disease.',
    chip: 'bg-green-100 text-green-700 border-green-200',
    tile: 'from-green-50',
    accent: 'bg-green-100 text-green-700',
    bar: 'bg-green-300',
    icon: '<ellipse cx="12" cy="14" rx="4" ry="6"/><path d="M12 8V4"/><path d="m8 10-4-2"/><path d="m16 10 4-2"/><path d="m8 15H3"/><path d="M16 15h5"/>',
  },
  {
    id: 'LU',
    label: 'Land use & land cover',
    description: 'Land use and land cover maps.',
    chip: 'bg-lime-100 text-lime-800 border-lime-200',
    tile: 'from-lime-50',
    accent: 'bg-lime-100 text-lime-800',
    bar: 'bg-lime-300',
    icon: '<path d="m12 3 9 5-9 5-9-5z"/><path d="m3 13 9 5 9-5"/>',
  },
  {
    id: 'ST',
    label: 'Research studies',
    description: 'Data from published studies and research projects.',
    chip: 'bg-purple-100 text-purple-700 border-purple-200',
    tile: 'from-purple-50',
    accent: 'bg-purple-100 text-purple-700',
    bar: 'bg-purple-300',
    icon: '<path d="M4 5a2 2 0 0 1 2-2h13v16H6a2 2 0 0 0-2 2z"/><path d="M4 19V5"/>',
  },
];

/** Shown as a placeholder until the theme has datasets (decision 5 in the redesign plan). */
export const COMING_SOON_THEME = {
  label: 'Environmental surveillance',
  description: 'Wastewater and other environmental monitoring data.',
  chip: 'bg-indigo-100 text-indigo-700 border-indigo-200',
  tile: 'from-indigo-50',
  accent: 'bg-indigo-100 text-indigo-700',
  bar: 'bg-indigo-300',
  icon: '<path d="M12 3s6 6.5 6 11a6 6 0 0 1-12 0c0-4.5 6-11 6-11z"/>',
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
