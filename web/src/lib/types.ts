/**
 * Shared TypeScript types for the DataIO web application.
 */

// Dataset list item (from /datasets endpoint)
export interface Dataset {
  ds_id: string;
  title: string;
  description: string | null;
  collection_id: number;
  collection_name: string;
  data_owner_id: number;
  data_owner_name: string;
  temporal_coverage_start_date: string | null;
  temporal_coverage_end_date: string | null;
  access_level: AccessLevel;
}

// Dataset detail (from /datasets/{id} endpoint)
export interface DatasetDetail {
  ds_id: string;
  title: string;
  description: string | null;
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
  spatial_resolution?: SpatialResolution;
  temporal_coverage_start_date?: string;
  temporal_coverage_end_date?: string;
  temporal_resolution?: TemporalResolution;
  access_level: AccessLevel;
  can_download: boolean;
  raw_datasets?: RawDataset[];
  tags?: string[];
  // Documentation fields (cached from file server)
  readme_md?: string | null;
  data_dictionary_json?: string | null;
  documentation_synced_at?: string | null;
}

export interface RawDataset {
  id: number;
  rds_id: string;
  title: string;
  source?: string;
}

export interface Collection {
  id: number;
  collection_id: string;
  collection_name: string;
  category_id: string;
  category_name: string;
}

export interface DataOwner {
  id: number;
  name: string;
}

// Enums matching backend
export type AccessLevel = 'NONE' | 'VIEW' | 'DOWNLOAD';

export type SpatialResolution =
  | 'COUNTRY'
  | 'STATE'
  | 'UT'
  | 'DISTRICT'
  | 'SUBDISTRICT'
  | 'MUNICIPALITY'
  | 'VILLAGE'
  | 'WARD'
  | 'PRABHAG'
  | 'ULB'
  | 'LAT/LONG'
  | 'OTHER';

export type TemporalResolution =
  | 'NONE'
  | 'YEAR'
  | 'MONTH'
  | 'WEEK'
  | 'DATE'
  | 'HOUR'
  | 'MINUTE'
  | 'SECOND';

// Filter state for dataset browser
export interface FilterState {
  search: string;
  collections: number[];
  accessLevels: AccessLevel[];
}

// API response types
export interface DatasetsResponse {
  datasets: Dataset[];
  total: number;
  limit: number;
  offset: number;
}

export interface CollectionsResponse {
  collections: Collection[];
}

export interface DataOwnersResponse {
  data_owners: DataOwner[];
}
