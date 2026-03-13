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
  manifest_yaml?: string | null;
  manifest_json?: Record<string, unknown> | null;
  manifest_updated_at?: string | null;
  manifest_updated_by?: string | null;
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

// Metadata JSON structure (from metadata.json files in S3)
export interface DataDictionaryField {
  description: string | null;
  comments: string | null;
  access: boolean;
}

export interface TableMetadata {
  table_name: string;
  description: string | null;
  source: string | null;
  data_dictionary: Record<string, DataDictionaryField>;
}

export interface MetadataJson {
  tables: Record<string, TableMetadata>;
}

// Download URLs response (from /datasets/{id}/download-urls endpoint)
export interface DatasetDownloadUrls {
  ds_id: string;
  title: string;
  tables: {
    table_name: string;
    download_url: string;
    metadata: Record<string, unknown>;
  }[];
  readme_md: string | null;
  data_dictionary_json: string | null;
}

export interface AdminDatasetSummary {
  ds_id: string;
  title: string;
  access_level: string | null;
}

export interface AdminManifestRecord {
  dataset_id: string;
  bucket_type: string;
  manifest_yaml: string | null;
  manifest_json: Record<string, unknown> | null;
  has_manifest: boolean;
  manifest_updated_at: string | null;
  manifest_updated_by: string | null;
}

export interface ValidationFinding {
  severity: string;
  code: string;
  message: string;
  path?: string | null;
  table?: string | null;
  row?: number | null;
  field?: string | null;
  rule_id?: string | null;
  hint?: string | null;
}

export interface ValidationResult {
  status: 'pass' | 'warn' | 'fail';
  dataset_kind: string;
  metadata_spec_version?: string | null;
  summary: {
    errors: number;
    warnings: number;
    infos: number;
    rows_checked: number;
    tables_checked: number;
  };
  findings: ValidationFinding[];
  inferred: Record<string, unknown>;
}
