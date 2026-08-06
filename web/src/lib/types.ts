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
  has_manifest?: boolean;
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
  manifest_yaml: string | null;
  manifest_json: Record<string, unknown> | null;
}

export interface AdminDatasetSummary {
  ds_id: string;
  title: string;
  access_level: string | null;
}

export interface AdminRawDataset {
  id: number;
  rds_id: string;
  title: string;
  source: string;
}

export interface AdminDatasetDetail {
  ds_id: string;
  title: string;
  collection_id: string | null;
  collection_name: string | null;
  data_owner_name: string | null;
  description: string | null;
  spatial_coverage_region_id: string | null;
  spatial_resolution: string | null;
  temporal_coverage_start_date: string | null;
  temporal_coverage_end_date: string | null;
  temporal_resolution: string | null;
  access_level: string | null;
  additional_metadata: Record<string, unknown> | null;
  tags: string[];
  raw_dataset_ids: string[];
  raw_datasets: AdminRawDataset[];
  readme_md: string | null;
  data_dictionary_json: string | null;
  manifest_yaml: string | null;
  manifest_json: Record<string, unknown> | null;
  manifest_updated_at: string | null;
  manifest_updated_by: string | null;
  documentation_synced_at: string | null;
}

export interface AdminDatasetTable {
  table_name: string;
  download_link: string;
  metadata: Record<string, unknown>;
}

export interface AdminDatasetTablesResponse {
  dataset_id: string;
  bucket_type: string;
  tables: AdminDatasetTable[];
}

export interface AdminDatasetPackageTable {
  table_name: string;
  description: string | null;
  source: string | null;
  table_metadata: Record<string, unknown>;
}

export interface AdminDatasetPackagePreview {
  dataset: {
    ds_id: string;
    title: string;
    collection_id: string;
    data_owner_name: string;
    description: string | null;
    spatial_coverage_region_id: string | null;
    spatial_resolution: string | null;
    temporal_coverage_start_date: string | null;
    temporal_coverage_end_date: string | null;
    temporal_resolution: string | null;
    access_level: string;
    additional_metadata: Record<string, unknown> | null;
    tags: string[];
    raw_dataset_ids: string[];
  };
  raw_dataset: {
    rds_id: string;
    title: string;
    source: string;
  };
  tables: AdminDatasetPackageTable[];
  manifest_yaml: string;
  findings: ValidationFinding[];
  suggested_dataset_id: string | null;
  can_import: boolean;
}

export interface ReservedDatasetId {
  ds_id: string;
  collection_id: string | null;
  note: string | null;
  reserved_by: string;
  created_at: string | null;
}

export interface AdminRawDatasetsResponse {
  raw_datasets: AdminRawDataset[];
  total: number;
  limit: number;
  offset: number;
}

export interface DatasetIdSuggestion {
  collection_id: string;
  suggested_dataset_id: string;
}

export interface RawDatasetIdSuggestion {
  collection_id: string;
  suggested_raw_dataset_id: string;
}

export interface NextDatasetIdNumber {
  next_number: number;
}

export interface RawDatasetIdSuggestionByCategory {
  category_id: string;
  suggested_raw_dataset_id: string;
}

export interface DocumentationSyncDatasetStatus {
  ds_id: string;
  needs_update: boolean;
  changed_fields: string[];
  has_remote_documentation?: boolean;
  manifest_updated_at?: string | null;
  documentation_synced_at?: string | null;
  updated?: boolean;
  skipped?: boolean;
}

export interface DocumentationSyncCheckResponse {
  datasets: DocumentationSyncDatasetStatus[];
  total: number;
  outdated: number;
}

export interface DocumentationSyncRunResponse {
  datasets: DocumentationSyncDatasetStatus[];
  total: number;
  updated: number;
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

export interface DatasetManifestRecord {
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
  line?: number | null;
  column?: number | null;
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

export interface ManifestDraftFlaggedField {
  field: string;
  reason: string;
}

export interface ManifestDraftReviewerNote {
  field?: string;
  note: string;
  by: string;
}

export type ManifestDraftStatus = 'pending' | 'approved' | 'rejected' | 'flagged';

export interface ManifestDraftSummary {
  draft_id: string;
  dataset_id: string | null;
  collection_id: string;
  category_id: string;
  status: ManifestDraftStatus;
  // null for a deterministic (rule-based, no-LLM) draft - see
  // adminGenerateDeterministicManifestDraft.
  llm_model_id: string | null;
  created_by: string;
  created_at: string | null;
  reviewed_by: string | null;
  reviewed_at: string | null;
  superseded_by_draft_id: string | null;
}

// The manifest fields no deterministic rule can derive from the CSV alone -
// supplied directly by the curator through the intake form. Mirrors
// dataio.api.services.deterministic_draft_service.CuratorMetadataInput.
export interface CuratorMetadataInput {
  datasetDescription: string;
  source: string[];
  references: string[];
  tags: { concept: string[]; epiType: string[] };
  spatialCoverage: string;
  spatialResolution: string;
  temporalCoverage: string;
  temporalResolution: string;
  updateFrequency: string;
  comments: string[];
  // The curator's confirmed/edited subset of the auto-suggested join-key
  // candidates - leave empty to accept the backend's own suggestion.
  joinKeyColumns: string[];
  // Required, one entry per table (keyed by table name - the CSV filename
  // without its extension). No rule can derive real table-level narrative
  // from a CSV alone.
  tableDescriptions: Record<string, string>;
  // Required, one entry per column not classified as "fixed" by
  // /admin/manifest-drafts/classify-columns (keyed table name -> column
  // name). Region-identifier and source/provenance columns are excluded -
  // those are auto-filled server-side.
  columnDescriptions: Record<string, Record<string, string>>;
  // Only meaningful (and required) with 2+ CSVs - a single-CSV dataset is
  // always named after that CSV's own filename regardless of this value.
  datasetTitle: string;
}

export interface ManifestDraftDetail extends ManifestDraftSummary {
  // Whether dataset_id (a reserved ID, always set) already corresponds to
  // a real Dataset row - a draft with dataset_id set could still be for a
  // brand-new dataset, since the ID gets reserved before the dataset
  // actually exists.
  dataset_exists: boolean | null;
  source_csv_path: string;
  digitization_log_path: string | null;
  draft_yaml: string;
  draft_json: Record<string, unknown>;
  flagged_fields: ManifestDraftFlaggedField[];
  reviewer_notes: ManifestDraftReviewerNote[];
  validation_result: ValidationResult | null;
}
