import type { ComponentChildren } from 'preact';
import { useEffect, useMemo, useState } from 'preact/hooks';
import { marked } from 'marked';

import { ApiRequestError, api } from '../../lib/api';
import type {
  AdminDatasetDetail,
  AdminDatasetPackagePreview,
  AdminDatasetSummary,
  AdminRawDataset,
  Collection,
  DataOwner,
  DocumentationSyncDatasetStatus,
  ReservedDatasetId,
  ValidationFinding,
} from '../../lib/types';

const STANDARDISED_BUCKET = 'STANDARDISED';
const PREPROCESSED_BUCKET = 'PREPROCESSED';

type DatasetAdminView = 'new' | 'catalog' | 'reservations' | 'sync' | 'detail' | 'ids';
type DetailWorkspaceTab = 'metadata' | 'sharing' | 'tables' | 'manifest' | 'documentation' | 'sync' | 'danger';

type DatasetFormState = {
  ds_id: string;
  title: string;
  collection_id: string;
  data_owner_name: string;
  description: string;
  spatial_coverage_region_id: string;
  spatial_resolution: string;
  temporal_coverage_start_date: string;
  temporal_coverage_end_date: string;
  temporal_resolution: string;
  access_level: string;
  additional_metadata: string;
  tags: string;
  raw_dataset_ids: string[];
};

function emptyDatasetForm(): DatasetFormState {
  return {
    ds_id: '',
    title: '',
    collection_id: '',
    data_owner_name: '',
    description: '',
    spatial_coverage_region_id: '',
    spatial_resolution: '',
    temporal_coverage_start_date: '',
    temporal_coverage_end_date: '',
    temporal_resolution: '',
    access_level: 'NONE',
    additional_metadata: '{}',
    tags: '',
    raw_dataset_ids: [],
  };
}

function datasetDetailToForm(detail: AdminDatasetDetail): DatasetFormState {
  return {
    ds_id: detail.ds_id,
    title: detail.title ?? '',
    collection_id: detail.collection_id ?? '',
    data_owner_name: detail.data_owner_name ?? '',
    description: detail.description ?? '',
    spatial_coverage_region_id: detail.spatial_coverage_region_id ?? '',
    spatial_resolution: detail.spatial_resolution ?? '',
    temporal_coverage_start_date: detail.temporal_coverage_start_date ?? '',
    temporal_coverage_end_date: detail.temporal_coverage_end_date ?? '',
    temporal_resolution: detail.temporal_resolution ?? '',
    access_level: detail.access_level ?? 'NONE',
    additional_metadata: JSON.stringify(detail.additional_metadata ?? {}, null, 2),
    tags: (detail.tags ?? []).join(', '),
    raw_dataset_ids: detail.raw_dataset_ids ?? [],
  };
}

function parseJsonObject(text: string) {
  const trimmed = text.trim();
  if (!trimmed) return null;
  const parsed = JSON.parse(trimmed) as Record<string, unknown>;
  return parsed;
}

function csvToList(text: string) {
  return text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function mergeRawDatasetOptions(existing: AdminRawDataset[], required: AdminRawDataset[]) {
  const merged = new Map(existing.map((item) => [item.rds_id, item]));
  for (const item of required) {
    merged.set(item.rds_id, item);
  }
  return Array.from(merged.values()).sort((left, right) => left.rds_id.localeCompare(right.rds_id));
}

function severityClasses(severity: string) {
  if (severity === 'error') return 'bg-red-50 text-red-700 ring-red-200';
  if (severity === 'warning') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-slate-50 text-slate-700 ring-slate-200';
}

function findingLabel(finding: ValidationFinding) {
  const parts = [
    finding.table ? `table ${finding.table}` : null,
    typeof finding.row === 'number' ? `row ${finding.row}` : null,
    finding.field ? `field ${finding.field}` : null,
    finding.path ? `path ${finding.path}` : null,
  ].filter(Boolean);

  return parts.join(' • ');
}

interface DatasetAdminManagerProps {
  view?: DatasetAdminView;
  datasetId?: string;
}

export default function DatasetAdminManager({
  view = 'catalog',
  datasetId = '',
}: DatasetAdminManagerProps) {
  const [datasets, setDatasets] = useState<AdminDatasetSummary[]>([]);
  const [rawDatasets, setRawDatasets] = useState<AdminRawDataset[]>([]);
  const [collections, setCollections] = useState<Collection[]>([]);
  const [dataOwners, setDataOwners] = useState<DataOwner[]>([]);
  const [datasetSearch, setDatasetSearch] = useState('');
  const [selectedDatasetId, setSelectedDatasetId] = useState('');
  const [selectedBucket, setSelectedBucket] = useState(STANDARDISED_BUCKET);
  const [datasetDetail, setDatasetDetail] = useState<AdminDatasetDetail | null>(null);
  const [editForm, setEditForm] = useState<DatasetFormState>(emptyDatasetForm());
  const [createForm, setCreateForm] = useState<DatasetFormState>(emptyDatasetForm());
  const [rawDatasetForm, setRawDatasetForm] = useState({ rds_id: '', title: '', source: '' });
  const [rawDatasetCollectionId, setRawDatasetCollectionId] = useState('');
  const [selectedRawDatasetId, setSelectedRawDatasetId] = useState('');
  const [rawDatasetEditForm, setRawDatasetEditForm] = useState({ title: '', source: '' });
  const [idsLookupCollectionId, setIdsLookupCollectionId] = useState('');
  const [idsLookupResult, setIdsLookupResult] = useState<{ collectionId: string; nextDatasetId: string; nextRawDatasetId: string } | null>(null);
  const [idsLookupLoading, setIdsLookupLoading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadingDatasetDetail, setLoadingDatasetDetail] = useState(false);
  const [statusMessage, setStatusMessage] = useState('');
  const [errorMessage, setErrorMessage] = useState('');
  const [manifestRecord, setManifestRecord] = useState<{ manifest_yaml: string | null; has_manifest: boolean; manifest_updated_at: string | null; manifest_updated_by: string | null } | null>(null);
  const [manifestFile, setManifestFile] = useState<File | null>(null);
  const [manifestFindings, setManifestFindings] = useState<ValidationFinding[]>([]);
  const [manifestLoadError, setManifestLoadError] = useState('');
  const [tableRows, setTableRows] = useState<{ table_name: string; download_link: string; metadata: Record<string, unknown> }[]>([]);
  const [tableLoadError, setTableLoadError] = useState('');
  const [tableFile, setTableFile] = useState<File | null>(null);
  const [tableMetadataText, setTableMetadataText] = useState('{\n  "table_name": "",\n  "description": "",\n  "source": "",\n  "data_dictionary": {}\n}');
  const [documentationStatuses, setDocumentationStatuses] = useState<DocumentationSyncDatasetStatus[]>([]);
  const [importInfoFile, setImportInfoFile] = useState<File | null>(null);
  const [importMetadataFile, setImportMetadataFile] = useState<File | null>(null);
  const [importCsvFiles, setImportCsvFiles] = useState<File[]>([]);
  const [importPreview, setImportPreview] = useState<AdminDatasetPackagePreview | null>(null);
  const [importingPackage, setImportingPackage] = useState(false);
  const [deleteVerificationCode, setDeleteVerificationCode] = useState('');
  const [deleteConfirmationDatasetId, setDeleteConfirmationDatasetId] = useState('');
  const [deletingDataset, setDeletingDataset] = useState(false);
  const [reservedDatasetIds, setReservedDatasetIds] = useState<ReservedDatasetId[]>([]);
  const [reserveDatasetId, setReserveDatasetId] = useState('');
  const [reserveCollectionId, setReserveCollectionId] = useState('');
  const [reserveNote, setReserveNote] = useState('');
  const [datasetUsers, setDatasetUsers] = useState<{ email: string; display_name: string | null }[]>([]);
  const [datasetGroups, setDatasetGroups] = useState<{ email: string; display_name: string | null }[]>([]);
  const [shareUserEmail, setShareUserEmail] = useState('');
  const [shareGroupEmail, setShareGroupEmail] = useState('');
  const [sharePermission, setSharePermission] = useState('VIEW');
  const [savingAccessLevelFor, setSavingAccessLevelFor] = useState('');
  const [quickEditDatasetId, setQuickEditDatasetId] = useState('');
  const [quickEditForm, setQuickEditForm] = useState<DatasetFormState>(emptyDatasetForm());
  const [shareModalDatasetId, setShareModalDatasetId] = useState('');
  const [detailTab, setDetailTab] = useState<DetailWorkspaceTab>('metadata');
  const [readmeDraft, setReadmeDraft] = useState('');
  const [dataDictionaryDraft, setDataDictionaryDraft] = useState('{}');
  const [savingDocumentation, setSavingDocumentation] = useState(false);
  const isCreateView = view === 'new';
  const isCatalogView = view === 'catalog';
  const isReservationsView = view === 'reservations';
  const isSyncView = view === 'sync';
  const isDetailView = view === 'detail';
  const isIdsView = view === 'ids';
  const showSelectionToolbar = isCatalogView || isSyncView || isDetailView;
  const showCatalogSection = isCatalogView;
  const showReservationsSection = isReservationsView;
  const showCreateSections = isCreateView;
  const showDetailSections = isDetailView;
  const showSyncSection = isSyncView || isDetailView;
  const showIdsSection = isIdsView;

  const loadReferenceData = async (search?: string) => {
    setLoading(true);
    setErrorMessage('');
    try {
      const [datasetResponse, rawDatasetResponse, collectionResponse, dataOwnerResponse] = await Promise.all([
        api.adminListDatasets({ search, limit: 100, offset: 0 }),
        api.adminListRawDatasets({ limit: 200, offset: 0 }),
        api.getCollections(),
        api.getDataOwners(),
      ]);
      const [reservationResponse, usersResponse, groupsResponse] = await Promise.all([
        api.adminListReservedDatasetIds({ limit: 100, offset: 0 }),
        api.adminListUsers({ limit: 100, offset: 0 }),
        api.adminListGroups({ limit: 100, offset: 0 }),
      ]);
      setDatasets(datasetResponse.datasets);
      setRawDatasets(rawDatasetResponse.raw_datasets);
      setCollections(collectionResponse.collections);
      setDataOwners(dataOwnerResponse.data_owners);
      setReservedDatasetIds(reservationResponse.reservations);
      setDatasetUsers(
        (usersResponse.users as { email: string; display_name: string | null; is_group: boolean }[])
          .filter((user) => !user.is_group)
          .map((user) => ({ email: user.email, display_name: user.display_name }))
      );
      setDatasetGroups(
        (groupsResponse.groups as { email: string; display_name: string | null }[]).map((group) => ({
          email: group.email,
          display_name: group.display_name,
        }))
      );

      const nextDatasetId = datasetResponse.datasets[0]?.ds_id ?? '';
      setSelectedDatasetId((current) => current || nextDatasetId);
      const nextRawDataset = rawDatasetResponse.raw_datasets[0];
      if (nextRawDataset && !selectedRawDatasetId) {
        setSelectedRawDatasetId(nextRawDataset.rds_id);
        setRawDatasetEditForm({
          title: nextRawDataset.title,
          source: nextRawDataset.source,
        });
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to load dataset admin data');
    } finally {
      setLoading(false);
    }
  };

  const loadDatasetDetail = async (datasetId: string) => {
    if (!datasetId) return;
    setLoadingDatasetDetail(true);
    setErrorMessage('');
    setManifestLoadError('');
    setTableLoadError('');
    try {
      const detail = await api.adminGetDatasetDetail(datasetId);
      setDatasetDetail(detail);
      setEditForm(datasetDetailToForm(detail));
      setRawDatasets((current) => mergeRawDatasetOptions(current, detail.raw_datasets ?? []));

      try {
        const manifestResponse = await api.adminGetManifest(datasetId, STANDARDISED_BUCKET);
        setManifestRecord(manifestResponse);
      } catch (err) {
        if (err instanceof ApiRequestError && err.statusCode === 404) {
          setManifestRecord({
            manifest_yaml: null,
            has_manifest: false,
            manifest_updated_at: null,
            manifest_updated_by: null,
          });
        } else {
          setManifestRecord(null);
          setManifestLoadError(err instanceof Error ? err.message : 'Failed to load canonical manifest');
        }
      }

      try {
        const tablesResponse = await api.adminListDatasetTables(datasetId, selectedBucket);
        setTableRows(tablesResponse.tables);
      } catch (err) {
        if (err instanceof ApiRequestError && err.statusCode === 404) {
          setTableRows([]);
        } else {
          setTableRows([]);
          setTableLoadError(err instanceof Error ? err.message : 'Failed to load dataset tables');
        }
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to load dataset detail');
      setDatasetDetail(null);
    } finally {
      setLoadingDatasetDetail(false);
    }
  };

  const loadTables = async (datasetId: string, bucketType: string) => {
    if (!datasetId) return;
    setTableLoadError('');
    try {
      const response = await api.adminListDatasetTables(datasetId, bucketType);
      setTableRows(response.tables);
    } catch (err) {
      if (err instanceof ApiRequestError && err.statusCode === 404) {
        setTableRows([]);
      } else {
        setTableRows([]);
        setTableLoadError(err instanceof Error ? err.message : 'Failed to load dataset tables');
      }
    }
  };

  const loadDocumentationStatuses = async (datasetId?: string) => {
    try {
      const response = await api.adminCheckDocumentationSync(datasetId);
      setDocumentationStatuses(response.datasets);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to check documentation sync');
    }
  };

  useEffect(() => {
    loadReferenceData();
  }, []);

  useEffect(() => {
    if (datasetId) {
      setSelectedDatasetId(datasetId);
    }
  }, [datasetId]);

  useEffect(() => {
    if (selectedDatasetId) {
      loadDatasetDetail(selectedDatasetId);
      loadDocumentationStatuses(selectedDatasetId);
    }
  }, [selectedDatasetId, selectedBucket]);

  useEffect(() => {
    const selected = rawDatasets.find((item) => item.rds_id === selectedRawDatasetId);
    if (selected) {
      setRawDatasetEditForm({ title: selected.title, source: selected.source });
    }
  }, [selectedRawDatasetId, rawDatasets]);

  useEffect(() => {
    if (quickEditDatasetId && datasetDetail?.ds_id === quickEditDatasetId) {
      setQuickEditForm(datasetDetailToForm(datasetDetail));
    }
  }, [quickEditDatasetId, datasetDetail]);

  useEffect(() => {
    if (datasetDetail) {
      setReadmeDraft(datasetDetail.readme_md ?? '');
      setDataDictionaryDraft(
        datasetDetail.data_dictionary_json
          ? (() => {
              try {
                return JSON.stringify(JSON.parse(datasetDetail.data_dictionary_json), null, 2);
              } catch {
                return datasetDetail.data_dictionary_json;
              }
            })()
          : '{\n  "tables": {}\n}'
      );
    }
  }, [datasetDetail]);

  const updateCreateForm = <K extends keyof DatasetFormState>(key: K, value: DatasetFormState[K]) => {
    setCreateForm((current) => ({ ...current, [key]: value }));
  };

  const updateEditForm = <K extends keyof DatasetFormState>(key: K, value: DatasetFormState[K]) => {
    setEditForm((current) => ({ ...current, [key]: value }));
  };

  const buildDatasetPayload = (form: DatasetFormState) => ({
    ds_id: form.ds_id,
    title: form.title,
    collection_id: form.collection_id,
    data_owner_name: form.data_owner_name,
    description: form.description || null,
    spatial_coverage_region_id: form.spatial_coverage_region_id || null,
    spatial_resolution: form.spatial_resolution || null,
    temporal_coverage_start_date: form.temporal_coverage_start_date || null,
    temporal_coverage_end_date: form.temporal_coverage_end_date || null,
    temporal_resolution: form.temporal_resolution || null,
    access_level: form.access_level || 'NONE',
    additional_metadata: parseJsonObject(form.additional_metadata),
    tags: csvToList(form.tags),
    raw_dataset_ids: form.raw_dataset_ids,
  });

  const buildImportDatasetOverride = () => {
    const payload = buildDatasetPayload(createForm);
    return {
      ds_id: payload.ds_id,
      title: payload.title,
      collection_id: payload.collection_id,
      data_owner_name: payload.data_owner_name,
      description: payload.description,
      spatial_coverage_region_id: payload.spatial_coverage_region_id,
      spatial_resolution: payload.spatial_resolution,
      temporal_coverage_start_date: payload.temporal_coverage_start_date,
      temporal_coverage_end_date: payload.temporal_coverage_end_date,
      temporal_resolution: payload.temporal_resolution,
      access_level: payload.access_level,
      additional_metadata: payload.additional_metadata,
      tags: payload.tags,
    };
  };

  const parsedReadmeHtml = useMemo(() => {
    if (!readmeDraft.trim()) return '';
    return marked.parse(readmeDraft) as string;
  }, [readmeDraft]);

  const parsedDataDictionary = useMemo(() => {
    if (!dataDictionaryDraft.trim()) return null;
    try {
      return JSON.parse(dataDictionaryDraft) as {
        tables?: Record<
          string,
          { description?: string | null; data_dictionary?: Record<string, { description?: string | null; comments?: string | null }> }
        >;
      };
    } catch {
      return null;
    }
  }, [dataDictionaryDraft]);

  const dsIdSequenceNote = useMemo(() => {
    const actual = importPreview?.dataset.ds_id;
    const suggested = importPreview?.suggested_dataset_id;
    if (!actual || !suggested) return null;

    const idPattern = /^([A-Z]{2}\d{4}DS)(\d{4})$/;
    const actualMatch = actual.match(idPattern);
    const suggestedMatch = suggested.match(idPattern);
    if (!actualMatch || !suggestedMatch || actualMatch[1] !== suggestedMatch[1]) return null;

    const actualNum = parseInt(actualMatch[2], 10);
    const suggestedNum = parseInt(suggestedMatch[2], 10);
    if (actualNum === suggestedNum) return null;

    const prefix = actualMatch[1];
    const pad = (n: number) => String(n).padStart(4, '0');

    if (actualNum > suggestedNum) {
      const gapRange =
        suggestedNum === actualNum - 1
          ? `${prefix}${pad(suggestedNum)}`
          : `${prefix}${pad(suggestedNum)}–${prefix}${pad(actualNum - 1)}`;
      return `This is higher than the next sequential ID (${suggested}) - ${gapRange} will remain unused in this collection.`;
    }

    return `This is lower than the next sequential ID (${suggested}) - it fills a previously unused ID in the sequence.`;
  }, [importPreview]);

  const applyImportPreview = (preview: AdminDatasetPackagePreview) => {
    setImportPreview(preview);
    setCreateForm({
      ds_id: preview.dataset.ds_id ?? '',
      title: preview.dataset.title ?? '',
      collection_id: preview.dataset.collection_id ?? '',
      data_owner_name: preview.dataset.data_owner_name ?? '',
      description: preview.dataset.description ?? '',
      spatial_coverage_region_id: preview.dataset.spatial_coverage_region_id ?? '',
      spatial_resolution: preview.dataset.spatial_resolution ?? '',
      temporal_coverage_start_date: preview.dataset.temporal_coverage_start_date ?? '',
      temporal_coverage_end_date: preview.dataset.temporal_coverage_end_date ?? '',
      temporal_resolution: preview.dataset.temporal_resolution ?? '',
      access_level: preview.dataset.access_level ?? 'NONE',
      additional_metadata: JSON.stringify(preview.dataset.additional_metadata ?? {}, null, 2),
      tags: (preview.dataset.tags ?? []).join(', '),
      raw_dataset_ids: preview.dataset.raw_dataset_ids ?? [],
    });
    setRawDatasetForm({
      rds_id: preview.raw_dataset.rds_id ?? '',
      title: preview.raw_dataset.title ?? '',
      source: preview.raw_dataset.source ?? '',
    });
  };

  const handleSuggestDatasetId = async (collectionId: string) => {
    if (!collectionId) return;
    try {
      const response = await api.adminSuggestDatasetId(collectionId);
      setCreateForm((current) => ({ ...current, collection_id: collectionId, ds_id: response.suggested_dataset_id }));
      setStatusMessage(`Suggested dataset ID: ${response.suggested_dataset_id}`);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to suggest dataset ID');
    }
  };

  const handleSuggestRawDatasetId = async (collectionId: string) => {
    setRawDatasetCollectionId(collectionId);
    if (!collectionId) return;
    try {
      const response = await api.adminSuggestRawDatasetId(collectionId);
      setRawDatasetForm((current) => ({ ...current, rds_id: response.suggested_raw_dataset_id }));
      setStatusMessage(`Suggested raw dataset ID: ${response.suggested_raw_dataset_id}`);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to suggest raw dataset ID');
    }
  };

  const handleLookupNextIds = async (collectionId: string) => {
    setIdsLookupCollectionId(collectionId);
    setIdsLookupResult(null);
    if (!collectionId) return;
    setIdsLookupLoading(true);
    setErrorMessage('');
    try {
      const [dsResponse, rdsResponse] = await Promise.all([
        api.adminSuggestDatasetId(collectionId),
        api.adminSuggestRawDatasetId(collectionId),
      ]);
      setIdsLookupResult({
        collectionId,
        nextDatasetId: dsResponse.suggested_dataset_id,
        nextRawDatasetId: rdsResponse.suggested_raw_dataset_id,
      });
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to look up next available IDs');
    } finally {
      setIdsLookupLoading(false);
    }
  };

  const handleReserveDatasetId = async (e: Event) => {
    e.preventDefault();
    if (!reserveDatasetId) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminReserveDatasetId({
        ds_id: reserveDatasetId,
        collection_id: reserveCollectionId || null,
        note: reserveNote || null,
      });
      setStatusMessage(`Reserved dataset ID ${reserveDatasetId}.`);
      setReserveDatasetId('');
      setReserveCollectionId('');
      setReserveNote('');
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to reserve dataset ID');
    }
  };

  const handleDeleteReservedDatasetId = async (datasetId: string) => {
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminDeleteReservedDatasetId(datasetId);
      setStatusMessage(`Removed reservation for ${datasetId}.`);
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to remove reservation');
    }
  };

  const handleCreateRawDataset = async (e: Event) => {
    e.preventDefault();
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminCreateRawDataset(rawDatasetForm);
      setStatusMessage(`Created raw dataset ${rawDatasetForm.rds_id}.`);
      setRawDatasetForm({ rds_id: '', title: '', source: '' });
      setRawDatasetCollectionId('');
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to create raw dataset');
    }
  };

  const handleUpdateRawDataset = async (e: Event) => {
    e.preventDefault();
    if (!selectedRawDatasetId) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminUpdateRawDataset(selectedRawDatasetId, rawDatasetEditForm);
      setStatusMessage(`Updated raw dataset ${selectedRawDatasetId}.`);
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to update raw dataset');
    }
  };

  const handlePreviewImport = async (e: Event, useCurrentEdits = false) => {
    e.preventDefault();
    if (!importInfoFile || !importMetadataFile) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      const preview = await api.adminPreviewDatasetImport({
        infoFile: importInfoFile,
        metadataFile: importMetadataFile,
        csvFiles: importCsvFiles,
        datasetOverride: useCurrentEdits ? buildImportDatasetOverride() : undefined,
        rawDatasetOverride: useCurrentEdits ? rawDatasetForm : undefined,
      });
      applyImportPreview(preview);
      setStatusMessage(
        preview.can_import
          ? 'Package preview passed validation and is ready to import.'
          : 'Package preview loaded. Review findings before importing.'
      );
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to preview dataset package');
    }
  };

  const handleApplyImport = async (e: Event) => {
    e.preventDefault();
    if (!importInfoFile || !importMetadataFile || importCsvFiles.length === 0) return;
    setImportingPackage(true);
    setErrorMessage('');
    setStatusMessage('');
    try {
      const response = await api.adminApplyDatasetImport({
        infoFile: importInfoFile,
        metadataFile: importMetadataFile,
        csvFiles: importCsvFiles,
        datasetOverride: buildImportDatasetOverride(),
        rawDatasetOverride: rawDatasetForm,
        bucketType: STANDARDISED_BUCKET,
      });
      setStatusMessage(`Imported dataset ${response.dataset_id} with ${response.uploaded_tables.length} table(s).`);
      setSelectedDatasetId(response.dataset_id);
      setImportPreview(null);
      setImportCsvFiles([]);
      setImportInfoFile(null);
      setImportMetadataFile(null);
      await loadReferenceData(datasetSearch || undefined);
      await loadDatasetDetail(response.dataset_id);
      await loadDocumentationStatuses(response.dataset_id);
    } catch (err) {
      if (err instanceof ApiRequestError) {
        const detailData = err.detailData as { findings?: ValidationFinding[] } | undefined;
        if (importPreview && detailData?.findings) {
          setImportPreview({ ...importPreview, findings: detailData.findings, can_import: false });
        }
      }
      setErrorMessage(err instanceof Error ? err.message : 'Failed to import dataset package');
    } finally {
      setImportingPackage(false);
    }
  };

  const handleCreateDataset = async (e: Event) => {
    e.preventDefault();
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminCreateDataset(buildDatasetPayload(createForm));
      setStatusMessage(`Created dataset ${createForm.ds_id}.`);
      setSelectedDatasetId(createForm.ds_id);
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to create dataset');
    }
  };

  const handleUpdateDataset = async (e: Event) => {
    e.preventDefault();
    if (!selectedDatasetId) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      const payload = buildDatasetPayload(editForm);
      await api.adminUpdateDataset(selectedDatasetId, {
        ds_id: payload.ds_id,
        title: payload.title,
        collection_id: payload.collection_id,
        data_owner_name: payload.data_owner_name,
        description: payload.description,
        spatial_coverage_region_id: payload.spatial_coverage_region_id,
        spatial_resolution: payload.spatial_resolution,
        temporal_coverage_start_date: payload.temporal_coverage_start_date,
        temporal_coverage_end_date: payload.temporal_coverage_end_date,
        temporal_resolution: payload.temporal_resolution,
        access_level: payload.access_level,
        additional_metadata: payload.additional_metadata,
        tags: payload.tags,
        raw_dataset_ids: payload.raw_dataset_ids,
      });
      setStatusMessage(`Updated dataset ${selectedDatasetId}.`);
      if (payload.ds_id && payload.ds_id !== selectedDatasetId) {
        setSelectedDatasetId(payload.ds_id);
        setDeleteConfirmationDatasetId('');
        setDeleteVerificationCode('');
      } else {
        await loadDatasetDetail(selectedDatasetId);
      }
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to update dataset');
    }
  };

  const handleManifestUpload = async (e: Event) => {
    e.preventDefault();
    if (!selectedDatasetId || !manifestFile || manifestLoadError) return;
    setErrorMessage('');
    setStatusMessage('');
    setManifestFindings([]);
    try {
      await api.adminUploadManifest(selectedDatasetId, STANDARDISED_BUCKET, manifestFile);
      setManifestFile(null);
      setStatusMessage(`Updated canonical manifest for ${selectedDatasetId}.`);
      await loadDatasetDetail(selectedDatasetId);
    } catch (err) {
      if (err instanceof ApiRequestError) {
        setErrorMessage(err.message);
        const detailData = err.detailData as { findings?: ValidationFinding[] } | undefined;
        setManifestFindings(detailData?.findings ?? []);
      } else {
        setErrorMessage(err instanceof Error ? err.message : 'Failed to upload manifest');
      }
    }
  };

  const handleTableUpload = async (e: Event) => {
    e.preventDefault();
    if (!selectedDatasetId || !tableFile || tableLoadError) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminUploadDatasetTable(selectedDatasetId, selectedBucket, tableFile, tableMetadataText);
      setStatusMessage(`Uploaded table ${tableFile.name} to ${selectedBucket}.`);
      setTableFile(null);
      await loadTables(selectedDatasetId, selectedBucket);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to upload table');
    }
  };

  const handleDocumentationSync = async (force = false) => {
    setErrorMessage('');
    setStatusMessage('');
    try {
      const response = await api.adminRunDocumentationSync({
        dataset_id: selectedDatasetId || undefined,
        only_outdated: !force,
        force,
      });
      setStatusMessage(`Documentation sync finished. Updated ${response.updated} dataset(s).`);
      await loadDocumentationStatuses(selectedDatasetId || undefined);
      if (selectedDatasetId) {
        await loadDatasetDetail(selectedDatasetId);
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to sync dataset documentation');
    }
  };

  const handleQuickAccessLevelUpdate = async (datasetId: string, accessLevel: string) => {
    setSavingAccessLevelFor(datasetId);
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminUpdateDataset(datasetId, { access_level: accessLevel });
      setStatusMessage(`Updated access level for ${datasetId} to ${accessLevel}.`);
      await loadReferenceData(datasetSearch || undefined);
      if (selectedDatasetId === datasetId) {
        await loadDatasetDetail(datasetId);
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to update access level');
    } finally {
      setSavingAccessLevelFor('');
    }
  };

  const openQuickEditModal = async (datasetId: string) => {
    setQuickEditDatasetId(datasetId);
    setSelectedDatasetId(datasetId);
    if (datasetDetail?.ds_id !== datasetId) {
      await loadDatasetDetail(datasetId);
    } else {
      setQuickEditForm(datasetDetailToForm(datasetDetail));
    }
  };

  const openShareModal = async (datasetId: string) => {
    setShareModalDatasetId(datasetId);
    setSelectedDatasetId(datasetId);
    if (datasetDetail?.ds_id !== datasetId) {
      await loadDatasetDetail(datasetId);
    }
  };

  const handleQuickEditSave = async (e: Event) => {
    e.preventDefault();
    if (!quickEditDatasetId) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      const payload = buildDatasetPayload(quickEditForm);
      await api.adminUpdateDataset(quickEditDatasetId, {
        title: payload.title,
        description: payload.description,
        access_level: payload.access_level,
        tags: payload.tags,
      });
      setStatusMessage(`Updated dataset ${quickEditDatasetId}.`);
      setQuickEditDatasetId('');
      await loadReferenceData(datasetSearch || undefined);
      await loadDatasetDetail(selectedDatasetId || quickEditDatasetId);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to update dataset');
    }
  };

  const handleShareWithUser = async () => {
    if (!selectedDatasetId || !shareUserEmail) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminSetUserPermission(shareUserEmail, selectedDatasetId, sharePermission);
      setStatusMessage(`Granted ${sharePermission} on ${selectedDatasetId} to ${shareUserEmail}.`);
      setShareUserEmail('');
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to share dataset with user');
    }
  };

  const handleShareWithGroup = async () => {
    if (!selectedDatasetId || !shareGroupEmail) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminSetGroupPermission(shareGroupEmail, selectedDatasetId, sharePermission);
      setStatusMessage(`Granted ${sharePermission} on ${selectedDatasetId} to ${shareGroupEmail}.`);
      setShareGroupEmail('');
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to share dataset with group');
    }
  };

  const handleSaveDocumentation = async (mode: 'readme' | 'data-dictionary') => {
    if (!selectedDatasetId) return;
    setSavingDocumentation(true);
    setErrorMessage('');
    setStatusMessage('');
    try {
      const payload =
        mode === 'readme'
          ? { readme_md: readmeDraft.trim() ? readmeDraft : null }
          : {
              data_dictionary_json: dataDictionaryDraft.trim()
                ? (JSON.parse(dataDictionaryDraft) as unknown)
                : null,
            };
      const updated = await api.adminUpdateDatasetDocumentation(selectedDatasetId, payload);
      setDatasetDetail(updated);
      setStatusMessage(
        mode === 'readme'
          ? `Updated README for ${selectedDatasetId}.`
          : `Updated data dictionary for ${selectedDatasetId}.`
      );
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(
        err instanceof Error
          ? err.message
          : mode === 'readme'
            ? 'Failed to update README'
            : 'Failed to update data dictionary'
      );
    } finally {
      setSavingDocumentation(false);
    }
  };

  const handleInitiateDatasetDeletion = async () => {
    if (!selectedDatasetId) return;
    setErrorMessage('');
    setStatusMessage('');
    try {
      const response = await api.adminInitiateDatasetDeletion(selectedDatasetId);
      setStatusMessage(response.message);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to initiate dataset deletion');
    }
  };

  const handleVerifyDatasetDeletion = async (e: Event) => {
    e.preventDefault();
    if (!selectedDatasetId || !deleteVerificationCode || !deleteConfirmationDatasetId) return;
    setDeletingDataset(true);
    setErrorMessage('');
    setStatusMessage('');
    try {
      await api.adminVerifyDatasetDeletion(selectedDatasetId, {
        code: deleteVerificationCode,
        confirmation_dataset_id: deleteConfirmationDatasetId,
      });
      setStatusMessage(`Deleted dataset ${selectedDatasetId}.`);
      setSelectedDatasetId('');
      setDatasetDetail(null);
      setEditForm(emptyDatasetForm());
      setDeleteVerificationCode('');
      setDeleteConfirmationDatasetId('');
      await loadReferenceData(datasetSearch || undefined);
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to delete dataset');
    } finally {
      setDeletingDataset(false);
    }
  };

  const handleDatasetSearch = async (e: Event) => {
    e.preventDefault();
    await loadReferenceData(datasetSearch || undefined);
  };

  const renderDatasetForm = (
    form: DatasetFormState,
    updateForm: <K extends keyof DatasetFormState>(key: K, value: DatasetFormState[K]) => void,
    submitLabel: string,
    onSubmit: (e: Event) => void,
    allowIdSuggestion = false,
    lockIdentity = false
  ) => (
    <form class="space-y-4" onSubmit={onSubmit}>
      <div class="grid gap-4 md:grid-cols-2">
        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Collection</span>
          <select
            value={form.collection_id}
            onChange={(e) => {
              const value = (e.currentTarget as HTMLSelectElement).value;
              updateForm('collection_id', value);
              if (allowIdSuggestion) {
                handleSuggestDatasetId(value);
              }
            }}
            disabled={lockIdentity}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="">Select collection</option>
            {collections.map((collection) => (
              <option key={collection.id} value={collection.collection_id}>
                {collection.collection_id} - {collection.collection_name}
              </option>
            ))}
          </select>
        </label>

        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Dataset ID</span>
          <input
            value={form.ds_id}
            onInput={(e) => updateForm('ds_id', (e.currentTarget as HTMLInputElement).value)}
            disabled={lockIdentity}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>
      </div>

      <div class="grid gap-4 md:grid-cols-2">
        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Title</span>
          <input
            value={form.title}
            onInput={(e) => updateForm('title', (e.currentTarget as HTMLInputElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>

        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Data owner</span>
          <input
            list="data-owners"
            value={form.data_owner_name}
            onInput={(e) => updateForm('data_owner_name', (e.currentTarget as HTMLInputElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
          <datalist id="data-owners">
            {dataOwners.map((owner) => (
              <option key={owner.id} value={owner.name} />
            ))}
          </datalist>
        </label>
      </div>

      <label class="block">
        <span class="mb-2 block text-sm font-medium text-slate-700">Description</span>
        <textarea
          value={form.description}
          onInput={(e) => updateForm('description', (e.currentTarget as HTMLTextAreaElement).value)}
          rows={3}
          class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
        />
      </label>

      <div class="grid gap-4 md:grid-cols-3">
        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Access level</span>
          <select
            value={form.access_level}
            onChange={(e) => updateForm('access_level', (e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="NONE">NONE</option>
            <option value="VIEW">VIEW</option>
            <option value="DOWNLOAD">DOWNLOAD</option>
          </select>
        </label>

        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Spatial resolution</span>
          <input
            value={form.spatial_resolution}
            onInput={(e) => updateForm('spatial_resolution', (e.currentTarget as HTMLInputElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>

        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Temporal resolution</span>
          <input
            value={form.temporal_resolution}
            onInput={(e) => updateForm('temporal_resolution', (e.currentTarget as HTMLInputElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>
      </div>

      <div class="grid gap-4 md:grid-cols-3">
        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Temporal start</span>
          <input
            value={form.temporal_coverage_start_date}
            onInput={(e) => updateForm('temporal_coverage_start_date', (e.currentTarget as HTMLInputElement).value)}
            placeholder="YYYY or YYYY-MM-DD"
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>

        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Temporal end</span>
          <input
            value={form.temporal_coverage_end_date}
            onInput={(e) => updateForm('temporal_coverage_end_date', (e.currentTarget as HTMLInputElement).value)}
            placeholder="YYYY or YYYY-MM-DD"
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>

        <label class="block">
          <span class="mb-2 block text-sm font-medium text-slate-700">Spatial coverage region ID</span>
          <input
            value={form.spatial_coverage_region_id}
            onInput={(e) => updateForm('spatial_coverage_region_id', (e.currentTarget as HTMLInputElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
        </label>
      </div>

      <label class="block">
        <span class="mb-2 block text-sm font-medium text-slate-700">Tags</span>
        <input
          value={form.tags}
          onInput={(e) => updateForm('tags', (e.currentTarget as HTMLInputElement).value)}
          placeholder="comma, separated, tags"
          class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
        />
      </label>

      <label class="block">
        <span class="mb-2 block text-sm font-medium text-slate-700">Linked raw datasets</span>
        <select
          multiple
          onChange={(e) =>
            updateForm(
              'raw_dataset_ids',
              Array.from((e.currentTarget as HTMLSelectElement).selectedOptions).map((option) => option.value)
            )
          }
          class="h-40 w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
        >
          {rawDatasets.map((dataset) => (
            <option
              key={dataset.rds_id}
              value={dataset.rds_id}
              selected={form.raw_dataset_ids.includes(dataset.rds_id)}
            >
              {dataset.rds_id} - {dataset.title}
            </option>
          ))}
        </select>
      </label>

      <label class="block">
        <span class="mb-2 block text-sm font-medium text-slate-700">Additional metadata (JSON)</span>
        <textarea
          value={form.additional_metadata}
          onInput={(e) => updateForm('additional_metadata', (e.currentTarget as HTMLTextAreaElement).value)}
          rows={6}
          class="w-full rounded-xl border border-slate-300 px-3 py-2 font-mono text-sm shadow-sm"
        />
      </label>

      <button
        type="submit"
        class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800"
      >
        {submitLabel}
      </button>
    </form>
  );

  const renderSharePanels = () => (
    <div class="grid gap-4 xl:grid-cols-2">
      <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
        <h4 class="text-sm font-semibold uppercase tracking-wide text-slate-500">Share With User</h4>
        <p class="mt-1 text-sm text-slate-600">Grant direct access for the currently selected dataset.</p>
        <div class="mt-4 grid gap-3 sm:grid-cols-[1fr_140px_auto]">
          <select
            value={shareUserEmail}
            onChange={(e) => setShareUserEmail((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="">Select user</option>
            {datasetUsers.map((user) => (
              <option key={user.email} value={user.email}>
                {user.display_name ? `${user.display_name} (${user.email})` : user.email}
              </option>
            ))}
          </select>
          <select
            value={sharePermission}
            onChange={(e) => setSharePermission((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="VIEW">VIEW</option>
            <option value="DOWNLOAD">DOWNLOAD</option>
            <option value="NONE">NONE</option>
          </select>
          <button
            type="button"
            onClick={handleShareWithUser}
            disabled={!selectedDatasetId || !shareUserEmail}
            class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
          >
            Share
          </button>
        </div>
      </div>

      <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
        <h4 class="text-sm font-semibold uppercase tracking-wide text-slate-500">Share With Group</h4>
        <p class="mt-1 text-sm text-slate-600">Grant group-level access for the currently selected dataset.</p>
        <div class="mt-4 grid gap-3 sm:grid-cols-[1fr_140px_auto]">
          <select
            value={shareGroupEmail}
            onChange={(e) => setShareGroupEmail((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="">Select group</option>
            {datasetGroups.map((group) => (
              <option key={group.email} value={group.email}>
                {group.display_name ? `${group.display_name} (${group.email})` : group.email}
              </option>
            ))}
          </select>
          <select
            value={sharePermission}
            onChange={(e) => setSharePermission((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="VIEW">VIEW</option>
            <option value="DOWNLOAD">DOWNLOAD</option>
            <option value="NONE">NONE</option>
          </select>
          <button
            type="button"
            onClick={handleShareWithGroup}
            disabled={!selectedDatasetId || !shareGroupEmail}
            class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
          >
            Share
          </button>
        </div>
      </div>
    </div>
  );

  const renderModalShell = (
    title: string,
    subtitle: string,
    onClose: () => void,
    content: ComponentChildren
  ) => (
    <div class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 p-4">
      <div class="max-h-[90vh] w-full max-w-3xl overflow-auto rounded-3xl bg-white shadow-2xl">
        <div class="flex items-start justify-between gap-4 border-b border-slate-200 px-6 py-5">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">{title}</h3>
            <p class="mt-1 text-sm text-slate-600">{subtitle}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            class="rounded-xl border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700"
          >
            Close
          </button>
        </div>
        <div class="px-6 py-5">{content}</div>
      </div>
    </div>
  );

  return (
    <div class="space-y-6">
      {errorMessage ? (
        <div class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{errorMessage}</div>
      ) : null}
      {statusMessage ? (
        <div class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
          {statusMessage}
        </div>
      ) : null}

      {showSelectionToolbar ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 class="text-lg font-semibold text-slate-900">
              {isDetailView ? 'Dataset Workspace' : isSyncView ? 'Documentation Sync Workspace' : 'Dataset Workspace'}
            </h2>
            <p class="mt-1 text-sm text-slate-600">
              {isDetailView
                ? 'Choose a dataset and work through its metadata, sharing, tables, manifest, and sync status in one place.'
                : isSyncView
                  ? 'Choose a dataset to inspect documentation freshness, or switch datasets quickly while reviewing sync state.'
                  : 'Search the catalog, switch datasets, and jump into the parts of the admin workflow that need attention.'}
            </p>
          </div>
          <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
            {loading ? 'Loading catalog…' : `${datasets.length} datasets`}
          </span>
        </div>

        <form class="mt-6 grid gap-4 md:grid-cols-[1fr_180px]" onSubmit={handleDatasetSearch}>
          <input
            value={datasetSearch}
            onInput={(e) => setDatasetSearch((e.currentTarget as HTMLInputElement).value)}
            placeholder="Search datasets by ID or title"
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
          <button
            type="submit"
            class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 md:w-auto"
          >
            Search
          </button>
        </form>

        <div class="mt-4 grid gap-4 md:grid-cols-[1fr_220px_220px]">
          <select
            value={selectedDatasetId}
            onChange={(e) => setSelectedDatasetId((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="">Select dataset</option>
            {datasets.map((dataset) => (
              <option key={dataset.ds_id} value={dataset.ds_id}>
                {dataset.ds_id} - {dataset.title}
              </option>
            ))}
          </select>

          <select
            value={selectedBucket}
            onChange={(e) => {
              const bucket = (e.currentTarget as HTMLSelectElement).value;
              setSelectedBucket(bucket);
              if (selectedDatasetId) {
                loadTables(selectedDatasetId, bucket);
              }
            }}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value={STANDARDISED_BUCKET}>{STANDARDISED_BUCKET}</option>
            <option value={PREPROCESSED_BUCKET}>{PREPROCESSED_BUCKET}</option>
          </select>

          <button
            type="button"
            onClick={() => {
              if (selectedDatasetId) {
                loadDatasetDetail(selectedDatasetId);
              }
            }}
            class="rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
          >
            {loadingDatasetDetail ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>
      </section>
      ) : null}

      {showCatalogSection ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Dataset Catalog</h3>
            <p class="mt-1 text-sm text-slate-600">
              View all datasets as a table, update access level quickly, and jump into full editing from one place.
            </p>
          </div>
          {selectedDatasetId ? (
            <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
              Selected: {selectedDatasetId}
            </span>
          ) : null}
        </div>

        <div class="mt-4 overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-200 text-sm">
            <thead>
              <tr class="text-left text-xs uppercase tracking-wide text-slate-500">
                <th class="px-3 py-2">Dataset ID</th>
                <th class="px-3 py-2">Title</th>
                <th class="px-3 py-2">Access Level</th>
                <th class="px-3 py-2">Actions</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-100">
              {datasets.map((dataset) => (
                <tr key={dataset.ds_id} class={selectedDatasetId === dataset.ds_id ? 'bg-slate-50' : ''}>
                  <td class="px-3 py-3 font-medium text-slate-900">{dataset.ds_id}</td>
                  <td class="px-3 py-3 text-slate-700">{dataset.title}</td>
                  <td class="px-3 py-3">
                    <select
                      value={dataset.access_level ?? 'NONE'}
                      onChange={(e) => handleQuickAccessLevelUpdate(dataset.ds_id, (e.currentTarget as HTMLSelectElement).value)}
                      disabled={savingAccessLevelFor === dataset.ds_id}
                      class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
                    >
                      <option value="NONE">NONE</option>
                      <option value="VIEW">VIEW</option>
                      <option value="DOWNLOAD">DOWNLOAD</option>
                    </select>
                  </td>
                  <td class="px-3 py-3">
                    <div class="flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => setSelectedDatasetId(dataset.ds_id)}
                        class="rounded-xl border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700"
                      >
                        Select
                      </button>
                      <button
                        type="button"
                        onClick={() => openQuickEditModal(dataset.ds_id)}
                        class="rounded-xl border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700"
                      >
                        Quick Edit
                      </button>
                      <button
                        type="button"
                        onClick={() => openShareModal(dataset.ds_id)}
                        class="rounded-xl border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700"
                      >
                        Share
                      </button>
                      <a
                        href={`/admin/datasets/workspace?dataset=${encodeURIComponent(dataset.ds_id)}`}
                        class="rounded-xl bg-slate-900 px-3 py-2 text-sm font-medium text-white"
                      >
                        Open Workspace
                      </a>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div class="mt-6 rounded-2xl border border-dashed border-slate-300 bg-slate-50 p-4 text-sm text-slate-600">
          Use <span class="font-medium text-slate-900">Quick Edit</span> or <span class="font-medium text-slate-900">Share</span> from a dataset row to open focused admin modals without leaving the catalog. Use <span class="font-medium text-slate-900">Open Workspace</span> for tables, manifest, README, and data dictionary work.
        </div>
      </section>
      ) : null}

      {showReservationsSection ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Reserved Dataset IDs</h3>
            <p class="mt-1 text-sm text-slate-600">
              Reserve identifiers for incoming datasets and keep a visible queue of upcoming work.
            </p>
          </div>
          <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
            {reservedDatasetIds.length} reserved
          </span>
        </div>

        <form class="mt-4 grid gap-4 md:grid-cols-[1fr_180px_1fr_auto]" onSubmit={handleReserveDatasetId}>
          <input
            value={reserveDatasetId}
            onInput={(e) => setReserveDatasetId((e.currentTarget as HTMLInputElement).value)}
            placeholder="Dataset ID to reserve"
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
          <select
            value={reserveCollectionId}
            onChange={(e) => setReserveCollectionId((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="">Optional collection</option>
            {collections.map((collection) => (
              <option key={collection.id} value={collection.collection_id}>
                {collection.collection_id}
              </option>
            ))}
          </select>
          <input
            value={reserveNote}
            onInput={(e) => setReserveNote((e.currentTarget as HTMLInputElement).value)}
            placeholder="Note"
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          />
          <button
            type="submit"
            class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white sm:w-auto"
          >
            Reserve ID
          </button>
        </form>

        <div class="mt-4 overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-200 text-sm">
            <thead>
              <tr class="text-left text-xs uppercase tracking-wide text-slate-500">
                <th class="px-3 py-2">Dataset ID</th>
                <th class="px-3 py-2">Collection</th>
                <th class="px-3 py-2">Reserved By</th>
                <th class="px-3 py-2">Created</th>
                <th class="px-3 py-2">Note</th>
                <th class="px-3 py-2">Actions</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-100">
              {reservedDatasetIds.map((reservation) => (
                <tr key={reservation.ds_id}>
                  <td class="px-3 py-3 font-medium text-slate-900">{reservation.ds_id}</td>
                  <td class="px-3 py-3 text-slate-700">{reservation.collection_id || '—'}</td>
                  <td class="px-3 py-3 text-slate-700">{reservation.reserved_by}</td>
                  <td class="px-3 py-3 text-slate-700">
                    {reservation.created_at ? new Date(reservation.created_at).toLocaleString() : '—'}
                  </td>
                  <td class="px-3 py-3 text-slate-700">{reservation.note || '—'}</td>
                  <td class="px-3 py-3">
                    <button
                      type="button"
                      onClick={() => handleDeleteReservedDatasetId(reservation.ds_id)}
                      class="rounded-xl border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700"
                    >
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      ) : null}

      {showIdsSection ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div>
          <h3 class="text-lg font-semibold text-slate-900">Next Available IDs</h3>
          <p class="mt-1 text-sm text-slate-600">
            Pick a collection to see the next free dataset ID and raw dataset ID - nothing is created or reserved, this is a read-only lookup for authoring metadata.yaml / info.yml.
          </p>
        </div>

        <div class="mt-4 grid gap-4 md:grid-cols-[1fr_auto]">
          <select
            value={idsLookupCollectionId}
            onChange={(e) => handleLookupNextIds((e.currentTarget as HTMLSelectElement).value)}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
          >
            <option value="">Select collection</option>
            {collections.map((collection) => (
              <option key={collection.id} value={collection.collection_id}>
                {collection.collection_id} - {collection.collection_name}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => handleLookupNextIds(idsLookupCollectionId)}
            disabled={!idsLookupCollectionId || idsLookupLoading}
            class="rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 disabled:opacity-50"
          >
            {idsLookupLoading ? 'Looking up…' : 'Refresh'}
          </button>
        </div>

        {idsLookupResult ? (
          <div class="mt-4 grid gap-4 sm:grid-cols-2">
            <div class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <div class="text-xs font-medium uppercase tracking-wide text-slate-500">Next Dataset ID</div>
              <div class="mt-1 font-mono text-lg font-semibold text-slate-900">{idsLookupResult.nextDatasetId}</div>
            </div>
            <div class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <div class="text-xs font-medium uppercase tracking-wide text-slate-500">Next Raw Dataset ID</div>
              <div class="mt-1 font-mono text-lg font-semibold text-slate-900">{idsLookupResult.nextRawDatasetId}</div>
            </div>
          </div>
        ) : (
          <p class="mt-4 text-sm text-slate-500">Select a collection above to see its next available IDs.</p>
        )}
      </section>
      ) : null}

      {showCreateSections ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Import Dataset Package</h3>
            <p class="mt-1 text-sm font-medium text-primary-700">
              Use this for real datasets — uploads your CSVs and creates a full, downloadable dataset with manifest and data dictionary.
            </p>
            <p class="mt-1 text-sm text-slate-600">
              Upload `info.yml` and `metadata.yml` to autofill the dataset form, preview server-side validation, then upload the matching CSV tables in one import.
            </p>
          </div>
          <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
            STANDARDISED
          </span>
        </div>

        <form class="mt-4 grid gap-4 md:grid-cols-2" onSubmit={(e) => handlePreviewImport(e, false)}>
          <label class="block">
            <span class="mb-2 block text-sm font-medium text-slate-700">`info.yml` / `info.yaml`</span>
            <input
              type="file"
              accept=".yml,.yaml"
              onChange={(e) => setImportInfoFile((e.currentTarget as HTMLInputElement).files?.[0] ?? null)}
              class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
            />
          </label>
          <label class="block">
            <span class="mb-2 block text-sm font-medium text-slate-700">`metadata.yml` / `metadata.yaml`</span>
            <input
              type="file"
              accept=".yml,.yaml"
              onChange={(e) => setImportMetadataFile((e.currentTarget as HTMLInputElement).files?.[0] ?? null)}
              class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
            />
          </label>
          <label class="block md:col-span-2">
            <span class="mb-2 block text-sm font-medium text-slate-700">Optional CSVs for validation preview</span>
            <input
              type="file"
              accept=".csv"
              multiple
              onChange={(e) => setImportCsvFiles(Array.from((e.currentTarget as HTMLInputElement).files ?? []))}
              class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
            />
          </label>
          <div class="flex flex-wrap gap-3 md:col-span-2">
            <button
              type="submit"
              disabled={!importInfoFile || !importMetadataFile}
              class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
            >
              Preview Package
            </button>
            <button
              type="button"
              onClick={(e) => handlePreviewImport(e as unknown as Event, true)}
              disabled={!importPreview || !importInfoFile || !importMetadataFile}
              class="w-full rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
            >
              Re-run with Current Edits
            </button>
            <button
              type="button"
              onClick={(e) => handleApplyImport(e as unknown as Event)}
              disabled={!importInfoFile || !importMetadataFile || importCsvFiles.length === 0 || importingPackage}
              class="w-full rounded-xl bg-emerald-700 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
            >
              {importingPackage ? 'Importing…' : 'Import Package'}
            </button>
          </div>
        </form>

        {importPreview ? (
          <div class="mt-6 space-y-4">
            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div class="flex flex-wrap gap-3 text-xs text-slate-600">
                <span>Suggested dataset ID: {importPreview.suggested_dataset_id ?? 'none'}</span>
                <span>{importPreview.tables.length} table definition(s)</span>
                <span>{importCsvFiles.length} CSV file(s) selected</span>
                <span>{importPreview.can_import ? 'Ready to import' : 'Needs review'}</span>
              </div>
              {dsIdSequenceNote ? (
                <div class="mt-2 rounded-lg bg-amber-100 px-3 py-2 text-xs font-medium text-amber-800">
                  {importPreview.dataset.ds_id}: {dsIdSequenceNote}
                </div>
              ) : null}
            </div>

            {importPreview.findings.length > 0 ? (
              <div class="space-y-3">
                {importPreview.findings.map((finding, index) => (
                  <div key={`${finding.code}-${index}`} class={`rounded-xl px-4 py-3 text-sm ring-1 ${severityClasses(finding.severity)}`}>
                    <div class="flex flex-wrap items-center gap-2">
                      <span class="font-semibold uppercase">{finding.severity}</span>
                      <span class="font-mono text-xs">{finding.code}</span>
                    </div>
                    <p class="mt-2">{finding.message}</p>
                    {findingLabel(finding) ? <p class="mt-1 text-xs opacity-80">{findingLabel(finding)}</p> : null}
                  </div>
                ))}
              </div>
            ) : (
              <p class="text-sm text-slate-500">No validation findings from the package preview.</p>
            )}

            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <h4 class="text-sm font-semibold uppercase tracking-wide text-slate-500">Generated Manifest Preview</h4>
              <pre class="mt-3 max-h-[24rem] overflow-auto rounded-xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
                {importPreview.manifest_yaml}
              </pre>
            </div>
          </div>
        ) : null}
      </section>
      ) : null}

      {showCreateSections ? (
      <div class="grid gap-6 xl:grid-cols-2">
        <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
          <h3 class="text-lg font-semibold text-slate-900">Create Raw Dataset</h3>
          <p class="mt-1 text-sm text-slate-600">Choosing a collection suggests the next sequential raw dataset ID.</p>
          <form class="mt-4 space-y-4" onSubmit={handleCreateRawDataset}>
            <select
              value={rawDatasetCollectionId}
              onChange={(e) => handleSuggestRawDatasetId((e.currentTarget as HTMLSelectElement).value)}
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            >
              <option value="">Select collection</option>
              {collections.map((collection) => (
                <option key={collection.id} value={collection.collection_id}>
                  {collection.collection_id} - {collection.collection_name}
                </option>
              ))}
            </select>
            <input
              value={rawDatasetForm.rds_id}
              onInput={(e) => setRawDatasetForm((current) => ({ ...current, rds_id: (e.currentTarget as HTMLInputElement).value }))}
              placeholder="Raw dataset ID"
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
            <input
              value={rawDatasetForm.title}
              onInput={(e) => setRawDatasetForm((current) => ({ ...current, title: (e.currentTarget as HTMLInputElement).value }))}
              placeholder="Raw dataset title"
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
            <input
              value={rawDatasetForm.source}
              onInput={(e) => setRawDatasetForm((current) => ({ ...current, source: (e.currentTarget as HTMLInputElement).value }))}
              placeholder="Source"
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
            <button type="submit" class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white sm:w-auto">
              Create Raw Dataset
            </button>
          </form>

          <h4 class="mt-8 text-sm font-semibold uppercase tracking-wide text-slate-500">Update existing raw dataset</h4>
          <form class="mt-4 space-y-4" onSubmit={handleUpdateRawDataset}>
            <select
              value={selectedRawDatasetId}
              onChange={(e) => setSelectedRawDatasetId((e.currentTarget as HTMLSelectElement).value)}
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            >
              <option value="">Select raw dataset</option>
              {rawDatasets.map((item) => (
                <option key={item.rds_id} value={item.rds_id}>
                  {item.rds_id} - {item.title}
                </option>
              ))}
            </select>
            <input
              value={rawDatasetEditForm.title}
              onInput={(e) => setRawDatasetEditForm((current) => ({ ...current, title: (e.currentTarget as HTMLInputElement).value }))}
              placeholder="Updated title"
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
            <input
              value={rawDatasetEditForm.source}
              onInput={(e) => setRawDatasetEditForm((current) => ({ ...current, source: (e.currentTarget as HTMLInputElement).value }))}
              placeholder="Updated source"
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
            <button type="submit" class="w-full rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 sm:w-auto">
              Update Raw Dataset
            </button>
          </form>
        </section>

        <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
          <h3 class="text-lg font-semibold text-slate-900">Create Dataset</h3>
          <p class="mt-1 text-sm font-medium text-amber-700">
            Not for real datasets — creates an empty record with no files, tables, or manifest. Use Import above instead.
          </p>
          <p class="mt-1 text-sm text-slate-600">Choosing a collection suggests the next sequential dataset ID.</p>
          <div class="mt-4">{renderDatasetForm(createForm, updateCreateForm, 'Create Dataset', handleCreateDataset, true)}</div>
        </section>
      </div>
      ) : null}

      {showDetailSections ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-2 shadow-sm sm:p-3">
        <div class="flex flex-wrap gap-2">
          {([
            ['metadata', 'Metadata'],
            ['sharing', 'Sharing'],
            ['tables', 'Tables'],
            ['manifest', 'Manifest'],
            ['documentation', 'README & Dictionary'],
            ['sync', 'Sync'],
            ['danger', 'Danger Zone'],
          ] as Array<[DetailWorkspaceTab, string]>).map(([tabId, label]) => (
            <button
              type="button"
              key={tabId}
              onClick={() => setDetailTab(tabId)}
              class={`rounded-xl px-4 py-2 text-sm font-medium transition ${
                detailTab === tabId
                  ? 'bg-slate-900 text-white'
                  : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </section>
      ) : null}

      {showDetailSections && detailTab === 'metadata' ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Update Existing Dataset</h3>
            <p class="mt-1 text-sm text-slate-600">
              Edit dataset details, replace raw dataset links, and review current documentation timestamps.
            </p>
          </div>
          {datasetDetail ? (
            <div class="text-xs text-slate-500">
              Synced: {datasetDetail.documentation_synced_at ? new Date(datasetDetail.documentation_synced_at).toLocaleString() : 'never'}
            </div>
          ) : null}
        </div>
        <div class="mt-4">
          {selectedDatasetId ? renderDatasetForm(editForm, updateEditForm, 'Save Dataset Changes', handleUpdateDataset, false, false) : (
            <p class="text-sm text-slate-500">Select a dataset above to edit it.</p>
          )}
        </div>
      </section>
      ) : null}

      {showDetailSections && detailTab === 'sharing' ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Sharing</h3>
            <p class="mt-1 text-sm text-slate-600">
              Grant dataset access directly to users or groups while staying in the current workspace.
            </p>
          </div>
          {selectedDatasetId ? (
            <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
              {selectedDatasetId}
            </span>
          ) : null}
        </div>
        <div class="mt-6">{renderSharePanels()}</div>
      </section>
      ) : null}

      {showDetailSections && detailTab === 'danger' ? (
      <section class="rounded-2xl border border-red-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Delete Dataset</h3>
            <p class="mt-1 text-sm text-slate-600">
              This is permanent. We send a verification code to your admin email before deleting dataset metadata, linked permissions, resource-group references, and stored dataset files.
            </p>
          </div>
          <button
            type="button"
            onClick={handleInitiateDatasetDeletion}
            disabled={!selectedDatasetId}
            class="w-full rounded-xl border border-red-300 px-4 py-2 text-sm font-medium text-red-700 disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
          >
            Send Delete Code
          </button>
        </div>

        <form class="mt-4 grid gap-4 md:grid-cols-2" onSubmit={handleVerifyDatasetDeletion}>
          <label class="block">
            <span class="mb-2 block text-sm font-medium text-slate-700">Type dataset ID to confirm</span>
            <input
              value={deleteConfirmationDatasetId}
              onInput={(e) => setDeleteConfirmationDatasetId((e.currentTarget as HTMLInputElement).value)}
              placeholder={selectedDatasetId || 'Select a dataset first'}
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
          </label>
          <label class="block">
            <span class="mb-2 block text-sm font-medium text-slate-700">Email verification code</span>
            <input
              value={deleteVerificationCode}
              onInput={(e) => setDeleteVerificationCode((e.currentTarget as HTMLInputElement).value)}
              placeholder="6-digit code"
              class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
            />
          </label>
          <div class="md:col-span-2">
            <button
              type="submit"
              disabled={!selectedDatasetId || !deleteConfirmationDatasetId || !deleteVerificationCode || deletingDataset}
              class="w-full rounded-xl bg-red-700 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
            >
              {deletingDataset ? 'Deleting…' : 'Delete Dataset Permanently'}
            </button>
          </div>
        </form>
      </section>
      ) : null}

      {showDetailSections && detailTab === 'tables' ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex items-start justify-between gap-3">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Tables</h3>
            <p class="mt-1 text-sm text-slate-600">Review existing tables and add new tables to the selected dataset.</p>
          </div>
          <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">{selectedBucket}</span>
        </div>

        {tableLoadError ? (
          <div class="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{tableLoadError}</div>
        ) : null}

        <div class="mt-4 space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4">
          {tableRows.length === 0 ? (
            <p class="text-sm text-slate-500">No tables found for this dataset/version.</p>
          ) : (
            tableRows.map((row) => (
              <div key={row.table_name} class="rounded-xl border border-slate-200 bg-white px-4 py-3">
                <div class="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
                  <div>
                    <div class="break-all font-medium text-slate-900">{row.table_name}</div>
                    <div class="mt-1 overflow-x-auto text-xs text-slate-500">{JSON.stringify(row.metadata)}</div>
                  </div>
                  <a href={row.download_link} target="_blank" rel="noreferrer" class="text-sm font-medium text-slate-700 underline">
                    Open file
                  </a>
                </div>
              </div>
            ))
          )}
        </div>

        <form class="mt-6 space-y-4" onSubmit={handleTableUpload}>
          <input
            type="file"
            accept=".csv,.geojson,.json"
            onChange={(e) => setTableFile((e.currentTarget as HTMLInputElement).files?.[0] ?? null)}
            class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
          />
          <textarea
            value={tableMetadataText}
            onInput={(e) => setTableMetadataText((e.currentTarget as HTMLTextAreaElement).value)}
            rows={8}
            class="w-full rounded-xl border border-slate-300 px-3 py-2 font-mono text-sm shadow-sm"
          />
          <button
            type="submit"
            disabled={!selectedDatasetId || !tableFile || !!tableLoadError}
            class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
          >
            Upload Table
          </button>
        </form>
      </section>
      ) : null}

      {showDetailSections && detailTab === 'manifest' ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex items-start justify-between gap-3">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Canonical Manifest</h3>
            <p class="mt-1 text-sm text-slate-600">Review and replace the standardised manifest for the selected dataset.</p>
          </div>
          <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">STANDARDISED</span>
        </div>

        <div class="mt-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
          <div class="flex flex-wrap items-center gap-3 text-xs text-slate-500">
            <span>{manifestRecord?.has_manifest ? 'Manifest present' : 'No manifest stored'}</span>
            {manifestRecord?.manifest_updated_at ? (
              <span>Updated {new Date(manifestRecord.manifest_updated_at).toLocaleString()}</span>
            ) : null}
            {manifestRecord?.manifest_updated_by ? <span>by {manifestRecord.manifest_updated_by}</span> : null}
          </div>
          <pre class="mt-4 max-h-[32rem] overflow-auto rounded-xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
            {manifestRecord?.manifest_yaml || '# No canonical manifest stored.'}
          </pre>
        </div>

        {manifestLoadError ? (
          <div class="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{manifestLoadError}</div>
        ) : null}

        <form class="mt-6 space-y-4" onSubmit={handleManifestUpload}>
          <input
            type="file"
            accept=".yaml,.yml"
            onChange={(e) => setManifestFile((e.currentTarget as HTMLInputElement).files?.[0] ?? null)}
            class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
          />
          <button
            type="submit"
            disabled={!selectedDatasetId || !manifestFile || !!manifestLoadError}
            class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto"
          >
            Update Manifest
          </button>
        </form>

        {manifestFindings.length > 0 ? (
          <div class="mt-4 space-y-3">
            {manifestFindings.map((finding, index) => (
              <div key={`${finding.code}-${index}`} class={`rounded-xl px-4 py-3 text-sm ring-1 ${severityClasses(finding.severity)}`}>
                <div class="flex flex-wrap items-center gap-2">
                  <span class="font-semibold uppercase">{finding.severity}</span>
                  <span class="font-mono text-xs">{finding.code}</span>
                </div>
                <p class="mt-2">{finding.message}</p>
                {findingLabel(finding) ? <p class="mt-1 text-xs opacity-80">{findingLabel(finding)}</p> : null}
              </div>
            ))}
          </div>
        ) : null}
      </section>
      ) : null}

      {showDetailSections && detailTab === 'documentation' ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">README And Data Dictionary</h3>
            <p class="mt-1 text-sm text-slate-600">
              Make small documentation edits directly from the admin workspace without leaving the dataset context.
            </p>
          </div>
          {datasetDetail?.documentation_synced_at ? (
            <span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
              Updated {new Date(datasetDetail.documentation_synced_at).toLocaleString()}
            </span>
          ) : null}
        </div>

        <div class="mt-6 grid gap-6 xl:grid-cols-2">
          <div class="space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div class="flex items-center justify-between gap-3">
              <h4 class="text-sm font-semibold uppercase tracking-wide text-slate-500">README</h4>
              <button
                type="button"
                onClick={() => handleSaveDocumentation('readme')}
                disabled={!selectedDatasetId || savingDocumentation}
                class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60"
              >
                Save README
              </button>
            </div>
            <textarea
              value={readmeDraft}
              onInput={(e) => setReadmeDraft((e.currentTarget as HTMLTextAreaElement).value)}
              rows={16}
              class="w-full rounded-xl border border-slate-300 px-3 py-2 font-mono text-sm shadow-sm"
            />
              <div class="rounded-2xl border border-slate-200 bg-white p-4">
              <div class="mb-3 text-sm font-medium text-slate-700">Rendered preview</div>
              {readmeDraft.trim() ? (
                <div class="prose prose-slate max-w-none text-sm" dangerouslySetInnerHTML={{ __html: parsedReadmeHtml }} />
              ) : (
                <p class="text-sm text-slate-500">No README saved yet.</p>
              )}
            </div>
          </div>

          <div class="space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div class="flex items-center justify-between gap-3">
              <h4 class="text-sm font-semibold uppercase tracking-wide text-slate-500">Data Dictionary</h4>
              <button
                type="button"
                onClick={() => handleSaveDocumentation('data-dictionary')}
                disabled={!selectedDatasetId || savingDocumentation}
                class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60"
              >
                Save Data Dictionary
              </button>
            </div>
            <textarea
              value={dataDictionaryDraft}
              onInput={(e) => setDataDictionaryDraft((e.currentTarget as HTMLTextAreaElement).value)}
              rows={16}
              class="w-full rounded-xl border border-slate-300 px-3 py-2 font-mono text-sm shadow-sm"
            />
            <div class="rounded-2xl border border-slate-200 bg-white p-4">
              <div class="mb-3 text-sm font-medium text-slate-700">Current structure</div>
              {parsedDataDictionary?.tables && Object.keys(parsedDataDictionary.tables).length > 0 ? (
                <div class="space-y-3">
                  {Object.entries(parsedDataDictionary.tables).map(([tableName, table]) => (
                    <div key={tableName} class="rounded-xl border border-slate-200 px-3 py-3">
                      <div class="font-medium text-slate-900">{tableName}</div>
                      <div class="mt-1 text-sm text-slate-600">{table.description || 'No table description yet.'}</div>
                      <div class="mt-2 text-xs text-slate-500">
                        {table.data_dictionary ? Object.keys(table.data_dictionary).length : 0} field(s)
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p class="text-sm text-slate-500">No parsed table metadata is currently stored.</p>
              )}
            </div>
          </div>
        </div>
      </section>
      ) : null}

      {showSyncSection && (!showDetailSections || detailTab === 'sync') ? (
      <section class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Documentation Sync</h3>
            <p class="mt-1 text-sm text-slate-600">
              Check whether the selected dataset is out of date versus filestore, then sync it manually when needed.
            </p>
          </div>
          <div class="grid w-full gap-2 sm:w-auto sm:grid-cols-2">
            <button
              type="button"
              onClick={() => loadDocumentationStatuses(selectedDatasetId || undefined)}
              class="w-full rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700"
            >
              Check selected
            </button>
            <button
              type="button"
              onClick={() => handleDocumentationSync(false)}
              class="w-full rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white"
            >
              Sync selected
            </button>
          </div>
        </div>

        <div class="mt-4 space-y-3">
          {documentationStatuses.length === 0 ? (
            <p class="text-sm text-slate-500">Run a check to see documentation sync status.</p>
          ) : (
            documentationStatuses.map((item) => (
              <div key={item.ds_id} class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                <div class="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div class="font-medium text-slate-900">{item.ds_id}</div>
                    <div class="mt-1 text-xs text-slate-500">
                      Changed fields: {item.changed_fields.length > 0 ? item.changed_fields.join(', ') : 'none'}
                    </div>
                    <div class="mt-1 text-xs text-slate-500">
                      Last synced: {item.documentation_synced_at ? new Date(item.documentation_synced_at).toLocaleString() : 'never'}
                    </div>
                  </div>
                  <span class={`rounded-full px-3 py-1 text-xs font-medium ${item.needs_update ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800'}`}>
                    {item.needs_update ? 'Outdated' : 'Up to date'}
                  </span>
                </div>
              </div>
            ))
          )}
        </div>
      </section>
      ) : null}

      {quickEditDatasetId
        ? renderModalShell(
            'Quick Edit Dataset',
            `Make small metadata and access changes for ${quickEditDatasetId} without leaving the catalog.`,
            () => setQuickEditDatasetId(''),
            <form class="space-y-4" onSubmit={handleQuickEditSave}>
              <div class="grid gap-4 md:grid-cols-2">
                <label class="block">
                  <span class="mb-2 block text-sm font-medium text-slate-700">Dataset ID</span>
                  <input
                    value={quickEditForm.ds_id}
                    disabled
                    class="w-full rounded-xl border border-slate-300 bg-slate-100 px-3 py-2 text-sm shadow-sm"
                  />
                </label>
                <label class="block">
                  <span class="mb-2 block text-sm font-medium text-slate-700">Access level</span>
                  <select
                    value={quickEditForm.access_level}
                    onChange={(e) => setQuickEditForm((current) => ({ ...current, access_level: (e.currentTarget as HTMLSelectElement).value }))}
                    class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
                  >
                    <option value="NONE">NONE</option>
                    <option value="VIEW">VIEW</option>
                    <option value="DOWNLOAD">DOWNLOAD</option>
                  </select>
                </label>
              </div>
              <label class="block">
                <span class="mb-2 block text-sm font-medium text-slate-700">Title</span>
                <input
                  value={quickEditForm.title}
                  onInput={(e) => setQuickEditForm((current) => ({ ...current, title: (e.currentTarget as HTMLInputElement).value }))}
                  class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
                />
              </label>
              <label class="block">
                <span class="mb-2 block text-sm font-medium text-slate-700">Description</span>
                <textarea
                  value={quickEditForm.description}
                  onInput={(e) => setQuickEditForm((current) => ({ ...current, description: (e.currentTarget as HTMLTextAreaElement).value }))}
                  rows={4}
                  class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
                />
              </label>
              <label class="block">
                <span class="mb-2 block text-sm font-medium text-slate-700">Tags</span>
                <input
                  value={quickEditForm.tags}
                  onInput={(e) => setQuickEditForm((current) => ({ ...current, tags: (e.currentTarget as HTMLInputElement).value }))}
                  class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm"
                />
              </label>
              <div class="flex flex-wrap gap-3">
                <button
                  type="submit"
                  class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white"
                >
                  Save Changes
                </button>
                <a
                  href={`/admin/datasets/workspace?dataset=${encodeURIComponent(quickEditDatasetId)}`}
                  class="rounded-xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700"
                >
                  Open Full Workspace
                </a>
              </div>
            </form>
          )
        : null}

      {shareModalDatasetId
        ? renderModalShell(
            'Share Dataset',
            `Grant or revoke direct access for ${shareModalDatasetId} using focused admin controls.`,
            () => setShareModalDatasetId(''),
            <div class="space-y-4">
              <div class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                Sharing updates are applied immediately to the selected dataset. Choose <code>{shareModalDatasetId}</code> recipients below.
              </div>
              {renderSharePanels()}
            </div>
          )
        : null}
    </div>
  );
}
