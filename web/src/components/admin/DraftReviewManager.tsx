import { useEffect, useMemo, useState } from 'preact/hooks';
import { stringify as stringifyYaml } from 'yaml';

import { ApiRequestError, api } from '../../lib/api';
import type {
  AccessLevel,
  Collection,
  CuratorMetadataInput,
  DataOwner,
  ManifestDraftDetail,
  ManifestDraftStatus,
  ManifestDraftSummary,
  ValidationFinding,
} from '../../lib/types';

function csvToList(text: string): string[] {
  return text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

async function readCsvHeaderColumns(file: File): Promise<string[]> {
  const headerLine = (await file.text()).split(/\r?\n/, 1)[0] ?? '';
  return headerLine.split(',').map((c) => c.trim()).filter(Boolean);
}

function statusClasses(status: ManifestDraftStatus) {
  if (status === 'approved') return 'bg-emerald-50 text-emerald-700 ring-emerald-200';
  if (status === 'rejected') return 'bg-red-50 text-red-700 ring-red-200';
  if (status === 'flagged') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-slate-100 text-slate-700 ring-slate-200';
}

// Backend timestamps (e.g. draft.created_at) are naive UTC - Python's
// datetime.utcnow().isoformat() has no trailing "Z"/offset. A bare
// date-time string with no timezone designator is parsed by JS `Date` as
// *local* time, not UTC, so without this the displayed time is the raw
// UTC clock reading mislabeled as local (off by the viewer's UTC offset -
// e.g. ~5.5 hours early in IST). Appending "Z" (only if not already
// timezone-qualified) makes the browser convert it correctly.
function formatUtcTimestamp(isoString: string): string {
  const hasTimezone = /[zZ]|[+-]\d\d:?\d\d$/.test(isoString);
  const date = new Date(hasTimezone ? isoString : `${isoString}Z`);
  return date.toLocaleString(undefined, {
    day: 'numeric',
    month: 'short',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function severityClasses(severity: string) {
  if (severity === 'error') return 'bg-red-50 text-red-700 ring-red-200';
  if (severity === 'warning') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-emerald-50 text-emerald-700 ring-emerald-200';
}

function errorMessage(err: unknown, fallback: string): string {
  if (err instanceof ApiRequestError) return err.message;
  return err instanceof Error ? err.message : fallback;
}

function GenerateDraftForm({ onGenerated }: { onGenerated: () => void }) {
  const [open, setOpen] = useState(false);
  const [csvFiles, setCsvFiles] = useState<File[]>([]);
  const [digitizationLogFile, setDigitizationLogFile] = useState<File | null>(null);
  const [collections, setCollections] = useState<Collection[]>([]);
  const [collectionsError, setCollectionsError] = useState('');
  const [dataOwners, setDataOwners] = useState<DataOwner[]>([]);
  const [dataOwnersError, setDataOwnersError] = useState('');
  const [categoryId, setCategoryId] = useState('');
  const [collectionId, setCollectionId] = useState('');
  const [dataOwnerName, setDataOwnerName] = useState('');
  const [datasetId, setDatasetId] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!open || collections.length > 0) return;
    api
      .getCollections()
      .then((res) => setCollections(res.collections))
      .catch((err) => setCollectionsError(errorMessage(err, 'Failed to load categories/collections')));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  useEffect(() => {
    if (!open || dataOwners.length > 0) return;
    api
      .getDataOwners()
      .then((res) => setDataOwners(res.data_owners))
      .catch((err) => setDataOwnersError(errorMessage(err, 'Failed to load data owners')));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const categories = useMemo(() => {
    const seen = new Map<string, string>();
    for (const c of collections) seen.set(c.category_id, c.category_name);
    return Array.from(seen.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  }, [collections]);

  const collectionsForCategory = useMemo(
    () => collections.filter((c) => c.category_id === categoryId).sort((a, b) => a.collection_name.localeCompare(b.collection_name)),
    [collections, categoryId]
  );

  const submit = async (e: Event) => {
    e.preventDefault();
    if (csvFiles.length === 0 || !categoryId || !collectionId || !dataOwnerName) return;
    setSubmitting(true);
    setError('');
    try {
      await api.adminGenerateManifestDraft({
        csvFiles,
        categoryId,
        collectionId,
        dataOwnerName,
        datasetId: datasetId || undefined,
        digitizationLogFile,
      });
      setCsvFiles([]);
      setDigitizationLogFile(null);
      setCategoryId('');
      setCollectionId('');
      setDataOwnerName('');
      setDatasetId('');
      setOpen(false);
      onGenerated();
    } catch (err) {
      setError(errorMessage(err, 'Failed to generate draft'));
    } finally {
      setSubmitting(false);
    }
  };

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800"
      >
        + Generate new draft
      </button>
    );
  }

  return (
    <form onSubmit={submit} class="w-full space-y-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div class="flex items-center justify-between">
        <h3 class="text-sm font-semibold text-slate-900">Generate new draft</h3>
        <button type="button" onClick={() => setOpen(false)} class="text-xs text-slate-400 hover:text-slate-600">
          Cancel
        </button>
      </div>

      {error ? (
        <div class="rounded-lg bg-red-50 p-3 text-xs text-red-700 ring-1 ring-red-200">{error}</div>
      ) : null}
      {collectionsError ? (
        <div class="rounded-lg bg-red-50 p-3 text-xs text-red-700 ring-1 ring-red-200">{collectionsError}</div>
      ) : null}
      {dataOwnersError ? (
        <div class="rounded-lg bg-red-50 p-3 text-xs text-red-700 ring-1 ring-red-200">{dataOwnersError}</div>
      ) : null}

      <div class="grid gap-3 md:grid-cols-2">
        <label class="block text-xs font-medium text-slate-600">
          Raw CSV(s) — select more than one for a multi-table dataset
          <input
            type="file"
            accept=".csv"
            multiple
            class="mt-1 block w-full text-sm"
            onChange={(e) => setCsvFiles(Array.from((e.target as HTMLInputElement).files ?? []))}
          />
          {csvFiles.length > 0 ? (
            <span class="mt-1 block text-[11px] text-slate-500">
              {csvFiles.length} table{csvFiles.length > 1 ? 's' : ''}: {csvFiles.map((f) => f.name).join(', ')}
            </span>
          ) : null}
        </label>
        <label class="block text-xs font-medium text-slate-600">
          Digitization log (optional)
          <input
            type="file"
            accept=".yaml,.yml"
            class="mt-1 block w-full text-sm"
            onChange={(e) => setDigitizationLogFile((e.target as HTMLInputElement).files?.[0] ?? null)}
          />
        </label>
        <label class="block text-xs font-medium text-slate-600">
          Category
          <select
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={categoryId}
            onChange={(e) => {
              setCategoryId((e.target as HTMLSelectElement).value);
              setCollectionId('');
            }}
          >
            <option value="">Select a category…</option>
            {categories.map(([id, name]) => (
              <option key={id} value={id}>
                {name} ({id})
              </option>
            ))}
          </select>
        </label>
        <label class="block text-xs font-medium text-slate-600">
          Collection
          <select
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={collectionId}
            disabled={!categoryId}
            onChange={(e) => setCollectionId((e.target as HTMLSelectElement).value)}
          >
            <option value="">{categoryId ? 'Select a collection…' : 'Select a category first'}</option>
            {collectionsForCategory.map((c) => (
              <option key={c.collection_id} value={c.collection_id}>
                {c.collection_name} ({c.collection_id})
              </option>
            ))}
          </select>
        </label>
        <label class="block text-xs font-medium text-slate-600">
          Data owner
          <select
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={dataOwnerName}
            onChange={(e) => setDataOwnerName((e.target as HTMLSelectElement).value)}
          >
            <option value="">Select a data owner…</option>
            {dataOwners.map((o) => (
              <option key={o.id} value={o.name}>
                {o.name}
              </option>
            ))}
          </select>
        </label>
        <label class="block text-xs font-medium text-slate-600">
          Existing dataset ID (optional — leave blank to auto-assign the next available ID)
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={datasetId}
            onInput={(e) => setDatasetId((e.target as HTMLInputElement).value)}
          />
        </label>
      </div>

      <button
        type="submit"
        disabled={submitting || csvFiles.length === 0 || !categoryId || !collectionId || !dataOwnerName}
        class="w-full rounded-lg bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
      >
        {submitting ? 'Generating… (this calls the LLM, may take a moment)' : 'Generate draft'}
      </button>
    </form>
  );
}

function FieldHint({ text }: { text: string }) {
  return (
    <span
      title={text}
      class="ml-1 inline-flex h-3.5 w-3.5 shrink-0 cursor-help items-center justify-center rounded-full bg-slate-200 text-[9px] font-bold leading-none text-slate-500 hover:bg-slate-300"
    >
      ?
    </span>
  );
}

function AutoDetectedBadge() {
  return (
    <span
      title="Filled in automatically from the uploaded CSV(s) - please check it's right, and edit it if not."
      class="ml-1.5 shrink-0 rounded bg-emerald-50 px-1 py-0.5 text-[9px] font-medium leading-none text-emerald-600 ring-1 ring-emerald-200"
    >
      auto-detected — verify
    </span>
  );
}

// Predefined options are drawn from what real ARTPARK datasets actually use
// (checked across data/*/metadata.yaml) - "Other" stays available since a
// few real datasets use genuinely bespoke phrasing (e.g. "Survey-round
// basis (not annual)") that a closed dropdown would otherwise block.
const OTHER_OPTION = '__other__';
const TEMPORAL_RESOLUTION_OPTIONS = [
  'Annual', 'Quinquennial', 'Decadal', 'Monthly', 'Weekly', 'Daily', 'Semester',
  'Survey round (single cross-section)',
];
const UPDATE_FREQUENCY_OPTIONS = [
  'One-time', 'Annual', 'Quinquennial', 'Biannual', 'Monthly', 'Weekly', 'Daily', 'Adhoc',
];

function PresetSelectField({
  label,
  hint,
  value,
  onChange,
  options,
  placeholder,
}: {
  label: string;
  hint: string;
  value: string;
  onChange: (value: string) => void;
  options: string[];
  placeholder: string;
}) {
  // Computed once at mount - this component is inside a form that fully
  // unmounts on close/reopen (see GenerateDeterministicDraftForm's `if
  // (!open) return ...` early return), so there's no stale-value case to
  // reconcile after a submit-triggered reset.
  const [customMode, setCustomMode] = useState(value !== '' && !options.includes(value));

  return (
    <label class="block text-xs font-medium text-slate-600">
      <span class="inline-flex items-center">
        {label}
        <FieldHint text={hint} />
      </span>
      {customMode ? (
        <div class="mt-1 flex gap-1.5">
          <input
            class="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={value}
            placeholder={placeholder}
            onInput={(e) => onChange((e.target as HTMLInputElement).value)}
          />
          <button
            type="button"
            onClick={() => {
              setCustomMode(false);
              onChange('');
            }}
            class="shrink-0 rounded-lg px-2 text-[11px] font-medium text-slate-500 hover:text-slate-700"
          >
            Use list
          </button>
        </div>
      ) : (
        <select
          class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
          value={value}
          onChange={(e) => {
            const next = (e.target as HTMLSelectElement).value;
            if (next === OTHER_OPTION) {
              setCustomMode(true);
              onChange('');
            } else {
              onChange(next);
            }
          }}
        >
          <option value="">Select…</option>
          {options.map((opt) => (
            <option key={opt} value={opt}>
              {opt}
            </option>
          ))}
          <option value={OTHER_OPTION}>Other (type my own)…</option>
        </select>
      )}
    </label>
  );
}

function RepeatableTextList({
  label,
  hint,
  values,
  onChange,
  placeholder,
}: {
  label: string;
  hint?: string;
  values: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
}) {
  return (
    <div class="block text-xs font-medium text-slate-600">
      <span class="inline-flex items-center">
        {label}
        {hint ? <FieldHint text={hint} /> : null}
      </span>
      <div class="mt-1 space-y-1.5">
        {values.map((value, idx) => (
          <div key={idx} class="flex gap-1.5">
            <textarea
              class="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm font-normal"
              rows={2}
              value={value}
              placeholder={placeholder}
              onInput={(e) => {
                const next = [...values];
                next[idx] = (e.target as HTMLTextAreaElement).value;
                onChange(next);
              }}
            />
            <button
              type="button"
              onClick={() => onChange(values.filter((_, i) => i !== idx))}
              class="shrink-0 rounded-lg px-2 text-xs text-slate-400 hover:text-red-600"
            >
              Remove
            </button>
          </div>
        ))}
        <button
          type="button"
          onClick={() => onChange([...values, ''])}
          class="rounded-lg bg-slate-100 px-2 py-1 text-[11px] font-medium text-slate-700 hover:bg-slate-200"
        >
          + Add entry
        </button>
      </div>
    </div>
  );
}

function GenerateDeterministicDraftForm({ onGenerated }: { onGenerated: () => void }) {
  const [open, setOpen] = useState(false);
  const [csvFiles, setCsvFiles] = useState<File[]>([]);
  const [collections, setCollections] = useState<Collection[]>([]);
  const [collectionsError, setCollectionsError] = useState('');
  const [dataOwners, setDataOwners] = useState<DataOwner[]>([]);
  const [dataOwnersError, setDataOwnersError] = useState('');
  const [categoryId, setCategoryId] = useState('');
  const [collectionId, setCollectionId] = useState('');
  const [dataOwnerName, setDataOwnerName] = useState('');
  const [datasetId, setDatasetId] = useState('');
  const [datasetTitle, setDatasetTitle] = useState('');
  const [datasetDescription, setDatasetDescription] = useState('');
  const [source, setSource] = useState<string[]>([]);
  const [references, setReferences] = useState<string[]>([]);
  const [tagsConcept, setTagsConcept] = useState('');
  const [tagsEpiType, setTagsEpiType] = useState('');
  const [spatialCoverage, setSpatialCoverage] = useState('');
  const [spatialResolution, setSpatialResolution] = useState('');
  const [temporalCoverage, setTemporalCoverage] = useState('');
  const [temporalResolution, setTemporalResolution] = useState('');
  const [updateFrequency, setUpdateFrequency] = useState('');
  const [comments, setComments] = useState<string[]>([]);
  const [joinKeyColumns, setJoinKeyColumns] = useState('');
  const [tableDescriptions, setTableDescriptions] = useState<Record<string, string>>({});
  const [columnsByTable, setColumnsByTable] = useState<Record<string, string[]>>({});
  const [columnDescriptions, setColumnDescriptions] = useState<Record<string, Record<string, string>>>({});
  const [classifyError, setClassifyError] = useState('');
  const [coverageAutoFilled, setCoverageAutoFilled] = useState<Record<string, boolean>>({});
  const [coverageInferError, setCoverageInferError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const clearAutoFilled = (field: string) => {
    setCoverageAutoFilled((prev) => (prev[field] ? { ...prev, [field]: false } : prev));
  };

  const tableNames = useMemo(() => csvFiles.map((f) => f.name.replace(/\.csv$/i, '')), [csvFiles]);

  useEffect(() => {
    if (csvFiles.length === 0) {
      setColumnsByTable({});
      return;
    }
    let cancelled = false;
    setClassifyError('');
    Promise.all(
      csvFiles.map(async (file, idx) => {
        const tableName = tableNames[idx];
        const columnNames = await readCsvHeaderColumns(file);
        const { needsDescription } = await api.adminClassifyColumns({ tableName, columnNames });
        return [tableName, needsDescription] as const;
      })
    )
      .then((entries) => {
        if (!cancelled) setColumnsByTable(Object.fromEntries(entries));
      })
      .catch((err) => {
        if (!cancelled) setClassifyError(errorMessage(err, 'Failed to classify CSV columns'));
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [csvFiles]);

  // Auto-detects spatialCoverage/spatialResolution/temporalCoverage from
  // the CSVs' own column names/values (e.g. a "state.name" column implies
  // India + state-level; a year-typed column's observed values become the
  // temporal coverage list) - a starting point only, never overwrites a
  // value the curator already typed, and stays a normal editable field.
  useEffect(() => {
    if (csvFiles.length === 0) return;
    let cancelled = false;
    setCoverageInferError('');
    api
      .adminInferDatasetCoverage(csvFiles)
      .then((result) => {
        if (cancelled) return;
        const filled: Record<string, boolean> = {};
        if (result.spatialCoverage && !spatialCoverage) {
          setSpatialCoverage(result.spatialCoverage);
          filled.spatialCoverage = true;
        }
        if (result.spatialResolution && !spatialResolution) {
          setSpatialResolution(result.spatialResolution);
          filled.spatialResolution = true;
        }
        if (result.temporalCoverage && !temporalCoverage) {
          setTemporalCoverage(result.temporalCoverage);
          filled.temporalCoverage = true;
        }
        if (Object.keys(filled).length > 0) {
          setCoverageAutoFilled((prev) => ({ ...prev, ...filled }));
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setCoverageInferError(
            errorMessage(err, 'Could not auto-detect coverage fields — fill them in manually below.')
          );
        }
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [csvFiles]);

  useEffect(() => {
    if (!open || collections.length > 0) return;
    api
      .getCollections()
      .then((res) => setCollections(res.collections))
      .catch((err) => setCollectionsError(errorMessage(err, 'Failed to load categories/collections')));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  useEffect(() => {
    if (!open || dataOwners.length > 0) return;
    api
      .getDataOwners()
      .then((res) => setDataOwners(res.data_owners))
      .catch((err) => setDataOwnersError(errorMessage(err, 'Failed to load data owners')));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const categories = useMemo(() => {
    const seen = new Map<string, string>();
    for (const c of collections) seen.set(c.category_id, c.category_name);
    return Array.from(seen.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  }, [collections]);

  const collectionsForCategory = useMemo(
    () => collections.filter((c) => c.category_id === categoryId).sort((a, b) => a.collection_name.localeCompare(b.collection_name)),
    [collections, categoryId]
  );

  const requiredFieldsFilled =
    csvFiles.length > 0 &&
    categoryId &&
    collectionId &&
    dataOwnerName &&
    (csvFiles.length === 1 || datasetTitle.trim()) &&
    datasetDescription &&
    spatialCoverage &&
    spatialResolution &&
    temporalCoverage &&
    temporalResolution &&
    updateFrequency &&
    tableNames.every((name) => (tableDescriptions[name] || '').trim()) &&
    tableNames.every((name) =>
      (columnsByTable[name] || []).every((col) => (columnDescriptions[name]?.[col] || '').trim())
    );

  const submit = async (e: Event) => {
    e.preventDefault();
    if (!requiredFieldsFilled) return;
    setSubmitting(true);
    setError('');
    try {
      const curatorInput: CuratorMetadataInput = {
        datasetTitle: datasetTitle.trim(),
        datasetDescription,
        source: source.map((s) => s.trim()).filter(Boolean),
        references: references.map((r) => r.trim()).filter(Boolean),
        tags: { concept: csvToList(tagsConcept), epiType: csvToList(tagsEpiType) },
        spatialCoverage,
        spatialResolution,
        temporalCoverage,
        temporalResolution,
        updateFrequency,
        comments: comments.map((c) => c.trim()).filter(Boolean),
        joinKeyColumns: csvToList(joinKeyColumns),
        tableDescriptions: Object.fromEntries(tableNames.map((name) => [name, tableDescriptions[name] || ''])),
        columnDescriptions: Object.fromEntries(
          tableNames.map((name) => [
            name,
            Object.fromEntries(
              (columnsByTable[name] || []).map((col) => [col, columnDescriptions[name]?.[col] || ''])
            ),
          ])
        ),
      };
      await api.adminGenerateDeterministicManifestDraft({
        csvFiles,
        categoryId,
        collectionId,
        dataOwnerName,
        curatorInput,
        datasetId: datasetId || undefined,
      });
      setCsvFiles([]);
      setCategoryId('');
      setCollectionId('');
      setDataOwnerName('');
      setDatasetId('');
      setDatasetTitle('');
      setDatasetDescription('');
      setSource([]);
      setReferences([]);
      setTagsConcept('');
      setTagsEpiType('');
      setSpatialCoverage('');
      setSpatialResolution('');
      setTemporalCoverage('');
      setTemporalResolution('');
      setUpdateFrequency('');
      setComments([]);
      setJoinKeyColumns('');
      setTableDescriptions({});
      setColumnsByTable({});
      setColumnDescriptions({});
      setOpen(false);
      onGenerated();
    } catch (err) {
      setError(errorMessage(err, 'Failed to generate deterministic draft'));
    } finally {
      setSubmitting(false);
    }
  };

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        class="rounded-xl bg-white px-4 py-2 text-sm font-medium text-slate-700 ring-1 ring-slate-300 hover:bg-slate-50"
      >
        + Generate deterministic draft (no LLM)
      </button>
    );
  }

  return (
    <form onSubmit={submit} class="w-full space-y-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div class="flex items-center justify-between">
        <div>
          <h3 class="text-sm font-semibold text-slate-900">Generate deterministic draft</h3>
          <p class="text-xs text-slate-500">
            Rule-based typing + curated region history - no CSV content is sent anywhere. Fields below can't be
            inferred from the data and must be supplied directly.
          </p>
        </div>
        <button type="button" onClick={() => setOpen(false)} class="text-xs text-slate-400 hover:text-slate-600">
          Cancel
        </button>
      </div>

      {error ? (
        <div class="rounded-lg bg-red-50 p-3 text-xs text-red-700 ring-1 ring-red-200">{error}</div>
      ) : null}
      {collectionsError ? (
        <div class="rounded-lg bg-red-50 p-3 text-xs text-red-700 ring-1 ring-red-200">{collectionsError}</div>
      ) : null}
      {dataOwnersError ? (
        <div class="rounded-lg bg-red-50 p-3 text-xs text-red-700 ring-1 ring-red-200">{dataOwnersError}</div>
      ) : null}

      <div class="grid gap-3 md:grid-cols-2">
        <label class="block text-xs font-medium text-slate-600 md:col-span-2">
          <span class="inline-flex items-center">
            Raw CSV(s) — select more than one for a multi-table dataset
            <FieldHint text="The actual data file(s) for this dataset. Upload one CSV per table — if you select more than one, each becomes its own table inside a single multi-table dataset." />
          </span>
          <input
            type="file"
            accept=".csv"
            multiple
            class="mt-1 block w-full text-sm"
            onChange={(e) => setCsvFiles(Array.from((e.target as HTMLInputElement).files ?? []))}
          />
          {csvFiles.length > 0 ? (
            <span class="mt-1 block text-[11px] text-slate-500">
              {csvFiles.length} table{csvFiles.length > 1 ? 's' : ''}: {csvFiles.map((f) => f.name).join(', ')}
            </span>
          ) : null}
        </label>

        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Category
            <FieldHint text="The top-level subject area this dataset belongs to, e.g. 'Census and Surveys'. Pick the closest match — this decides where the dataset is filed in the catalog." />
          </span>
          <select
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={categoryId}
            onChange={(e) => {
              setCategoryId((e.target as HTMLSelectElement).value);
              setCollectionId('');
            }}
          >
            <option value="">Select a category…</option>
            {categories.map(([id, name]) => (
              <option key={id} value={id}>
                {name} ({id})
              </option>
            ))}
          </select>
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Collection
            <FieldHint text="The specific group of related datasets this one belongs to within the category above, e.g. 'Livestock Census'." />
          </span>
          <select
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={collectionId}
            disabled={!categoryId}
            onChange={(e) => setCollectionId((e.target as HTMLSelectElement).value)}
          >
            <option value="">{categoryId ? 'Select a collection…' : 'Select a category first'}</option>
            {collectionsForCategory.map((c) => (
              <option key={c.collection_id} value={c.collection_id}>
                {c.collection_name} ({c.collection_id})
              </option>
            ))}
          </select>
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Data owner
            <FieldHint text="The organisation or person responsible for this data — who to credit, and who to contact with questions about it." />
          </span>
          <select
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={dataOwnerName}
            onChange={(e) => setDataOwnerName((e.target as HTMLSelectElement).value)}
          >
            <option value="">Select a data owner…</option>
            {dataOwners.map((o) => (
              <option key={o.id} value={o.name}>
                {o.name}
              </option>
            ))}
          </select>
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Existing dataset ID (optional — leave blank to auto-assign the next available ID)
            <FieldHint text="Only fill this in if you're replacing/updating a dataset that's already published. For a brand-new dataset, leave this blank — an ID is assigned automatically." />
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={datasetId}
            onInput={(e) => setDatasetId((e.target as HTMLInputElement).value)}
          />
        </label>
        {csvFiles.length > 1 ? (
          <label class="block text-xs font-medium text-slate-600 md:col-span-2">
            <span class="inline-flex items-center">
              Dataset title — required with multiple CSVs (a single CSV is always named after its own filename)
              <FieldHint text="A short, URL-friendly name for the dataset as a whole (not any single table), e.g. 'bahs-milk-production-statistics-1950-2024'. Only needed when you've uploaded more than one CSV." />
            </span>
            <input
              class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
              value={datasetTitle}
              placeholder="e.g. bahs-milk-production-statistics-1950-2024"
              onInput={(e) => setDatasetTitle((e.target as HTMLInputElement).value)}
            />
          </label>
        ) : null}

        <label class="block text-xs font-medium text-slate-600 md:col-span-2">
          <span class="inline-flex items-center">
            Dataset description
            <FieldHint text="A plain-language summary of what this dataset contains: what each row represents, and what it's broken down by (e.g. state x year x species)." />
          </span>
          <textarea
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            rows={2}
            value={datasetDescription}
            placeholder="e.g. State-level livestock population counts disaggregated by species, breed, sex, age group, utility, and locality."
            onInput={(e) => setDatasetDescription((e.target as HTMLTextAreaElement).value)}
          />
        </label>
        <div class="md:col-span-2">
          <RepeatableTextList
            label="Source document(s) — optional, one citation per entry"
            hint="Where this data originally came from — the report, survey, or publication it was extracted from, e.g. '16th Livestock Census 1997 – Department of Animal Husbandry'."
            values={source}
            onChange={setSource}
            placeholder="e.g. 16th Livestock Census 1997 – Department of Animal Husbandry, Dairying & Fisheries, Government of India"
          />
        </div>
        <div class="md:col-span-2">
          <RepeatableTextList
            label="References / URLs — optional, one per entry"
            hint="Links to the original source document(s) online, if publicly available, so a future user can go look them up."
            values={references}
            onChange={setReferences}
            placeholder="e.g. https://dahd.gov.in/sites/default/files/2019-12/16thLivestockCensusBook.pdf"
          />
        </div>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Tags — concept (comma-separated)
            <FieldHint text="Keywords describing the subject matter, used to help people find this dataset when searching or browsing the catalog, e.g. 'livestock, cattle, buffalo'." />
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={tagsConcept}
            placeholder="e.g. livestock, cattle, buffalo"
            onInput={(e) => setTagsConcept((e.target as HTMLInputElement).value)}
          />
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Tags — epiType (comma-separated)
            <FieldHint text="The kind of measurement this dataset records, e.g. 'population', 'incidence', 'mortality'." />
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={tagsEpiType}
            placeholder="e.g. population"
            onInput={(e) => setTagsEpiType((e.target as HTMLInputElement).value)}
          />
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Spatial coverage
            <FieldHint text="The geographic area this dataset covers, e.g. 'India', or a specific state/region. Auto-detected as 'India' when a state/UT-level region column is found - check it's right." />
            {coverageAutoFilled.spatialCoverage ? <AutoDetectedBadge /> : null}
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={spatialCoverage}
            placeholder="e.g. india"
            onInput={(e) => {
              setSpatialCoverage((e.target as HTMLInputElement).value);
              clearAutoFilled('spatialCoverage');
            }}
          />
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Spatial resolution
            <FieldHint text="The smallest geographic unit each row represents, e.g. 'state', 'district', or 'village'. Auto-detected from the finest-grained region column found - check it's right." />
            {coverageAutoFilled.spatialResolution ? <AutoDetectedBadge /> : null}
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={spatialResolution}
            placeholder="e.g. state"
            onInput={(e) => {
              setSpatialResolution((e.target as HTMLInputElement).value);
              clearAutoFilled('spatialResolution');
            }}
          />
        </label>
        <label class="block text-xs font-medium text-slate-600">
          <span class="inline-flex items-center">
            Temporal coverage
            <FieldHint text="The years or time period this dataset covers, e.g. '1997, 2003, 2007, 2012, 2019' for irregular census years, or '1950-2024' for a continuous range. Auto-detected from a year-typed column's observed values - check it's right." />
            {coverageAutoFilled.temporalCoverage ? <AutoDetectedBadge /> : null}
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={temporalCoverage}
            placeholder="e.g. 1997, 2003, 2007, 2012, 2019"
            onInput={(e) => {
              setTemporalCoverage((e.target as HTMLInputElement).value);
              clearAutoFilled('temporalCoverage');
            }}
          />
        </label>
        {coverageInferError ? (
          <div class="rounded-lg bg-amber-50 p-2 text-[11px] text-amber-700 ring-1 ring-amber-200 md:col-span-2">
            {coverageInferError}
          </div>
        ) : null}
        <PresetSelectField
          label="Temporal resolution"
          hint="How often the data is recorded/reported, e.g. 'Annual', 'Quinquennial' (every 5 years), 'Monthly'."
          value={temporalResolution}
          onChange={setTemporalResolution}
          options={TEMPORAL_RESOLUTION_OPTIONS}
          placeholder="e.g. survey round (single cross-section)"
        />
        <PresetSelectField
          label="Update frequency"
          hint="How often this dataset itself is expected to be refreshed with new data going forward, e.g. 'Annual', 'Quinquennial', 'One-time'."
          value={updateFrequency}
          onChange={setUpdateFrequency}
          options={UPDATE_FREQUENCY_OPTIONS}
          placeholder="e.g. Survey-round basis (not annual)"
        />
        <div class="md:col-span-2">
          <RepeatableTextList
            label="Comments — optional analyst notes, one per entry (region-history notes are added automatically, no need to repeat them here)"
            hint="Any other useful fact about the data a future user should know — units used, known gaps, caveats, or how a value was derived. One distinct fact per entry."
            values={comments}
            onChange={setComments}
            placeholder="e.g. The 'total' locality row equals rural + urban exactly for every observation."
          />
        </div>
        <label class="block text-xs font-medium text-slate-600 md:col-span-2">
          <span class="inline-flex items-center">
            Join key columns (comma-separated, optional — leave blank to accept the auto-suggested candidates)
            <FieldHint text="The column(s) that together uniquely identify each row and would be used to combine/join this data with other datasets, e.g. 'state.ID, year'. Leave blank to accept the automatically-suggested columns." />
          </span>
          <input
            class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
            value={joinKeyColumns}
            placeholder="e.g. state.ID, year"
            onInput={(e) => setJoinKeyColumns((e.target as HTMLInputElement).value)}
          />
        </label>

        {tableNames.length > 0 ? (
          <div class="space-y-4 md:col-span-2">
            {classifyError ? (
              <div class="rounded-lg bg-red-50 p-2 text-xs text-red-700 ring-1 ring-red-200">{classifyError}</div>
            ) : null}
            {tableNames.map((name) => (
              <div key={name} class="space-y-2 rounded-xl border border-slate-100 p-3">
                <label class="block text-xs font-medium text-slate-600">
                  <span class="inline-flex items-center">
                    Table description — {name}
                    <FieldHint text="A plain-language summary of what this specific table/CSV contains — if the dataset has multiple tables, how this one differs from the others." />
                  </span>
                  <textarea
                    class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                    rows={2}
                    value={tableDescriptions[name] || ''}
                    placeholder="e.g. State-level livestock population counts disaggregated by species, breed, sex, age group, utility, and locality."
                    onInput={(e) =>
                      setTableDescriptions((prev) => ({ ...prev, [name]: (e.target as HTMLTextAreaElement).value }))
                    }
                  />
                </label>

                {(columnsByTable[name] || []).length > 0 ? (
                  <div class="space-y-1.5 pl-3">
                    <p class="text-[11px] font-medium uppercase tracking-wide text-slate-400">
                      Column descriptions — {name}
                    </p>
                    {(columnsByTable[name] || []).map((col) => (
                      <label key={col} class="block text-xs font-medium text-slate-600">
                        <span class="inline-flex items-center">
                          {col}
                          <FieldHint text="Explain in plain language what this column represents and what its values mean — this column's type/format was already worked out automatically, only a human-readable description is needed here." />
                        </span>
                        <input
                          class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                          value={columnDescriptions[name]?.[col] || ''}
                          placeholder={`e.g. what the '${col}' column represents and its allowed values`}
                          onInput={(e) => {
                            const value = (e.target as HTMLInputElement).value;
                            setColumnDescriptions((prev) => ({
                              ...prev,
                              [name]: { ...prev[name], [col]: value },
                            }));
                          }}
                        />
                      </label>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        ) : null}
      </div>

      <button
        type="submit"
        disabled={submitting || !requiredFieldsFilled}
        class="w-full rounded-lg bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
      >
        {submitting ? 'Generating…' : 'Generate deterministic draft'}
      </button>
    </form>
  );
}

function DraftQueue() {
  const [drafts, setDrafts] = useState<ManifestDraftSummary[]>([]);
  const [statusFilter, setStatusFilter] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const result = await api.adminListManifestDrafts(statusFilter ? { status: statusFilter } : undefined);
      setDrafts(result.drafts);
    } catch (err) {
      setError(errorMessage(err, 'Failed to load manifest drafts'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter]);

  return (
    <div class="space-y-4">
      <div class="flex flex-col items-start gap-2">
        <GenerateDraftForm onGenerated={load} />
        <GenerateDeterministicDraftForm onGenerated={load} />
      </div>

      <div class="flex flex-wrap items-center gap-2">
        {['', 'pending', 'flagged', 'approved', 'rejected'].map((value) => (
          <button
            type="button"
            key={value || 'all'}
            onClick={() => setStatusFilter(value)}
            class={`rounded-xl px-3 py-1.5 text-sm font-medium transition ${
              statusFilter === value
                ? 'bg-slate-900 text-white'
                : 'bg-white text-slate-600 ring-1 ring-slate-200 hover:bg-slate-50'
            }`}
          >
            {value ? value[0].toUpperCase() + value.slice(1) : 'All'}
          </button>
        ))}
      </div>

      {error ? (
        <div class="rounded-xl bg-red-50 p-4 text-sm text-red-700 ring-1 ring-red-200">{error}</div>
      ) : null}

      {loading ? (
        <div class="rounded-2xl border border-slate-200 bg-white p-6 text-sm text-slate-500 shadow-sm">Loading…</div>
      ) : drafts.length === 0 ? (
        <div class="rounded-2xl border border-slate-200 bg-white p-6 text-sm text-slate-500 shadow-sm">
          No drafts yet — use "Generate new draft" above, or run{' '}
          <code class="rounded bg-slate-100 px-1.5 py-0.5">dataio draft generate</code> from the CLI.
        </div>
      ) : (
        <div class="space-y-2">
          {drafts.map((draft) => (
            <a
              key={draft.draft_id}
              href={`/admin/datasets/drafts/view?id=${encodeURIComponent(draft.draft_id)}`}
              class="flex items-center justify-between rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-slate-300 hover:shadow"
            >
              <div>
                <div class="font-medium text-slate-900">
                  {draft.dataset_id || draft.collection_id}
                </div>
                <div class="mt-1 text-xs text-slate-500">
                  {draft.llm_model_id || 'Deterministic (rule-based)'} · created by {draft.created_by}
                  {draft.created_at ? ` · ${formatUtcTimestamp(draft.created_at)}` : ''}
                </div>
              </div>
              <div class="flex items-center gap-3">
                <span class={`rounded-full px-3 py-1 text-xs font-medium ring-1 ${statusClasses(draft.status)}`}>
                  {draft.status}
                </span>
                <button
                  type="button"
                  title="Delete draft"
                  onClick={async (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    if (!window.confirm('Delete this draft permanently? This cannot be undone.')) return;
                    await api.adminDeleteManifestDraft(draft.draft_id);
                    load();
                  }}
                  class="rounded-lg px-2 py-1 text-xs font-medium text-red-500 hover:bg-red-50 hover:text-red-700"
                >
                  Delete
                </button>
              </div>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}

function findingRows(findings: ValidationFinding[] | undefined) {
  if (!findings || findings.length === 0) return null;
  return (
    <div class="space-y-1.5">
      {findings.map((finding, idx) => (
        <div
          key={idx}
          class={`rounded-lg px-3 py-2 text-xs ring-1 ${severityClasses(finding.severity)}`}
        >
          <span class="font-semibold uppercase">{finding.severity}</span> {finding.message}
          {finding.path ? <span class="opacity-70"> ({finding.path})</span> : null}
        </div>
      ))}
    </div>
  );
}

interface NumericFieldEntry {
  table: string;
  column: string;
  additive: boolean;
  min: string;
}

// Every non-join-key int/float data_dictionary column across all tables -
// additive/aggregation/min aren't safely inferable by rule (a rate or an ID
// isn't summable just because it's numeric), so this surfaces them here for
// the curator to opt into per column, post-generation. Reads only
// draft_json already on the client - no CSV, no network, no AI.
function extractNumericFields(draftJson: Record<string, unknown>): NumericFieldEntry[] {
  const tables = (draftJson.tables as Record<string, unknown>) || {};
  const entries: NumericFieldEntry[] = [];
  for (const [tableName, table] of Object.entries(tables)) {
    const dataDictionary =
      ((table as Record<string, unknown>)?.data_dictionary as Record<string, Record<string, unknown>>) || {};
    for (const [columnName, field] of Object.entries(dataDictionary)) {
      if ((field.type !== 'int' && field.type !== 'float') || field.isJoinKey) continue;
      if (columnName.toLowerCase().startsWith('source')) continue; // provenance, e.g. sourcePage - never a measure
      entries.push({
        table: tableName,
        column: columnName,
        additive: field.additive === true,
        min: field.min === undefined || field.min === null ? '' : String(field.min),
      });
    }
  }
  return entries;
}

function DraftDetail({ draftId }: { draftId: string }) {
  const [draft, setDraft] = useState<ManifestDraftDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [actionError, setActionError] = useState('');
  const [busy, setBusy] = useState(false);
  const [flagFieldPath, setFlagFieldPath] = useState('');
  const [flagNote, setFlagNote] = useState('');
  const [editing, setEditing] = useState(false);
  const [editedYaml, setEditedYaml] = useState('');
  const [numericFields, setNumericFields] = useState<NumericFieldEntry[]>([]);
  const [infoYamlAccessLevel, setInfoYamlAccessLevel] = useState<AccessLevel>('NONE');
  const [infoYamlBusy, setInfoYamlBusy] = useState(false);
  const [infoYamlError, setInfoYamlError] = useState('');
  const [uploadAccessLevel, setUploadAccessLevel] = useState<AccessLevel>('NONE');
  const [uploadBucketType, setUploadBucketType] = useState('STANDARDISED');
  const [uploadBusy, setUploadBusy] = useState(false);
  const [uploadError, setUploadError] = useState('');
  const [uploadResult, setUploadResult] = useState<{ dataset_id: string; uploaded_tables: string[] } | null>(null);

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const result = await api.adminGetManifestDraft(draftId);
      setDraft(result);
      setNumericFields(extractNumericFields(result.draft_json));
    } catch (err) {
      setError(errorMessage(err, 'Failed to load manifest draft'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draftId]);

  const runAction = async (action: () => Promise<unknown>) => {
    setBusy(true);
    setActionError('');
    try {
      await action();
      await load();
    } catch (err) {
      setActionError(errorMessage(err, 'Action failed'));
    } finally {
      setBusy(false);
    }
  };

  const applyNumericFieldSettings = () => {
    if (!draft) return;
    runAction(async () => {
      const updated = JSON.parse(JSON.stringify(draft.draft_json)) as Record<string, any>;
      for (const entry of numericFields) {
        const field = updated.tables?.[entry.table]?.data_dictionary?.[entry.column];
        if (!field) continue;
        if (entry.additive) {
          field.additive = true;
          field.aggregation = 'sum';
          const min = entry.min.trim();
          if (min !== '' && !Number.isNaN(Number(min))) {
            field.min = Number(min);
          } else {
            delete field.min;
          }
        } else {
          delete field.additive;
          delete field.aggregation;
          delete field.min;
        }
      }
      await api.adminUpdateManifestDraftContent(draftId, stringifyYaml(updated));
    });
  };

  const downloadInfoYaml = async () => {
    if (!draft) return;
    setInfoYamlBusy(true);
    setInfoYamlError('');
    try {
      const result = await api.adminGenerateManifestDraftInfoYaml(draftId, infoYamlAccessLevel);
      const blob = new Blob([result.info_yaml], { type: 'application/x-yaml' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'info.yml';
      link.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setInfoYamlError(errorMessage(err, 'Failed to generate info.yml'));
    } finally {
      setInfoYamlBusy(false);
    }
  };

  const uploadDatasetNow = async () => {
    setUploadBusy(true);
    setUploadError('');
    setUploadResult(null);
    try {
      const result = await api.adminImportDatasetFromDraft(draftId, uploadAccessLevel, uploadBucketType);
      setUploadResult({ dataset_id: result.dataset_id, uploaded_tables: result.uploaded_tables });
      await load(); // refreshes draft.dataset_exists now that it's live
    } catch (err) {
      setUploadError(errorMessage(err, 'Failed to upload dataset'));
    } finally {
      setUploadBusy(false);
    }
  };

  if (loading) {
    return <div class="rounded-2xl border border-slate-200 bg-white p-6 text-sm text-slate-500 shadow-sm">Loading…</div>;
  }
  if (error || !draft) {
    return <div class="rounded-xl bg-red-50 p-4 text-sm text-red-700 ring-1 ring-red-200">{error || 'Draft not found'}</div>;
  }

  // dataset_id is always set (a reserved ID) - dataset_exists is what
  // actually tells us whether that ID belongs to a real Dataset row yet.
  const isNewDataset = draft.dataset_exists === false;

  return (
    <div class="space-y-6">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 class="text-lg font-semibold text-slate-900">
            {draft.dataset_id}
            {isNewDataset ? <span class="ml-2 text-sm font-normal text-slate-400">(new dataset — reserved ID)</span> : null}
          </h2>
          <p class="text-xs text-slate-500">
            Model: {draft.llm_model_id || 'Deterministic (rule-based)'} · Created by {draft.created_by}
          </p>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <span class={`rounded-full px-3 py-1 text-xs font-medium ring-1 ${statusClasses(draft.status)}`}>
            {draft.status}
          </span>
          <button
            type="button"
            disabled={busy || draft.status === 'approved'}
            onClick={() => runAction(() => api.adminApproveManifestDraft(draftId))}
            class="rounded-lg bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {draft.status === 'approved' ? 'Approved' : 'Approve'}
          </button>
          <button
            type="button"
            disabled={busy || draft.status === 'approved' || draft.status === 'rejected'}
            onClick={() => {
              const reason = window.prompt('Reason for rejecting this draft (optional):') ?? undefined;
              runAction(() => api.adminRejectManifestDraft(draftId, reason || undefined));
            }}
            class="rounded-lg bg-white px-3 py-1.5 text-xs font-medium text-red-700 ring-1 ring-red-200 hover:bg-red-50 disabled:opacity-50"
          >
            Reject
          </button>
          <button
            type="button"
            disabled={busy || draft.status === 'approved'}
            onClick={async () => {
              if (!window.confirm('Regenerate this draft from the same CSV(s)? The current draft will be marked rejected and superseded by a new one.')) return;
              setBusy(true);
              setActionError('');
              try {
                const result = await api.adminRegenerateManifestDraft(draftId);
                window.location.href = `/admin/datasets/drafts/view?id=${encodeURIComponent(result.draft_id)}`;
              } catch (err) {
                setActionError(errorMessage(err, 'Failed to regenerate draft'));
                setBusy(false);
              }
            }}
            class="rounded-lg bg-slate-100 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
          >
            Regenerate
          </button>
          <button
            type="button"
            onClick={() => {
              const blob = new Blob([draft.draft_yaml], { type: 'application/x-yaml' });
              const url = URL.createObjectURL(blob);
              const link = document.createElement('a');
              link.href = url;
              link.download = `${draft.dataset_id || draft.collection_id}-metadata.yaml`;
              link.click();
              URL.revokeObjectURL(url);
            }}
            class="rounded-lg bg-slate-100 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-200"
          >
            Download YAML
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={async () => {
              if (!window.confirm('Delete this draft permanently? This cannot be undone.')) return;
              await api.adminDeleteManifestDraft(draftId);
              window.location.href = '/admin/datasets/drafts';
            }}
            class="rounded-lg bg-red-50 px-3 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100 disabled:opacity-50"
          >
            Delete
          </button>
        </div>
      </div>

      {isNewDataset ? (
        <p class="text-xs text-slate-500">
          Dataset ID <code class="rounded bg-slate-100 px-1 py-0.5">{draft.dataset_id}</code> is reserved (see the
          "Reserved IDs" tab) and stays reserved until this dataset is actually imported/uploaded.
        </p>
      ) : null}

      {actionError ? (
        <div class="rounded-xl bg-red-50 p-4 text-sm text-red-700 ring-1 ring-red-200">{actionError}</div>
      ) : null}

      {draft.flagged_fields.length > 0 ? (
        <div class="rounded-2xl border border-amber-200 bg-amber-50 p-4">
          <h3 class="text-sm font-semibold text-amber-900">Flagged by the drafter</h3>
          <ul class="mt-2 space-y-1 text-sm text-amber-800">
            {draft.flagged_fields.map((f, idx) => (
              <li key={idx}>
                <span class="font-medium">{f.field}</span>: {f.reason}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {draft.reviewer_notes.length > 0 ? (
        <div class="rounded-2xl border border-slate-200 bg-white p-4">
          <h3 class="text-sm font-semibold text-slate-900">Reviewer notes</h3>
          <ul class="mt-2 space-y-1 text-sm text-slate-600">
            {draft.reviewer_notes.map((n, idx) => (
              <li key={idx}>
                {n.field ? <span class="font-medium">{n.field}: </span> : null}
                {n.note} <span class="text-xs text-slate-400">— {n.by}</span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <h3 class="text-sm font-semibold text-slate-900">Flag a field</h3>
        <p class="mt-1 text-xs text-slate-500">
          Mark a specific field as needing attention (e.g. a value the drafter got wrong) - sets the draft's status
          to "flagged" and records this note for the next reviewer.
        </p>
        <form
          class="mt-3 flex flex-wrap items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (!flagFieldPath.trim() || !flagNote.trim()) return;
            runAction(() => api.adminFlagManifestDraftField(draftId, flagFieldPath.trim(), flagNote.trim())).then(
              () => {
                setFlagFieldPath('');
                setFlagNote('');
              }
            );
          }}
        >
          <label class="block text-xs font-medium text-slate-600">
            Field path
            <input
              class="mt-1 w-40 rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
              placeholder="e.g. sourceTableID"
              value={flagFieldPath}
              onInput={(e) => setFlagFieldPath((e.target as HTMLInputElement).value)}
            />
          </label>
          <label class="block flex-1 text-xs font-medium text-slate-600">
            Note
            <input
              class="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
              placeholder="Why this field needs attention"
              value={flagNote}
              onInput={(e) => setFlagNote((e.target as HTMLInputElement).value)}
            />
          </label>
          <button
            type="submit"
            disabled={busy || !flagFieldPath.trim() || !flagNote.trim()}
            class="rounded-lg bg-amber-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-amber-600 disabled:opacity-50"
          >
            Flag field
          </button>
        </form>
      </div>

      {numericFields.length > 0 ? (
        <div class="rounded-2xl border border-slate-200 bg-white p-4">
          <div class="mb-2 flex items-center justify-between">
            <div>
              <h3 class="text-sm font-semibold text-slate-900">Numeric field settings</h3>
              <p class="mt-1 text-xs text-slate-500">
                Whether a numeric column can be summed across rows isn't safe to guess (a rate or an ID isn't
                summable just because it's a number) - tick "Additive" only for columns that genuinely are, e.g. a
                head-count. Unchecked columns are left exactly as generated.
              </p>
            </div>
            <button
              type="button"
              disabled={busy || draft.status === 'approved' || draft.status === 'rejected'}
              onClick={applyNumericFieldSettings}
              class="shrink-0 rounded-lg bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
            >
              Apply
            </button>
          </div>
          <div class="space-y-1.5">
            {numericFields.map((entry, idx) => (
              <div key={`${entry.table}.${entry.column}`} class="flex items-center gap-3 text-xs text-slate-600">
                <label class="flex items-center gap-1.5">
                  <input
                    type="checkbox"
                    checked={entry.additive}
                    disabled={draft.status === 'approved' || draft.status === 'rejected'}
                    onChange={(e) => {
                      const checked = (e.target as HTMLInputElement).checked;
                      setNumericFields((prev) =>
                        prev.map((f, i) => (i === idx ? { ...f, additive: checked } : f))
                      );
                    }}
                  />
                  <span class="font-medium text-slate-700">
                    {entry.table}.{entry.column}
                  </span>
                  additive (summable)?
                </label>
                {entry.additive ? (
                  <label class="flex items-center gap-1.5">
                    min
                    <input
                      type="number"
                      value={entry.min}
                      placeholder="e.g. 0"
                      class="w-20 rounded-lg border border-slate-300 px-2 py-1 text-xs"
                      onInput={(e) => {
                        const value = (e.target as HTMLInputElement).value;
                        setNumericFields((prev) => prev.map((f, i) => (i === idx ? { ...f, min: value } : f)));
                      }}
                    />
                  </label>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="mb-2 flex items-center justify-between gap-3">
          <div>
            <h3 class="text-sm font-semibold text-slate-900">Generate info.yml</h3>
            <p class="mt-1 text-xs text-slate-500">
              {draft.status === 'approved'
                ? "info.yml is the second file, alongside metadata.yaml, that the dataset import step needs to turn this draft into a real, live dataset. It carries the catalog-level fields metadata.yaml doesn't - dataset ID, collection, title, data owner, description, temporal coverage & resolution, spatial resolution, and the raw dataset's ID & source - plus access level (who can view/download this dataset), which isn't captured anywhere upstream, so pick it here before generating. metadata.yaml + info.yml together are exactly what the Import / Create Dataset tab (or the \"Upload dataset now\" button below) needs."
                : 'Available once this draft is approved, so info.yml reflects the final reviewed manifest rather than a still-changeable draft. Approve it above first.'}
            </p>
          </div>
          <div class="flex shrink-0 items-center gap-2">
            <select
              class="rounded-lg border border-slate-300 px-2 py-1 text-xs disabled:opacity-50"
              value={infoYamlAccessLevel}
              disabled={draft.status !== 'approved'}
              onChange={(e) => setInfoYamlAccessLevel((e.target as HTMLSelectElement).value as AccessLevel)}
            >
              <option value="NONE">NONE</option>
              <option value="VIEW">VIEW</option>
              <option value="DOWNLOAD">DOWNLOAD</option>
            </select>
            <button
              type="button"
              disabled={infoYamlBusy || draft.status !== 'approved'}
              onClick={downloadInfoYaml}
              class="rounded-lg bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
            >
              {infoYamlBusy ? 'Generating…' : 'Download info.yml'}
            </button>
          </div>
        </div>
        {infoYamlError ? (
          <div class="rounded-lg bg-red-50 p-2 text-xs text-red-700 ring-1 ring-red-200">{infoYamlError}</div>
        ) : null}
      </div>

      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="mb-2 flex items-center justify-between">
          <h3 class="text-sm font-semibold text-slate-900">Draft manifest.yaml</h3>
          <div class="flex items-center gap-2">
            {editing ? (
              <>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => setEditing(false)}
                  class="rounded-lg bg-white px-3 py-1 text-xs font-medium text-slate-600 ring-1 ring-slate-200 hover:bg-slate-50 disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() =>
                    runAction(async () => {
                      await api.adminUpdateManifestDraftContent(draftId, editedYaml);
                      setEditing(false);
                    })
                  }
                  class="rounded-lg bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                >
                  Save & re-validate
                </button>
              </>
            ) : (
              <>
                <button
                  type="button"
                  disabled={busy || draft.status === 'approved' || draft.status === 'rejected'}
                  onClick={() => {
                    setEditedYaml(draft.draft_yaml);
                    setEditing(true);
                  }}
                  class="rounded-lg bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
                >
                  Edit
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => runAction(() => api.adminValidateManifestDraft(draftId))}
                  class="rounded-lg bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
                >
                  Re-validate
                </button>
              </>
            )}
          </div>
        </div>
        {editing ? (
          <textarea
            class="h-96 w-full rounded-xl border border-slate-300 bg-slate-950 p-4 font-mono text-xs text-slate-100"
            value={editedYaml}
            onInput={(e) => setEditedYaml((e.target as HTMLTextAreaElement).value)}
          />
        ) : (
          <pre class="max-h-96 overflow-auto rounded-xl bg-slate-950 p-4 text-xs text-slate-100">{draft.draft_yaml}</pre>
        )}
      </div>

      {draft.validation_result ? (
        <div class="rounded-2xl border border-slate-200 bg-white p-4">
          <h3 class="mb-2 text-sm font-semibold text-slate-900">
            Validation:{' '}
            <span
              class={`rounded px-2 py-0.5 text-xs ring-1 ${severityClasses(
                draft.validation_result.status === 'fail'
                  ? 'error'
                  : draft.validation_result.status === 'warn'
                    ? 'warning'
                    : 'info'
              )}`}
            >
              {draft.validation_result.status}
            </span>
          </h3>
          {findingRows(draft.validation_result.findings)}
        </div>
      ) : null}

      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="mb-2 flex items-center justify-between gap-3">
          <div>
            <h3 class="text-sm font-semibold text-slate-900">Upload dataset now</h3>
            <p class="mt-1 text-xs text-slate-500">
              {draft.status !== 'approved'
                ? 'Available once this draft is approved. Approve it above first.'
                : draft.dataset_exists === true || uploadResult
                  ? 'This draft has already been uploaded to the DataIO catalog. To make further changes to the live dataset, use the Import / Create Dataset tab.'
                  : 'Publishes this approved draft straight to the DataIO catalog - the same process as the Import / Create Dataset tab, without the manual download-and-re-upload step. Use this right after generating a draft’s metadata. For uploads later on, or from outside this draft flow, use the Import / Create Dataset tab instead.'}
            </p>
          </div>
          <div class="flex shrink-0 items-center gap-2">
            <select
              class="rounded-lg border border-slate-300 px-2 py-1 text-xs disabled:opacity-50"
              value={uploadAccessLevel}
              disabled={uploadBusy || draft.status !== 'approved' || draft.dataset_exists === true || !!uploadResult}
              onChange={(e) => setUploadAccessLevel((e.target as HTMLSelectElement).value as AccessLevel)}
            >
              <option value="NONE">NONE</option>
              <option value="VIEW">VIEW</option>
              <option value="DOWNLOAD">DOWNLOAD</option>
            </select>
            <select
              class="rounded-lg border border-slate-300 px-2 py-1 text-xs disabled:opacity-50"
              value={uploadBucketType}
              disabled={uploadBusy || draft.status !== 'approved' || draft.dataset_exists === true || !!uploadResult}
              onChange={(e) => setUploadBucketType((e.target as HTMLSelectElement).value)}
            >
              <option value="STANDARDISED">STANDARDISED</option>
              <option value="PREPROCESSED">PREPROCESSED</option>
            </select>
            <button
              type="button"
              disabled={uploadBusy || draft.status !== 'approved' || draft.dataset_exists === true || !!uploadResult}
              onClick={uploadDatasetNow}
              class="rounded-lg bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
            >
              {uploadBusy
                ? 'Uploading…'
                : draft.dataset_exists === true || uploadResult
                  ? 'Already uploaded'
                  : 'Upload dataset now'}
            </button>
          </div>
        </div>
        {uploadResult ? (
          <div class="rounded-lg bg-emerald-50 p-2 text-xs text-emerald-700 ring-1 ring-emerald-200">
            Uploaded dataset <span class="font-mono">{uploadResult.dataset_id}</span> ({uploadResult.uploaded_tables.length}{' '}
            table{uploadResult.uploaded_tables.length === 1 ? '' : 's'}).
          </div>
        ) : null}
        {uploadError ? (
          <div class="rounded-lg bg-red-50 p-2 text-xs text-red-700 ring-1 ring-red-200">{uploadError}</div>
        ) : null}
      </div>
    </div>
  );
}

export default function DraftReviewManager() {
  return <DraftQueue />;
}

/**
 * The site is built with `output: 'static'`, so there's no server-side
 * dynamic route for a per-draft page - the draft id comes from a query
 * param (?id=...) read client-side, the same way the rest of this static
 * site handles anything that isn't known at build time.
 */
export function DraftReviewDetail() {
  const [draftId, setDraftId] = useState<string | null>(null);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    setDraftId(params.get('id'));
  }, []);

  if (!draftId) {
    return (
      <div class="rounded-xl bg-red-50 p-4 text-sm text-red-700 ring-1 ring-red-200">
        No draft id in the URL (expected ?id=&lt;draft-id&gt;).
      </div>
    );
  }
  return <DraftDetail draftId={draftId} />;
}
