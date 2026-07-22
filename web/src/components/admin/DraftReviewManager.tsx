import { useEffect, useMemo, useState } from 'preact/hooks';

import { ApiRequestError, api } from '../../lib/api';
import type {
  Collection,
  DataOwner,
  ManifestDraftDetail,
  ManifestDraftStatus,
  ManifestDraftSummary,
  ValidationFinding,
} from '../../lib/types';

function statusClasses(status: ManifestDraftStatus) {
  if (status === 'approved') return 'bg-emerald-50 text-emerald-700 ring-emerald-200';
  if (status === 'rejected') return 'bg-red-50 text-red-700 ring-red-200';
  if (status === 'flagged') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-slate-100 text-slate-700 ring-slate-200';
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
    <form onSubmit={submit} class="space-y-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
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
      <GenerateDraftForm onGenerated={load} />

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
                  {draft.llm_model_id} · created by {draft.created_by}
                  {draft.created_at ? ` · ${new Date(draft.created_at).toLocaleString()}` : ''}
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

function DraftDetail({ draftId }: { draftId: string }) {
  const [draft, setDraft] = useState<ManifestDraftDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [actionError, setActionError] = useState('');
  const [busy, setBusy] = useState(false);
  const [flagFieldPath, setFlagFieldPath] = useState('');
  const [flagNote, setFlagNote] = useState('');

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const result = await api.adminGetManifestDraft(draftId);
      setDraft(result);
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
      <div class="flex items-center justify-between">
        <div>
          <h2 class="text-lg font-semibold text-slate-900">
            {draft.dataset_id}
            {isNewDataset ? <span class="ml-2 text-sm font-normal text-slate-400">(new dataset — reserved ID)</span> : null}
          </h2>
          <p class="text-xs text-slate-500">
            Model: {draft.llm_model_id} · Created by {draft.created_by}
          </p>
        </div>
        <div class="flex items-center gap-3">
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

      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="mb-2 flex items-center justify-between">
          <h3 class="text-sm font-semibold text-slate-900">Draft manifest.yaml</h3>
          <button
            type="button"
            disabled={busy}
            onClick={() => runAction(() => api.adminValidateManifestDraft(draftId))}
            class="rounded-lg bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
          >
            Re-validate
          </button>
        </div>
        <pre class="max-h-96 overflow-auto rounded-xl bg-slate-950 p-4 text-xs text-slate-100">{draft.draft_yaml}</pre>
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
