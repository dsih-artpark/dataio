import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';
import type { AdminDatasetChange, ChangelogDraft, DatasetChangeStatus } from '../../lib/types';
import { CHANGE_KIND_LABELS, ChangelogEntryForm, emptyChangelogDraft, isChangelogComplete } from './ChangelogEntryForm';

const TABS: { status: DatasetChangeStatus | undefined; label: string }[] = [
  { status: undefined, label: 'All' },
  { status: 'DRAFT', label: 'Drafts' },
  { status: 'PUBLISHED', label: 'Published' },
];

const formatDateTime = (iso: string | null) =>
  iso ? new Date(iso).toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' }) : '';
const formatDay = (iso: string) => new Date(`${iso}T00:00:00`).toLocaleDateString('en-IN', { dateStyle: 'medium' });

const toDraft = (entry: AdminDatasetChange): ChangelogDraft => ({
  date: entry.date,
  type: entry.type,
  breaking: entry.breaking,
  summary: entry.summary,
  reason: entry.reason,
  changes: entry.changes,
});

/** Admin list of changelog entries: add, edit, publish and delete drafts (DIO-53). */
export default function DatasetChangeManager() {
  const [status, setStatus] = useState<DatasetChangeStatus | undefined>(undefined);
  const [datasetFilter, setDatasetFilter] = useState('');
  const [entries, setEntries] = useState<AdminDatasetChange[] | null>(null);
  const [drafts, setDrafts] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ id: string; original: ChangelogDraft; draft: ChangelogDraft } | null>(null);
  const [confirming, setConfirming] = useState<{ id: string; action: 'publish' | 'delete' } | null>(null);
  const [adding, setAdding] = useState<{ datasetId: string; draft: ChangelogDraft } | null>(null);

  const load = () => {
    setError(null);
    api
      .adminListDatasetChanges({ status, datasetId: datasetFilter.trim() || undefined, limit: 200 })
      .then((response) => {
        setEntries(response.entries);
        setDrafts(response.drafts);
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load changelog entries'));
  };

  useEffect(load, [status]);

  const run = async (id: string, action: () => Promise<unknown>, done: string) => {
    setBusy(id);
    setMessage(null);
    setError(null);
    try {
      await action();
      setMessage(done);
      setConfirming(null);
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed');
    } finally {
      setBusy(null);
    }
  };

  const saveEdit = () => {
    if (!editing) return;
    const { id, original, draft } = editing;
    // Send the date and rows only if they were changed, so the stored ones stay.
    const body = {
      type: draft.type,
      breaking: draft.breaking,
      summary: draft.summary,
      reason: draft.reason,
      ...(draft.date !== original.date ? { date: draft.date } : {}),
      ...(JSON.stringify(draft.changes) !== JSON.stringify(original.changes) ? { changes: draft.changes } : {}),
    };
    run(id, () => api.adminUpdateDatasetChange(id, body).then(() => setEditing(null)), 'Saved.');
  };

  const saveNew = () => {
    if (!adding) return;
    const datasetId = adding.datasetId.trim();
    const { date, ...rest } = adding.draft;
    const body = date ? adding.draft : rest; // no date: the server uses today (IST)
    run('new', () => api.adminCreateDatasetChange(datasetId, body).then(() => setAdding(null)), `Draft saved for ${datasetId}.`);
  };

  const button = 'rounded-lg px-3 py-1.5 text-sm font-medium disabled:cursor-not-allowed disabled:opacity-60';

  return (
    <div class="space-y-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="flex flex-wrap items-center gap-2" role="tablist" aria-label="Entry status">
          {TABS.map((tab) => (
            <button
              key={tab.label}
              type="button"
              role="tab"
              aria-selected={status === tab.status}
              onClick={() => setStatus(tab.status)}
              class={`${button} ${status === tab.status ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
            >
              {tab.label}
              {tab.status === 'DRAFT' && drafts > 0 ? (
                <span class="ml-2 rounded-full bg-amber-100 px-2 text-xs text-amber-800">{drafts}</span>
              ) : null}
            </button>
          ))}
          <form
            class="flex items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              load();
            }}
          >
            <input
              type="search"
              value={datasetFilter}
              onInput={(e) => setDatasetFilter((e.currentTarget as HTMLInputElement).value)}
              placeholder="Dataset ID, e.g. CS0007DS0103"
              aria-label="Filter by dataset ID"
              class="w-64 rounded-lg border border-slate-300 px-3 py-1.5 font-mono text-sm"
            />
            <button type="submit" class={`${button} border border-slate-300 text-slate-700 hover:bg-slate-50`}>
              Filter
            </button>
          </form>
        </div>
        <button
          type="button"
          onClick={() => setAdding({ datasetId: '', draft: emptyChangelogDraft() })}
          disabled={!!adding}
          class={`${button} bg-slate-900 text-white`}
        >
          Add entry
        </button>
      </div>

      {message && <div class="rounded-lg border border-primary-200 bg-primary-50 p-3 text-sm text-primary-900">{message}</div>}
      {error && <div class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-800">{error}</div>}

      {adding ? (
        <section class="space-y-4 rounded-2xl border border-slate-200 bg-white p-4">
          <h2 class="font-semibold text-slate-900">New changelog entry</h2>
          <label class="block space-y-1 text-sm">
            <span class="font-medium text-slate-800">Dataset ID</span>
            <input
              type="text"
              value={adding.datasetId}
              onInput={(e) => setAdding({ ...adding, datasetId: (e.currentTarget as HTMLInputElement).value })}
              placeholder="CS0007DS0103"
              class="block w-64 rounded-xl border border-slate-300 px-3 py-2 font-mono text-sm"
            />
          </label>
          <ChangelogEntryForm value={adding.draft} onChange={(draft) => setAdding({ ...adding, draft })} showDate />
          <p class="text-xs text-slate-500">Leave the date empty to use today. Saved as a draft.</p>
          <div class="flex gap-2">
            <button
              type="button"
              onClick={saveNew}
              disabled={busy === 'new' || !adding.datasetId.trim() || !isChangelogComplete(adding.draft)}
              class={`${button} bg-slate-900 text-white`}
            >
              Save draft
            </button>
            <button type="button" onClick={() => setAdding(null)} class={`${button} text-slate-600 hover:bg-slate-100`}>
              Cancel
            </button>
          </div>
        </section>
      ) : null}

      {!entries && !error && <p class="text-sm text-gray-500">Loading…</p>}
      {entries && entries.length === 0 && (
        <p class="text-sm text-gray-500">
          {status || datasetFilter.trim() ? 'No entries match these filters.' : 'No changelog entries yet.'}
        </p>
      )}

      {entries?.map((entry) => {
        const isEditing = editing?.id === entry.id;
        const isOpen = expanded === entry.id || isEditing;
        const pending = confirming?.id === entry.id ? confirming.action : null;
        return (
          <article key={entry.id} class="space-y-3 rounded-lg border border-gray-200 bg-white p-4">
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div class="space-y-1">
                <div class="flex flex-wrap items-center gap-2 text-sm">
                  <span class="font-medium text-slate-900">{formatDay(entry.date)}</span>
                  <span class="font-mono text-slate-700">{entry.dataset_id}</span>
                  {entry.dataset_title ? <span class="text-slate-500">{entry.dataset_title}</span> : null}
                  <span
                    class={`rounded-full px-2 py-0.5 text-xs font-medium ${
                      entry.status === 'DRAFT' ? 'bg-amber-100 text-amber-800' : 'bg-green-100 text-green-800'
                    }`}
                  >
                    {entry.status === 'DRAFT' ? 'Draft' : 'Published'}
                  </span>
                  {entry.breaking ? (
                    <span class="rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">Breaking</span>
                  ) : null}
                </div>
                <p class="text-sm text-slate-800">{entry.summary}</p>
              </div>
              <div class="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => setExpanded(isOpen ? null : entry.id)}
                  class={`${button} text-slate-600 hover:bg-slate-100`}
                  aria-expanded={isOpen}
                >
                  {isOpen ? 'Hide details' : 'Details'}
                </button>
                <button
                  type="button"
                  onClick={() => setEditing({ id: entry.id, original: toDraft(entry), draft: toDraft(entry) })}
                  disabled={isEditing}
                  class={`${button} border border-slate-300 text-slate-700 hover:bg-slate-50`}
                >
                  Edit
                </button>
                {entry.status === 'DRAFT' && !isEditing ? (
                  <>
                    <button
                      type="button"
                      onClick={() => setConfirming({ id: entry.id, action: 'publish' })}
                      class={`${button} bg-emerald-700 text-white`}
                    >
                      Publish
                    </button>
                    <button
                      type="button"
                      onClick={() => setConfirming({ id: entry.id, action: 'delete' })}
                      class={`${button} text-red-700 hover:bg-red-50`}
                    >
                      Delete
                    </button>
                  </>
                ) : null}
              </div>
            </div>

            {pending ? (
              <div class="flex flex-wrap items-center gap-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
                <span>
                  {pending === 'publish'
                    ? `Publish this entry? It will show on ${entry.dataset_id}'s public Version history.`
                    : 'Delete this draft? This cannot be undone.'}
                </span>
                <button
                  type="button"
                  disabled={busy === entry.id}
                  onClick={() =>
                    pending === 'publish'
                      ? run(entry.id, () => api.adminPublishDatasetChange(entry.id), 'Published.')
                      : run(entry.id, () => api.adminDeleteDatasetChange(entry.id), 'Draft deleted.')
                  }
                  class={`${button} ${pending === 'publish' ? 'bg-emerald-700' : 'bg-red-700'} text-white`}
                >
                  {pending === 'publish' ? 'Yes, publish' : 'Yes, delete'}
                </button>
                <button type="button" onClick={() => setConfirming(null)} class={`${button} hover:bg-amber-100`}>
                  Cancel
                </button>
              </div>
            ) : null}

            {isEditing && editing ? (
              <div class="space-y-3 border-t border-slate-100 pt-3">
                <ChangelogEntryForm value={editing.draft} onChange={(draft) => setEditing({ ...editing, draft })} showDate />
                {!isChangelogComplete(editing.draft) ? (
                  <p class="text-xs text-amber-800">Summary, why and a table name on every row are needed to save.</p>
                ) : null}
                <div class="flex gap-2">
                  <button
                    type="button"
                    onClick={saveEdit}
                    disabled={busy === entry.id || !isChangelogComplete(editing.draft)}
                    class={`${button} bg-slate-900 text-white`}
                  >
                    Save
                  </button>
                  <button type="button" onClick={() => setEditing(null)} class={`${button} text-slate-600 hover:bg-slate-100`}>
                    Cancel
                  </button>
                </div>
              </div>
            ) : isOpen ? (
              <div class="space-y-2 border-t border-slate-100 pt-3 text-sm text-slate-700">
                <p>
                  <span class="font-medium">Why:</span> {entry.reason}
                </p>
                <p>
                  <span class="font-medium">Type:</span> {entry.type === 'data' ? 'Data change' : 'Metadata change'}
                </p>
                {entry.changes.length > 0 ? (
                  <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs">
                      <thead class="text-slate-500">
                        <tr>
                          <th class="py-1 pr-3 font-medium">Change</th>
                          <th class="py-1 pr-3 font-medium">Table</th>
                          <th class="py-1 pr-3 font-medium">Column</th>
                          <th class="py-1 pr-3 font-medium">Old</th>
                          <th class="py-1 pr-3 font-medium">New</th>
                          <th class="py-1 font-medium">Note</th>
                        </tr>
                      </thead>
                      <tbody>
                        {entry.changes.map((row, index) => (
                          <tr key={index}>
                            <td class="py-1 pr-3">{CHANGE_KIND_LABELS[row.kind] ?? row.kind}</td>
                            <td class="py-1 pr-3 font-mono">{row.table || '—'}</td>
                            <td class="py-1 pr-3 font-mono">{row.column || '—'}</td>
                            <td class="py-1 pr-3 font-mono">{row.old ?? '—'}</td>
                            <td class="py-1 pr-3 font-mono">{row.new ?? '—'}</td>
                            <td class="py-1">{row.note || '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p class="text-slate-500">No change rows.</p>
                )}
                {entry.archive_prefix ? (
                  <p>
                    <span class="font-medium">Archive copy:</span>{' '}
                    <code class="rounded bg-slate-100 px-1 text-xs">{entry.archive_prefix}/</code>
                  </p>
                ) : null}
                <p class="text-xs text-slate-500">
                  Created by {entry.created_by ?? 'unknown'} {formatDateTime(entry.created_at)}
                  {entry.updated_at ? ` · edited by ${entry.updated_by ?? 'unknown'} ${formatDateTime(entry.updated_at)}` : ''}
                  {entry.published_at ? ` · published by ${entry.published_by ?? 'unknown'} ${formatDateTime(entry.published_at)}` : ''}
                </p>
              </div>
            ) : null}
          </article>
        );
      })}
    </div>
  );
}
