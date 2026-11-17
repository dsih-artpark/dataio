import { useEffect, useState } from 'preact/hooks';
import { api } from '../../../lib/api';
import type { PublicDatasetChange } from '../../../lib/types';
import DatasetChangeEntries from './DatasetChangeEntries';

const PAGE_SIZE = 20;

function datasetFromAddress(): string {
  return new URLSearchParams(window.location.search).get('dataset')?.trim().toUpperCase() ?? '';
}

/** All published dataset changes, newest first, 20 per page, filterable by dataset ID (DIO-53). */
export default function DatasetChangesBrowser() {
  const [dataset, setDataset] = useState('');
  const [draft, setDraft] = useState('');
  const [offset, setOffset] = useState(0);
  const [entries, setEntries] = useState<PublicDatasetChange[] | null>(null);
  const [total, setTotal] = useState(0);
  const [failed, setFailed] = useState(false);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const initial = datasetFromAddress();
    setDataset(initial);
    setDraft(initial);
    setReady(true);
  }, []);

  useEffect(() => {
    if (!ready) return;
    let current = true; // ignore an older response that arrives after a newer one
    setEntries(null);
    setFailed(false);
    api
      .getPublicDatasetChangeList({ datasetId: dataset || undefined, limit: PAGE_SIZE, offset })
      .then((response) => {
        if (!current) return;
        setEntries(response.entries);
        setTotal(response.total);
      })
      .catch(() => current && setFailed(true));
    return () => {
      current = false;
    };
  }, [ready, dataset, offset]);

  const applyFilter = (value: string) => {
    const next = value.trim().toUpperCase();
    setDataset(next);
    setOffset(0);
    const url = new URL(window.location.href);
    if (next) url.searchParams.set('dataset', next);
    else url.searchParams.delete('dataset');
    window.history.replaceState(null, '', url);
  };

  const from = total === 0 ? 0 : offset + 1;
  const to = Math.min(offset + PAGE_SIZE, total);
  const pager = 'rounded-lg border border-gray-300 px-3 py-1.5 text-sm font-semibold text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50';

  return (
    <div class="flex flex-col gap-5">
      <form
        class="flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          applyFilter(draft);
        }}
      >
        <label class="flex flex-col gap-1 text-sm font-semibold text-gray-700">
          Dataset ID
          <input
            type="search"
            value={draft}
            onInput={(e) => setDraft((e.currentTarget as HTMLInputElement).value)}
            placeholder="e.g. CS0007DS0103"
            class="w-64 rounded-lg border border-gray-300 px-3 py-2 font-mono text-sm font-normal"
          />
        </label>
        <button type="submit" class="rounded-lg bg-primary-700 px-4 py-2 text-sm font-semibold text-white hover:bg-primary-800">
          Show
        </button>
        {dataset ? (
          <button type="button" onClick={() => { setDraft(''); applyFilter(''); }} class="px-2 py-2 text-sm font-semibold text-primary-700 hover:text-primary-900">
            Show all datasets
          </button>
        ) : null}
      </form>

      {failed ? (
        <p class="m-0 text-sm text-gray-600">The changes couldn’t be loaded. Please try again in a moment.</p>
      ) : entries === null ? (
        <p class="m-0 text-sm text-gray-500" aria-busy="true">Loading…</p>
      ) : entries.length === 0 ? (
        <p class="m-0 text-sm text-gray-600">
          {dataset ? `No changes have been published for ${dataset}.` : 'No dataset changes have been published yet.'}
        </p>
      ) : (
        <>
          <p class="m-0 text-sm text-gray-500" aria-live="polite">
            Showing {from}–{to} of {total}
          </p>
          <DatasetChangeEntries entries={entries} showDataset />
          {total > PAGE_SIZE ? (
            <nav class="flex gap-2" aria-label="Pages">
              <button type="button" class={pager} disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}>
                ← Newer
              </button>
              <button type="button" class={pager} disabled={to >= total} onClick={() => setOffset(offset + PAGE_SIZE)}>
                Older →
              </button>
            </nav>
          ) : null}
        </>
      )}
    </div>
  );
}
