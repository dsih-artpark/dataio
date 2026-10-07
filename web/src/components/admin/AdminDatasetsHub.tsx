import { useEffect, useState } from 'preact/hooks';

import { api } from '../../lib/api';

// null = could not be determined (that one call failed) - distinct from a
// real 0, so the card can say "unavailable" instead of lying with a zero.
type HubStats = {
  datasets: number | null;
  reservedIds: number | null;
  outdatedDocs: number | null;
  rawDatasets: number | null;
  pendingDrafts: number | null;
};

const defaultStats: HubStats = {
  datasets: null,
  reservedIds: null,
  outdatedDocs: null,
  rawDatasets: null,
  pendingDrafts: null,
};

const actionCards = [
  {
    title: 'Metadata-Drafts',
    description: 'Review LLM-drafted metadata.yaml pending curator approval before anything is uploaded.',
    href: '/admin/datasets/drafts',
    tone: 'bg-slate-900 text-white',
  },
  {
    title: 'Import Dataset Package',
    description: 'Validate info.yml and metadata.yml, review fixes, then upload matching CSV tables.',
    href: '/admin/datasets/new',
    tone: 'bg-white text-slate-900 border border-slate-200',
  },
  {
    title: 'Browse Existing Datasets',
    description: 'Search the catalog, update access quickly, and jump into a full dataset workspace.',
    href: '/admin/datasets/catalog',
    tone: 'bg-white text-slate-900 border border-slate-200',
  },
  {
    title: 'Reserve Dataset ID',
    description: 'Claim identifiers for incoming datasets and keep the intake queue visible.',
    href: '/admin/datasets/reservations',
    tone: 'bg-white text-slate-900 border border-slate-200',
  },
  {
    title: 'Next Available ID',
    description: 'Look up the next free dataset ID and raw dataset ID for a collection before authoring metadata.',
    href: '/admin/datasets/next-id',
    tone: 'bg-white text-slate-900 border border-slate-200',
  },
  {
    title: 'Check Documentation Sync',
    description: 'Review stale README, manifest, and data dictionary cache state before syncing.',
    href: '/admin/datasets/sync',
    tone: 'bg-white text-slate-900 border border-slate-200',
  },
];

export default function AdminDatasetsHub() {
  const [stats, setStats] = useState<HubStats>(defaultStats);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError('');

      // allSettled, not all: a single failed call must not zero out every
      // other stat on this page (see the check_all=true note below for what
      // used to fail here specifically).
      const [datasetsResult, reservationsResult, syncResult, rawDatasetsResult, draftsResult] =
        await Promise.allSettled([
          api.adminListDatasets({ limit: 1, offset: 0 }),
          api.adminListReservedDatasetIds({ limit: 1, offset: 0 }),
          // check_all=true: this summary card needs every dataset's status,
          // not one interactive dataset_id lookup - the bare call used to
          // 400 every time since the endpoint requires one or the other.
          api.adminCheckDocumentationSync(undefined, true),
          api.adminListRawDatasets({ limit: 1, offset: 0 }),
          api.adminListManifestDrafts({ status: 'pending', limit: 1, offset: 0 }),
        ]);

      if (cancelled) return;

      setStats({
        datasets: datasetsResult.status === 'fulfilled' ? datasetsResult.value.total : null,
        // .total, not .reservations.length - the array is capped by `limit`,
        // so length silently stops growing past that cap while the real
        // count keeps climbing.
        reservedIds: reservationsResult.status === 'fulfilled' ? reservationsResult.value.total : null,
        outdatedDocs: syncResult.status === 'fulfilled' ? syncResult.value.outdated : null,
        rawDatasets: rawDatasetsResult.status === 'fulfilled' ? rawDatasetsResult.value.total : null,
        pendingDrafts: draftsResult.status === 'fulfilled' ? draftsResult.value.total : null,
      });

      const failed = [datasetsResult, reservationsResult, syncResult, rawDatasetsResult, draftsResult].some(
        (r) => r.status === 'rejected'
      );
      if (failed) {
        setError('Some stats could not be loaded - see individual cards below.');
      }

      setLoading(false);
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div class="space-y-6">
      {error ? (
        <div class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{error}</div>
      ) : null}

      <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Datasets</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.datasets ?? '—'}</div>
          <p class="mt-2 text-sm text-slate-600">Published datasets currently visible in the admin catalog.</p>
        </div>

        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Reserved IDs</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.reservedIds ?? '—'}</div>
          <p class="mt-2 text-sm text-slate-600">Held for incoming datasets before metadata or files are uploaded.</p>
        </div>

        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Metadata-Drafts</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.pendingDrafts ?? '—'}</div>
          <p class="mt-2 text-sm text-slate-600">LLM-drafted metadata.yaml awaiting curator review.</p>
        </div>

        <a
          href="/admin/datasets/sync"
          class="block rounded-2xl border border-slate-200 bg-white p-5 shadow-sm transition hover:border-slate-300 hover:shadow-md"
        >
          <div class="text-sm font-medium text-slate-500">Outdated Docs</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.outdatedDocs ?? '—'}</div>
          <p class="mt-2 text-sm text-slate-600">
            {stats.outdatedDocs === null && !loading
              ? 'Could not be checked just now - try refreshing.'
              : 'Datasets whose cached README, manifest, or dictionary may need a refresh - click to see the list.'}
          </p>
        </a>

        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Raw Datasets</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.rawDatasets ?? '—'}</div>
          <p class="mt-2 text-sm text-slate-600">Available raw sources that can be linked into dataset metadata.</p>
        </div>
      </div>

      <div class="grid gap-4 xl:grid-cols-2">
        {actionCards.map((card) => (
          <a key={card.href} href={card.href} class={`rounded-3xl p-6 shadow-sm transition hover:-translate-y-0.5 ${card.tone}`}>
            <div class="text-lg font-semibold">{card.title}</div>
            <p class={`mt-2 text-sm ${card.tone.includes('bg-slate-900') ? 'text-slate-200' : 'text-slate-600'}`}>
              {card.description}
            </p>
            <div class={`mt-5 text-sm font-medium ${card.tone.includes('bg-slate-900') ? 'text-white' : 'text-slate-900'}`}>
              Open
            </div>
          </a>
        ))}
      </div>

      <div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
        <h2 class="text-lg font-semibold text-slate-900">How the new admin flow is organized</h2>
        <div class="mt-4 grid gap-4 lg:grid-cols-3">
          <div class="rounded-2xl bg-slate-50 p-4">
            <div class="text-sm font-semibold uppercase tracking-wide text-slate-500">Create</div>
            <p class="mt-2 text-sm text-slate-600">
              Use a dedicated page for package import, validation findings, metadata fixes, and first-time table upload.
            </p>
          </div>
          <div class="rounded-2xl bg-slate-50 p-4">
            <div class="text-sm font-semibold uppercase tracking-wide text-slate-500">Manage</div>
            <p class="mt-2 text-sm text-slate-600">
              Use the catalog to scan datasets quickly, then jump into a dataset workspace for deeper edits and sharing.
            </p>
          </div>
          <div class="rounded-2xl bg-slate-50 p-4">
            <div class="text-sm font-semibold uppercase tracking-wide text-slate-500">Maintain</div>
            <p class="mt-2 text-sm text-slate-600">
              Keep reservations visible and documentation health separate so operational tasks do not get buried under forms.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
