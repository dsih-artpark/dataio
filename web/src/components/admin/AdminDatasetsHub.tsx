import { useEffect, useState } from 'preact/hooks';

import { api } from '../../lib/api';

type HubStats = {
  datasets: number;
  reservedIds: number;
  outdatedDocs: number;
  rawDatasets: number;
};

const defaultStats: HubStats = {
  datasets: 0,
  reservedIds: 0,
  outdatedDocs: 0,
  rawDatasets: 0,
};

const actionCards = [
  {
    title: 'Import Dataset Package',
    description: 'Validate info.yml and metadata.yml, review fixes, then upload matching CSV tables.',
    href: '/admin/datasets/new',
    tone: 'bg-slate-900 text-white',
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
      try {
        const [datasetsResponse, reservationsResponse, syncResponse, rawDatasetsResponse] = await Promise.all([
          api.adminListDatasets({ limit: 1, offset: 0 }),
          api.adminListReservedDatasetIds({ limit: 100, offset: 0 }),
          api.adminCheckDocumentationSync(),
          api.adminListRawDatasets({ limit: 1, offset: 0 }),
        ]);

        if (cancelled) return;
        setStats({
          datasets: datasetsResponse.total,
          reservedIds: reservationsResponse.reservations.length,
          outdatedDocs: syncResponse.outdated,
          rawDatasets: rawDatasetsResponse.total,
        });
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : 'Failed to load dataset admin summary');
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
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

      <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Datasets</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.datasets}</div>
          <p class="mt-2 text-sm text-slate-600">Published datasets currently visible in the admin catalog.</p>
        </div>

        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Reserved IDs</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.reservedIds}</div>
          <p class="mt-2 text-sm text-slate-600">Held for incoming datasets before metadata or files are uploaded.</p>
        </div>

        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Outdated Docs</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.outdatedDocs}</div>
          <p class="mt-2 text-sm text-slate-600">Datasets whose cached README, manifest, or dictionary may need a refresh.</p>
        </div>

        <div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div class="text-sm font-medium text-slate-500">Raw Datasets</div>
          <div class="mt-2 text-3xl font-semibold text-slate-900">{loading ? '…' : stats.rawDatasets}</div>
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
