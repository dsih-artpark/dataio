import { useMemo } from 'preact/hooks';

import DatasetAdminManager from './DatasetAdminManager';

export default function AdminDatasetWorkspace() {
  const datasetId = useMemo(() => {
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('dataset') ?? '';
  }, []);

  return (
    <div class="space-y-4">
      {!datasetId ? (
        <div class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          No dataset selected yet. Open a dataset from the catalog to load its full workspace.
        </div>
      ) : null}
      <DatasetAdminManager view="detail" datasetId={datasetId} />
    </div>
  );
}
