// How /datasets shows its results: tiles (a grid, the default) or cards (one
// wide row per dataset). The choice is kept in this browser only; if storage is
// blocked the page simply uses tiles.

export type DatasetView = 'tile' | 'card';

const STORAGE_KEY = 'dataio.datasetView';

export function readDatasetView(): DatasetView {
  try {
    return window.localStorage.getItem(STORAGE_KEY) === 'card' ? 'card' : 'tile';
  } catch {
    return 'tile';
  }
}

export function saveDatasetView(view: DatasetView): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, view);
  } catch {
    // Storage blocked (private window, site data off): the choice lasts until the page is left.
  }
}
