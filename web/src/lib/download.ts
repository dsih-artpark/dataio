// Dataset download from the public pages: fetch the presigned table links,
// zip them with the README (and manifest when signed in) in the browser, and
// save the zip as <last four of ID>-<title>/ with one file per table.
import JSZip from 'jszip';
import { api, ApiRequestError } from './api';
import { getVisitorId, saveVisitorId } from './visitor';

/** A signed-out download needs the email + terms pop-up first. */
export class ConsentRequiredError extends Error {
  constructor() {
    super('Enter your email and accept the terms to download.');
    this.name = 'ConsentRequiredError';
  }
}

/**
 * Links for an Open dataset without signing in. With `email` (from the pop-up)
 * the visitor accepts the terms now; without it, the visitor cookie from an
 * earlier download stands in. Throws ConsentRequiredError when neither works.
 */
async function openDownloadTables(dsId: string, email?: string) {
  const visitorId = getVisitorId() ?? undefined;
  if (!email && !visitorId) throw new ConsentRequiredError();
  try {
    const data = await api.getOpenDownloadUrls(
      dsId,
      email ? { email, accept_terms: true, visitor_id: visitorId } : { visitor_id: visitorId },
    );
    saveVisitorId(data.visitor_id);
    return data.tables;
  } catch (err) {
    if (err instanceof ApiRequestError && err.statusCode === 428) throw new ConsentRequiredError();
    throw err;
  }
}

/** "0042-dengue-cases-by-district" from CS0001DS0042 + title. */
export function datasetFileStem(dsId: string, title: string): string {
  const lastFour = dsId.slice(-4).padStart(4, '0');
  const safeName = title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, 50);
  return `${lastFour}-${safeName}`;
}

/**
 * open: Open download dataset, no sign-in (README from the public page);
 *   `email` is set when the visitor has just filled in the pop-up.
 * signed-in: a dataset the user may download (README and manifest come with the links).
 */
export async function downloadDatasetZip(
  dsId: string,
  title: string,
  mode: 'open' | 'signed-in',
  readmeMd?: string | null,
  email?: string,
): Promise<void> {
  let tables: { table_name: string; download_url: string }[];
  let manifest: Record<string, unknown> | null = null;
  if (mode === 'signed-in') {
    const data = await api.getDatasetDownloadUrls(dsId);
    tables = data.tables;
    readmeMd = data.readme_md ?? readmeMd;
    manifest = data.manifest_json;
  } else {
    tables = await openDownloadTables(dsId, email);
  }
  const data = { tables };
  if (!data.tables.length) {
    throw new Error('No data files are available for this dataset yet. Please try again later.');
  }

  const stem = datasetFileStem(dsId, title);
  const zip = new JSZip();
  const folder = zip.folder(stem);
  if (!folder) throw new Error('Couldn’t prepare the download.');
  if (readmeMd) folder.file('README.md', readmeMd);
  if (manifest) folder.file('metadata.json', JSON.stringify(manifest, null, 2));

  const results = await Promise.allSettled(
    data.tables.map(async (table) => {
      const response = await fetch(table.download_url);
      if (!response.ok) throw new Error(`${table.table_name}: HTTP ${response.status}`);
      const blob = await response.blob();
      const isParquet =
        response.headers.get('content-type')?.includes('parquet') || table.download_url.includes('.parquet');
      folder.file(`${table.table_name}${isParquet ? '.parquet' : '.csv'}`, blob);
    }),
  );
  const failures = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
  if (failures.length === data.tables.length) {
    throw new Error('The files couldn’t be downloaded. The links may have expired: please try again.');
  }

  const blob = await zip.generateAsync({ type: 'blob' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `${stem}.zip`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}
