import { CHANGE_KIND_LABELS, formatChangeDate } from '../../../lib/changelog';
import { datasetPath } from '../../../lib/catalogue';
import type { PublicDatasetChange } from '../../../lib/types';

const th = 'px-3 py-2 font-semibold text-gray-700 text-left whitespace-nowrap';
const td = 'px-3 py-2 text-gray-700 align-top';
const code = 'font-mono text-[13px] break-all';

/** Published changelog entries (DIO-53), newest first. Used by a dataset's
 * "Version history" and by the all-changes page (`showDataset`). */
export default function DatasetChangeEntries(props: { entries: PublicDatasetChange[]; showDataset?: boolean }) {
  return (
    <ol class="m-0 flex list-none flex-col gap-4 p-0">
      {props.entries.map((entry) => (
        <li key={entry.id} class="flex flex-col gap-2 border-l-2 border-gray-200 pl-4">
          <div class="flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
            <time dateTime={entry.date} class="font-semibold text-gray-900">
              {formatChangeDate(entry.date)}
            </time>
            {props.showDataset ? (
              <a href={datasetPath(entry.dataset_id)} class="font-semibold text-primary-700 hover:text-primary-900">
                {entry.dataset_title || entry.dataset_id}
              </a>
            ) : null}
            <span class="text-gray-500">{entry.type === 'data' ? 'Data change' : 'Metadata change'}</span>
            {entry.breaking ? (
              <span class="rounded-full bg-red-50 px-2 py-0.5 text-xs font-semibold text-red-800 ring-1 ring-red-200">
                Breaking change
              </span>
            ) : null}
          </div>
          <p class="m-0 text-[15px] text-gray-900">{entry.summary}</p>
          <p class="m-0 text-sm text-gray-600">
            <span class="font-semibold text-gray-700">Why: </span>
            {entry.reason}
          </p>
          {entry.changes.length > 0 ? (
            <details class="text-sm">
              <summary class="cursor-pointer font-semibold text-primary-700 hover:text-primary-900">
                Details ({entry.changes.length} {entry.changes.length === 1 ? 'change' : 'changes'})
              </summary>
              <div class="mt-2 overflow-x-auto rounded-lg border border-gray-200">
                <table class="w-full border-collapse text-sm">
                  <thead class="bg-gray-50">
                    <tr>
                      <th scope="col" class={th}>Change</th>
                      <th scope="col" class={th}>Table</th>
                      <th scope="col" class={th}>Column</th>
                      {entry.restricted ? null : (
                        <>
                          <th scope="col" class={th}>Old</th>
                          <th scope="col" class={th}>New</th>
                          <th scope="col" class={th}>Note</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {entry.changes.map((row, index) => (
                      <tr key={index} class="border-t border-gray-100">
                        <td class={td}>{CHANGE_KIND_LABELS[row.kind] ?? row.kind}</td>
                        <td class={`${td} ${code}`}>{row.table}</td>
                        <td class={`${td} ${code}`}>{row.column || '—'}</td>
                        {entry.restricted ? null : (
                          <>
                            <td class={`${td} ${code}`}>{row.old ?? '—'}</td>
                            <td class={`${td} ${code}`}>{row.new ?? '—'}</td>
                            <td class={td}>{row.note || '—'}</td>
                          </>
                        )}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {entry.restricted ? (
                <p class="m-0 mt-2 text-xs text-gray-500">Old and new values are not shown for Restricted datasets.</p>
              ) : null}
            </details>
          ) : null}
        </li>
      ))}
    </ol>
  );
}
