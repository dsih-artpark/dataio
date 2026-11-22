import { useId } from 'preact/hooks';
import { CHANGE_KIND_LABELS } from '../../lib/changelog';
import type { ChangeKind, ChangeRow, ChangelogDraft } from '../../lib/types';

export { CHANGE_KIND_LABELS };

/** Same limits as the server (dataset_changes.py). */
export const CHANGELOG_LIMITS = { summary: 300, reason: 2000, cell: 500, rows: 200 } as const;

export function emptyChangelogDraft(): ChangelogDraft {
  return { type: 'data', breaking: false, summary: '', reason: '', changes: [] };
}

/** Summary and reason are required, and every row needs a table name
 * (the server rejects these too). */
export function isChangelogComplete(draft: ChangelogDraft): boolean {
  return (
    draft.summary.trim() !== '' &&
    draft.reason.trim() !== '' &&
    draft.changes.every((row) => row.table.trim() !== '')
  );
}

/** Keeps the admin's rows and adds suggested ones that aren't there yet. */
export function mergeSuggestedRows(rows: ChangeRow[], suggested: ChangeRow[]): ChangeRow[] {
  const has = (s: ChangeRow) =>
    rows.some((row) => row.kind === s.kind && row.table === s.table && (row.column ?? null) === (s.column ?? null));
  // Never more rows than the server accepts.
  return [...rows, ...suggested.filter((s) => !has(s))].slice(0, CHANGELOG_LIMITS.rows);
}

const input = 'block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm';
const cellInput = 'block w-full min-w-[7rem] rounded-lg border border-slate-300 px-2 py-1 text-xs';
const mono = `${cellInput} font-mono`;

/** Text fields: empty means "not set" (null), as the server stores it. */
const orNull = (value: string) => (value === '' ? null : value);

export function ChangelogEntryForm(props: {
  value: ChangelogDraft;
  onChange: (next: ChangelogDraft) => void;
  showDate?: boolean;
  /** The dataset is Restricted: names are public, values and notes are not. */
  restricted?: boolean;
}) {
  const { value, onChange } = props;
  // Each form gets its own radio group, so two open forms can't share one.
  const typeGroup = useId();
  const set = (patch: Partial<ChangelogDraft>) => onChange({ ...value, ...patch });
  const setRow = (index: number, patch: Partial<ChangeRow>) =>
    set({ changes: value.changes.map((row, i) => (i === index ? { ...row, ...patch } : row)) });
  const field = (e: Event) => (e.currentTarget as HTMLInputElement).value;

  return (
    <div class="space-y-4">
      <label class="block space-y-1 text-sm">
        <span class="font-medium text-slate-800">Summary</span>
        <input
          type="text"
          value={value.summary}
          onInput={(e) => set({ summary: field(e) })}
          placeholder="One sentence: what changed"
          class={input}
          maxLength={CHANGELOG_LIMITS.summary}
          required
        />
      </label>
      <label class="block space-y-1 text-sm">
        <span class="font-medium text-slate-800">Why</span>
        <textarea
          value={value.reason}
          onInput={(e) => set({ reason: field(e) })}
          placeholder="Why the change was made"
          rows={2}
          class={input}
          maxLength={CHANGELOG_LIMITS.reason}
          required
        />
      </label>
      <div class="flex flex-wrap items-center gap-6 text-sm">
        <fieldset class="flex items-center gap-4">
          <legend class="sr-only">Type of change</legend>
          {(['data', 'metadata'] as const).map((type) => (
            <label key={type} class="flex items-center gap-2">
              <input type="radio" name={typeGroup} checked={value.type === type} onChange={() => set({ type })} />
              {type === 'data' ? 'Data change' : 'Metadata change'}
            </label>
          ))}
        </fieldset>
        <label class="flex items-center gap-2">
          <input
            type="checkbox"
            checked={value.breaking}
            onChange={(e) => set({ breaking: (e.currentTarget as HTMLInputElement).checked })}
          />
          <span>
            Breaking change <span class="text-slate-500">(users may need to change their code)</span>
          </span>
        </label>
        {props.showDate ? (
          <label class="flex items-center gap-2">
            Date
            <input
              type="date"
              value={value.date ?? ''}
              onInput={(e) => set({ date: orNull(field(e)) })}
              class="rounded-lg border border-slate-300 px-2 py-1 text-sm"
            />
          </label>
        ) : null}
      </div>

      <div class="space-y-2">
        <div class="flex items-center justify-between">
          <span class="text-sm font-medium text-slate-800">
            Changes <span class="font-normal text-slate-500">(optional)</span>
          </span>
          <button
            type="button"
            onClick={() => set({ changes: [...value.changes, { kind: 'column_renamed', table: '' }] })}
            disabled={value.changes.length >= CHANGELOG_LIMITS.rows}
            class="rounded-lg border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50"
          >
            Add row
          </button>
        </div>
        {props.restricted ? (
          <p class="text-xs text-amber-800">
            This dataset is Restricted. Table and column names in this entry will be public; old and new values and notes
            are hidden.
          </p>
        ) : null}
        {value.changes.length >= CHANGELOG_LIMITS.rows ? (
          <p class="text-xs text-slate-500">An entry can have at most {CHANGELOG_LIMITS.rows} rows.</p>
        ) : null}
        {value.changes.length === 0 ? (
          <p class="text-xs text-slate-500">No rows. Add one for each renamed, added or removed table or column, unit change or recode.</p>
        ) : (
          <div class="overflow-x-auto">
            <table class="w-full text-left text-xs">
              <thead class="text-slate-500">
                <tr>
                  <th class="py-1 pr-2 font-medium">Change</th>
                  <th class="py-1 pr-2 font-medium">Table</th>
                  <th class="py-1 pr-2 font-medium">Column</th>
                  <th class="py-1 pr-2 font-medium">Old</th>
                  <th class="py-1 pr-2 font-medium">New</th>
                  <th class="py-1 pr-2 font-medium">Note</th>
                  <th class="py-1" />
                </tr>
              </thead>
              <tbody>
                {value.changes.map((row, index) => (
                  <tr key={index} class="align-top">
                    <td class="py-1 pr-2">
                      <select
                        value={row.kind}
                        onChange={(e) => setRow(index, { kind: (e.currentTarget as HTMLSelectElement).value as ChangeKind })}
                        class={cellInput}
                        aria-label={`Row ${index + 1}: change`}
                      >
                        {(Object.keys(CHANGE_KIND_LABELS) as ChangeKind[]).map((kind) => (
                          <option key={kind} value={kind}>
                            {CHANGE_KIND_LABELS[kind]}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.table} onInput={(e) => setRow(index, { table: field(e) })} class={mono} maxLength={CHANGELOG_LIMITS.cell} aria-label={`Row ${index + 1}: table`} />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.column ?? ''} onInput={(e) => setRow(index, { column: orNull(field(e)) })} class={mono} maxLength={CHANGELOG_LIMITS.cell} aria-label={`Row ${index + 1}: column`} />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.old ?? ''} onInput={(e) => setRow(index, { old: orNull(field(e)) })} class={mono} maxLength={CHANGELOG_LIMITS.cell} aria-label={`Row ${index + 1}: old`} />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.new ?? ''} onInput={(e) => setRow(index, { new: orNull(field(e)) })} class={mono} maxLength={CHANGELOG_LIMITS.cell} aria-label={`Row ${index + 1}: new`} />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.note ?? ''} onInput={(e) => setRow(index, { note: orNull(field(e)) })} class={cellInput} maxLength={CHANGELOG_LIMITS.cell} aria-label={`Row ${index + 1}: note`} />
                    </td>
                    <td class="py-1">
                      <button
                        type="button"
                        onClick={() => set({ changes: value.changes.filter((_, i) => i !== index) })}
                        class="rounded-lg px-2 py-1 text-xs text-red-700 hover:bg-red-50"
                        aria-label={`Remove row ${index + 1}`}
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
