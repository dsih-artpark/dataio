import type { ChangeKind, ChangeRow, ChangelogDraft } from '../../lib/types';

/** The 10 change kinds, in plain words for the dropdown. */
export const CHANGE_KIND_LABELS: Record<ChangeKind, string> = {
  table_added: 'Table added',
  table_removed: 'Table removed',
  table_renamed: 'Table renamed',
  column_added: 'Column added',
  column_removed: 'Column removed',
  column_renamed: 'Column renamed',
  unit_changed: 'Unit changed',
  values_recoded: 'Values recoded',
  rows_changed: 'Rows changed',
  metadata_updated: 'Metadata updated',
};

export function emptyChangelogDraft(): ChangelogDraft {
  return { type: 'data', breaking: false, summary: '', reason: '', changes: [] };
}

/** Summary and reason are required (the server rejects blank ones too). */
export function isChangelogComplete(draft: ChangelogDraft): boolean {
  return draft.summary.trim() !== '' && draft.reason.trim() !== '';
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
}) {
  const { value, onChange } = props;
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
          required
        />
      </label>
      <div class="flex flex-wrap items-center gap-6 text-sm">
        <fieldset class="flex items-center gap-4">
          <legend class="sr-only">Type of change</legend>
          {(['data', 'metadata'] as const).map((type) => (
            <label key={type} class="flex items-center gap-2">
              <input type="radio" name="changelog-type" checked={value.type === type} onChange={() => set({ type })} />
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
            class="rounded-lg border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            Add row
          </button>
        </div>
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
                        aria-label="Change"
                      >
                        {(Object.keys(CHANGE_KIND_LABELS) as ChangeKind[]).map((kind) => (
                          <option key={kind} value={kind}>
                            {CHANGE_KIND_LABELS[kind]}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.table} onInput={(e) => setRow(index, { table: field(e) })} class={mono} aria-label="Table" />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.column ?? ''} onInput={(e) => setRow(index, { column: orNull(field(e)) })} class={mono} aria-label="Column" />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.old ?? ''} onInput={(e) => setRow(index, { old: orNull(field(e)) })} class={mono} aria-label="Old" />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.new ?? ''} onInput={(e) => setRow(index, { new: orNull(field(e)) })} class={mono} aria-label="New" />
                    </td>
                    <td class="py-1 pr-2">
                      <input value={row.note ?? ''} onInput={(e) => setRow(index, { note: orNull(field(e)) })} class={cellInput} aria-label="Note" />
                    </td>
                    <td class="py-1">
                      <button
                        type="button"
                        onClick={() => set({ changes: value.changes.filter((_, i) => i !== index) })}
                        class="rounded-lg px-2 py-1 text-xs text-red-700 hover:bg-red-50"
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
