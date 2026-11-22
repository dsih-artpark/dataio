import type { ChangeKind } from './types';

/** The 10 dataset change kinds, in plain words (DIO-53). */
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

/** "2026-11-20" as "20 Nov 2026", without a time-zone shift. */
export function formatChangeDate(isoDate: string): string {
  return new Date(`${isoDate}T00:00:00`).toLocaleDateString('en-IN', { dateStyle: 'medium' });
}
