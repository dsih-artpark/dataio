interface MetaChipProps {
  label: string;
  value: string | null | undefined;
  /** Shown when value is empty; the chip is left out if this is unset too. */
  fallback?: string;
}

/** "Where: Karnataka" style metadata chip on cards and dataset pages. */
export default function MetaChip({ label, value, fallback }: MetaChipProps) {
  const shown = value || fallback;
  if (!shown) return null;
  return (
    <span class="inline-block max-w-full px-2.5 py-1 rounded-md bg-gray-100 text-[13px] leading-snug text-gray-700">
      <span class="text-gray-600">{label}:</span>{' '}
      <span class={value ? '' : 'italic'}>{shown}</span>
    </span>
  );
}
