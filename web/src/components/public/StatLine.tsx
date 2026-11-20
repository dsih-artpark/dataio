import type { CatalogueStats } from '../../lib/catalogue';

interface StatLineProps {
  stats: Pick<CatalogueStats, 'total_datasets' | 'total_sources' | 'earliest_year'> | null;
  class?: string;
}

/** "100 datasets · 52 sources · records from 1950", from live catalogue counts. */
export default function StatLine({ stats, class: className = '' }: StatLineProps) {
  if (!stats) {
    return (
      <p
        class={`m-0 h-6 w-72 max-w-full rounded bg-gray-100 animate-pulse ${className}`}
        aria-hidden="true"
      />
    );
  }
  const parts = [
    {
      value: stats.total_datasets,
      label: stats.total_datasets === 1 ? 'dataset' : 'datasets',
    },
    {
      value: stats.total_sources,
      label: stats.total_sources === 1 ? 'source' : 'sources',
    },
  ];
  return (
    <p class={`m-0 text-base text-gray-600 ${className}`}>
      {parts.map((part, index) => (
        <span key={part.label}>
          {index > 0 && ' · '}
          <strong class="font-bold text-gray-900">{part.value.toLocaleString('en-IN')}</strong> {part.label}
        </span>
      ))}
      {stats.earliest_year && (
        <span>
          {' · '}records from <strong class="font-bold text-gray-900">{stats.earliest_year}</strong>
        </span>
      )}
    </p>
  );
}
