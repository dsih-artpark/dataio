import { useEffect, useState } from 'preact/hooks';
import type { CatalogueStats } from '../../../lib/catalogue';
import { COMING_SOON_THEME, THEMES, datasetsHref, loadCatalogueStats, themeCounts } from '../../../lib/landing';

const CARD = 'flex-[1_1_260px] flex flex-col gap-1.5 p-[22px] rounded-[14px] bg-white border border-gray-200';

/** "Browse by theme" cards with live dataset counts per category. */
export default function ThemeGrid() {
  const [stats, setStats] = useState<CatalogueStats | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    loadCatalogueStats()
      .then(setStats)
      .catch(() => setFailed(true));
  }, []);

  const counts = themeCounts(stats);
  const loading = !stats && !failed;

  // Known themes first in wireframe order, then any category we have no
  // wording for yet, so a new collection never disappears from the page.
  const known = new Set(THEMES.map((theme) => theme.id));
  const cards = [
    ...THEMES.filter((theme) => failed || (counts.get(theme.id)?.count ?? 0) > 0).map((theme) => ({
      id: theme.id,
      label: theme.label,
      description: theme.description,
      count: counts.get(theme.id)?.count,
    })),
    ...[...counts.entries()]
      .filter(([id, value]) => !known.has(id) && value.count > 0)
      .map(([id, value]) => ({ id, label: value.name, description: null, count: value.count })),
  ];

  if (loading) {
    return (
      <div class="flex flex-wrap gap-4" aria-hidden="true">
        {THEMES.slice(0, 6).map((theme) => (
          <div key={theme.id} class="flex-[1_1_260px] h-[132px] rounded-[14px] bg-gray-100 animate-pulse" />
        ))}
      </div>
    );
  }

  return (
    <ul class="m-0 p-0 list-none flex flex-wrap gap-4">
      {cards.map((card) => (
        <li key={card.id} class="flex-[1_1_260px] flex">
          <a
            href={datasetsHref({ category_id: [card.id] })}
            class={`${CARD} w-full text-gray-900 no-underline hover:border-primary-300 hover:shadow-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500`}
          >
            <span class="text-lg font-bold">{card.label}</span>
            {card.description && <span class="text-sm leading-normal text-gray-600">{card.description}</span>}
            {card.count != null && (
              <span class="mt-2 text-[13px] font-semibold text-primary-700">
                {card.count.toLocaleString('en-IN')} {card.count === 1 ? 'dataset' : 'datasets'}
              </span>
            )}
          </a>
        </li>
      ))}
      <li class="flex-[1_1_260px] flex">
        <div class={`${CARD} w-full border-dashed bg-gray-50`}>
          <span class="text-lg font-bold text-gray-700">{COMING_SOON_THEME.label}</span>
          <span class="text-sm leading-normal text-gray-600">{COMING_SOON_THEME.description}</span>
          <span class="mt-2 text-[13px] font-semibold text-gray-500">Coming soon</span>
        </div>
      </li>
    </ul>
  );
}
