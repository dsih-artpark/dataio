import { useEffect, useState } from 'preact/hooks';
import type { CatalogueStats } from '../../../lib/catalogue';
import { COMING_SOON_THEME, THEMES, datasetsHref, loadCatalogueStats, themeCounts } from '../../../lib/landing';

const CARD = 'flex-[1_1_260px] flex flex-col gap-2 p-[22px] rounded-[14px] border border-gray-200 bg-gradient-to-br to-white';

/** Colours and icon for categories we have no wording for yet. */
const DEFAULT_LOOK = {
  tile: 'from-gray-50',
  accent: 'bg-gray-100 text-gray-700',
  bar: 'bg-gray-300',
  icon: '<path d="M4 6h16M4 12h16M4 18h10"/>',
};

interface TileLook {
  id: string;
  accent: string;
  icon: string;
}

/** Icon square on the left, theme code under it; the arrow sits top right. */
function TileHead({ look, arrow }: { look: TileLook; arrow: boolean }) {
  return (
    <>
      <span class="flex justify-between items-start">
        <span
          class={`inline-flex items-center justify-center w-10 h-10 rounded-[10px] ${look.accent}`}
          aria-hidden="true"
        >
          <svg
            class="w-5 h-5"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
            dangerouslySetInnerHTML={{ __html: look.icon }}
          />
        </span>
        {arrow && (
          <span
            class="inline-flex items-center justify-center w-7 h-7 rounded-full border border-gray-200 bg-white text-gray-500 text-sm"
            aria-hidden="true"
          >
            ↗
          </span>
        )}
      </span>
      <span class={`self-start mt-2 px-1.5 py-0.5 rounded text-[11px] font-semibold tracking-[0.08em] ${look.accent}`}>
        {look.id}
      </span>
    </>
  );
}

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
      ...theme,
      count: counts.get(theme.id)?.count,
    })),
    ...[...counts.entries()]
      .filter(([id, value]) => !known.has(id) && value.count > 0)
      .map(([id, value]) => ({ ...DEFAULT_LOOK, id, label: value.name, description: null, count: value.count })),
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
            class={`${CARD} ${card.tile} w-full text-gray-900 no-underline hover:border-primary-300 hover:shadow-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500`}
          >
            <TileHead look={card} arrow />
            <span class="text-lg font-bold">{card.label}</span>
            {card.description && <span class="text-sm leading-normal text-gray-600">{card.description}</span>}
            <span class="mt-auto pt-3 flex items-center justify-between gap-3">
              {card.count != null && (
                <span class="text-[13px] font-semibold text-gray-900">
                  {card.count.toLocaleString('en-IN')} {card.count === 1 ? 'dataset' : 'datasets'}
                </span>
              )}
              <span class={`ml-auto w-10 h-0.5 rounded-full ${card.bar}`} aria-hidden="true" />
            </span>
          </a>
        </li>
      ))}
      <li class="flex-[1_1_260px] flex">
        <div class={`${CARD} ${COMING_SOON_THEME.tile} w-full border-dashed`}>
          <TileHead look={{ ...COMING_SOON_THEME, id: 'ES' }} arrow={false} />
          <span class="text-lg font-bold text-gray-700">{COMING_SOON_THEME.label}</span>
          <span class="text-sm leading-normal text-gray-600">{COMING_SOON_THEME.description}</span>
          <span class="mt-auto pt-3 text-[13px] font-semibold text-gray-500">Coming soon</span>
        </div>
      </li>
    </ul>
  );
}
