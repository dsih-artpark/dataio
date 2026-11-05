import { useEffect, useMemo, useState } from 'preact/hooks';
import type { CatalogueStats } from '../../../lib/catalogue';
import { STATE_TILES, datasetsHref, loadCatalogueStats } from '../../../lib/landing';

// Tile shades: all-India data only / some state data / 10 or more state datasets.
const TONES = {
  national: 'bg-primary-100 text-primary-900',
  some: 'bg-primary-300 text-primary-950',
  deep: 'bg-primary-700 text-white',
};
const DEEP_FROM = 10;

interface Area {
  regionId: string;
  name: string;
  /** Datasets for this state or a place inside it (all-India ones not included). */
  own: number;
}

// A state's own datasets plus the all-India ones, as the tiles and finder describe.
const stateHref = (regionId: string) => datasetsHref({ region_id: [regionId, 'country_IN'] });

const plural = (n: number, word: string) => `${n.toLocaleString('en-IN')} ${n === 1 ? word : `${word}s`}`;

/** "Data for your area": state tile map plus a state finder, from live coverage counts. */
export default function CoverageMap() {
  const [stats, setStats] = useState<CatalogueStats | null>(null);
  const [failed, setFailed] = useState(false);
  const [query, setQuery] = useState('');

  useEffect(() => {
    loadCatalogueStats()
      .then(setStats)
      .catch(() => setFailed(true));
  }, []);

  const national = stats?.coverage.national ?? 0;
  const ownCounts = useMemo(
    () => new Map((stats?.coverage.states ?? []).map((state) => [state.region_id, state.count])),
    [stats],
  );

  // Every state and UT we know about: the tiles plus any with data but no tile.
  const areas: Area[] = useMemo(() => {
    const list = STATE_TILES.map((tile) => ({
      regionId: tile.regionId,
      name: tile.name,
      own: ownCounts.get(tile.regionId) ?? 0,
    }));
    const tiled = new Set(list.map((area) => area.regionId));
    for (const state of stats?.coverage.states ?? []) {
      if (!tiled.has(state.region_id)) {
        list.push({ regionId: state.region_id, name: state.region_name, own: state.count });
      }
    }
    return list;
  }, [stats, ownCounts]);

  const ranked = [...areas].filter((area) => area.own > 0).sort((a, b) => b.own - a.own);
  const term = query.trim().toLowerCase();
  const matches = term
    ? areas.filter((area) => area.name.toLowerCase().includes(term)).sort((a, b) => b.own - a.own).slice(0, 6)
    : ranked.slice(0, 3);
  const showAllIndia = !term || 'all india'.includes(term) || 'india'.includes(term);

  const tileLabel = (name: string, own: number) =>
    stats
      ? `${name}: ${plural(national + own, 'dataset')} (${national} all-India${own ? ` + ${own} for this state` : ''})`
      : name;

  return (
    <div class="max-w-page mx-auto px-4 sm:px-6 flex flex-wrap gap-10 items-stretch">
      <figure class="flex-[1_1_420px] min-w-0 m-0 p-6 rounded-2xl bg-gray-50 border border-gray-200 flex flex-col gap-4 items-center">
        <ul
          aria-label="States of India, one square each"
          class="m-0 p-0 list-none grid gap-1 w-full max-w-[428px]"
          style={{ gridTemplateColumns: 'repeat(9, minmax(0, 44px))', justifyContent: 'center' }}
        >
          {STATE_TILES.map((tile) => {
            const own = ownCounts.get(tile.regionId) ?? 0;
            const tone = !stats ? 'bg-gray-200 text-gray-500' : own >= DEEP_FROM ? TONES.deep : own > 0 ? TONES.some : TONES.national;
            const label = tileLabel(tile.name, own);
            return (
              <li key={tile.code} class="contents">
                <a
                  href={stateHref(tile.regionId)}
                  title={label}
                  aria-label={label}
                  class={`aspect-square rounded-md flex items-center justify-center text-xs font-bold no-underline hover:ring-2 hover:ring-primary-500 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-1 ${tone}`}
                  style={{ gridColumn: tile.col, gridRow: tile.row }}
                >
                  {tile.code}
                </a>
              </li>
            );
          })}
        </ul>
        {stats && (
          <figcaption class="flex flex-wrap justify-center gap-x-5 gap-y-2 text-[13px] text-gray-700">
            <span class="flex items-center gap-1.5">
              <span aria-hidden="true" class={`w-3.5 h-3.5 rounded-sm ${TONES.national}`} />
              {plural(national, 'all-India dataset')}
            </span>
            <span class="flex items-center gap-1.5">
              <span aria-hidden="true" class={`w-3.5 h-3.5 rounded-sm ${TONES.some}`} />
              +1–{DEEP_FROM - 1} state datasets
            </span>
            <span class="flex items-center gap-1.5">
              <span aria-hidden="true" class={`w-3.5 h-3.5 rounded-sm ${TONES.deep}`} />+{DEEP_FROM} or more state datasets
            </span>
          </figcaption>
        )}
        <p class="m-0 text-xs text-gray-600 text-center">
          Simplified tile map: one square per state, not drawn to scale. Some union territories not shown.
        </p>
      </figure>

      <div class="flex-[1_1_380px] min-w-0 flex flex-col gap-4">
        <p class="m-0 text-[13px] font-semibold uppercase tracking-[0.08em] text-primary-700">Data for your area</p>
        <h2 id="coverage-heading" class="m-0 text-2xl sm:text-[32px] leading-tight font-bold text-gray-900">
          Is there data for your state?
        </h2>
        <p class="m-0 text-base leading-relaxed text-gray-600">
          {stats ? (
            <>
              {plural(national, 'dataset')} cover the whole country, many broken down by state or district.
              {ranked[0] && ` ${ranked[0].name} has the deepest coverage, with ${plural(ranked[0].own, 'dataset')} of its own.`}
            </>
          ) : failed ? (
            'Many datasets cover the whole country, broken down by state or district.'
          ) : (
            <span class="block h-12 rounded bg-gray-100 animate-pulse" aria-hidden="true" />
          )}
        </p>

        <label for="area-search" class="mt-1 text-sm font-semibold text-gray-700">
          Find a state
        </label>
        <input
          id="area-search"
          type="search"
          autoComplete="off"
          aria-controls="area-results"
          aria-describedby="area-results-count"
          placeholder="e.g. Karnataka, Odisha"
          value={query}
          onInput={(event) => setQuery((event.target as HTMLInputElement).value)}
          class="w-full min-h-[44px] px-3 py-3 border border-gray-300 rounded-lg text-base focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
        />
        <p id="area-results-count" class="sr-only" aria-live="polite">
          {term ? `${matches.length + (showAllIndia ? 1 : 0)} matches` : ''}
        </p>
        <ul id="area-results" class="m-0 mt-1 p-0 list-none flex flex-col border-t border-gray-200">
          {showAllIndia && (
            <AreaRow
              href={datasetsHref({ region_id: ['country_IN'] })}
              name="All India"
              note="National datasets, often by state or district"
              count={stats ? plural(national, 'dataset') : null}
            />
          )}
          {matches.map((area) => (
            <AreaRow
              key={area.regionId}
              href={stateHref(area.regionId)}
              name={area.name}
              note={area.own ? 'State and district datasets, plus all-India ones' : 'All-India datasets only, so far'}
              count={stats ? (area.own ? `+${plural(area.own, 'dataset')}` : 'All-India only') : null}
            />
          ))}
          {term && !matches.length && !showAllIndia && (
            <li class="py-3 px-1 text-[15px] text-gray-600 border-b border-gray-200">
              No state matches “{query.trim()}”.
            </li>
          )}
        </ul>
        <a href="/datasets" class="text-[15px] font-semibold text-primary-700 hover:text-primary-900">
          See all datasets →
        </a>
      </div>
    </div>
  );
}

function AreaRow({ href, name, note, count }: { href: string; name: string; note: string; count: string | null }) {
  return (
    <li class="border-b border-gray-200">
      <a
        href={href}
        class="flex justify-between items-center gap-3 py-3 px-1 min-h-[44px] text-gray-900 no-underline text-[15px] hover:bg-gray-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
      >
        <span class="flex flex-col gap-0.5">
          <span class="font-semibold">{name}</span>
          <span class="text-[13px] text-gray-600">{note}</span>
        </span>
        {count && <span class="text-sm text-gray-600 whitespace-nowrap">{count} →</span>}
      </a>
    </li>
  );
}
