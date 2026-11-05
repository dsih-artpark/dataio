import { useEffect, useState } from 'preact/hooks';
import StatLine from '../StatLine';
import type { CatalogueStats } from '../../../lib/catalogue';
import { loadCatalogueStats } from '../../../lib/landing';

/** Live "N datasets · N sources · records from YYYY" under the hero search. */
export default function HeroStats() {
  const [stats, setStats] = useState<CatalogueStats | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    loadCatalogueStats()
      .then(setStats)
      .catch(() => setFailed(true));
  }, []);

  // No numbers is better than wrong ones: leave the line out if stats fail.
  if (failed) return null;
  return <StatLine stats={stats} class="mt-5 text-[15px]" />;
}
