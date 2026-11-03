import { ACCESS_LABELS, type CatalogueAccessLevel } from '../../lib/catalogue';

const STYLES: Record<CatalogueAccessLevel, string> = {
  DOWNLOAD: 'bg-green-100 text-green-800',
  VIEW: 'bg-blue-100 text-blue-800',
  NONE: 'bg-gray-100 text-gray-700',
};

interface AccessBadgeProps {
  level: CatalogueAccessLevel | string | null | undefined;
  class?: string;
}

/** "Open download" / "View only" / "Restricted" pill. */
export default function AccessBadge({ level, class: className = '' }: AccessBadgeProps) {
  const key = (level && level in ACCESS_LABELS ? level : 'NONE') as CatalogueAccessLevel;
  return (
    <span
      class={`inline-flex items-center self-start whitespace-nowrap px-2.5 py-1 rounded-full text-[13px] font-semibold ${STYLES[key]} ${className}`}
    >
      {ACCESS_LABELS[key]}
    </span>
  );
}
