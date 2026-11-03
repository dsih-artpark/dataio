import type { ComponentChildren } from 'preact';

interface SectionHeaderProps {
  /** Small uppercase label above the title, e.g. "Our approach". */
  eyebrow?: string;
  title: string;
  /** Heading id, for aria-labelledby on the section. */
  id?: string;
  level?: 1 | 2 | 3;
  children?: ComponentChildren;
  /** Link at the right, e.g. "See all datasets →". */
  action?: { label: string; href: string };
}

/** Eyebrow + heading + intro paragraph used by every public page section. */
export default function SectionHeader({
  eyebrow,
  title,
  id,
  level = 2,
  children,
  action,
}: SectionHeaderProps) {
  const Heading = `h${level}` as 'h1' | 'h2' | 'h3';
  const size = level === 1 ? 'text-3xl sm:text-[40px]' : level === 2 ? 'text-2xl sm:text-[28px]' : 'text-xl';
  return (
    <div class="flex flex-wrap items-end justify-between gap-x-6 gap-y-3 mb-7">
      <div class="flex flex-col gap-2 max-w-[680px]">
        {eyebrow && (
          <p class="m-0 text-[13px] font-semibold uppercase tracking-[0.08em] text-primary-700">{eyebrow}</p>
        )}
        <Heading id={id} class={`m-0 ${size} leading-tight font-bold text-gray-900`}>
          {title}
        </Heading>
        {children && <div class="text-base leading-relaxed text-gray-600">{children}</div>}
      </div>
      {action && (
        <a href={action.href} class="text-sm font-semibold text-primary-700 hover:text-primary-900">
          {action.label}
        </a>
      )}
    </div>
  );
}
