// Terms of Use and Privacy Policy pages: rendered at build time from the
// Markdown drafts in docs/legal/ at the repository root, so the team reviews
// and edits one copy. Server-side only (Astro frontmatter).
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { marked } from 'marked';

/** docs/legal/<file> as HTML. The build runs from web/, so the repo root is one level up. */
export function legalDocHtml(file: 'terms-of-use.md' | 'privacy-policy.md'): string {
  const source = path.resolve(process.cwd(), '..', 'docs', 'legal', file);
  const html = marked.parse(readFileSync(source, 'utf-8'), { async: false }) as string;
  // The drafts link to each other by file name; on the site they are pages.
  return html.replaceAll('href="privacy-policy.md"', 'href="/privacy"').replaceAll('href="terms-of-use.md"', 'href="/terms"');
}
