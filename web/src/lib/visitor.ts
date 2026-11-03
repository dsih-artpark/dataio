// The dataio_visitor cookie: a random ID the API hands out after a signed-out
// visitor gives their email and accepts the terms for a download. With it,
// later downloads from this browser don't show the pop-up again. Set only with
// the visitor's agreement (see the Privacy Policy); delete it to be asked again.

const COOKIE = 'dataio_visitor';
const MAX_AGE_SECONDS = 365 * 24 * 60 * 60; // about 12 months
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function getVisitorId(): string | null {
  const match = document.cookie.split('; ').find((part) => part.startsWith(`${COOKIE}=`));
  const value = match ? decodeURIComponent(match.slice(COOKIE.length + 1)) : '';
  return UUID.test(value) ? value : null;
}

export function saveVisitorId(id: string): void {
  if (!UUID.test(id)) return;
  const secure = window.location.protocol === 'https:' ? '; Secure' : '';
  document.cookie = `${COOKIE}=${encodeURIComponent(id)}; Max-Age=${MAX_AGE_SECONDS}; Path=/; SameSite=Lax${secure}`;
}
