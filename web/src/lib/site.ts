// Public site name and contact, shared by layouts and components.
export const PLATFORM_NAME = 'Data and AI Commons for One Health';
export const CONTACT_EMAIL = 'onehealth@artpark.in';

// Terms of Use and Privacy Policy pages. They are left out of the site until the
// drafts are approved; the sign-up and download pop-up link to them only once
// the pages exist, so a visitor is never sent to a missing page.
const legalPages = import.meta.glob('../pages/{terms,privacy}.astro', { query: '?raw', import: 'default' });
export const LEGAL_PAGES_PUBLISHED = Object.keys(legalPages).length === 2;
