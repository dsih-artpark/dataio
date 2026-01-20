import { defineConfig } from 'astro/config';
import preact from '@astrojs/preact';
import tailwind from '@astrojs/tailwind';

// https://astro.build/config
export default defineConfig({
  integrations: [
    preact({ compat: true }),
    tailwind(),
  ],
  output: 'static',
  server: {
    port: 3000,
  },
  vite: {
    ssr: {
      noExternal: ['@simplewebauthn/browser'],
    },
  },
});
