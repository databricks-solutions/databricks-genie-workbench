import { defineConfig } from 'vitest/config'
import path from 'path'

// Vitest-specific config. Deliberately separate from vite.config.ts so that
// the Tailwind v4 plugin (which touches Vite's asset pipeline) and the dev
// server proxy stay out of the test runtime. Only the `@` alias is shared.
export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  test: {
    environment: 'node',
    include: ['src/**/*.{test,spec}.{ts,tsx}'],
    globals: false,
  },
})
