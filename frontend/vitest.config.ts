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
  // Use the automatic JSX runtime (matching the app's @vitejs/plugin-react
  // transform) so .tsx test files can render components without importing React.
  esbuild: {
    jsx: 'automatic',
  },
  test: {
    environment: 'node',
    include: ['src/**/*.{test,spec}.{ts,tsx}'],
    globals: false,
  },
})
