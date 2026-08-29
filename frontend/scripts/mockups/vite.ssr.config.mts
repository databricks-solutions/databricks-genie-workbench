// MV-advisor mockups — REVIEW SCAFFOLD (see src/.../mockups/mvMockData.ts).
// Bundles emit.tsx (imports .tsx frames + react-dom/server) into a Node-runnable
// SSR module at scripts/mockups/.dist/emit.mjs. Run it with node afterwards.
// Deleted with the mockups.
import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import path from "node:path"

const root = path.resolve(__dirname, "../..") // frontend/

export default defineConfig({
  root,
  configFile: false,
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(root, "src") },
    // Prompt 15.8 fidelity frames pull the production cards into the SSR bundle;
    // those use forwardRef/memo (lucide icons, ui primitives). A second React
    // copy makes their `$$typeof` unrecognizable and renderToStaticMarkup throws
    // "Element type is invalid … got: object". Dedupe pins one React instance.
    dedupe: ["react", "react-dom"],
  },
  // Bundle first-party + npm UI deps (icons, cva) so they share the deduped
  // React; only Node builtins stay external.
  ssr: { noExternal: true },
  build: {
    ssr: true,
    outDir: path.resolve(root, "scripts/mockups/.dist"),
    emptyOutDir: true,
    rollupOptions: {
      input: path.resolve(root, "scripts/mockups/emit.tsx"),
      output: { entryFileNames: "emit.mjs", format: "es" },
    },
  },
})
