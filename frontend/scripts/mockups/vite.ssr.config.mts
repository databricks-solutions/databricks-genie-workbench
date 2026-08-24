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
  resolve: { alias: { "@": path.resolve(root, "src") } },
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
