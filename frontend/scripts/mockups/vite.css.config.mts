// MV-advisor mockups — REVIEW SCAFFOLD (see src/.../mockups/mvMockData.ts).
// Compiles index.css -> scripts/mockups/.dist-css/mockups.css via the SAME
// @tailwindcss/vite pipeline production uses (offline, no CDN). emit.tsx copies
// the result into docs/design/mockups/. Deleted with the mockups.
import { defineConfig } from "vite"
import tailwindcss from "@tailwindcss/vite"
import path from "node:path"

const root = path.resolve(__dirname, "../..") // frontend/

export default defineConfig({
  root,
  configFile: false,
  plugins: [tailwindcss()],
  resolve: { alias: { "@": path.resolve(root, "src") } },
  build: {
    outDir: path.resolve(root, "scripts/mockups/.dist-css"),
    emptyOutDir: true,
    cssMinify: false,
    rollupOptions: {
      input: path.resolve(root, "scripts/mockups/css-entry.ts"),
      output: {
        assetFileNames: "mockups.css",
        entryFileNames: "css-entry.js",
      },
    },
  },
})
