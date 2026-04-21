import { defineConfig } from "vitest/config";
import path from "node:path";

// Minimal Vitest config scoped to the UI lib.
// Path aliases mirror the `@/` root used by IterationExplorer.tsx so tests
// can import from "@/lib/exclusions" just like the component does.
export default defineConfig({
  resolve: {
    alias: {
      "@": path.resolve(
        __dirname,
        "src/genie_space_optimizer/ui",
      ),
    },
  },
  test: {
    include: ["src/genie_space_optimizer/ui/**/*.test.ts"],
    environment: "node",
    globals: true,
  },
});
