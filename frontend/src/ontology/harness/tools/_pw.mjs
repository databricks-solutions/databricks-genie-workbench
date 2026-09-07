/**
 * Ontology Map visual loop — Playwright resolver (dev-only, MV-D80 §11).
 *
 * The loop drives a headless browser to screenshot the harness. To keep the runtime
 * dependency graph BYTE-IDENTICAL (MV-D45 / build-spec §11.6), we do NOT add playwright
 * to package.json. Instead we resolve it from wherever it already lives:
 *   1. a local devDependency (if a maintainer chose to add one), else
 *   2. the npm `npx` cache (`~/.npm/_npx/<hash>/node_modules/playwright`) — this machine
 *      already has it because `npx playwright` has been run here.
 *
 * If neither exists, we print the exact one-liner to provision it (still no lockfile
 * change — it lands in the npx cache):  npx playwright@1.58 install chromium
 */
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"

async function importPlaywright() {
  // 1) local node_modules (devDependency), if present.
  try {
    return await import("playwright")
  } catch {
    /* fall through to the npx cache */
  }
  // 2) npx cache scan (hashes are machine-specific, so we discover, not hardcode).
  const cache = path.join(os.homedir(), ".npm", "_npx")
  const rels = [
    "node_modules/playwright/index.js",
    "node_modules/@playwright/test/node_modules/playwright/index.js",
  ]
  if (fs.existsSync(cache)) {
    for (const hash of fs.readdirSync(cache)) {
      for (const rel of rels) {
        const p = path.join(cache, hash, rel)
        if (fs.existsSync(p)) {
          try {
            return await import(pathToFileURL(p).href)
          } catch {
            /* try the next candidate */
          }
        }
      }
    }
  }
  throw new Error(
    "Playwright not found. This is dev-only tooling and adds NO lockfile dependency.\n" +
      "Provision it into the npx cache (one time) and re-run:\n" +
      "  npx playwright@1.58 install chromium\n",
  )
}

/** Launch headless Chromium from the resolved Playwright. `args` for e.g. file access. */
export async function launchChromium(args = []) {
  const mod = await importPlaywright()
  // CJS→ESM interop: chromium hangs off the default export on this build.
  const pw = mod?.chromium ? mod : (mod.default ?? mod)
  const chromium = pw.chromium
  if (!chromium) throw new Error("playwright resolved but `chromium` is unavailable")
  const browser = await chromium.launch({ headless: true, args })
  return { browser }
}
