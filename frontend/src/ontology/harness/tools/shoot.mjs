#!/usr/bin/env node
/**
 * Ontology Map visual loop — screenshot matrix (dev-only, MV-D80 §11).
 *
 * Renders every {scene × theme} cell (matrix.mjs) of the running harness and writes one
 * PNG per cell. Waits on `window.__ontologyHarness.ready` (deterministic seeded layout ⇒
 * no flaky sleeps); card scenes (empty/error) have no cytoscape instance, so we soft-wait
 * then shoot the composed card. Output dir is a loop artifact (git-ignored under shots/)
 * UNLESS you shoot into `baselines/` (tracked) to (re)set the visual-regression baseline.
 *
 * Usage (from frontend/, with `npm run dev` running):
 *   node src/ontology/harness/tools/shoot.mjs --out baselines          # set baseline
 *   node src/ontology/harness/tools/shoot.mjs --phase p1               # → shots/p1/
 *   node src/ontology/harness/tools/shoot.mjs --base http://localhost:5173 --phase p2
 */
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { launchChromium } from "./_pw.mjs"
import { cells } from "./matrix.mjs"

const HERE = path.dirname(fileURLToPath(import.meta.url))
const HARNESS = path.resolve(HERE, "..")

function arg(name, def) {
  const i = process.argv.indexOf(name)
  return i >= 0 ? process.argv[i + 1] : def
}

const base = arg("--base", "http://localhost:5173").replace(/\/$/, "")
const outArg = arg("--out", null)
const phase = arg("--phase", "current")
const outDir = outArg ? path.resolve(HARNESS, outArg) : path.join(HARNESS, "shots", phase)
const VIEWPORT = { width: 1440, height: 900 }

fs.mkdirSync(outDir, { recursive: true })

const { browser } = await launchChromium()
const ctx = await browser.newContext({ viewport: VIEWPORT, deviceScaleFactor: 1 })
const meta = []

for (const c of cells()) {
  const page = await ctx.newPage()
  const url = `${base}/graph-harness.html?${c.q}`
  try {
    await page.goto(url, { waitUntil: "load", timeout: 30000 })
    // Graph scenes flip ready===true after the synchronous seeded layout; card scenes
    // never do — soft-wait either way, then a fixed settle for fonts + camera ease.
    await page
      .waitForFunction("window.__ontologyHarness && window.__ontologyHarness.ready===true", {
        timeout: c.card ? 3000 : 9000,
      })
      .catch(() => {})
    await page.waitForTimeout(c.scene.includes("select") || c.scene.includes("expand") ? 1100 : 700)
    const m = await page.evaluate(() => {
      const h = window.__ontologyHarness
      return { ready: !!(h && h.ready), layoutMs: h?.layoutMs ?? null, nodes: h?.nodeCount ?? null }
    })
    await page.screenshot({ path: path.join(outDir, c.file), fullPage: false })
    meta.push({ ...c, ...m })
    console.log(`  ✓ ${c.file.padEnd(24)} ready=${m.ready} nodes=${m.nodes ?? "-"} layout=${m.layoutMs ?? "-"}ms`)
  } catch (e) {
    console.log(`  ✗ ${c.file.padEnd(24)} ${e.message}`)
    meta.push({ ...c, error: e.message })
  } finally {
    await page.close()
  }
}

fs.writeFileSync(
  path.join(outDir, "meta.json"),
  JSON.stringify({ base, when: new Date().toISOString(), viewport: VIEWPORT, cells: meta }, null, 2),
)
await ctx.close()
await browser.close()

const ok = meta.filter((m) => !m.error).length
console.log(`\nwrote ${ok}/${meta.length} shots → ${path.relative(process.cwd(), outDir)}`)
process.exit(ok === meta.length ? 0 : 1)
