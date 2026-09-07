#!/usr/bin/env node
/**
 * Ontology Map visual loop — visual-regression diff (dev-only, MV-D80 §11 / DESIGN G7).
 *
 * Compares a current shot set against the committed `baselines/` per cell. Because the
 * layout is seeded/deterministic, a pixel delta is meaningful: it answers "did I regress
 * a state/theme I wasn't touching?". No new dependency — PNGs are decoded on a <canvas>
 * in the same headless Chromium, diffed per-pixel, and a red heatmap is written per cell.
 * Exits non-zero if any cell exceeds --threshold (so it can gate).
 *
 * Usage (from frontend/):
 *   node src/ontology/harness/tools/diff.mjs --current shots/p1
 *   node src/ontology/harness/tools/diff.mjs --current shots/p1 --baseline baselines --threshold 0.2
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

const baseDir = path.resolve(HARNESS, arg("--baseline", "baselines"))
const curArg = arg("--current", null)
if (!curArg) {
  console.error("diff.mjs: pass --current <dir under harness/> (e.g. shots/p1)")
  process.exit(2)
}
const curDir = path.resolve(HARNESS, curArg)
const threshold = Number(arg("--threshold", "0.2")) // % of pixels changed to flag
const outDir = path.join(curDir, "diff")
fs.mkdirSync(outDir, { recursive: true })

function dataUrl(file) {
  return `data:image/png;base64,${fs.readFileSync(file).toString("base64")}`
}

const { browser } = await launchChromium()
const page = await (await browser.newContext({ deviceScaleFactor: 1 })).newPage()
await page.setContent("<!doctype html><body></body>", { waitUntil: "load" })

const results = []
for (const c of cells()) {
  const a = path.join(baseDir, c.file)
  const b = path.join(curDir, c.file)
  if (!fs.existsSync(a) || !fs.existsSync(b)) {
    results.push({ file: c.file, pct: null, note: !fs.existsSync(a) ? "no baseline" : "no current" })
    continue
  }
  const r = await page.evaluate(async ([au, bu]) => {
    const load = (u) =>
      new Promise((res, rej) => {
        const img = new Image()
        img.onload = () => res(img)
        img.onerror = () => rej(new Error("decode failed"))
        img.src = u
      })
    const [ia, ib] = await Promise.all([load(au), load(bu)])
    const w = Math.max(ia.width, ib.width)
    const h = Math.max(ia.height, ib.height)
    const mk = () => {
      const cv = document.createElement("canvas")
      cv.width = w
      cv.height = h
      return cv
    }
    const xa = mk().getContext("2d")
    const xb = mk().getContext("2d")
    const cd = mk()
    const xd = cd.getContext("2d")
    xa.drawImage(ia, 0, 0)
    xb.drawImage(ib, 0, 0)
    const da = xa.getImageData(0, 0, w, h).data
    const db = xb.getImageData(0, 0, w, h).data
    const out = xd.createImageData(w, h)
    let changed = 0
    for (let i = 0; i < da.length; i += 4) {
      const delta = Math.abs(da[i] - db[i]) + Math.abs(da[i + 1] - db[i + 1]) + Math.abs(da[i + 2] - db[i + 2])
      if (delta > 24) {
        changed++
        out.data[i] = 255
        out.data[i + 1] = 0
        out.data[i + 2] = 90
        out.data[i + 3] = 255
      } else {
        // faint ghost of the baseline so the heatmap is readable in context
        out.data[i] = da[i]
        out.data[i + 1] = da[i + 1]
        out.data[i + 2] = da[i + 2]
        out.data[i + 3] = 55
      }
    }
    xd.putImageData(out, 0, 0)
    return { pct: +((100 * changed) / (w * h)).toFixed(3), heatmap: cd.toDataURL("image/png") }
  }, [dataUrl(a), dataUrl(b)])

  fs.writeFileSync(
    path.join(outDir, c.file.replace(/\.png$/, ".diff.png")),
    Buffer.from(r.heatmap.split(",")[1], "base64"),
  )
  results.push({ file: c.file, pct: r.pct, over: r.pct >= threshold })
}
await browser.close()

console.log(
  `\nvisual-diff  baseline=${path.relative(process.cwd(), baseDir)}  current=${path.relative(process.cwd(), curDir)}  threshold=${threshold}%`,
)
let flagged = 0
for (const r of results) {
  let tag
  if (r.pct == null) tag = `—      (${r.note})`
  else if (r.over) {
    tag = `⚠ ${r.pct}%`
    flagged++
  } else tag = `  ${r.pct}%`
  console.log(`  ${r.file.padEnd(24)} ${tag}`)
}
fs.writeFileSync(path.join(outDir, "diff.json"), JSON.stringify({ baseDir, curDir, threshold, results }, null, 2))
console.log(`\n${flagged} state(s) over ${threshold}%. heatmaps → ${path.relative(process.cwd(), outDir)}`)
process.exit(flagged > 0 ? 2 : 0)
