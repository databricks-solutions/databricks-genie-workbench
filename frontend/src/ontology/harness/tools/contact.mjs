#!/usr/bin/env node
/**
 * Ontology Map visual loop — contact-sheet montage (dev-only, MV-D80 §11).
 *
 * Tiles a shot directory into ONE image (rows = scenes, cols = light | dark) so the
 * Developer/Reviewer reason over the whole surface at once. No new dependency: it lays
 * the PNGs out in an HTML grid and screenshots THAT with the same headless Chromium the
 * shooter uses (images are inlined as data URLs so there are no file:// origin issues).
 *
 * Usage (from frontend/):
 *   node src/ontology/harness/tools/contact.mjs --dir baselines
 *   node src/ontology/harness/tools/contact.mjs --dir shots/p1 --out contact.png
 */
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { launchChromium } from "./_pw.mjs"
import { SCENES, THEMES } from "./matrix.mjs"

const HERE = path.dirname(fileURLToPath(import.meta.url))
const HARNESS = path.resolve(HERE, "..")

function arg(name, def) {
  const i = process.argv.indexOf(name)
  return i >= 0 ? process.argv[i + 1] : def
}

const dir = path.resolve(HARNESS, arg("--dir", "baselines"))
const outFile = path.resolve(dir, arg("--out", "contact.png"))
const title = arg("--title", path.relative(HARNESS, dir))

function dataUrl(file) {
  if (!fs.existsSync(file)) return null
  return `data:image/png;base64,${fs.readFileSync(file).toString("base64")}`
}

const rowsHtml = SCENES.map((s) => {
  const tds = THEMES.map((t) => {
    const src = dataUrl(path.join(dir, `${s.name}.${t}.png`))
    const cell = src ? `<img src="${src}">` : `<div class="miss">missing</div>`
    return `<td><div class="lbl">${s.name} · ${t}</div>${cell}</td>`
  }).join("")
  return `<tr>${tds}</tr>`
}).join("")

const html = `<!doctype html><meta charset="utf8"><style>
  body{margin:0;background:#0b0f17;font-family:ui-sans-serif,system-ui}
  h1{color:#e2e8f0;font:600 15px ui-sans-serif;margin:14px 16px 4px}
  p{color:#64748b;font:400 12px ui-sans-serif;margin:0 16px 10px}
  table{border-collapse:separate;border-spacing:10px;margin:0 6px 14px}
  td{vertical-align:top}
  img{width:520px;display:block;border:1px solid #1e293b;border-radius:10px}
  .lbl{color:#94a3b8;font:600 12px ui-monospace,monospace;margin:0 0 5px}
  .miss{width:520px;height:325px;display:grid;place-items:center;border:1px dashed #334155;border-radius:10px;color:#64748b;font:500 13px ui-sans-serif}
</style>
<h1>Ontology Map — ${title}</h1><p>rows = scenes · columns = light | dark · ${new Date().toISOString()}</p>
<table>${rowsHtml}</table>`

const { browser } = await launchChromium()
const page = await (await browser.newContext({ deviceScaleFactor: 1 })).newPage()
await page.setContent(html, { waitUntil: "load" })
await page.waitForTimeout(200)
const table = await page.$("table")
await (table ?? page).screenshot({ path: outFile })
await browser.close()

console.log("wrote", path.relative(process.cwd(), outFile))
