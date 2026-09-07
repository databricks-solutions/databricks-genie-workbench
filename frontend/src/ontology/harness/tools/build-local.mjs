#!/usr/bin/env node
/**
 * Ontology Map v3 — offline one-file harness build (loop tooling, §2.4, MV-D77).
 *
 * The canonical harness flow is `npm run dev` → /graph-harness.html. Environments that
 * can't run vite (no registry access, no platform-native binaries) still need the §2.4
 * vision loop, so this script builds a SELF-CONTAINED harness page with zero package
 * changes and zero network needs at build time:
 *   - my sources (EstateGraph + model + harness) compiled with the repo's own
 *     TypeScript compiler (pure JS) into one AMD bundle;
 *   - third-party deps inlined VERBATIM from node_modules dist files — the exact
 *     locked versions (react/react-dom CJS production, cytoscape/fcose/cose-base/
 *     layout-base/react-cytoscapejs/lucide-react UMD) via a tiny CJS/UMD wrapper;
 *   - fonts base64-inlined (label crispness is judged against real type);
 *   - fixtures inlined (never hits the live API);
 *   - Tailwind via the Play CDN (the one external script; the page needs internet
 *     for that single fetch) configured with the app's theme tokens.
 *
 * Usage:  node src/ontology/harness/tools/build-local.mjs   (from frontend/)
 * Output (git-ignored):
 *   src/ontology/harness/local/graph-harness.local.html     full page, open via file://
 *   src/ontology/harness/local/graph-harness.artifact.html  same content, no page
 *     skeleton — publishable to a private hosted page when file:// isn't reachable
 *     by the screenshotting browser. State is driven via #hash params there.
 *
 * Dev-only: nothing here is imported by the app or the harness entry, and
 * `npm run build` never runs it.
 */
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { createRequire } from "node:module"

const HERE = path.dirname(fileURLToPath(import.meta.url))
const HARNESS = path.resolve(HERE, "..")
const SRC = path.resolve(HARNESS, "../..")
const FRONTEND = path.resolve(SRC, "..")
const NM = path.join(FRONTEND, "node_modules")
const OUTDIR = path.join(HARNESS, "local")
const require_ = createRequire(import.meta.url)
const ts = require_(path.join(NM, "typescript/lib/typescript.js"))

// ── 1. Compile the ontology + harness sources → one AMD bundle ──────────────
const options = {
  module: ts.ModuleKind.AMD,
  outFile: "bundle.js",
  jsx: ts.JsxEmit.ReactJSX,
  target: ts.ScriptTarget.ES2020,
  moduleResolution: ts.ModuleResolutionKind.Node10,
  baseUrl: SRC,
  paths: { "@/*": ["*"] },
  rootDir: SRC,
  esModuleInterop: true,
  skipLibCheck: true,
  noEmitOnError: false,
  strict: false,
  types: [],
}
const program = ts.createProgram([path.join(HARNESS, "main.tsx")], options)
let bundle = ""
program.emit(undefined, (_name, text) => {
  bundle = text
})
if (!bundle) {
  console.error("tsc emitted nothing")
  process.exit(1)
}

// ── 2. Vendor modules, verbatim from node_modules (exact locked versions) ───
const read = (p) => fs.readFileSync(path.join(NM, p), "utf8")

/**
 * Register one CJS/UMD dist file under an AMD module id. `define` is shadowed to
 * undefined inside the wrapper so UMDs take their CommonJS branch and resolve their
 * deps through the harness loader (single shared react/cytoscape instance).
 */
const vendor = (name, file) => `
define(${JSON.stringify(name)}, ["require", "exports"], function (require, exports) {
  var module = { exports: exports };
  var process = { env: { NODE_ENV: "production" } };
  var define = undefined;
  (function (module, exports, require) {
${read(file)}
  })(module, module.exports, require);
  return module.exports;
});
`

// The renderer is d3/SVG now (MV-D84) — vendor the d3 UMD dists (they detect the CJS
// wrapper and require their siblings; the loader resolves define/require lazily, so order
// is irrelevant as long as every transitive module is defined). No cytoscape, no prop-types.
const vendorDefs = [
  vendor("react", "react/cjs/react.production.js"),
  vendor("react/jsx-runtime", "react/cjs/react-jsx-runtime.production.js"),
  vendor("scheduler", "scheduler/cjs/scheduler.production.js"),
  vendor("react-dom", "react-dom/cjs/react-dom.production.js"),
  vendor("react-dom/client", "react-dom/cjs/react-dom-client.production.js"),
  vendor("d3-hierarchy", "d3-hierarchy/dist/d3-hierarchy.min.js"),
  vendor("d3-path", "d3-path/dist/d3-path.min.js"),
  vendor("d3-shape", "d3-shape/dist/d3-shape.min.js"),
  vendor("d3-color", "d3-color/dist/d3-color.min.js"),
  vendor("d3-timer", "d3-timer/dist/d3-timer.min.js"),
  vendor("d3-ease", "d3-ease/dist/d3-ease.min.js"),
  vendor("d3-dispatch", "d3-dispatch/dist/d3-dispatch.min.js"),
  vendor("d3-interpolate", "d3-interpolate/dist/d3-interpolate.min.js"),
  vendor("d3-selection", "d3-selection/dist/d3-selection.min.js"),
  vendor("d3-transition", "d3-transition/dist/d3-transition.min.js"),
  vendor("d3-drag", "d3-drag/dist/d3-drag.min.js"),
  vendor("d3-zoom", "d3-zoom/dist/d3-zoom.min.js"),
  vendor("lucide-react", "lucide-react/dist/umd/lucide-react.min.js"),
].join("\n")

// ── 3. Assets ────────────────────────────────────────────────────────────────
const b64 = (p) => fs.readFileSync(p).toString("base64")
const font = (f) => `url(data:font/woff2;base64,${b64(path.join(FRONTEND, "public/fonts", f))}) format('woff2')`
const fontCss = `
@font-face{font-family:'Cabinet Grotesk';src:${font("CabinetGrotesk-Bold.woff2")};font-weight:700;font-display:block;}
@font-face{font-family:'Cabinet Grotesk';src:${font("CabinetGrotesk-Extrabold.woff2")};font-weight:800;font-display:block;}
@font-face{font-family:'General Sans';src:${font("GeneralSans-Regular.woff2")};font-weight:400;font-display:block;}
@font-face{font-family:'General Sans';src:${font("GeneralSans-Medium.woff2")};font-weight:500;font-display:block;}
@font-face{font-family:'General Sans';src:${font("GeneralSans-Semibold.woff2")};font-weight:600;font-display:block;}
@font-face{font-family:'JetBrains Mono';src:${font("JetBrainsMono-Regular.woff2")};font-weight:400;font-display:block;}
@font-face{font-family:'JetBrains Mono';src:${font("JetBrainsMono-Medium.woff2")};font-weight:500;font-display:block;}
`

// index.css adapted for a non-vite page: drop the tailwind import + the file-based
// @font-face block (data-URI versions above); turn @theme into :root so its custom
// properties exist without the Tailwind v4 compiler.
let appCss = fs.readFileSync(path.join(SRC, "index.css"), "utf8")
appCss = appCss.replace(/@import "tailwindcss";/g, "")
appCss = appCss.replace(/@custom-variant[^;]*;/g, "")
appCss = appCss.replace(/@font-face\s*\{[^}]*\}/g, "")
appCss = appCss.replace(/@theme\s*\{/, ":root{")

const fixture = (f) => fs.readFileSync(path.join(HARNESS, "fixtures", f), "utf8")
const rawDefine = (id, content) =>
  `define(${JSON.stringify(id)}, [], function(){ return { __esModule: true, default: ${JSON.stringify(content)} }; });`

const fixtureDefs = [
  rawDefine("ontology/harness/fixtures/graph.applied.json?raw", fixture("graph.applied.json")),
  rawDefine("ontology/harness/fixtures/graph.proposed.json?raw", fixture("graph.proposed.json")),
  rawDefine("ontology/harness/fixtures/expand.mv.json?raw", fixture("expand.mv.json")),
  rawDefine("ontology/harness/fixtures/expand.subdomain.json?raw", fixture("expand.subdomain.json")),
  `define("index.css", [], function(){ return {}; });`,
].join("\n")

// ── 4. Page template ─────────────────────────────────────────────────────────
const twConfig = `
tailwind.config = {
  darkMode: 'class',
  theme: { extend: {
    colors: {
      accent: { DEFAULT: '#4F46E5', light: '#818CF8' },
      cyan: { DEFAULT: '#06B6D4', light: '#22D3EE' },
      success: { DEFAULT: '#10B981', light: '#34D399' },
      warning: { DEFAULT: '#F59E0B', light: '#FBBF24' },
      danger: { DEFAULT: '#EF4444', light: '#F87171' },
      info: { DEFAULT: '#3B82F6', light: '#60A5FA' },
    },
    fontFamily: {
      display: ['Cabinet Grotesk','Inter','system-ui','sans-serif'],
      body: ['General Sans','Inter','system-ui','sans-serif'],
      mono: ['JetBrains Mono','SF Mono','Menlo','monospace'],
    },
  } },
}
`

const loader = `
var __registry = {}, __cache = {};
function define(name, deps, factory) { __registry[name] = { deps: deps, factory: factory }; }
define.amd = true;
function __resolve(id, from) {
  if (id.slice(0, 2) === '@/') return id.slice(2);
  if (id[0] !== '.') return id;
  var parts = from.split('/'); parts.pop();
  id.split('/').forEach(function (seg) {
    if (seg === '.' || seg === '') return;
    if (seg === '..') parts.pop(); else parts.push(seg);
  });
  return parts.join('/');
}
function __req(id, from) {
  id = __resolve(id, from || '');
  if (Object.prototype.hasOwnProperty.call(__cache, id)) return __cache[id];
  var m = __registry[id];
  if (!m) throw new Error('harness loader: module not found: ' + id);
  var module = { exports: {} };
  __cache[id] = module.exports;
  var args = m.deps.map(function (d) {
    if (d === 'require') return function (x) { return __req(x, id); };
    if (d === 'exports') return module.exports;
    return __req(d, id);
  });
  var r = m.factory.apply(null, args);
  if (r !== undefined) { module.exports = r; __cache[id] = r; }
  return __cache[id];
}
`

const boot = `
(function start() {
  var go = function () { __req('ontology/harness/main'); };
  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(go, go);
    // Belt & braces: some engines never settle fonts.ready on data-URI faces.
    setTimeout(function () { if (!window.__ontologyHarness) go(); }, 1500);
  } else { go(); }
})();
`

const dark = `document.documentElement.classList.add('dark');`

const inner = `
<title>Ontology Map — dev harness</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>${dark}${twConfig}</script>
<style>${fontCss}</style>
<style>${appCss}</style>
<div id="root"></div>
<script>${loader}</script>
<script>${fixtureDefs}</script>
<script>
${vendorDefs}
</script>
<script>
${bundle}
</script>
<script>${boot}</script>
`

const fullPage = `<!doctype html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
</head>
<body>
${inner}
</body>
</html>
`

fs.mkdirSync(OUTDIR, { recursive: true })
const outLocal = path.join(OUTDIR, "graph-harness.local.html")
const outArtifact = path.join(OUTDIR, "graph-harness.artifact.html")
fs.writeFileSync(outLocal, fullPage)
fs.writeFileSync(outArtifact, inner)
console.log(
  "wrote",
  outLocal,
  (fullPage.length / 1024).toFixed(0) + "KB total,",
  "bundle " + (bundle.length / 1024).toFixed(0) + "KB",
)
