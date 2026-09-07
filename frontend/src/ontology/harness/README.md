# Ontology Map — dev-only visual harness (MV-D77, Map v3 §2)

A local, fixture-backed feedback loop for the estate graph: mount the REAL
`EstateGraph` (same theme, same fonts, dark shell) against captured API payloads,
make every state URL-addressable, screenshot it, iterate. **No live API calls, no
new dependencies, no effect on the prod bundle** (`npm run build`'s rollup input is
`index.html` only; nothing under `harness/` is imported by the app).

## Canonical flow (any normal dev machine)

```bash
cd frontend && npm run dev
# → http://localhost:5173/graph-harness.html?lod=assets&focus=auto
```

State is driven entirely by URL query (or `#hash`, which wins — useful on hosts
that drop query strings). Parameters (see `main.tsx`):

| Param | Values | Meaning |
|---|---|---|
| `scene` | `default` `mv` `stale` `empty` `loading` `error` `slow-expand` `fail-expand` `stress` | which fixture/failure variant backs the mock API (`stress` ≈ 2,950 nodes / 1,560 edges for §1C smoothness checks) |
| `origin` | `applied` (default) `proposed` | initial source toggle |
| `lod` | `domains` (default) `subdomains` `assets` | initial level of detail |
| `focus` | a top-domain id, or `auto` (first real top) | drill-down focus (required for `lod=assets`) |
| `select` | node label substring or exact id | tap this node once layout settles (opens inspector; expandables auto-expand) |
| `delay` | ms (default 4000) | delay for `slow-expand` |

Useful states for a full screenshot set:

```
?lod=domains                                   ?origin=proposed
?lod=subdomains                                ?scene=loading&origin=proposed
?lod=assets&focus=auto                         ?scene=error&origin=proposed
?lod=assets&focus=auto&select=ticket_coupon    ?scene=empty
?lod=subdomains&select=Ticketing               ?scene=stale
?scene=mv&lod=assets&focus=auto&select=cost attribution
?scene=fail-expand&lod=subdomains&select=Ticketing
```

Dev hooks for drivers: `window.__ontologyHarness = { scene, origin, lod, ready,
layoutMs, nodeCount, cy, tapByLabel(q) }`. `ready` is set as soon as the instance
is wired — react-cytoscapejs runs the synchronous seeded layout BEFORE invoking
the `cy` callback, so `layoutstop` can never be a mount hook. Determinism can be
regression-checked by hashing `cy.nodes()` positions across reloads (the layout
runs in a fixed bounding box precisely so this hash is stable).

## Offline one-file build (no vite / no registry / no installed browser toolchain)

`tools/build-local.mjs` compiles the same sources with the repo's own TypeScript
compiler and inlines everything (fonts, fixtures, exact-version vendor dists from
`node_modules`) into a page that runs from `file://`:

```bash
cd frontend && node src/ontology/harness/tools/build-local.mjs
open src/ontology/harness/local/graph-harness.local.html?lod=assets&focus=auto
```

The only network the *page* uses is the Tailwind Play CDN script; the build itself
is fully offline. `local/graph-harness.artifact.html` is the same content without
the page skeleton, for hosting on a private page when a screenshotting browser
can't reach `file://`. Both outputs and `shots/` are git-ignored.

Screenshotting: any driven browser works (Playwright, an agent-browser MCP, or a
human). Wait for `window.__ontologyHarness.ready === true` (or ~2s idle — the
layout is deterministic, `randomize:false`), then capture. Because layout is
seeded and stable, shots double as visual-regression baselines.

## The visual loop: `tools/` (MV-D80 §11)

The Director/Developer/Reviewer/Gatekeeper loop (build spec `ontology-map-v3-fable-build.md`
§11) runs on three dev-only scripts over the `{scene × theme}` matrix in `tools/matrix.mjs`
(the single source of truth — keep it in sync with `DESIGN.md` §6). None of them add a
package dependency: they resolve Playwright from wherever it already lives (a local
devDependency, else the `npx` cache — see `tools/_pw.mjs`), so `package-lock.json` stays
byte-identical. If Playwright is missing, the error prints the one-liner
(`npx playwright@1.58 install chromium` — lands in the npx cache, still no lockfile change).

```bash
cd frontend && npm run dev            # harness must be running for shoot
npm run map:shots -- --out baselines  # (re)set the committed baseline set (tracked)
npm run map:shots -- --phase p1       # capture a phase → shots/p1/ (git-ignored)
npm run map:contact -- --dir shots/p1 # tile → shots/p1/contact.png (rows=scenes,cols=themes)
npm run map:diff -- --current shots/p1 # per-cell pixel diff vs baselines/ → heatmaps; exit≠0 if any cell ≥ threshold
```

- **`shoot.mjs`** drives headless Chromium across all 20 cells, waiting on
  `__ontologyHarness.ready` (card scenes soft-wait), writing `<scene>.<theme>.png` + `meta.json`.
- **`contact.mjs`** inlines the PNGs into an HTML grid and screenshots it — one montage, no
  image-processing dep.
- **`diff.mjs`** decodes both PNGs on a `<canvas>` in the browser, diffs per pixel, writes a red
  heatmap per cell + `diff.json`, and exits non-zero over `--threshold` (default 0.2%) so it gates.

**`baselines/` is TRACKED** (the regression reference); `shots/` is git-ignored (per-run output).
Because the layout is seeded/deterministic, an independent re-shoot diffs at 0% — so baselines are
trustworthy and a non-zero diff means a real visual change. Update baselines deliberately
(`map:shots -- --out baselines`) only when a change is intended.

## Mock seam

`mockApi.ts` builds an `EstateGraphApi` (the injectable seam on `EstateGraph`)
from `fixtures/` — real payloads captured once from the deployed app (see
`fixtures/README.md`). Slow/failing variants are synthesized (delay/reject); the
`mv` scene reveals the real metric view that `expand.mv.json` was captured
against (the snapshot's asset level happens to hold only tables on this estate).
