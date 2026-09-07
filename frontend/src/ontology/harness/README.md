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
| `scene` | `default` `mv` `stale` `empty` `loading` `error` `slow-expand` `fail-expand` | which fixture/failure variant backs the mock API |
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
cy, tapByLabel(q) }` — `ready` flips true on the first `layoutstop`.

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

## Mock seam

`mockApi.ts` builds an `EstateGraphApi` (the injectable seam on `EstateGraph`)
from `fixtures/` — real payloads captured once from the deployed app (see
`fixtures/README.md`). Slow/failing variants are synthesized (delay/reject); the
`mv` scene reveals the real metric view that `expand.mv.json` was captured
against (the snapshot's asset level happens to hold only tables on this estate).
