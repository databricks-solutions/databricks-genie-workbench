# Ontology Map — render virtualization / viewport culling · Goal-Mode driver

> **Frontend-only, on the `ontology` branch.** CHEAP-FIRST: Phase 1 is viewport culling of the
> React-owned SVG (fewer DOM nodes when zoomed in); Phase 2 (WebGL/LOD) stays DEFERRED unless
> Phase 1 proves insufficient near the 2000-node cap. Additive + DEFAULT-SAFE (below a node-count
> threshold, or with no viewport yet, the map renders EVERYTHING → byte-identical DOM, so every
> approved MV-D80 mockup and existing test is unchanged). Proposed register line: **MV-D106**
> (viewport-culled SVG rendering for the estate map; the second enterprise scale lever after the
> backend `TOP_N_BY_CENTRALITY` cap). Determinism-preserving (a pure culler in
> `ontologyTreeLayout.ts`), dual-theme-neutral (no token change).

## Why now
The estate map is the last unbounded render surface. The backend already caps the served graph at
`TOP_N_BY_CENTRALITY = 2000` display nodes (`layout.py:32`, truncation `layout.py:555-559`), and the
tree is progressively disclosed (per-parent `+N more`, relationships-on-focus). But the frontend still
puts **every laid-out node and edge in the DOM** — `layout.spineLinks.map` (`EstateGraph.tsx:1383`),
`layout.crossLinks.map` (`:1406`), `layout.nodes.map` (`:1531`), tray (`:1515`). When a large estate is
expanded and the user zooms IN, thousands of off-screen `<g>`/`<path>` elements stay mounted, paid for
on every React reconcile. Zoom/drag are already O(1)/frame (imperative d3 writes the `<g>` transform
each tick; React `transform` state syncs only on `end` — `EstateGraph.tsx:447,458`), so the remaining
cost is DOM element COUNT, not per-frame math. Culling to the viewport is the targeted fix.

## Grounded facts (code, 2026-09-21)
- **The viewport rect is already computed, in content coords.** `viewportContentRect(transform, size)`
  (`ontologyTreeLayout.ts:711`, unit-tested `ontologyTreeLayout.test.ts:506`) is set into
  `viewportRect` state on every zoom-end (`EstateGraph.tsx:1050`; cleared to `null` at `:1045`) and fed
  to `GraphMinimap`. So the culler's input already exists — nothing new to measure.
- **Rendering is React-owned SVG.** d3 is imperative-only (zoom on `<svg>`, drag on nodes); React owns
  every element (`EstateGraph.tsx:2-7`). Nodes carry `data-node-id` (`:1540,:1604`) — the drag/glide
  paths select `[data-node-id]` (`:633,:1146`), so the culler MUST keep any node those paths touch.
- **Edges already clip-to-visible.** Both edge maps early-return `null` when an endpoint isn't laid out
  (`:1385` spine, `:1411` cross), so dropping off-viewport nodes from the node set makes their incident
  edges vanish for free — but pre-filtering the edge arrays too is the actual DOM win.
- **Layout is pure + memoized.** `layout: Layout` (`EstateGraph.tsx:366-376`) exposes `nodes`,
  `spineLinks`, `crossLinks`, `trayItems`; `nodeById` at `:379`. The minimap dots derive from
  `layout.nodes` (`:1030`) and MUST keep the FULL set (navigation shows the whole scene).
- **Interaction keep-set sources:** `selectedId` (`:231`), `searchHits` (`:233`), `hoveredId`
  (`:237`), breadcrumb ancestors, the drag target. Culling any of these breaks select/search/drag.

## Testability seam
One pure helper in `ontologyTreeLayout.ts` (keeps logic out of the component so the
react-refresh/only-export-components lint rule stays green and the culler is unit-testable exactly like
`viewportContentRect`/`scaleRadius`), plus a thin `useMemo` in `EstateGraph` feeding the three existing
`.map()`s. No backend, no wheel, no job, no new dep, no API change. Default-off below threshold ⇒ the
MV-D80 mockup harness (fixtures under threshold, fixed viewport) is byte-identical.

---

## GOAL PROMPT (paste verbatim into Goal Mode — Phase 1 only, then STOP)

Implement **Ontology Map render virtualization** (viewport culling) on the `ontology` branch,
frontend-only. Additive + DEFAULT-SAFE, determinism-preserving, dual-theme-neutral. Proposed
**MV-D106**. Build Phase 1 ONLY (SVG viewport culling); leave Phase 2 (WebGL/LOD) unbuilt.

PHASE 1 — viewport culling of the SVG.
1. Add a PURE, exported helper to `frontend/src/ontology/ontologyTreeLayout.ts`:
   `cullToViewport(nodes, spineLinks, crossLinks, viewportRect, { keep, pad, threshold })` returning
   `{ nodes, spineLinks, crossLinks }`. Rules, all deterministic:
   - If `viewportRect == null` OR `nodes.length <= threshold` (default **600**) ⇒ return the inputs
     UNCHANGED (referential passthrough) — this is the byte-identical default path.
   - Else keep a node iff its `(x,y)` (± its radius) intersects `viewportRect` PADDED by `pad`
     (default **1.0** viewport-width/height on each side, so a pan reveals already-mounted nodes before
     the zoom-end re-render) OR its id ∈ `keep`. Keep an edge iff BOTH endpoints are in the kept set.
   - Never invent nodes; preserve input order (stable) so React keys are stable.
2. Wire it in `EstateGraph.tsx`: a `useMemo` over `(layout, viewportRect, keepSet)` where
   `keepSet = selectedId ∪ searchHits ∪ hoveredId ∪ breadcrumb-ancestors ∪ drag-target`, producing
   `visibleNodes/visibleSpine/visibleCross`; point the three `.map()`s (`:1383/:1406/:1531`) at them.
   The minimap (`:1030`) and any full-scene math KEEP using `layout.nodes` (navigation shows the whole
   estate). Tray rendering unchanged.
3. Threshold + pad are module constants (no new prop, no new setting).

GUARDRAILS: additive; default-off below threshold / no-viewport ⇒ referential passthrough ⇒
byte-identical DOM (existing snapshots + mockups unchanged); the culler is a PURE export in
`ontologyTreeLayout.ts` (no new export from the component — react-refresh rule); the keep-set
guarantees selected/searched/hovered/dragged/ancestor nodes are ALWAYS mounted; the minimap stays
full-scene; no d3-render change (React still owns elements); no new dep; `frontend/package-lock.json`
untouched.

TESTS (vitest): (a) `cullToViewport` — passthrough when `viewportRect==null` or `count<=threshold`;
keeps in-rect + padded nodes, drops far ones, always keeps `keep` ids; edges kept iff both endpoints
kept; stable order. (b) An `EstateGraph` component test with a > threshold fixture zoomed in: fewer
`[data-node-id]` elements than the full model, YET a `selectedId`/`searchHit` OUTSIDE the viewport is
still mounted. (c) A regression test that a small (< threshold) graph renders the SAME node count as
before (default-safe).

ACCEPTANCE (offline): `cd frontend && npx tsc -b` clean · `npm run lint` clean · `npm run test`
green (report the new vitest count). No `./scripts/test.sh` change expected (frontend-only), but run it
if any shared fixture moved. `git status -- frontend/package-lock.json` clean. Then STOP for
deploy-verify.

---

## Deploy-verify gate (human, after offline-green — the STOP checkpoint)
This changes the frontend bundle, so **frontend build ON**: `./scripts/deploy.sh --update` (do NOT set
`SKIP_FRONTEND_BUILD=1`). `deploy.sh` reads `GENIE_DEPLOY_PROFILE` from `.env.deploy` (today
`fevm-serverless` → 6t92c3) and IGNORES a `DATABRICKS_CONFIG_PROFILE=` prefix. **No materialize run
needed** — this is render-only, no data change.

On the deployed app, open the Ontology Map on the largest estate and:
1. Expand a large sub-domain and zoom IN. Confirm the mounted node count drops as you zoom
   (`document.querySelectorAll('[data-node-id]').length` shrinks vs the zoomed-out overview) while the
   visible scene is unchanged — no popping WITHIN the viewport, and panning reveals nodes on release
   (the pad covers a modest pan mid-gesture).
2. Interactions intact: SELECT a node, SEARCH for an off-screen node, and DRAG — each must still work
   even when its target is outside the viewport (keep-set), and the minimap still shows the whole scene
   + you-are-here box.
3. Small estate unaffected: on an estate under the threshold, the map renders exactly as before.
Review with a human (smooth deep-zoom on the big estate, no lost selection/search, minimap whole) —
then mark BUILT and register **MV-D106**.

## Phase 2 — WebGL / LOD (DEFERRED, do NOT build now)
Only if Phase 1 culling still stutters near the 2000-node cap: swap the SVG node/edge layers for a
WebGL renderer (or add coarse LOD that collapses dense clusters to a single glyph until zoomed). New
dep + a determinism/dual-theme re-proof of the render path — a separate driver when the need is real.
