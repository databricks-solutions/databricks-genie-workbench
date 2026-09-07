# Ontology Map v2 — **frontend** Goal-Mode driver (Lane 3 of Wave 3 — MV-D74/75 UI)

## ⚙️ Parallel-build lane header — Lane 3 of Wave 3 (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Two sibling lanes edit the repo concurrently. This lane owns a
disjoint subtree (`frontend/src/ontology/**`), so merges are conflict-free — keep it that way.
Build the UI against the **frozen contract** below; the endpoints live in Lane 2's worktree, so
they won't exist in yours — they integrate at merge. Merge LAST.

- **OWNS (create/edit freely):**
  - `frontend/src/ontology/estateGraphModel.ts` — pure element/model transforms (add origin styling
    inputs, satellite `measure`/`page` children, expand-merge helper).
  - `frontend/src/ontology/components/EstateGraph.tsx` — the renderer (origin toggle, expand-on-tap,
    inspector rail, search, minimap, Assets-LOD-requires-focus).
  - `frontend/src/ontology/api.ts` — extend `getGraph(origin)`; add `expandNode(node, origin)`.
  - `frontend/src/ontology/types.ts` — mirror `origin` on `OntologyGraphNode`; add `OntologyGraphExpand`.
  - `frontend/src/ontology/estateGraphModel.test.ts`, `.../components/EstateGraph.test.tsx` — tests.
  - Optionally new presentational components under `frontend/src/ontology/components/` (inspector,
    search, minimap) — new files only.
- **REUSE (do NOT add deps):** the Wave-2-pinned `cytoscape` / `react-cytoscapejs` / `cytoscape-fcose`
  and the existing seeded-compound-fcose layout in `EstateGraph.tsx`. **NO new npm dependency**
  (MV-D45) — `package.json`/`package-lock.json` MUST stay byte-identical; icons via inline SVG /
  existing lucide-react.
- **OFF-LIMITS (do NOT touch):** all of `backend/`, all of `packages/…/genie_space_optimizer/`,
  every non-ontology frontend area, and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** LAST (after Lane 1 then Lane 2).
- **Launch:** via the `ontology-lane-builder` subagent — see `ontology-wave3-launcher.md`.

### 🔒 Frozen API contract (mirror Lane 2; do not drift)

- `GET /api/ontology/graph?origin=applied|proposed` (default `applied`) → `OntologyGraph` + `origin`
  on each node.
- `GET /api/ontology/graph/expand?node=<id>&origin=<applied|proposed>`
  → `OntologyGraphExpand { nodes: OntologyGraphNode[], edges: OntologyGraphEdge[], parent_id: string,
    as_of: string | null }`; measure children are `kind="measure"` (edge `mv_measure`), page children
    `kind="page"` (edge `page_source`).

---

## Spec & decisions

- **Spec (source of truth):** `docs/design/ontology-map-v2-build.md` §3 (Applied-vs-Proposed), §4
  (visual system + shell), §2.4 (types). **Decisions:** `mv-advisor-playbook.md` **MV-D74/MV-D75**;
  honor **MV-D23** (zero-jargon copy), **MV-D43** (loading/empty/error/stale), **MV-D45** (no new dep),
  read-only.
- **Project rules:** `AGENTS.md` (React 19 + TS + Tailwind v4, functional components, `@`→`frontend/src`).
  Read `EstateGraph.tsx` + `estateGraphModel.ts` + `types.ts` + `api.ts` first.

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: Ontology Map v2 — FRONTEND ONLY. Make the map modern + honest: (1) an Applied|Proposed source
toggle (default Applied) so proposed engine clusters read as "Suggested" not current state (MV-D74);
(2) expand-on-demand — tapping an mv/sub-domain fetches its measures/Pages as satellite nodes (MV-D73
§2.3); (3) modern visual system + shell — per-type icons, semantic color, curved edges, right-rail
inspector, search, minimap, Assets-LOD REQUIRES a focused domain (MV-D75). Read-only, additive, NO new
dependency (MV-D45); tsc/lint/vitest green; STOP before deploy. Branch: ontology. NO backend/wheel edits.

SPEC: docs/design/ontology-map-v2-build.md §3/§4/§2.4. DECISIONS: MV-D74/D75; honor MV-D23/D43/D45.
RULES: AGENTS.md. Read EstateGraph.tsx + estateGraphModel.ts + types.ts + api.ts first.

CONTEXT: estateGraphModel.ts is a pure builder (Domains|Sub-domains|Assets LOD, seeded-compound-fcose,
emitted-only guard), tested in estateGraphModel.test.ts; EstateGraph.tsx only mounts cytoscape (Wave-2-
pinned + fcose). api.getGraph=fetchJson<OntologyGraph>("/graph"). Lane 2 adds ?origin= + /graph/expand→
OntologyGraphExpand; OntologyGraphNode gains origin; the "measure" kind is reserved in types.ts.

BUILD A — types + api (types.ts, api.ts):
  Add origin?: string|null to OntologyGraphNode; add OntologyGraphExpand{nodes,edges,parent_id,as_of}.
  getGraph(origin: "applied"|"proposed" = "applied") -> fetchJson(`/graph?origin=${origin}`);
  expandNode(node, origin="applied") -> fetchJson(`/graph/expand?node=${enc(node)}&origin=${origin}`).

BUILD B — model (estateGraphModel.ts):
  Thread origin into node data so the renderer styles applied (solid) vs proposed (dashed) + Suggested
  chip. Add a pure mergeExpand(elements, expand, parentId) appending measure/page satellite nodes
  (data.parent=parentId container) + edges, deduped, emitted-only guard; side-effect-free + testable.
  Jargon-free nodeFacts copy for measure/page (MV-D23).

BUILD C — renderer + shell (EstateGraph.tsx, new components):
  Source toggle Applied|Proposed (default Applied) -> getGraph(origin) + restyle; proposed rollups
  dashed + Suggested chip; sparse/empty applied map -> honest empty state (MV-D43) + one-tap "View
  suggested". Per-type icons (inline SVG/lucide) + semantic color by domain; curved edges dashed/
  colored by kind; edges hidden deep, revealed on select (keep FK-on-demand). Assets LOD REQUIRES a
  focused domain (drill in, not all assets) + breadcrumb Estate>Domain>Sub-domain. Tap an mv/sub ->
  expandNode + mergeExpand to add measure/Page satellites (loading+error, MV-D43). Right-rail inspector
  (upgrade the popover; plain language, MV-D23) + search-to-focus + minimap + legend + LOD toggle.
  Reuse the seeded-compound-fcose layout; relayout on expand.

HARD GUARDRAILS: read-only; additive within frontend/src/ontology only; NO new dependency (MV-D45) —
package.json/package-lock.json byte-identical, icons via inline SVG or existing lucide-react; zero
jargon/SQL in user copy (MV-D23); every view handles loading/empty/error/stale (MV-D43); determinism
preserved (seeded layout, stable on reload). NEVER touch backend/, the wheel, non-ontology frontend,
or the playbook.

ACCEPTANCE (offline):
  - estateGraphModel.test.ts: proposed rollups carry the dashed/suggested flag, applied do not;
    mergeExpand appends measure+page satellites under the right parent, dedupes, never emits a dangling
    edge; Assets LOD without a focus yields no asset nodes; reload is byte-stable.
  - EstateGraph.test.tsx: the toggle renders (Applied default) + calls getGraph with the chosen origin;
    tapping mv/sub triggers expandNode; inspector+search render.
  - cd frontend && npm ci && npm run lint && npx tsc -b && the test runner — all green; lockfile unchanged.

WORKFLOW: branch ontology. Do NOT deploy. When offline-green, STOP and report the worktree branch,
`git diff --stat`, and the test summary; a human runs the deploy-verify gate.
```

---

## After the run (human-gated — the integration gate)

Full `./scripts/deploy.sh --update` (`fevm-serverless`) → materialize job scoped to
`serverless_stable_6t92c3_catalog`. Eyeball: Applied is the default and reads as current-state;
Proposed shows dashed/"Suggested"; drill Domain→Sub-domain→Assets→(tap) measures/Pages; no
overlapping-box mush; reload is byte-stable. Record the pass in the playbook (human edit).
