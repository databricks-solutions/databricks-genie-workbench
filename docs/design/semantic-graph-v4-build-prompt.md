# Build prompt — Semantic model visualization v4 ("Semantic Blueprint")

**Paste this whole file to the implementing agent.** It is the per-phase
execution handoff. It is self-contained but points at two sources of truth you
must open before writing code.

---

## 0. Mission

Rebuild the Genie Workbench semantic-model canvas into the "Semantic Blueprint":
an adaptive, layered, column-accurate, self-annotating diagram with a Join
Advisor and an Insights inset — **without** adding a layout/physics dependency and
**without** breaking the deterministic, pure-function render contract.

You are not designing. The design is settled. You are matching a north star.

---

## 1. Sources of truth (read these first, in this order)

1. **North star — the interactive prototype:**
   `docs/design/mockups/10-blueprint-prototype.html`
   Open it in a browser. This is the **visual-fidelity and feature-parity
   target**. The shipped canvas must look and behave like this, adapted to live
   data. It is a self-contained HTML/JS mock (no build step) that *computes*
   layout, routing, cardinality, zoom, lineage, insights, and the join advisor —
   so its functions are the reference implementation for the real pure modules.

2. **Design contract — the blueprint note:**
   `docs/design/semantic-graph-v4-blueprint-note.md`
   Rationale for every choice and the non-negotiable invariants (§2 constraints,
   §8 determinism). **§11 has the consolidated parity checklist and the real-code
   starting points** — treat §11.3 as your definition of done per phase.

3. **Genie schema references (MANDATORY before any schema/config/optimizer
   change), per `AGENTS.md` → References:**
   - `serialized_space` schema + validation rules (WebFetch the Databricks docs
     linked in `AGENTS.md`).
   - Local mirror: `backend/references/schema.md`.

**Rule of authority:** on *look/behavior*, the prototype wins; on *data contract
or determinism*, the blueprint note wins.

---

## 2. Hard constraints (do not violate — these kill whole approaches)

- **Deterministic, pure render.** The entire canvas must remain a pure function of
  `(nodes, edges, selection, dragOffsets, measuredViewport)`, testable with
  `renderToStaticMarkup`. **No force/physics solver, no auto-layout library.**
  "New layout" means a new *deterministic* algorithm (longest-path ranks +
  one-shot barycenter ordering), not a dependency.
- **Arrows require proof.** A base-canvas edge is drawn only where a relationship
  is *declared* (metric-view `joins.on`/`using`, or a declared config
  relationship). No declared relationship → no arrow. Advisor candidates are
  **overlay proposals**, never base edges.
- **Grounding, not invention.** Anything without a backing read is not drawn. No
  guessed FACT/DIM roles — render neutral `TABLE` when the backend leaves
  `role = None`.
- **No node duplication.** One node per table; a shared dim is one node.
- **Robust to any shape.** Must render a single wide table, unknown-role schemas,
  snowflakes, and islands without erroring (blueprint §5.11). Relationships +
  measure lineage are load-bearing; role classification is a bonus.
- **Additive data changes.** A Phase-1 response (no `columns`) must render
  identically before and after the Phase-2 field exists.

### Repo/platform rules (from `AGENTS.md` — enforce all)
- **No local dev server.** Do **not** run `uvicorn` or `npm run dev` to "test";
  the app needs Databricks OBO auth, Lakebase, and serving endpoints. Integration
  testing is done by deploying.
- **`npm ci`, never `npm install`** in scripts; exact versions only (no `^`/`~`).
- **Do not edit `requirements.txt` by hand** (generated from `uv.lock`).
- **Keep types in sync:** Pydantic in `backend/models.py` ↔ TS in
  `frontend/src/types`.
- **Do not run `databricks bundle init`.**

---

## 3. Starting points (what exists today)

- **Canvas + pure fns:** `frontend/src/components/model/SemanticGraph.tsx`
  already exports the building blocks you'll extend/mirror: `buildCards`,
  `layoutCards`, `distributeEdgePorts`, `computeFit`, `collapsePairJoins`,
  `relationshipGlyph`, `edgeBundleAnchors`, `memberBoundary`, `unmodeledTableIds`,
  `focusSet`, `impactSet`, `countGovernance`, `collapseThreshold`, the coverage
  badge, drag/pan helpers.
- **Host:** `frontend/src/components/model/SemanticModelTab.tsx`.
- **Existing tests (pattern to follow):** `SemanticGraph.v3.test.tsx`,
  `SemanticGraph.grouped.test.tsx`, `SemanticGraph.crash.test.tsx`,
  `SemanticModelTab.test.tsx`.
- **Fidelity-frame mockups + gate:** `frontend/src/components/auto-optimize/mockups/`
  (`Mv*FidelityFrames.tsx` + `mockups.test.tsx`); fixtures in `mvMockData.ts`.
- **Backend graph builder:** `_build_semantic_graph` in
  `backend/routers/auto_optimize.py` — assigns `role` only when a metric view
  proves it; emits `derives` / `uses` / membership edges and per-table `coverage`.
- **Types:** `SemanticGraphResponse` / `SemanticGraphNode` / `SemanticGraphEdge`
  in `frontend/src/types` mirroring `backend/models.py`.
- **Optimizer join facts (for Phase 3 framing):** the Auto-Optimize loop can
  `add_join_spec` / `update_join_spec` but **never** remove a join — its allowlist
  `_ALLOWED_PATCH_TYPES` in
  `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py`
  drops any `remove_join_spec` (which exists only as an add's rollback).

---

## 4. Phased plan

Build in three independently shippable phases. **Each phase ships its fidelity
frame first** (the visual contract, gated by `mockups.test.tsx`), then the
production component. Keep `SemanticGraph.tsx` alive until the new canvas reaches
parity; swap behind a flag in `SemanticModelTab.tsx`.

### Phase 1 — New canvas skeleton + craft (frontend-only, no data change)
Blueprint §5, §5.1–§5.12. Build new pure modules beside the existing component:
- `layout.ts` — longest-path ranking + one-shot barycenter ordering + adaptive
  rank-x from present ranks + widest card (no fixed `COL_X`); **fact-center**
  re-ranking (§5.12) with a **Fact-center / Source-left** toggle.
- `routing.ts` — orthogonal paths, rounded elbows, index-stable crossing bridges,
  port fanning/channelization; the same gutter/lane/bridge discipline for
  measure/MV **lineage on select** (§5.10).
- `cardinality.ts` — orientation-aware crow's-foot / one-bar markers.
- `annotate.ts` — health headline, callout anchors (ungoverned region, worst cold
  spot, unmodeled/island), region + MV-membership boundary boxes, legend.
- Semantic-zoom bands **Overview / Standard / Columns**; Columns LOD uses a single
  `COL_TOP/COL_H/COL_PAD` band (§6) and draws join-column rows from `ON` leaf
  names client-side until Phase 2.
- Detail inset (mirror `NodeDetail`) for table / metric view / measure / Space
  config, listing measure **lineage → source tables**.

**Data needed: none new.** Everything is computable from today's
`SemanticGraphResponse`.

**Done when:** §11.3 "Canvas & layout", "Linework (P1 subset)", "Nodes",
"Lineage on select", "Annotations", "Detail inset" boxes are checked; §9 tests
pass; the P1 fidelity frame matches the prototype in dark + light over
3/10/30-table fixtures.

### Phase 2 — Column model (backend + frontend)
Blueprint §6. `SemanticGraphNode` (table) gains an optional `columns`
sub-structure (**participating columns only** — join keys + dimension bindings,
never the full list). A **server-side `ON`-predicate parser** emits
`(table, column) ↔ (table, column)` endpoints alongside the existing join edge
(retain `on` text for the inset). Frontend: cards expand to column rows; join
lines re-terminate at exact column ports; both columns light on select. Reuse the
single `COL_TOP/COL_H/COL_PAD` band; highlight join-key rows.

**Constraints:** parsing is server-side + unit-tested; additive (no `columns` →
renders exactly like Phase 1). Read the Genie `serialized_space` reference before
touching the parser. Keep TS/Pydantic types in sync.

**Done when:** Columns LOD is column-accurate against real data; additivity test
passes; backend parser unit tests green via `./scripts/test.sh`.

### Phase 3 — Join Advisor + Insights inset
Blueprint §7 (Join Advisor) and §7.5 (Insights).

- **Insights inset (frontend-only):** top **1–2** ranked deal-breakers from the
  existing `/api/spaces/{id}/scan` result (island, >30-table limit, ≥9 sources,
  wide table, cold spot, name collision). Severity colors; click-to-focus via
  `locateInGraph`; IQ Scan pointer; clean-state line. Only new wiring is a
  finding→node-id map. **Do not** duplicate or modify the IQ Scan.
- **Join Advisor (validated-seed model):** data-grounded candidates
  (`MvProposal`-shaped: FK metadata, name/type match, warehouse containment
  probe) rendered in an inset below the canvas. Each row shows evidence + a
  **containment-probe verdict bar** (validated / partial / unverified). Checking a
  candidate ghosts a dashed `proposed_join` **overlay** edge (never a base edge).
  - **Persistence = seed, not declaration.** Accepting persists the candidate as a
    **proposed seed** handed to Auto-Optimize (the existing `MvProposal` → run
    path), which re-validates and adds it via `add_join_spec`. **Never write it as
    a locked declared `join_spec`** — the optimizer can add/update but never remove
    a join (see §3), so a wrong declared join can't be undone.
  - **Guardrails:** a **confirm gate** before seeding a weak-containment (<50%)
    candidate; a standing **ground-truth warning**; reversible until seeded;
    button reads **"Seed to Auto-Optimize"**.

**Done when:** §11.3 "Insights inset" and "Join Advisor" boxes are checked; the
overlay never mutates base edges; the seed survives to a real Auto-Optimize run;
"arrows require proof" regression test still passes.

---

## 5. Testing & verification

- **Pure-function tests (primary):** `renderToStaticMarkup`, per blueprint §9 —
  byte-stable placement; index-stable bridge over/under; correct crow's-foot per
  `relationship` (no marker for unknown); zoom bands render exactly their detail
  set; annotations render iff backing data present; every base edge maps to a
  declared join; `proposed_join` only under the overlay; Phase-1 additivity.
- **Fidelity-frame gate:** extend `mockups.test.tsx` to assert the vocabulary
  renders (crow's-foot markers, bridges, callouts, headline, probe bars).
- **Frontend build/lint:** `cd frontend && npm ci && npm run build` and
  `npm run lint` must be clean.
- **Backend (Phase 2/3):** `./scripts/test.sh` (offline pytest) green.
- **Integration/E2E:** deploy to a real workspace — `./scripts/deploy.sh --update`
  — and verify in the Databricks App. **No local server.**

---

## 6. Workflow

- Work phase by phase; land the fidelity frame + tests before the production
  swap. Do not delete `SemanticGraph.tsx` until parity is reached and the flag is
  flipped.
- Only commit when the user asks. Keep exact-version deps; keep TS/Pydantic types
  mirrored. Update the blueprint note if an approved change alters the contract.
- If you hit a genuine fork the design doesn't cover, resolve toward the prototype
  for look/behavior and the blueprint for data/determinism; surface it, don't
  silently invent.

## 7. Do NOT
- Run `uvicorn` / `npm run dev` to "test", or `databricks bundle init`.
- Use `npm install` (use `npm ci`) or hand-edit `requirements.txt`.
- Add a layout/physics library or make the render non-deterministic.
- Draw a base edge without a declared relationship, or guess FACT/DIM roles.
- Write advisor selections as locked declared `join_specs`.
