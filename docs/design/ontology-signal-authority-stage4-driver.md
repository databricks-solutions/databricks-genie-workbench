# Ontology Signal Authority — Stage 4 (visual encoding: popularity-as-size, certification-as-ring) · Goal-Mode driver (MV-D97)

> **One stage per run, on the `ontology` branch.** Build spec:
> `ontology-signal-authority-build.md` §6 (authoritative). The LAST Signal Authority sub-part —
> Stages 1-3 are deploy-verified; this makes the enriched signals legible on the map (a PageRank-
> graph read: big = popular, ringed = certified, thick = joined). Mostly **frontend + dual-theme**,
> plus **ONE thin additive producer hop** (§6 assumed "frontend-only", but the certified/deprecated
> signal is not surfaced to the snapshot yet — it must be, additively). Determinism-preserving,
> additive, no contract break, **FULL frontend build**, and **STOP before deploy** for a human MV-D80
> visual-review round.

## Why now
Stage 1 (popularity) → the rank `score`, Stage 2 (certification) → the `certification` map, Stage 3
→ PageRank centrality are all live, but the map still renders every node the same static size with no
authority cue. §6 reads those signals into radius / ring / edge-thickness channels.

## Grounded facts (code, 2026-09-17)
- `OntologyGraphNode.size: number` ALREADY ships (`layout._compute_node_size`, clamped `[0.5,2.0]`,
  from rank `score` — which includes the 0.40 usage factor — + `cost`). But `estateGraphModel.ts`
  DROPS it: `EstateNode` has no `size`, and `ontologyTreeLayout.ts` sets `radius: cfg.radius[node.type]
  ?? 10` (static per-type). → radius-from-popularity is a pure frontend read of an existing channel.
- `OntologyGraphEdge.weight?: number` ALREADY ships (co_query/join_key) → edge thickness is
  frontend-only too. Cross-link arcs currently render a fixed stroke in `EstateGraph.tsx`.
- Confidence tier is ALREADY encoded (`transforms.confidence_band` → `bandColor`) — reuse as-is.
- Certified/deprecated is NOT in the payload: `layout._node_meta` only emits rows/format/freshness/
  queries_28d; the Stage-2 `certification` map (`materialize`, `{fqn:"certified"|"deprecated"}`) never
  reaches `build_graph_snapshot`. → the ONE thin producer hop: stamp it onto asset-node `meta`.
- `EstateNode.meta` is already threaded to render (GraphInspector/tooltip) → once producer adds the
  keys, the frontend reads them for free. Tokens live in `graphTokens.ts` (MV-D79, dual-theme, AA);
  the MV-D80 harness contact sheet (`frontend/src/ontology/harness`) is the review loop.

## Testability seam
Producer: `build_graph_snapshot(..., certification=None)` stamps `meta["certified"]/["deprecated"]`
when a node FQN is in the map (absent ⇒ omitted ⇒ byte-identical). Frontend: thread `size`+`weight`
onto `EstateNode`/`EstateEdge`; `ontologyTreeLayout` scales the base per-type radius by a bounded
log fn of `size`; `EstateGraph` reads `meta.certified` (green ring/check) / `meta.deprecated` (muted)
and scales arc stroke by `weight`. All deterministic; degrade-clean (no size ⇒ base radius; no
certified ⇒ plain node).

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 4 (visual encoding)** of `ontology-signal-authority-build.md` §6 on the `ontology`
branch. Additive, dual-theme, determinism-preserving. STOP before deploy (human MV-D80 review).

BUILD A — popularity → radius + join strength → edge thickness (FRONTEND ONLY, no payload change):
- `estateGraphModel.ts`: thread the existing `OntologyGraphNode.size` onto `EstateNode` (new
  `size?: number`) and `OntologyGraphEdge.weight` onto the cross-link/edge model. Both already ship.
- `ontologyTreeLayout.ts`: scale the base `cfg.radius[node.type]` by a BOUNDED log fn of `size`
  (e.g. `base * (1 + k*ln(1+size))` clamped to a max ~1.6×) so a popular hub reads bigger without a
  hairball; a node with no `size` keeps the base radius exactly (MV-D43). Deterministic (fixed `k`).
- `EstateGraph.tsx`: scale cross-link arc `strokeWidth` by a bounded fn of `weight` (missing weight ⇒
  today's fixed stroke). Reuse the existing `bandColor`/confidence tier UNCHANGED.

BUILD B — certification → ring (THIN PRODUCER HOP + frontend read):
- `layout.build_graph_snapshot`: add keyword `certification: Mapping[str,str] | None = None`; when an
  asset node's FQN maps to `"certified"`/`"deprecated"`, add `meta["certified"]="true"` /
  `meta["deprecated"]="true"` (else omit). Absent map ⇒ byte-identical snapshot. Thread the Stage-2
  `certification` map into the call in `materialize.run_materialize`.
- `EstateGraph.tsx`: when `node.meta?.certified`, draw a green ring + check glyph using a `graphTokens`
  token that clears WCAG AA on BOTH themes (add one if needed, MV-D79); when `node.meta?.deprecated`,
  a muted/hatched de-emphasis (Catalog-Explorer restricted style). No meta ⇒ plain node.

BUILD C — tests:
- Wheel (`test_ontology_graph.py`, where `build_graph_snapshot` is tested): `certification` stamps `meta.certified/deprecated` on matching
  asset nodes; `certification=None` (and an unmatched FQN) ⇒ byte-identical meta.
- Frontend (vitest): model threads `size`/`weight`; `ontologyTreeLayout` radius grows with `size` and
  clamps, no-size ⇒ base radius; `EstateGraph` renders the ring iff `meta.certified` and the muted
  style iff `meta.deprecated`; arc stroke grows with `weight`.
- Harness: regenerate the MV-D80 contact sheet (`frontend/src/ontology/harness`) for human review.

GUARDRAILS (hard): additive only — the producer change is a defaulted keyword and an absent map is
byte-identical (MV-D45/D82); do NOT change `rank.py`/`cluster.py`/blend/tiering, `_compute_node_size`,
any DDL/table/column, or the graph payload shape (new node meta keys are additive + optional). No new
dependency; dual-theme + WCAG AA (MV-D79) with the no-hairball/legibility bars (MV-D80). Deterministic;
degrade-clean (no size ⇒ base radius, no weight ⇒ fixed stroke, no certified ⇒ plain node).

ACCEPTANCE (offline): `./scripts/test.sh` green (layout tests incl. the byte-identical case); from
`frontend/`: `npx tsc -b` clean, `npm run lint` clean, vitest green; the MV-D80 contact sheet
regenerated. `uv.lock`/`frontend/package-lock.json` untouched. Then STOP for human visual review.

---

## Deploy-verify gate (human, after offline-green + MV-D80 review)
FULL frontend build this time (SKIP_FRONTEND_BUILD OFF so the visual ships):
`DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`, then `run-now` job
`512526067383740` with `catalog_allowlist=["serverless_stable_tbzqg7_catalog"]`,
`domain_facet_denylist=["modeled"]`, `industry_alignment_enabled=true`,
`industry_alignment_reference_model=retail`. Confirm:
- The map reads as popularity-SIZED + certification-RINGED in BOTH light + dark themes; deprecated
  assets read de-emphasized; edge thickness tracks join/co-query strength; MV-D80 contact-sheet +
  §8 rubric PASS.
- Snapshot is otherwise byte-identical (only additive `meta` keys); MV-D59 harness flat; MV-D99
  loyalty taxonomy intact. This is a pure-read visual layer — no P/R/F movement expected.
