# Ontology Map — Lane D2 (snapshot node enrichment: description + meta + deeper containment) — MV-D86 — BUILD-READY

Closes the ~60% **data half** of the north-star gap (region analysis 2026-09-07): even a
perfect tooltip has almost nothing to show because a snapshot node carries only
`kind`/`member_count`/`cost`/`attach_level`, and trees render as flat lists of tables under a
sub-area (no agent⊃MV⊃table depth). Additive, blob-only (MV-D49 — NO DDL), degrades to today's
contract; renderer (Lane P) surfaces these fields when present.

## 🚀 How to launch (Claude Code, isolated worktree)
> Worktree pinned to `ontology` HEAD (after the pre-seed carve lands), branch `laneD2-enrich`.
> Run the paste-prompt in Goal Mode; wheel/backend only; offline + green wheel tests; **STOP
> before deploy** (a human runs the scoped materialize + eyeballs meta present).

## OWNS (wheel + backend)
- `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/layout.py`
  (emit per-node `description` + a compact `meta` bag; strengthen asset→asset containment)
- `backend/ontology/routers/graph.py` (fill `description`/`meta` from the blob — the fields
  exist from the pre-seed carve)
- `packages/genie-space-optimizer/tests/unit/test_ontology_graph.py` (+ new accept tests)
- `backend/tests/test_ontology_graph_route.py` (contract parity)

## OFF-LIMITS
`frontend/**` (Lane P owns the renderer; the `types.ts` optional `description?`/`meta?` come
from the pre-seed carve and Lane D2 does NOT edit `types.ts`), the mockup, the playbook, any
**DDL / schema change** (MV-D49 — enrichment rides the JSON blob), `src/watch/**`.

## MERGE-ORDER
0. **Pre-seed carve (prerequisite — lands on `ontology` first):** `OntologyGraphNode.description?`
   + `.meta?` added in `models.py` + `types.ts` (+ `graph.py` passthrough). Branch from that HEAD.
1. Lane D2 and **Lane P (MV-D85)** are PARALLEL & independent. Lane D2 is inert (null/omitted
   fields) until Lane P surfaces it — ship them in either order.
2. Merge after wheel tests green; a scoped `--update` deploy + materialize shows `meta` present.

## Spec / decisions
`docs/design/ontology-map-DESIGN.md` §3.4 (data-contract gap); MV-D86 (this lane); additive to
the MV-D82 contract + MV-D73 snippet layer; MV-D49 (metastore grain, blob-only, no DDL).

---
## PASTE-PROMPT (Goal Mode) — enrich the snapshot node payload, additive, no DDL

You are the Developer for Lane D2 (Ontology Map data enrichment), branch `laneD2-enrich`,
worktree pinned to `ontology` HEAD. Additive/blob-only; STOP before deploy.

1. **Per-node `description`:** domains/sub-domains ← `genie_ont_domains.description`; assets ← the
   UC table/MV comment already carried in the fused 17d signal graph (else null). Thread it onto
   each node in `layout.build_graph_snapshot` and pass through in `graph.py`.
2. **Per-node `meta` bag** (compact `Dict[str,str]`, ALL optional, omit keys with no signal — never
   fabricate): `table` → rows, format, freshness, path; `metric_view` → measures (count),
   dimensions, freshness; `measure` → expression, format; `agent` → sample_questions, queries_28d;
   `dashboard` → viewers_28d, refresh. Pull ONLY from what the 17d inventory / signal graph already
   has (reveal-don't-invent, MV-D82 discipline).
3. **Deeper containment:** ensure `agent ⊃ metric_view ⊃ table` renders as tree children — populate
   asset `parent_id` + `attach_level` from the existing `agent:`/`mv:`/`asset:` edges in the fused
   graph so sub-areas aren't flat lists of tables. Measures stay expand-on-demand (MV-D73), NOT in
   the base snapshot.
4. Everything additive to the JSON blob (MV-D49): NO DDL, NO new grant, NO new dependency. Degrade
   cleanly — emit `null`/omit when a signal is missing; the map still renders today's contract.

Gates: `./scripts/test.sh` green + NEW accept tests: (a) a node carries `description` when the
source has one; (b) `meta` carries the type-appropriate keys when signals exist and omits them
otherwise; (c) containment depth reaches `agent→metric_view→table` on a fixture (not a flat
sub-area); (d) byte-stable determinism with the new keys. Contract parity: update
`test_ontology_graph_route.py` for the additive `description`/`meta: None`. Commit on
`laneD2-enrich`. Do NOT merge/deploy, no Databricks/GSO job or API, no governed tags, no uvicorn.
