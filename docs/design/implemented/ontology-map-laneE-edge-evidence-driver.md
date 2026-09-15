# Ontology Map — Lane E (edge-evidence enrichment) — MV-D88 — BUILD-READY

The data companion to Lane P2's richer relationship lines (MV-D87 §P0-b). Cross-links today
carry only `kind`/`verb`/`rel_class`/`weight` — enough for a verb caption, but not the
"what it shares" detail a knowledge-graph edge tooltip wants. This lane adds a compact,
reveal-don't-invent per-edge `detail` bag sourced from the fused signal graph. Additive,
blob-only (MV-D49 — NO DDL); degrades to today's contract (Lane P2 falls back to verb-only).

## 🚀 How to launch (Claude Code, isolated worktree)
> Worktree pinned to `ontology` HEAD (after the edge pre-seed carve lands), branch
> `laneE-edge`. Goal Mode; wheel/backend only; offline + green wheel tests; **STOP before
> deploy** (a human runs the scoped materialize + confirms `detail` present on edges).

## OWNS (wheel + backend)
- `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/layout.py`
  (emit per-edge `detail` from the fused-graph edge attrs, across all three edge sets)
- `backend/ontology/routers/graph.py` (flow `detail` through — pre-seed adds the field)
- `packages/genie-space-optimizer/tests/unit/test_ontology_graph.py` (+ new accept tests)
- `backend/tests/test_ontology_graph_route.py` (contract parity)

## OFF-LIMITS
`frontend/**` (Lane P2 owns the renderer; the `types.ts` optional `detail?` comes from the
edge pre-seed carve and Lane E does NOT edit `types.ts`), the mockup, the playbook, any DDL /
schema change (MV-D49 — enrichment rides the JSON blob), `src/watch/**`.

## MERGE-ORDER
0. **Edge pre-seed carve (prerequisite — lands on `ontology` first):** `OntologyGraphEdge.detail?`
   added in `models.py` + `types.ts` (+ `graph.py` passthrough). Branch from that HEAD.
1. Lane E and **Lane P2 (MV-D87)** are PARALLEL & independent; ship in either order (Lane E is
   inert until Lane P2 surfaces `detail`).
2. Merge after wheel tests green; a scoped `--update` deploy + materialize shows `detail` present.

## Spec / decisions
`docs/design/ontology-map-DESIGN.md` §3.3/§5 (typed edges + hover detail); MV-D88 (this lane);
additive to the MV-D82 edge contract; MV-D49 (blob-only, no DDL).

---
## PASTE-PROMPT (Goal Mode) — per-edge evidence bag, additive, no DDL

You are the Developer for Lane E (Ontology Map edge-evidence enrichment), branch `laneE-edge`,
worktree pinned to `ontology` HEAD. Additive/blob-only; STOP before deploy.

Emit an optional compact `detail: Dict[str,str]` on each edge in `layout.build_graph_snapshot`
(all three edge sets — assets/domains/subdomains), sourced ONLY from what the fused signal-graph
edge already carries (reveal-don't-invent; omit keys with no signal; `None`/omit when empty):
- `join_key` → `columns` (the shared/FK join column(s) if present), `kind: "foreign key"` vs
  `"shared column"` from the edge `source`.
- `co_query` → `co_queried` as a plain count/label derived from the edge `weight` (e.g. "42
  sessions"); never a raw float.
- `lineage_adjacency` → `flow: "feeds"` (source→target direction).
- `mv_membership` → `role: "aggregates"` (+ measure count if cheaply available from `snippets`).
- `semantic_sim` → `similarity` as a plain band/label from `weight` (not a bare float).
- `agent_scope` → `role: "queries"`. Unknown kinds → omit `detail` (None).

Contract: the pre-seed carve added `OntologyGraphEdge.detail` (models.py + types.ts); Lane E only
FILLS it wheel-side and flows it through `graph.py` `_level` (already constructs `OntologyGraphEdge(**e)`).
Do NOT edit `types.ts`.

Gates: `./scripts/test.sh` green + NEW accept tests: (a) a `join_key` edge carries `detail.columns`
when the fused edge has them and omits it otherwise; (b) `co_query`/`semantic_sim` render plain
labels not raw floats; (c) an edge with no signal has `detail=None`; (d) byte-stable determinism with
the new key. Contract parity in `test_ontology_graph_route.py` (additive `detail: None`). Commit on
`laneE-edge`. Do NOT merge/deploy, no Databricks/GSO job or API, no governed tags, no dependency
(`uv.lock` byte-identical), no uvicorn.
