# Ontology Signal Authority — Stage 4b (thread PageRank centrality → node_scores → size) · Goal-Mode driver (MV-D97)

> **One stage per run, on the `ontology` branch.** Build spec:
> `ontology-signal-authority-build.md` §6 (authoritative). A tiny **backend-only** follow-on to
> Stage 4: Stage 4 lit up the popularity-as-SIZE *channel* (`ontologyTreeLayout.scaleRadius` reads
> `OntologyGraphNode.size`), but the producer passes `node_scores=None`, so every node's `size`
> is the `1.0` default and the channel renders flat. This threads the ALREADY-computed Stage-3a
> PageRank centrality into `node_scores` so hubs finally read bigger. Additive, determinism-
> preserving, degrade-clean, **harness-gated**, and **STOP before deploy** for a human MV-D80
> visual-review round. No frontend change (Stage 4 already consumes `size`).

## Why now
Stage 4 deploy-verify (tbzqg7 run `742471949652671`) confirmed asset `size` is a uniform `1.0`
across all 131 assets — because `materialize` discards the popularity signal before layout. The
signal already exists one line away; this is a one-hop wiring fix, not new machinery.

## Grounded facts (code, verified live 2026-09-17)
- `materialize.py:978` calls `build_graph_snapshot(..., node_scores=None, ...)` → the producer's
  `_compute_node_size(node, node_scores.get(node_id, 0.0))` sees `0.0` for EVERY node → `size=1.0`
  for all assets (confirmed live: asset `size` distinct = `{1.0}`; `cost` is `None` for all).
- `graph.pagerank_centrality(signal_graph)` is ALREADY computed at `materialize.py:837` (fed to
  `RankSignals.centrality`): a per-asset `[0,1]` map, busiest asset = `1.0`, **keyed by BARE FQN**
  (the `asset:` prefix stripped — `graph.py:268`). It degrades to `{}` if `igraph` is unavailable
  or the graph is edgeless (`graph.py:283-291`).
- `_compute_node_size(node, score)` (`layout.py:874`) maps `score>0 → size=max(1.0, 0.5+score)`
  clamped `[0.5,2.0]` (+ a cost boost, dead here since `cost=None`). So a hub at centrality `1.0`
  → `size 1.5`; an asset below `0.5` stays `1.0`. A few hubs pop, leaves stay base — exactly
  "big = popular". (`_compute_node_size`'s curve is UNCHANGED here — guardrail.)
- **KEY MISMATCH to handle:** `_compute_node_size` is reached via `node_scores.get(node_id)` where
  `node_id` is the RAW `asset:<fqn>`; `pagerank_centrality` is keyed by BARE `fqn`. Re-key when
  building `node_scores`.
- Frontend already threads + scales `size` (Stage 4: `estateGraphModel` + `scaleRadius`) → **no
  frontend change**. This is backend-only.
- `cost=None` and `co_query=0` (flat edge weight) are SEPARATE upstream signal gaps — out of scope.

## Testability seam
`build_graph_snapshot` already takes `node_scores` (a defaulted param); pass a re-keyed centrality
map. An empty centrality (igraph missing / edgeless) ⇒ `{}` ⇒ `size=1.0` for all ⇒ snapshot
BYTE-IDENTICAL to today (MV-D43/D45/D82). Deterministic — PageRank is a sorted-input PRPACK direct
solve (`graph.py`), so two runs match.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 4b (thread PageRank centrality → node_scores)** of
`ontology-signal-authority-build.md` §6 on the `ontology` branch. **BACKEND-ONLY** — the frontend
already scales node radius by `OntologyGraphNode.size` (Stage 4). Additive, determinism-preserving,
degrade-clean. STOP before deploy (human MV-D80 review).

CHANGE (`materialize.run_materialize`, `materialize.py`):
- Compute PageRank centrality ONCE into a local and reuse it for `RankSignals` — do NOT call it
  twice: `centrality = graph.pagerank_centrality(signal_graph)` then `RankSignals(centrality=
  centrality, ...)`.
- Build the layout score map, re-keyed to the layout node-id space (centrality is keyed by BARE
  `fqn`; `_compute_node_size` is looked up by the raw `asset:<fqn>` node id):
  `node_scores = {f"asset:{fqn}": v for fqn, v in centrality.items()}`.
- In the `build_graph_snapshot(...)` call, replace `node_scores=None` with `node_scores=node_scores`.

DO NOT change `_compute_node_size` (keep its existing `[0.5,2.0]` curve), `pagerank_centrality`,
`rank.py`/`cluster.py`/blend/tiering, any DDL/table/column, or the payload SHAPE. `size` already
ships in the blob — only its VALUES change.

GUARDRAILS (hard): additive + degrade-clean — an empty centrality map (igraph unavailable OR an
edgeless graph, where `pagerank_centrality` returns `{}`) ⇒ `node_scores={}` ⇒ every
`_compute_node_size(node, 0.0)` ⇒ `size=1.0` ⇒ snapshot BYTE-IDENTICAL to today (MV-D43/D45/D82).
Deterministic (sorted-input PRPACK direct solve). No new dependency. No frontend change.

TESTS:
- Wheel (`test_ontology_graph.py`, where `build_graph_snapshot` is tested): with a `node_scores`
  map, a high-score asset (≥0.5) gets `size>1.0` and the top-scoring asset the largest; with
  `node_scores={}` all sizes are `1.0` (the byte-identical case). Add/extend a `run_materialize`-seam
  test asserting `node_scores` is DERIVED from `pagerank_centrality` and re-keyed to `asset:<fqn>`
  (a centrality entry lands on the matching asset node's `size`).
- No frontend change ⇒ run the existing frontend vitest to confirm it stays green.

ACCEPTANCE (offline): `./scripts/test.sh` green (incl. the `node_scores={}` byte-identical case);
from `frontend/`: `npx tsc -b` + `npm run lint` + vitest green (unchanged). `uv.lock`/
`frontend/package-lock.json` untouched. Then STOP for human review.

---

## Deploy-verify gate (human, after offline-green)
Backend-only, so a FULL frontend build is not required (harmless if run):
`DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`, then `run-now` job
`512526067383740` with `catalog_allowlist=["serverless_stable_tbzqg7_catalog"]`,
`domain_facet_denylist=["modeled"]`, `industry_alignment_enabled=true`,
`industry_alignment_reference_model=retail`. Confirm:
- The snapshot's asset `size` now VARIES (distinct > 1; top-PageRank hubs ~1.5, leaves 1.0) — query
  `genie_ont_graph_snapshot.graph`.
- The map reads as popularity-SIZED (hubs bigger) in BOTH light + dark themes; Stage-4 certification
  rings intact.
- MV-D59 harness FLAT — `size` is visual-only, never scored, so P/R/F/singleton/depth are unchanged
  vs the Stage-4 baseline (P1.00 / R0.923 / F0.96 / singleton 0.0 / depth 2).
