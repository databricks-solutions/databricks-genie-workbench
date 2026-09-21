# Ontology — Signal-graph edge coverage (lineage / co-query / mv-membership / semantic) · Goal-Mode driver

> **One PHASE per run, on the `ontology` branch** — this driver is CHEAP-FIRST PHASED (Phase 0→3),
> each phase its own commit + STOP at a harness gate. Additive, degrade-safe (MV-D43). Closes
> Stage 4.1j/4.1k **follow-up (b)**: `lineage_adjacency`/`co_query`/`mv_membership`/`semantic_sim`
> yield **0** related whys on the live estate. Proposed register line: **MV-D105** (fused-graph
> structural-edge coverage: a real lineage producer, a query-history co-query producer, an
> mv-membership coverage confirm, and — optional — asset-level semantic edges).

## Why now
Stage 4.1j made Page **Related assets** a graph-traversal feature and Stage 4.1k ranked the page↔page
pass. The live deploy-verify showed Related is carried almost entirely by `join_key`, `dashboard_scope`
and `agent_scope`; the four *other* relatedness kinds return **0 whys**. Read-only triage
(2026-09-20) found these are NOT one problem — three are code/wiring gaps and one is data coverage.
Wiring them enriches Related AND improves **domain detection**: `cluster.py` weights `co_query` at 2.0,
lists `lineage_adjacency`/`co_query` in `STRUCTURAL_KINDS`, and runs per-domain PageRank over
`join_key`+`co_query` (`cluster.py:82,88,676-689`) — so today domains are detected with two of four
structural signals EMPTY. This is core signal coverage, not cosmetics.

## Grounded facts (code, 2026-09-20) — CORRECTED triage
- **`lineage_adjacency` — CODE gap (plumbing present).** `reader.lineage_edges()` is a hardcoded
  `return []` STUB (`run_ontology_materialize.py:616-618`), yet the SAME reader class already reads
  `system.access.table_lineage` for `dashboard_scope` (`:568-589`) and `usage_signals` (`:951-964`).
  It is passed positionally into `graph.build_signal_graph(...)` (`materialize.py:707`), so a real
  producer lights up immediately.
- **`co_query` — CODE gap (no producer).** No `co_query_edges` producer exists; the
  `build_signal_graph` call never passes `co_query_edges` (`materialize.py:706-715`), though the graph
  (`graph.py:37,130-137`) AND the clusterer both expect it.
- **`semantic_sim` — needs a NEW asset producer (NOT a free ER rewire).** ER is TAG-scoped:
  `er.candidates_from_graph` builds `tag` candidates (`er.py:209`) and `run_er` merges only
  `kind=="tag"` (`er.py:256`), and it runs AFTER the graph is built (`materialize.py:740`). Tag
  similarity never touches `asset:` anchors, so asset Related needs asset embeddings + pairwise
  cosine → `semantic_sim_edges` (`graph.py:43,173`). Softest signal (`_KIND_PRIOR["semantic_sim"]=0.4`,
  `graph.py:408`). DEFERRED / optional last phase.
- **`mv_membership` — DATA/coverage (already wired).** `reader.mv_membership()`
  (`run_ontology_materialize.py:678-711`) reads estate MV-YAMLs via `metric_view_fqns` →
  `estate_metric_view_yamls` → `schema_signals.mv_membership_map`, is passed at `materialize.py:713`,
  and returns `{}` only when the estate surfaces no MVs. VERIFY before assuming a bug.
- Consumers unchanged: `graph.related_assets` (`graph.py:443`) already traverses all of
  `_RELATED_KINDS` (`graph.py:397-408`); `cluster.cluster(signal_graph, ...)` (`materialize.py:777`).

## Testability seam
Additive to the reader (`run_ontology_materialize.py`: real `lineage_edges`, new `co_query_edges`)
and ONE `build_signal_graph` call site + `_gather_structural_signals` (`materialize.py`). Producers
degrade to `[]`/`{}` on any missing grant via `_rows_safe` (MV-D43), allowlist-scoped and
schema-denylist filtered exactly like `join_key_edges`. **No new job parameter** (window is a module
constant), so no databricks.yml / gso_job.py / job_launcher lockstep. Because Phases 1–3 feed
CLUSTERING (not just Related), the merge gate is the real §10 harness (`genie_ont_eval`,
MV-D59 flat-or-up) — NOT byte-identical.

---

## GOAL PROMPT (paste verbatim into Goal Mode — run ONE phase, then STOP)

Implement **Signal-graph edge coverage** on the `ontology` branch, ONE PHASE per run, CHEAP-FIRST.
Additive, degrade-safe (MV-D43), no new job parameter, no new router, no new dep. Proposed
**MV-D105**. Each producer is allowlist-scoped + schema-denylist filtered + `_rows_safe` degrade-to-
empty; an empty result ⇒ byte-identical; a non-empty result SHIFTS clustering, so each phase must
clear the §10 harness (`genie_ont_eval`) FLAT-OR-UP (MV-D59) — validate on an alignment-ON run.

PHASE 0 — mv_membership coverage confirm (verify-only, tiny). Add a read-only diagnostic/test that
calls `reader.metric_view_fqns(allowlist)` + `reader.mv_membership(allowlist)` and asserts the
mapping is non-empty WHEN MVs exist; if the estate has MVs but membership is empty, fix the
`estate_metric_view_yamls` read; if it has none, record "data gap — no MVs on estate" and close the
`mv_membership` limb of follow-up (b). No graph change.

PHASE 1 — real lineage_adjacency producer (cheapest real win). Replace the `lineage_edges()` stub
(`run_ontology_materialize.py:616-618`) with a `system.access.table_lineage` read that emits
`(source_table_full_name, target_table_full_name)` asset↔asset pairs — BOTH non-null, lower-cased,
deduped, allowlist-scoped (both endpoints in an allowlisted catalog), schema-denylist filtered —
reusing the `usage_signals` read shape (`:951-964`) and `_rows_safe` degrade. It is already passed
positionally at `materialize.py:707`, so no call-site change. Verify Related gains "Connected in table
lineage" whys and the clustering backbone strengthens.

PHASE 2 — co_query producer (query.history co-occurrence). Add `reader.co_query_edges(allowlist)`:
from `system.query.history` (or `system.access.audit` read events) over a trailing window (module
constant, e.g. 30 days), build asset↔asset co-occurrence pairs (tables referenced by the same
statement/session), COUNT-weighted and normalized to a bounded weight, capped fan-out, allowlist +
denylist filtered, `_rows_safe` degrade. Thread it into `build_signal_graph(..., co_query_edges=...)`
(`materialize.py:706-715`) and, if the clusterer reads it from the structural bundle, into
`_gather_structural_signals` (`materialize.py:343-363`). Verify "Frequently queried together" whys +
non-empty `co_query` in clustering (weight 2.0, `cluster.py:82`).

PHASE 3 — asset semantic_sim (OPTIONAL, softest, last). Only if Phases 1–2 leave Related thin: embed
asset name+comment via the sanctioned similarity backend (`similarity.get_similarity_backend`),
compute bounded pairwise cosine above a threshold → asset↔asset `semantic_sim_edges`, pass into
`build_signal_graph(..., semantic_sim_edges=...)`. Bounded (top-K per asset), deterministic, degrade
to `[]`. Prior 0.4 — it ranks BELOW every structural kind by design.

GUARDRAILS (every phase): additive; producer degrades to empty on missing grant (MV-D43); empty ⇒
byte-identical; allowlist-scoped + schema-denylist filtered (mirror `join_key_edges`); NO new job
parameter (window/threshold are module constants); no new router/dep; lockfiles untouched.

TESTS (wheel, per phase): P0 — mv_membership non-empty-when-MVs-exist + empty-estate degrade.
P1 — `lineage_edges` emits source→target pairs from fixture rows, drops null/out-of-scope/denylisted,
degrades to `[]` on read error; `related_assets` surfaces a lineage neighbor with the
"Connected in table lineage" why. P2 — `co_query_edges` builds COUNT-weighted co-occurrence, caps
fan-out, degrades to `[]`; graph carries `co_query`; `related_assets` surfaces the co-query why.
P3 — bounded top-K asset semantic edges, threshold gate, deterministic, degrade to `[]`. Each phase:
a `genie_ont_eval` flat-or-up assertion on a fixture with the new edges present.

ACCEPTANCE (offline, per phase): `./scripts/test.sh` green (report the new floor + update the
playbook copy in the SAME commit — the two parity-mirrored floors move together, test_rules_parity
enforces it); `git status -- uv.lock` clean. Then STOP for deploy-verify.

---

## Deploy-verify gate (human, after each phase's offline-green — the STOP checkpoint)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (backend/wheel only — no frontend change).
**`deploy.sh` reads `GENIE_DEPLOY_PROFILE` from `.env.deploy` (today `fevm-serverless` → 6t92c3) and
IGNORES a `DATABRICKS_CONFIG_PROFILE=…` prefix.** These producers run in the materialize batch, so
re-trigger the ontology-materialize job with the app's `run_now` recipe — pass `workspace_id` +
`metastore_id` (MV-D49; the job's empty default is a no-op on the app dataset; replay a prior
app-triggered run's `job_parameters`). Then verify against `genie_ont_pages` (app-keyed rows):
1. The phase's edge kind now produces related whys — Phase 1: `"Connected in table lineage"`;
   Phase 2: `"Frequently queried together"`; Phase 3: `"Semantically similar"` — count > 0 in
   `evidence.asset_why`, and `related_fqns` carries the new neighbors (no invented/empty FQN).
2. Clustering health did not regress: `genie_ont_domains`/`genie_ont_pages` counts are sane and the
   §10 harness `precision/recall/f1` is **flat-or-up** vs the prior run. Because P/R/F are `None`
   unless a run opts into industry alignment (MV-D44), run this verify with `industry_alignment_enabled=true`
   (and a reference model) so the gate is meaningful — otherwise only the structural-health metrics
   (singleton/orphan/depth/branching) are comparable.
Review with a human — the new edges are grounded (real lineage/query-history, no invented FQNs), the
whys read plainly, and the harness held — before marking the phase BUILT and (after the final phase)
registering **MV-D105**.
