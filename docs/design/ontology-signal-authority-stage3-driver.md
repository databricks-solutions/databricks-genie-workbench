# Ontology Signal Authority — Stage 3 (Centrality: PageRank) · Goal-Mode driver (MV-D96)

> **One stage per run, on the `ontology` branch.** Build spec:
> `ontology-signal-authority-build.md` §5 (authoritative). Stage 3 is the deepest change and the
> spec splits it into **three independent, harness-gated sub-parts**. This driver is **sub-part 1
> of 3 — PageRank centrality only** (the deterministic prerequisite the other two build on).
> Sub-parts 2 (certified-seeded personalized-PageRank assignment) and 3 (usage-weighted clustering
> + PageRank sub-domain hubs) are **separate follow-on drivers (Stage 3b / 3c)** — do NOT attempt
> them here. Additive, read-only, no new table/dependency. STOP before deploy.

## Why now
Stage 1 (popularity) and Stage 2 (certification authority) are deploy-verified and the §10 harness
(MV-D59) is live to gate this. Today the `centrality` factor is **degree** over just two edge kinds,
so authority from heavily-used dashboards/agents/MVs and declared FKs never reaches the tables they
touch. A real PageRank over the *full* fused graph is the honest centrality signal — and it is the
machinery sub-parts 2/3 reuse, so it lands first, alone, harness-gated.

## Grounded facts (code, 2026-09-16)
- `graph.lineage_centrality(signal_graph)` (`graph.py` ~L219–244) is **degree centrality** over
  ONLY `lineage_adjacency` + `co_query`, normalized by max degree → `{bare_fqn: [0,1]}`; edgeless ⇒
  `{}` (honest-gap). It ignores `join_key`, `mv_membership`, `agent_scope`, `dashboard_scope`.
- Seam: `materialize.py:821` calls `centrality=graph.lineage_centrality(signal_graph)` →
  `RankSignals.centrality` → `rank.blend` (0.35 factor). Only this one call site changes.
- `igraph` is ALREADY a lazy import in `cluster.py` (~L226) — **no new dependency** (MV-D45). Use the
  same lazy pattern in `graph.py`.
- Signal-graph nodes are prefixed (`asset:`/`tag:`/`mv:`/`agent:`/`dashboard:`/`schema:`); rank
  addresses assets by **bare FQN** (the `asset:` prefix stripped) — the new function must key the
  same way as `lineage_centrality`.

## Testability seam
Add a pure `pagerank_centrality(signal_graph) -> {bare_fqn: float}` beside `lineage_centrality` in
`graph.py` (igraph lazy-imported INSIDE the function). It is deterministic (fixed params, sorted
inputs) and I/O-free. `rank.py`'s blend arithmetic is untouched (MV-D35).

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 3 sub-part 1 (PageRank centrality)** of
`docs/design/ontology-signal-authority-build.md` §5.1 on the `ontology` branch. This sub-part ONLY —
do NOT build certified-seeded assignment (§5.2) or usage-weighted clustering / sub-domain hubs
(§5.3); those are separate drivers. Additive, read-only, offline-green, STOP before deploy.

BUILD A — pure PageRank (wheel, `graph.py`, unit-tested):
- Add `pagerank_centrality(signal_graph: dict) -> dict[str, float]` beside `lineage_centrality`.
  Build a graph over the fused nodes touched by the structural + hub edge kinds — `lineage_adjacency`,
  `co_query`, `join_key`, `mv_membership`, `agent_scope`, `dashboard_scope` — so authority flows from
  heavily-connected dashboards/agents/MVs and FK spines into the tables they touch. Run `igraph`
  PageRank with FIXED params (damping 0.85, fixed tol/max-iters), then keep the **asset** vertices
  only (`asset:` prefix), normalize to `[0,1]` by the max asset score, key by **bare FQN**.
- `igraph` lazy-imported INSIDE the function. If `igraph` is unavailable OR the graph is edgeless,
  DEGRADE to `lineage_centrality(signal_graph)` (MV-D43/D45) — never raise, never a false 0.
- Deterministic: sort vertices/edges before building; identical output across two runs.

BUILD B — wire it (one call site):
- `materialize.py:821` — replace `centrality=graph.lineage_centrality(signal_graph)` with
  `centrality=graph.pagerank_centrality(signal_graph)`. No other change; `RankSignals.centrality`
  contract (bare-FQN → [0,1]) is byte-identical in shape.

BUILD C — tests (`test_ontology_graph.py` / centrality tests):
- On a known small graph a load-bearing spine (fan-in/out) outranks a leaf; ordering matches
  expected PageRank; result is stable across two calls (determinism).
- A hub edge lifts authority: an asset reachable only via `mv_membership`/`agent_scope`/
  `dashboard_scope` gets a non-zero, higher score than an isolated leaf (proves the extra kinds
  are included — the degree function would miss it).
- Degrade: monkeypatch the `igraph` import to raise ⇒ output equals `lineage_centrality` exactly;
  an edgeless graph ⇒ `{}`.
- Blend: a proposal whose anchor gains centrality has `factors.centrality.present == true`; the
  blend/tier code is unchanged.

GUARDRAILS (hard): do NOT change `FACTOR_WEIGHTS`/`blend`/`coverage_cap`/`tier_of`/`confidence_band`
or any firewall (MV-D35) — this feeds a better centrality input, it does not re-weight/re-tier. Do
NOT touch clustering (`cluster.py`), sub-domain derivation, or naming (§5.2/§5.3 are later drivers).
No new Delta table/column (MV-D49); NO new dependency — `igraph` is already lazy in `cluster.py`;
`uv.lock`/`package-lock.json` byte-identical (MV-D45); read-only, no governed-tag write (MV-D26);
deterministic (MV-D82). Live path default-correct: no `igraph` ⇒ degree fallback = today's bytes.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted graph/rank/materialize tests green;
`uv.lock` + `frontend/package-lock.json` untouched; only wheel files changed. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
`SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`,
then `run-now` the materialize job (`512526067383740`) with
`catalog_allowlist=["serverless_stable_tbzqg7_catalog"]`, `domain_facet_denylist=["modeled"]`,
and `industry_alignment_enabled=true`, `industry_alignment_reference_model=retail` (so the MV-D59
harness produces P/R/F on this retail estate). Capture the current `genie_ont_eval` row FIRST.
Confirm:
- `evidence.rank.factors.centrality.present == true` on surfaced anchors; the busiest spine/hub
  tables carry the highest centrality (sanity-order the top few).
- The MV-D59 harness P/R/F + structural health are **flat-or-up** vs the captured baseline — if a
  metric regresses, the sub-part does not merge (spec §5 gate).
- MV-D99 loyalty taxonomy still intact; no domain named `certified` (Stage-2 invariant holds).
