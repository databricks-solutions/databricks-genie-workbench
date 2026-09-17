# Ontology Signal Authority — Stage 3c (usage-weighted clustering + PageRank sub-domain hubs) · Goal-Mode driver (MV-D96)

> **One stage per run, on the `ontology` branch.** Build spec:
> `ontology-signal-authority-build.md` §5.3 (authoritative). This is **sub-part 3 of 3** — the last
> and DEEPEST, the only one that touches `cluster.py` (community detection + sub-domain derivation).
> 3.1 (`pagerank_centrality`, `09491fbd`) and 3.2 (`certified_home`, `6c5ce11e`) are deploy-verified.
> **Two INDEPENDENT, each-harness-gated builds** — Goal Mode MAY land them as two commits; a build
> that does not hold-or-improve the MV-D59 harness (P/R/F + structural health) does NOT merge.
> Additive, read-only, no new table/dependency, off-safe. STOP before deploy.

## Why now
Boundaries today ignore traffic (an incidental unqueried FK pulls as hard as the load-bearing spine)
and, where no explicit boundary exists, sub-domains fall back to raw FK/MV components with no notion
of a hub. §5.3 makes domain boundaries follow where traffic flows, and names the derived sub-domain
by its busiest table — the two remaining places the enriched usage/centrality signals aren't yet read.

## Grounded facts (code, 2026-09-16)
- `cluster.cluster(signal_graph, *, identity, company, namer, facet_tiebreaker, facet_denylist,
  gamma_coarse, gamma_fine)` (`cluster.py:770`) is called at `materialize.py:760` — **before**
  `_gather_usage(reader, allowlist)` / `graph.pagerank_centrality` (L831-832). So materialize must
  compute usage BEFORE cluster and thread it in (usage is pure/cheap; reuse `_gather_usage`).
- `_run_multiplex` (`cluster.py:211`) builds one igraph layer per edge kind from
  `edges_by_kind[kind] = [(a, b, w)]`, `g.es["weight"] = e_w`, CPM-Leiden. Determinism pinned by
  `LEIDEN_SEED=1729` + `N_ITERATIONS=5` + single thread + sorted layer order. The structural layers
  `join_key` / `co_query` are where usage should scale the per-edge `w`.
- `_derive_subdomains` (`cluster.py:634`, called L917) derives in precedence: (1) governed slash
  sub-tags → (2) value tag (`mvm_subdomain=…`) → (3) schema-within-domain → **(4) FK/MV component**
  (the structural fallback, L690-702). §5.3's PageRank hub belongs in slot (4): it names/anchors each
  derived structural sub-group by its top-PageRank table — it must NEVER fire when (1)-(3) do.
- `leidenalg`/`igraph` are lazy in `cluster.py` (+ `graph.pagerank_centrality`) — **no new dep**.

## Testability seam
BUILD A: `cluster(..., usage: Mapping[str,float] = {})` — empty ⇒ scale factor 1.0 ⇒ byte-identical.
BUILD B: a per-domain PageRank over the domain's structural subgraph inside `_derive_subdomains`'s
slot (4), igraph lazy; missing igraph / no structural edges ⇒ current FK/MV behavior exactly.
`LEIDEN_SEED`/`N_ITERATIONS`/`LAYER_WEIGHTS`/`GAMMA_*` and rules (1)-(3) + all curated-tag precedence
(MV-D53 #1) are untouched.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 3 sub-part 3 (usage-weighted clustering + PageRank sub-domain hubs)** of
`docs/design/ontology-signal-authority-build.md` §5.3 on the `ontology` branch. TWO independent builds
below — each may be its own commit and each must independently hold-or-improve the MV-D59 harness.
Additive, read-only, offline-green, STOP before deploy.

BUILD A — usage-weighted clustering (`cluster.py` + `materialize.py`):
- Add `usage: Mapping[str, float] = {}` to `cluster.cluster(...)` (keyword-only, default empty). Where
  the STRUCTURAL edges `join_key` / `co_query` are assembled for `_run_multiplex`, scale each edge
  weight `w` by a BOUNDED endpoint-usage factor: `w * (1 + USAGE_BETA * mean(usage.get(a,0),
  usage.get(b,0)))`, with a new fixed module constant `USAGE_BETA` (e.g. 0.5) — so a trafficked spine
  pulls harder than an incidental FK, but usage NUDGES and never dominates the structural signal. Do
  NOT touch the non-structural layers (`tag_assignment`/`agent_scope`/`mv_membership`/etc.). Empty
  `usage` ⇒ factor exactly 1.0 ⇒ byte-identical partition. Deterministic (fixed `USAGE_BETA`, sorted
  inputs, existing seed/iterations unchanged).
- In `materialize.run_materialize`, compute `usage = _gather_usage(reader, allowlist)` BEFORE the
  `cluster.cluster(...)` call and pass `usage=usage` (reuse it for `RankSignals` so it is read once).

BUILD B — PageRank sub-domain hubs (`cluster.py`, `_derive_subdomains` slot (4) ONLY):
- When rules (1)-(3) do NOT fire and the FK/MV structural fallback (4) produces ≥2 sub-groups,
  compute a per-domain PageRank over the domain's `join_key`+`co_query` subgraph (igraph lazy, damping
  0.85, deterministic sorted inputs) and anchor/name each sub-group by its **top-PageRank table**
  (deterministic tie-break by FQN). The MEMBERSHIP of the fallback groups is unchanged — this only
  sets the hub the sub-group is named/anchored from (a `"hub: <fqn>"`-style boundary reason). If
  igraph is unavailable or the subgraph is edgeless, keep today's FK/MV naming exactly. NEVER fire
  when an explicit boundary (1)-(3) exists; NEVER override a curated governed-tag sub-domain.

BUILD C — tests (`test_ontology_cluster.py`):
- A: a fixture where an incidental UNqueried FK vs a heavily-used co-query edge shifts a boundary when
  `usage` is supplied, and `usage={}` reproduces the current partition byte-for-byte; determinism
  across two runs.
- B: a domain with NO explicit boundary and ≥2 FK components names each sub-group by its top-PageRank
  hub; a domain WITH slash/`mvm_subdomain`/schema boundaries is unaffected (rule 1-3 still win);
  igraph-unavailable ⇒ identical to today's FK/MV output.

GUARDRAILS (hard): do NOT change `LEIDEN_SEED`/`N_ITERATIONS`/`LAYER_WEIGHTS`/`GAMMA_COARSE`/
`GAMMA_FINE`, the rules-first precedence, `_derive_subdomains` rules (1)-(3), ER, or naming/curated
precedence (MV-D53 #1). Do NOT touch `rank.py` blend/tiering/firewalls. No new Delta table/column
(MV-D49); NO new dependency — `igraph`/`leidenalg` already lazy; `uv.lock`/`package-lock.json`
byte-identical (MV-D45); read-only, no governed-tag write (MV-D26); deterministic (MV-D82). Live path
default-correct: no usage / no igraph ⇒ byte-identical to today.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted cluster/materialize/graph tests green;
`uv.lock` + `frontend/package-lock.json` untouched; only wheel files changed. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
`SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`,
then `run-now` job `512526067383740` with `catalog_allowlist=["serverless_stable_tbzqg7_catalog"]`,
`domain_facet_denylist=["modeled"]`, `industry_alignment_enabled=true`,
`industry_alignment_reference_model=retail`. Capture the current `genie_ont_eval` row FIRST. Confirm:
- MV-D59 harness P/R/F + **structural health (singleton_rate / max_depth / orphans) hold-or-improve**
  vs baseline (P1.00/R0.9231/F0.96). A regression on any metric ⇒ the offending build does not merge.
- Any domain that gained a PageRank-hub sub-domain shows a `hub:`-style boundary; governed
  slash/`mvm_subdomain`/schema sub-domains are unchanged; MV-D99 loyalty taxonomy intact.
- Note: the tbzqg7 estate is heavily governed (explicit boundaries fire), so expect the hub fallback
  to be rare and the usage-weighting effect small — the gate here is "no regression", the payoff is
  on estates with structural (ungoverned) domains.
