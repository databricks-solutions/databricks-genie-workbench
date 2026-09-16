# Ontology Signal Authority — Stage 3b (certified-seeded PPR assignment) · Goal-Mode driver (MV-D96)

> **One stage per run, on the `ontology` branch.** Build spec:
> `ontology-signal-authority-build.md` §5.2 (authoritative). This is **sub-part 2 of 3** of Stage 3.
> Sub-part 1 (PageRank centrality, `graph.pagerank_centrality`) is BUILT + deploy-verified
> (`09491fbd`). This driver replaces the **schema heuristic** for below-bar assets with a
> **certified-seeded personalized PageRank** — the "add to existing domain" target becomes *earned*
> by connectivity to trusted (certified) anchors, not by naive schema co-location. Do NOT build
> sub-part 3 (§5.3 usage-weighted clustering + PageRank sub-domain hubs). Additive, read-only, no
> new table/dependency, off-safe. STOP before deploy.

## Why now
3.1 gives us a real PageRank machine and Stage 2 (MV-D95) put certified assets on the map. Today a
Domain that falls **below the legitimacy bar** (MV-D57: too few tables/schemas/connections) is kept
but `surfaced=false` with a hint `add to existing domain: <most-common catalog.schema>` — a naive
schema guess. §5.2 makes that target the domain whose **certified seeds** the fragment is most
connected to (random-walk-from-seeds), so certified tables become centers of gravity (OntoRank).

## Grounded facts (code, 2026-09-16)
- The heuristic being replaced: `rank._legitimacy_home(members, evidence)` (~L525) returns the most
  common `catalog.schema` (via `_schema_of`), and `rank._apply_legitimacy_gate` (~L538–564) writes
  `evidence["gate_hint"] = f"add to existing domain: {home}"` + `evidence["surfaced"]=False` when
  `transforms.legitimacy_ok(...)` fails. Sub-domains / reassign / pages and **curated governed-tag
  Domains are exempt** (`_is_curated_domain`) — 3b must keep every one of those exemptions.
- `RankSignals` (`rank.py` L71–91) is the batch-precomputed pure-map channel the ranker reads
  (`usage`/`centrality`/`governance`/`deprecated`). `_score_row` (L641) ALREADY holds `signals`, and
  it is what calls `_apply_legitimacy_gate` — so a new map on `RankSignals` is the clean seam (same
  pattern `centrality` and `deprecated` already use).
- Certification is available in `materialize` via `_gather_certification(reader, allowlist)` →
  `{fqn: "certified"|"deprecated"}` (Stage 2). Domain rows carry `members`; materialize builds
  `members_by_domain`.
- `igraph` is lazy in `cluster.py` + `graph.pagerank_centrality` — **no new dep**. It exposes
  `Graph.personalized_pagerank(reset_vertices=[...], damping=0.85)` (PRPACK, deterministic solve).

## Testability seam
Pure `graph.certified_home(signal_graph, seeds_by_domain) -> {bare_fqn: domain_id}` beside
`pagerank_centrality` (igraph lazy inside; degrade to `{}`). New `RankSignals.trusted_home` field
(default empty). `_apply_legitimacy_gate` prefers it, schema heuristic stays the fallback. Blend /
tiering / surfacing math untouched (MV-D35) — only the *target of the below-bar hint* changes.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 3 sub-part 2 (certified-seeded personalized-PageRank assignment)** of
`docs/design/ontology-signal-authority-build.md` §5.2 on the `ontology` branch. This sub-part ONLY —
do NOT build §5.3 (usage-weighted clustering / sub-domain hubs). Additive, read-only, offline-green,
STOP before deploy.

BUILD A — pure PPR home (wheel, `graph.py`, unit-tested):
- Add `certified_home(signal_graph: dict, seeds_by_domain: Mapping[str, Sequence[str]]) -> dict[str, str]`
  beside `pagerank_centrality`. `seeds_by_domain` maps a domain_id → its CERTIFIED asset FQNs. Build
  the same directed fused graph as `pagerank_centrality` (the six edge kinds). For each domain with
  ≥1 seed present in the graph, run `personalized_pagerank(reset_vertices=<that domain's seed
  vertices>, damping=0.85)`. Assign each `asset:` vertex to the domain whose seeded run gives it the
  highest mass; key by bare FQN → domain_id. Deterministic: sort domains, seeds, vertices; tie-break
  a vertex to the lexicographically-smallest domain_id. igraph lazy INSIDE; unavailable, edgeless, or
  no seeds ⇒ return `{}` (MV-D43/D45) — never raise.

BUILD B — wire the map (`rank.py` + `materialize.py`):
- Add `trusted_home: Mapping[str, str] = field(default_factory=dict)` to `RankSignals` (fqn→domain_id).
- In `materialize.run_materialize`, build `seeds_by_domain` = for each domain row, its `members` that
  are certified (`certification[m]=="certified"`); then
  `trusted_home=graph.certified_home(signal_graph, seeds_by_domain)` and pass it into `RankSignals`.
  Empty certification ⇒ `{}` = today's bytes.

BUILD C — consume in the legitimacy gate (`rank.py`):
- Thread `signals.trusted_home` into `_apply_legitimacy_gate`. When a Domain is below the bar, look up
  each of its `members` in `trusted_home`, majority-vote the target domain_id EXCLUDING the row's own
  domain_id (deterministic tie-break). If a target is found: `evidence["gate_hint"]="add to existing
  domain: <target>"`, `rank["legitimacy_home_basis"]="certified_ppr"`, `rank["legitimacy_home_target"]
  =<domain_id>`. Else FALL BACK to the existing `_legitimacy_home` schema hint with
  `legitimacy_home_basis="schema"`. `surfaced=False` behavior and every exemption (curated governed-tag
  Domain, sub-domain/reassign/page) are UNCHANGED.

BUILD D — tests (`test_ontology_graph.py` + `test_ontology_rank.py`):
- `certified_home`: a bridge asset seeded-closest to domain A's certified anchor is assigned to A, not
  its schema-mate in B; determinism across two runs; no seeds / igraph-unavailable / edgeless ⇒ `{}`.
- Gate: a below-bar fragment with a certified-PPR target gets `gate_hint`→that domain +
  `basis=="certified_ppr"`; with no trusted_home entry it falls back to the schema hint
  (`basis=="schema"`, byte-identical to today); a curated governed-tag Domain is still never gated.

GUARDRAILS (hard): do NOT change `FACTOR_WEIGHTS`/`blend`/`coverage_cap`/`tier_of`/`confidence_band`,
`legitimacy_ok`/the diffuseness gate, or any firewall (MV-D35). Do NOT change the surfacing DECISION —
only the below-bar *hint target*. Do NOT touch `cluster.py`, sub-domain derivation, or naming. No new
Delta table/column (MV-D49); NO new dependency — `igraph` already lazy; `uv.lock`/`package-lock.json`
byte-identical (MV-D45); read-only, no governed-tag write (MV-D26); deterministic (MV-D82). Live path
default-correct: no certification / no igraph ⇒ `trusted_home={}` ⇒ schema fallback = today's bytes.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted graph/rank/materialize tests green;
`uv.lock` + `frontend/package-lock.json` untouched; only wheel files changed. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
`SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`,
then `run-now` job `512526067383740` with `catalog_allowlist=["serverless_stable_tbzqg7_catalog"]`,
`domain_facet_denylist=["modeled"]`, `industry_alignment_enabled=true`,
`industry_alignment_reference_model=retail`. Capture the current `genie_ont_eval` row FIRST. Confirm:
- Below-bar Domains that fold now show `legitimacy_home_basis="certified_ppr"` with a real domain
  target (spot-check a few are the certified-connected home, not just the schema-mate).
- Estates without certification still show `basis="schema"` (fallback intact).
- MV-D59 harness P/R/F + structural health are **flat-or-up** vs baseline (surfacing is unchanged, so
  expect flat); MV-D99 loyalty taxonomy intact; no domain named `certified`. If a metric regresses the
  sub-part does not merge (spec §5 gate).
