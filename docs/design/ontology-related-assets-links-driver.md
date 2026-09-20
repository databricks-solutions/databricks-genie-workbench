# Ontology — Stage 4.1j: Related-assets & links (graph traversal) · Goal-Mode driver

> **One stage per run, on the `ontology` branch.** Self-contained (no separate build spec).
> Additive, deterministic-first. **STOP before the live write** — the human deploy-verify gate
> (below) confirms Related assets + Links surface in the live Discover copy-out. Proposed
> register lines: **MV-D102** (graph-derived Related assets) · **MV-D103** (best-effort external
> Links + Copy-for-Discover surfacing).

## Why now
A Page's highest-signal fields per the [Databricks Pages model](https://docs.databricks.com/aws/en/uc-semantics/pages)
are **Related assets** (parent/child Pages, dependent metrics, associated tables) and **Sources**
(incl. external **links**). The workbench computes a full relatedness graph every run but the page
layer never consults it: `related_fqns` is hardcoded to the serving Genie Agent, so measure pages
show an empty Related section. Meanwhile "Draft with AI" only paraphrases the same facts — it adds
no new information. Traversing the graph we already have makes Related assets a **deterministic,
grounded** feature (higher trust than LLM prose) at near-zero new cost.

## Grounded facts (code, 2026-09-19)
- Fused heterograph is built EVERY run: `graph.build_signal_graph(...)` (graph.py:33), assembled at
  materialize.py:705-712. Edge kinds carry `kind` + `source` + `as_of` + optional `weight` +
  `columns`: `join_key` (FK/shared-join-column, **names the column**, MV-D88),
  `lineage_adjacency`, `co_query`, `mv_membership`, `semantic_sim`, `agent_scope`,
  `dashboard_scope`, `tag_assignment`, `schema_affinity`.
- `graph.pagerank_centrality(signal_graph)` (graph.py:261) is computed at **materialize.py:840 —
  AFTER `mine_pages` (materialize.py:790)**. It depends ONLY on `signal_graph`, so it can move up.
- Traversal precedent already in-repo: `_domain_adjacency(members_by_domain, signal_graph)`
  (materialize.py:531) walks `join_key`/`lineage_adjacency`/`co_query` (filter at :547-548).
- The miner is graph-blind: `mine_pages` (pages.py:1233-1250) has NO `signal_graph`/`centrality`
  param; every detector sets `related_fqns=tuple(agents)` (pages.py:748-749, 770, 789, 827, 883,
  907). Why-strings: `_asset_why` (pages.py:964), `_SOURCE_WHY` (:955), `_RELATED_WHY` (:961,
  agent-only).
- Persistence: `genie_ont_pages.related_fqns ARRAY<STRING>` (ddl.py:149); page-row expansion
  copies `related_fqns` (materialize.py:313); `evidence.asset_why` is consumed by the card.
- Surfacing: the Related `AssetRows` already renders (PageDraftCard.tsx:154) but `copyText` (Copy
  for Discover) drops Related and has no Links block (PageDraftCard.tsx:16-21).

## Testability seam
Additive to `graph.py` (one pure fn), `pages.py` (thread param + detector wiring + page↔page
post-pass), `materialize.py` (reorder centrality + pass-through), `models.py`/`types.ts` (`links`),
and `frontend/` (copyText + Links section). No new dep, no new router. Traversal is
deterministic/offline (igraph-free, stable order); Links are best-effort/degrade (MV-D43). Harness
stays flat-or-up: `related_fqns` changes NO domain/page membership set.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 4.1j — Related-assets & links** on the `ontology` branch. Additive,
offline-green, no new dep, no new router. Related assets DETERMINISTIC (graph traversal); external
Links best-effort/degrade. Proposed **MV-D102** (Related assets), **MV-D103** (Links + surfacing).

BUILD A — centrality before mining (`materialize.py`): move
`centrality = graph.pagerank_centrality(signal_graph)` (today L840) to right after `signal_graph`
is built (~L712), BEFORE `mine_pages` (L790). It reads only `signal_graph`; later
`node_scores`/`RankSignals` reuse the var ⇒ byte-identical.

BUILD B — pure traversal (`graph.py`, igraph-free): add
`related_assets(signal_graph, anchor_fqns, centrality, *, max_out=6, exclude=frozenset()) ->
list[dict]`. From each `asset:<anchor>` node take 1-hop neighbors over the relatedness kinds
(`join_key`, `lineage_adjacency`, `co_query`, `mv_membership`, `semantic_sim`, `dashboard_scope`,
`agent_scope`); `score = (edge weight or 1.0) × KIND_PRIOR[kind] × (centrality.get("asset:"+t,0)+
ε)`; dedupe keep-max; drop anchors/exclude/self; return `{fqn, kind, source, columns, why, score}`
sorted score desc then fqn, capped `max_out`. Per-kind `why`:
join_key→"Shares join key `<col>`" (edge `columns`, MV-D88); lineage_adjacency→"Connected in table
lineage"; co_query→"Frequently queried together"; mv_membership→"Measure on the same metric view";
semantic_sim→"Semantically similar"; dashboard_scope→"Used by a governed dashboard";
agent_scope→reuse `_RELATED_WHY`.

BUILD C — thread + wire (`pages.py`+`materialize.py`): add `signal_graph=None, centrality=None`
kwargs to `mine_pages` (pages.py:1233); pass both from materialize.py:790. After Pass A finalizes a
candidate, compute `related_assets(signal_graph, spec.source_fqns, centrality,
exclude=set(spec.source_fqns))`, UNION with the existing serving-Agent relations, set the
ranked/deduped result as `candidate.related_fqns`, and fold each `why` into `evidence.asset_why`
(extend `_asset_why`; keep `_RELATED_WHY` for agents). Then a post-pass over ALL candidates adds the
parent-domain page + same-subdomain sibling Pages (via `domain_id`+`asset_domain`), capped/deduped
after asset relations. `signal_graph=None` ⇒ agents-only, byte-identical (MV-D43). Only real graph
nodes ⇒ no invented FQN.

BUILD D — external Links (best-effort, MV-D103): additive `evidence.links:
list[{url,title,as_of,note}]` via the sanctioned `llm_utils`/web path, BOUNDED like autodraft, each
labeled "informational, as of <DATE> — not certified" and `LeakageOracle`-scanned; ANY failure ⇒
[], never blocks/hangs (MV-D43). Links live ONLY in `evidence`. Scope-tight ⇒ land A–C and gate D
behind the flag.

BUILD E — surfacing (`frontend`+`models.py`/`types.ts`): extend `copyText` (PageDraftCard.tsx:16)
with a "Related assets:" block (`related_fqns`+why) and a "Links:" block (`evidence.links`); the
Related `AssetRows` already renders (PageDraftCard.tsx:154) — add a Links section; mirror `links`
in `types.ts`/`models.py`. Copy-for-Discover then carries Related+Links (Pages have no write API,
17.x).

GUARDRAILS: additive; traversal deterministic (igraph-free, stable sort); only real graph nodes;
`signal_graph=None` ⇒ byte-identical agents-only; Links best-effort/degrade + leakage-scanned +
labeled; no new dep/router; DEFAULT-safe.

TESTS (wheel + `frontend/`): `related_assets` ranking, dedupe keep-max, anchor/self/exclude drop,
per-kind why incl. join-key column; edgeless graph ⇒ []; `signal_graph=None` ⇒
`related_fqns==agents`; page↔page parent/sibling; Links degrade (web down ⇒ [], no raise) + leakage
drop; `copyText` emits Related+Links; `genie_ont_eval` flat-or-up + a Related-precision fixture.

ACCEPTANCE (offline): `./scripts/test.sh` green (report new floor + update the playbook copy in the
SAME commit); `cd frontend && npx tsc -b && npm run lint && npm run test`; `git status -- uv.lock`
clean. Then STOP for deploy-verify.

---

## Deploy-verify gate (human, after offline-green — the STOP checkpoint)
`./scripts/deploy.sh --update` — **`deploy.sh` reads `GENIE_DEPLOY_PROFILE` from `.env.deploy`
(today `fevm-serverless` → workspace 6t92c3) and IGNORES a `DATABRICKS_CONFIG_PROFILE=…` prefix;
to target another workspace edit `.env.deploy`, do not prefix the command.** A related-asset
change flows through the materialize batch, so trigger a fresh materialize (or wait for the mirror
to refresh) before verifying. Then in the LIVE app:
1. Open a measure Routing page (e.g. an "Average … Delay" page): the **Related** section now lists
   graph neighbors — associated tables (via join key / lineage), sibling metrics on the same
   metric view, and any governed dashboards — each with a plain-language "why".
2. **Copy for Discover** now includes "Related assets:" and (if enabled) "Links:" blocks.
3. Confirm in `genie_ont_pages`: `related_fqns` is populated beyond the serving Agent, and
   `evidence.asset_why` (and `evidence.links` if E landed) are present and valid JSON.
4. Confirm the harness (`genie_ont_eval`) precision/recall/F1 is **flat-or-up** vs the prior run
   (Related assets must not perturb domain/page membership).
Review with a human — grounding (no invented FQNs), the deterministic ranking, and Links labeling
(best-effort, not-certified) — before marking Stage 4.1j BUILT.
