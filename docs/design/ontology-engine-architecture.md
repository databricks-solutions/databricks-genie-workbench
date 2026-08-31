# Ontology engine architecture (MV-D36 · MV-D37 · MV-D38)

> Status: canonical architecture for the standalone Ontology page and its
> detection engine. This is the build spec the 17.x Ontology track implements
> (Prompt 17a onward). It memorializes the design that MV-D36 (standalone
> estate-wide page), MV-D37 (governed-tag substrate) and MV-D38 (external
> enrichment tier) decided. Companion docs: `mv-advisor-playbook.md` (the MV-D
> register + prompt sequence), `page-archetypes.md` (the curation standard —
> archetypes, Domain/Sub-Domain draft format, permission tiers, dedupe), and
> `mv-advisor-gap-report.md` §2.9 (reuse anchors, verified against HEAD).
>
> **Building this? Start with the slice, not the whole engine.** The build
> decisions are closed (MV-D39–D47, incl. Lakebase Search + AI Gateway MCP context
> sources). The first Goal-Mode-runnable deliverable is
> the **read-only spine** — preflight → OBO inventory → tag/lineage taxonomy →
> serve frames 17.0a/b/c, with no proposal engine, no external enrichment, and no
> writes — fully specified (contracts, DDL, routes, tests) in
> `docs/design/ontology-phase1-build.md`. This doc is the design it is a slice of;
> §12 of that spec lists what each later phase pulls in.

## 1. The mental model

One **heterogeneous signal graph** → an **entity-resolution + clustering
engine** → **evidence-ranked proposals** → a **consent-gated apply**. Everything
is read-heavy and estate-wide; the only write is the optional `SET TAG`
membership apply. The trust rule (MV-D35) is a cross-cutting concern, not a
stage: every edge and every proposal carries its provenance so ranking leads
with proven facts, never a naked confidence score.

## 2. The load-bearing decision: batch engine + thin interactive page

Estate-wide graph construction + embeddings + community detection + LLM naming
is **minutes of work over the whole account** — not a request-thread workload.
So the architecture splits exactly like Auto-Optimize already does:

- **Ontology batch job** (mirrors the GSO engine in
  `packages/genie-space-optimizer/`, deployed as a wheel + Databricks Job) runs
  Layers 1–7 and materializes candidates to Delta + Lakebase.
- **Interactive page** (`/api/ontology/*` FastAPI router) is thin: it reads
  materialized candidates, renders the seven frames, runs the preflight, and
  triggers the optional apply. Sub-second.
- **Fast-path exception:** the OBO-only *inventory* tier (metric-view + tag
  counts via `information_schema`) is cheap enough to run in-request, so the
  page renders something instantly on first load while the full signal-weighted
  run is kicked off in the background. This is the "degrade-not-hang" rule made
  concrete.

```
                       ┌─────────────────────────────────────────┐
"Refresh ontology"  →  │   ONTOLOGY BATCH JOB (GSO-style wheel)   │
                       │   L1 readers → L2 graph → L3 ER/embed    │
                       │   → L4 cluster → L5 propose → L6 rank     │
                       └───────────────┬─────────────────────────┘
                                       │ writes candidates
                       ┌───────────────▼──────────────┐
                       │  Delta  genie_ont_*  (durable)│
                       │  Lakebase mirror (fast reads) │
                       └───────────────┬──────────────┘
                                       │ reads
Browser  ◄──►  /api/ontology/*  (thin FastAPI, OBO) ──► optional SET TAG apply
               preflight · taxonomy · tags-lens · drafts · apply(dry-run→consent)
```

## 3. The layered architecture

| Layer | Responsibility | Reuse anchor | New? |
|---|---|---|---|
| **L0 Access/preflight** | Resolve permission tiers; pick OBO vs SP per reader | `scripts/grant_permissions.py` (grant list), `get_service_principal_client` / `require_obo_workspace_client` | thin new |
| **L1 Signal readers** | Typed readers → normalized signal frames, TTL-cached | GenieWatch `watch/services/system_tables.py` (extend), `genie_client.list_spaces` | mostly reuse |
| **L2 Graph builder** | Fuse signals into one weighted heterograph | concept from `auto_optimize.py:_build_semantic_graph` (generalize estate-wide) | new (estate) |
| **L3 Embedding + ER** | Lakebase Search (`lakebase_vector` ANN + `lakebase_text` BM25) on the existing Lakebase; block→score→LLM-adjudicate dedupe | Lakebase Search extensions; `leakage.get_embedding` (gte); `llm_utils.call_serving_endpoint`; `mv_scoring.dedup_gate` pattern; in-process cosine fallback | new + reuse |
| **L4 Clustering** | Leiden (multiplex + soft `initial_membership`, CPM) → domains → sub-domains, seeded by tags/Agents/schema; tag conflicts → `reassign` proposals | — | new |
| **L5 Proposers** | Domain/subdomain, Page miners, MV gap/dup/quality, Agent domain/overlap | MV fingerprint/scoring shapes; `manage_metric_views`; `ask_genie` (validate routing Pages) | new + reuse |
| **L6 Rank/trust** | `usage × lineage-centrality × governance` (MV-D35); PII firewall on tag names; policy conformance; provenance ladder (MV-D38) | `leakage.LeakageOracle` (extend to tag names) | new + reuse |
| **L7 Persistence** | `genie_ont_*` candidate/consent/suppression/applied/context tables | `genie_opt_mv_*` + `mv_state.py` + `gso_lakebase.py` precedent | reuse pattern |
| **L8 Serving API** | Preflight, taxonomy, tags-lens, drafts, apply | FastAPI router + Lakebase reads | new |
| **L9 Apply (optional)** | Consented `SET TAG`, dry-run-first | `manage_uc_tags` MCP + MV consent rails | new + reuse |

### The signal readers (L1), in building blocks

L1 is the layer that actually touches Databricks — every layer above it works
off L1's output, never off raw system tables. Its job (§3): **typed readers →
normalized "signal frames," TTL-cached.** The deceptively simple part is "read
some tables"; the real complexity L1 owns is **identity** — different signals
need different clients, per the §8 map:

- **OBO** for `system.information_schema.*` — auto-filtered to what the user may
  see, needs no grant, cheap.
- **SP** for `system.access.*`, `billing.usage`, `query.history`, and
  `system.tags.governed_tags` — **not OBO-readable**; the GenieWatch pattern
  (`watch/services/system_tables.py`).

Two behavioral rules L1 must honor (§2):

1. **Degrade-not-hang** — if an SP grant is missing, that one reader returns an
   empty frame + a `tier_degraded` flag instead of erroring; the run continues
   with fewer edges. The L0 preflight decides which readers may run.
2. **Fast-path** — the OBO **inventory** tier (table/MV/tag counts via
   `information_schema`) is cheap enough to run **in-request**, so the page
   renders instantly while the full signal-weighted read runs in the batch job.

Readers, each mapped to its source, identity, and the L2 element it feeds:

| Reader | Source | Identity | Feeds (L2) |
|---|---|---|---|
| Inventory | `information_schema.*` (tables, columns, MV defs, `*_tags`) | **OBO** | `table`/`column`/`metric_view`/`measure` nodes + `tag_assignment` edges |
| Join keys | `information_schema.key_column_usage` / `constraints` | **OBO** | `join_key` edges |
| Lineage | `system.access.table_lineage` / `column_lineage` | **SP** | `lineage` edges (the backbone) |
| Co-query | `system.query.history` | **SP** | `co_query` edges (freq × recency) |
| Governed-tag catalog | `system.tags.governed_tags` | **SP** | `governed_tag` nodes (+ allowed values) |
| Agent scope | `list_spaces` + `serialized_space` | **OBO** (+ SP scope fallback) | `genie_agent` nodes + `agent_scope` edges |
| Cost | `system.billing.usage` | **SP** | `cost` node attribute |

`semantic_sim` is **not** an L1 read (it is L3's Lakebase Search pass); the
external Context Pack is **not** L1 either (Batch, §6, resolved before readers run).
L1 is purely the internal estate signals.

| Building block | What it does | Reuse anchor |
|---|---|---|
| Identity resolver | picks OBO vs SP per reader, from the preflight tiers | `get_service_principal_client` / `require_obo_workspace_client`; §8 map |
| SP system-table reader | the non-OBO reads + shared query plumbing | `watch/services/system_tables.py` (extend estate-wide) |
| OBO inventory reader | `information_schema` counts/rows, fast-path in-request | `require_obo_workspace_client` |
| Agent reader | enumerate spaces + parse `serialized_space` | `genie_client.list_spaces` (OBO, SP scope-error fallback) |
| Normalizer | raw rows → typed **signal frames** (the schemas L2 consumes) | new (thin) |
| TTL cache | in-process cache so repeat reads are cheap | GenieWatch TTL-cache pattern |
| Degrade flags | missing-grant → empty frame + `tier_degraded`, run continues | `scripts/grant_permissions.py` (grant list) drives preflight |

Per §10, L1 is **reused nearly wholesale** — the SP reader + TTL cache and the
grant-preflight list already exist; L1 mostly extends them from per-space to
estate-wide.

```
   L0 PREFLIGHT  →  resolves permission tiers → decides which readers may run
                    (degrade-not-hang: missing SP grant ⇒ that reader returns empty)
                                        │
                                        ▼
  ┌──────────────────────────────── L1  SIGNAL READERS ──────────────────────────────┐
  │                                                                                    │
  │   IDENTITY RESOLVER  (OBO vs SP per §8 map)                                        │
  │        │                                                                           │
  │        ├──[OBO]──► inventory      ← information_schema.* (tables/cols/MV/*_tags)    │
  │        │           join keys      ← key_column_usage / constraints                 │
  │        │           agent scope    ← list_spaces + serialized_space  (SP fallback)  │
  │        │                                                                           │
  │        └──[SP ]──► lineage        ← system.access.table/column_lineage             │
  │                    co-query       ← system.query.history                           │
  │                    tag catalog    ← system.tags.governed_tags                      │
  │                    cost           ← system.billing.usage                           │
  │                          │                                                         │
  │                          ▼                                                         │
  │             NORMALIZE → typed "signal frames"  →  TTL CACHE                        │
  │             (+ tier_degraded flags where a grant was missing)                      │
  │                                                                                    │
  │   ⚡ FAST-PATH: the OBO inventory frame runs in-request (page renders instantly);   │
  │      the full SP-weighted read runs in the batch job.                              │
  └───────────────────────────────────────┬───────────────────────────────────────────┘
                                           │  normalized signal frames (cached)
                                           ▼
                                   L2  GRAPH BUILDER
```

## 4. The signal graph (the heart of it)

Everything hinges on one typed graph. Get the node/edge schema right and
clustering, centrality, and dedupe all fall out of it.

**Nodes:** `table`, `column`, `measure`, `metric_view`, `genie_agent`,
`governed_tag`, `page` (proposed).

**Edges (each weighted + provenanced):**

- `lineage` (table→table, column→column) — from
  `system.access.table_lineage`/`column_lineage`; **the clustering backbone**.
- `join_key` (table↔table via shared FK/PK) — from
  `information_schema.key_column_usage`/`constraints`.
- `co_query` (table↔table) — co-occurrence in `system.query.history` statement
  parse; weight = frequency × recency (tables queried together belong together).
- `tag_assignment` (asset→governed_tag) — from `information_schema.*_tags`;
  **strongest domain prior** (reuse over invent).
- `agent_scope` (genie_agent→table/measure) — from `serialized_space`; a
  pre-clustered domain hint (an Agent's table set ≈ a candidate sub-domain).
- `semantic_sim` (any↔any name/comment) — Lakebase Search cosine; powers dedupe +
  weak clustering.
- `cost` (attribute on table/agent nodes) — `billing.usage`; a ranking weight,
  not an edge.

Community detection runs on the fused edge set; **degree/betweenness centrality
on the lineage subgraph** gives the "how load-bearing is this asset" signal that
anchors MV-D35 ranking. Sub-domains are just finer communities inside a domain
community.

### The graph builder (L2), in building blocks

L2 is the layer that *produces* the graph above. Its one job (§3): **fuse the
signals into one weighted heterograph.** Everything downstream — dedupe (L3),
clustering (L4), centrality-ranking (L6) — reads this single structure, which is
why it is "the heart of it."

The mental model: **L1 readers each emit a typed, normalized "signal frame"**
(one small table per system-table source, TTL-cached). L2 turns those rows into
typed **nodes** and typed, weighted, provenanced **edges** — one edge kind per
signal. It is not new in *concept*: the app already builds a semantic graph for
a **single** Genie Agent in `auto_optimize.py:_build_semantic_graph` (reads one
space's `serialized_space` — its tables, metric views, join specs, MV-YAML
`uses`/join edges — and returns nodes + edges). **L2 is that same idea
generalized estate-wide**, fed by system tables across *all* Agents and *all*
account metric views instead of one space's config. Per §10 the *concept* is
reused; the estate-scale builder is built new.

What L2 assembles:

1. **Node builder** — rows → the 7 typed nodes above, each with a **stable
   FQN-based ID** so the same table is one node no matter how many frames mention
   it. (`page` nodes don't exist yet — L5 proposes them later.)
2. **Edge builder** — one weighted edge per signal kind: `lineage` ←
   `table/column_lineage`, `join_key` ← `key_column_usage`/`constraints`,
   `co_query` ← `query.history` (weight = freq × recency), `tag_assignment` ←
   `*_tags` (strongest prior), `agent_scope` ← `serialized_space`. `semantic_sim`
   is layered on by L3's embedding pass (Lakebase Search), then fed back for
   clustering; `cost` (`billing.usage`) is a node **attribute**, not an edge.
3. **Weight + provenance-stamp** — every edge carries its weight and a `source` +
   `as-of` stamp (the §6 `Provenanced<T>` discipline), so ranking and audit can
   trace why any two nodes are connected.
4. **Fuse** the per-signal edge sets into one heterograph.
5. **Precompute centrality** — degree/betweenness on the **lineage subgraph** —
   the "how load-bearing is this asset" number that anchors MV-D35 ranking in L6.

L2 runs in the **batch job** (§2), not per-request — it is the heavy fuse step.

| Building block | What it does | Reuse anchor |
|---|---|---|
| Signal frames | typed, normalized, TTL-cached L1 output L2 consumes | `watch/services/system_tables.py`, `genie_client.list_spaces` |
| Node builder | rows → typed nodes with stable FQN IDs | generalize `_build_semantic_graph` node assembly |
| Edge builder | one weighted, provenanced edge per signal kind | generalize `_build_semantic_graph` `uses`/join-edge assembly |
| Weighting | `co_query` = freq × recency; `tag_assignment` = strongest prior; etc. | new |
| Provenance stamp | `source` + `as-of` on every edge | mirror `Provenanced<T>` (§6) |
| Centrality precompute | degree/betweenness on lineage subgraph → MV-D35 signal | graph lib (e.g. networkx), new |
| Graph persistence | serialize graph → `genie_ont_*` for L3/L4 to read | `genie_opt_mv_*` + `gso_lakebase.py` pattern |

```
  L1  SIGNAL READERS  (SP + OBO; typed, normalized, TTL-cached "signal frames")

  table_lineage   key_column   query.history   *_tags      serialized_space   billing
  column_lineage  _usage /      (co-occurrence  (governed   (via list_spaces)  .usage
       │          constraints    freq×recency)  tags)            │              │
       │              │              │             │             │              │
       ▼              ▼              ▼             ▼             ▼              ▼
  ┌──────────────────────────────── L2  GRAPH BUILDER ───────────────────────────────┐
  │                                                                                   │
  │   ① NODE BUILDER                         ② EDGE BUILDER (one kind per signal)     │
  │  ┌───────────────────────┐              ┌──────────────────────────────────────┐ │
  │  │ table · column ·       │              │ lineage      ← table/column_lineage   │ │
  │  │ measure · metric_view ·│  stable      │ join_key     ← key_column_usage       │ │
  │  │ genie_agent ·          │  FQN IDs     │ co_query     ← query.history (f×r)     │ │
  │  │ governed_tag           │─────────────►│ tag_assignm. ← *_tags  (strongest)    │ │
  │  │ (page = proposed later)│              │ agent_scope  ← serialized_space       │ │
  │  └───────────────────────┘              │ (semantic_sim added by L3 embed pass) │ │
  │            │                             └──────────────────────────────────────┘ │
  │            │        ③ WEIGHT + PROVENANCE-STAMP every edge (source + as-of)        │
  │            │                             │                                         │
  │            ▼                             ▼                                         │
  │   ④ FUSE → ONE WEIGHTED HETEROGRAPH   +   cost as node attribute (billing.usage)   │
  │            │                                                                       │
  │   ⑤ PRECOMPUTE centrality (degree/betweenness on the lineage subgraph → MV-D35)    │
  │            │                                                                       │
  └────────────┼──────────────────────────────────────────────────────────────────────┘
               │  one graph (nodes + weighted, provenanced edges + centrality)
               ▼
     L3 ER / dedupe   ·   L4 clustering   ·   L6 rank (uses centrality)
     (also persisted to genie_ont_* for the batch → serving handoff)
```

## 5. Detection, dedupe & population methods (per object)

The signal kinds above feed an entity-resolution + clustering engine with the
evidence-first trust rule. Per-object methods:

- **Domains / Sub-Domains** — cluster the asset graph by combining: (a)
  catalog/schema structure prior, (b) community detection (Leiden via
  `leidenalg`) on the lineage + join-key graph, (c) co-query co-occurrence,
  (d) existing governed tags (strongest — reuse over invent), (e) Genie Agent
  groupings, then (f) LLM naming with company context. Sub-domains = finer
  communities inside a domain community. Every proposal carries evidence chips
  (shared spine, N co-queries, cost share) and a **reuse / create / reassign tag
  decision** (`reassign` = a soft-seed conflict flagged for human adjudication).
- **Dedupe (the entity-resolution core)** — a canonical pass over {governed
  tags, domains, measures, Pages, Agents}: block by prefix/schema, score by
  string + embedding similarity, adjudicate near-ties with the LLM. Outputs:
  merge/alias suggestions, collision warnings, orphan/near-empty flags. Runs
  **before any create** so we never grow the tag sprawl we're taming.
- **Pages** — mine concepts from metric-view measures (each measure → routing
  Page), disambiguation conflicts (same term → multiple measures across MVs),
  coded columns (taxonomy Pages), non-additive rates (guardrail Pages).
  **Asymmetry:** tags/domains are fully queryable for dedupe, but Pages have no
  read API — so Page-dedupe is best-effort (name/synonym heuristics), a real gap
  worth flagging.
- **Metric views** — detect gaps (aggregations recurring in
  `query.history`/dashboards but ungoverned → propose an MV), duplicates
  (overlapping measure definitions → dedupe/consolidate), and quality (missing
  synonyms/comments that hurt Genie retrieval). Validate any proposed MV body
  actually parses/executes (the existing MV-advisor rails).
- **Genie Agents** — infer each Agent's domain from its tables' tags/lineage;
  detect overlap (two Agents over the same tables → consolidation candidate);
  recommend `SET TAG`-ing Agents into the right domain (Agents are taggable);
  flag Agents whose curated SQL implies a Page that doesn't exist yet.

**Trust & safety across all of it:** rank by `usage × lineage-centrality ×
governance status`; lead with proven facts, never a naked confidence % (MV-D35);
everything is dry-run/preview with consent before any `SET TAG` write (reuse the
MV consent rails + the Databricks Automate-tag-assignment dry-run precedent); PII
firewall on tag names (plaintext, globally replicated).

**MCPs beyond system tables:** `manage_uc_tags` (read + propose governed tags),
**Lakebase Search** (`lakebase_vector` + `lakebase_text` on the existing Lakebase
for semantic + keyword dedupe; MV-D40), `ask_genie` (validate a routing Page —
does the NL question resolve to the intended measure?), `manage_metric_views`
(inspect/validate MV bodies), `execute_sql` (run the detection SQL under the
right identity), plus model serving for naming/description and the **AI Gateway
MCP context sources** (`system.ai.web_search` + registry, §6.1, MV-D46/D47) for
the industry prior.

### The ER / dedupe engine (L3), in building blocks

The dedupe bullet above is the load-bearing core; here is how L3 is actually
built. **ER = entity resolution** — the classic data-engineering problem of
deciding whether two records refer to the *same real-world thing*. In this
engine it answers, for example:

- Is proposed domain `Sales` the *same* thing as the existing governed tag
  `Commercial/Sales`? → **reuse, don't create a duplicate.**
- Are `finance.sales.order_revenue.total_revenue` and a second MV's `net_rev`
  the *same measure* defined twice? → **consolidate.**
- Is the ungoverned tag `marketing` the same concept as a proposed `Marketing`
  domain? → **promote / govern the existing one.**

It runs **before any create** so we never grow the sprawl we are taming, and —
per §9 — **before clustering (L4)**, so communities form over *canonical*
entities, not duplicates. It is the standard **three-step ER pipeline**, and
almost none of it is new infrastructure: L3 wires together primitives that
already exist plus the **Lakebase Search** extensions on the Lakebase we already
run (`lakebase_vector` + `lakebase_text`) — no separate managed service, and it
degrades to in-process cosine where Lakebase Search is not enabled (MV-D40/D45).

1. **Block** — never compare every pair (that is O(n²)); bucket candidates
   cheaply first by name prefix / schema / node type, and only compare things in
   the same bucket.
2. **Score** — within a bucket, measure similarity two ways: **string/keyword**
   (edit distance + `lakebase_text` BM25 — catches `order_revenue` vs
   `orders_revenue`) and **embedding** (`lakebase_vector` cosine — catches
   `net revenue` vs `revenue after discount`, which are string-distant but
   semantically identical).
3. **Adjudicate** — high scores auto-merge, low scores auto-reject, and only the
   **near-ties in the ambiguous middle** escalate to an LLM for a yes/no + reason.
   This keeps LLM cost/latency on the small uncertain set.

The output is never a silent merge — it is the **suggestions** that feed the
Tags lens (17.0c) and the reuse-vs-create line on the Domain draft (17.0d).
The **asymmetry** from the dedupe bullet still applies: tags/domains/measures
are fully queryable (system tables) so their dedupe is real, but Pages have no
read API, so Page-dedupe is best-effort name/synonym heuristics — a flagged gap.

| ER step | Building block | Reuse anchor |
|---|---|---|
| Candidate inventory | tags, measures, MV bodies, Agent scopes from system tables | `watch/services/system_tables.py` (extend), `genie_client.list_spaces` |
| Block | prefix / schema / type bucketing (avoids O(n²)) | new, trivial (`GROUP BY` / dict-of-lists) |
| String score | token / edit-distance similarity | small new util |
| Embedding + keyword score | Lakebase Search over names + comments: `lakebase_vector` cosine kNN + `lakebase_text` BM25 | Lakebase Search on the existing Lakebase; embeddings via `leakage.get_embedding` (gte); in-process cosine fallback |
| Adjudicate near-ties | "same concept? yes/no + reason" | `llm_utils.call_serving_endpoint` |
| Confidence gate | auto-merge / auto-reject / escalate thresholds | `mv_scoring.dedup_gate` pattern |
| PII firewall on tag names | leakage detection (tag names replicate globally in plaintext) | `leakage.LeakageOracle` (extend to tag names) |
| Persist verdicts | `genie_ont_*` Delta + Lakebase mirror | `genie_opt_mv_*` + `mv_state.py` + `gso_lakebase.py` |

L3 emits **two** things: **canonical entities** into L4 clustering, and
**merge / collision / orphan verdicts** into L7 persistence that surface in the
17.0c Tags lens and the 17.0d reuse-vs-create line.

```
        L2  ONE WEIGHTED GRAPH  (tables, columns, measures, tags, agents, pages)
                     │  candidate entities to resolve
                     ▼
   ┌───────────────────────────── L3  ER / DEDUPE ENGINE ─────────────────────────────┐
   │                                                                                   │
   │   ① BLOCK            ② SCORE (two signals)          ③ ADJUDICATE                   │
   │  ┌──────────┐       ┌────────────────────┐        ┌────────────────────┐         │
   │  │ bucket by│       │ string sim          │ high → │  auto-MERGE        │         │
   │  │ prefix / │──────►│ (edit distance)     │──────► │                    │         │
   │  │ schema / │ pairs │                     │ low  → │  auto-REJECT       │         │
   │  │ type     │       │ embedding sim  ◄────┼── Lakebase Search           │         │
   │  └──────────┘       │ (cosine kNN)        │ mid  → │  LLM adjudicate    │         │
   │   (avoids O(n²))    └────────────────────┘        │  yes/no + reason ◄─┼─ model  │
   │                              │                     └─────────┬──────────┘  serving │
   │                              ▼                               │                     │
   │                     ④ CONFIDENCE GATE  (dedup_gate pattern)  │                     │
   │                              │                               │                     │
   │                     ⑤ PII FIREWALL on tag names (LeakageOracle)                    │
   │                              │                                                     │
   └──────────────┬───────────────────────────────────┬──────────────────────────────┘
                  │ canonical entities                 │ merge / collision / orphan
                  ▼                                     ▼   verdicts
        L4  CLUSTERING (Leiden)               L7 genie_ont_* (Delta + Lakebase)
        → domains / sub-domains                        │
                                                       ▼
                                        L8 API → 17.0c Tags lens
                                                 17.0d "REUSE vs CREATE" line
```

### The domain / sub-domain clustering engine (L4), in building blocks

L3 hands L4 two things: the **canonical entities** (deduped nodes) and the **one
weighted graph** that connects them. L4 is unsupervised **community detection**
run at two resolutions — a Domain is a coarse community, a Sub-Domain a finer
community inside it. Same algorithm, two passes. Because it runs **after L3**
(per §9), it clusters canonical entities, never duplicates.

Four building blocks, and — like L3 — most of it is a re-point of existing
primitives plus one graph library:

1. **Assemble the weighted layers (multiplex)** — the weights are *not* equal,
   and that is the whole game. `lineage` is the clustering backbone; `join_key`
   and `co_query` reinforce it; `semantic_sim` is weak glue; **`tag_assignment`
   is the strongest prior** (reuse an existing tag over inventing a domain).
   Rather than hand-collapse these into one scalar edge weight (lossy, needs
   tuned coefficients), keep each signal as its own **layer** and let Leiden weigh
   them via **multiplex** community detection (per-layer weights). `cost` is not a
   layer — it is a ranking weight saved for L6.
2. **Seeded community detection (Domains)** — **Leiden** (via `leidenalg`, the
   reference implementation over `python-igraph`) on the multiplex layers, but
   *soft-seeded*: existing governed tags seed the partition via `initial_membership`
   (a **strong prior, not a hard pin** — don't re-derive `Finance` if a `Finance`
   tag already anchors 18 assets, but *let strong graph evidence override a stale or
   over-broad tag*), Agent scopes act as **seeds** (a curated "these go together"),
   and catalog/schema is the **fallback prior** when signals are thin. Leiden is used
   over Louvain deliberately: Louvain can emit **disconnected communities** (up to
   ~16% when run iteratively — exactly our recursive-split pattern), whereas Leiden
   *guarantees* every community and sub-community is connected, at higher modularity
   and lower runtime.
3. **Recursive split (Sub-Domains)** — take each Domain community's subgraph and
   run detection again at a **finer resolution**; the sub-communities become
   Sub-Domains. This is what turns `Commercial` into `{Sales, Marketing,
   Partnerships}` instead of a flat list. Resolution is controlled by the
   **Constant Potts Model (CPM)** objective with a tunable `γ` — this sidesteps
   the modularity **resolution limit** (which would otherwise bury small domains
   next to large ones), giving one interpretable granularity knob.
4. **Centrality + naming** — betweenness / degree centrality on the **lineage
   subgraph** picks the load-bearing anchor of each cluster (the spine everything
   joins to) — the MV-D35 headline chip; then LLM naming turns the anonymous
   cluster into `"Commercial"` using the company prior + Context Pack
   `industry`/`canonical_domains` as a **vocabulary** prior (never structure, §6);
   finally each cluster is bound to L3's dedupe verdict → **REUSE** an existing
   tag, **CREATE** a new one, or — when the soft seed let the graph move assets
   *away* from an existing tag beyond a confidence margin — **REASSIGN** (a conflict
   proposal flagged for human adjudication, never an auto-switch).

| Building block | Reuse anchor |
|---|---|
| Multiplex layer assembly | L2 graph + provenance weights (new, thin) |
| Community detection (Domains) | `leidenalg` Leiden over `python-igraph` — multiplex layers + soft-seeded (`initial_membership`), CPM objective — in the GSO batch job (§11.1) |
| Recursive resolution split (Sub-Domains) | same library, per-subgraph second pass at a finer CPM `γ` |
| Lineage centrality (load-bearing anchor) | betweenness/degree on the `lineage` subgraph → MV-D35 |
| Cluster naming | `llm_utils.call_serving_endpoint` + Context Pack vocabulary (§6) |
| Reuse / create / reassign binding | L3 verdicts + soft-seed conflict check (`reassign` = human-adjudicated in 17g) |
| Persist the tree | `genie_ont_*` Delta + Lakebase mirror |

**Soft seeding + reassignment (the human-in-the-loop rule).** Because the seed is
soft, the graph can disagree with an existing governed tag. The engine never silently
honours a stale tag *and never silently switches one*: when a cluster's membership
contradicts a tag beyond a confidence margin `τ_reassign`, it emits a **`reassign`**
(conflict) proposal — carrying the evidence for the disagreement (shared spine,
co-query pattern, the members that pulled away) — for a human to approve or dismiss in
17g. Below `τ_reassign` the engine stays conservative and proposes **`reuse`** (honour
the tag), keeping the reviewer's queue small (the zero-burden ethos). Approve/dismiss
is recorded in `genie_ont_consents`/`genie_ont_suppressions`; a dismissed reassignment
is suppressed on future runs (MV-D26), which is what removes soft seeding's churn risk;
and the actual `SET TAG` happens only in the Phase-5 (L9) consented apply. So soft
seeding buys evidence-first correction *without* auto-switching and *without* churn.

L4 emits the **Domain → Sub-Domain tree** — each node carrying evidence chips
(shared spine, N co-queries, cost share) and a reuse / create / reassign decision — into
**L5** (which mines each sub-domain's measures into Pages) and **L8** (the 17.0b
taxonomy tree and the 17.0d Domain draft).

```
   L3 OUT:  canonical entities  +  weighted edges  (dedupe already applied)
                        │
                        ▼
 ┌──────────────────────── L4  CLUSTERING ────────────────────────┐
 │  ① FUSE EDGES (weights matter — not equal)                      │
 │     lineage ▓▓▓▓▓ backbone   join_key ▓▓▓   co_query ▓▓         │
 │     tag_assignment ▓▓▓▓▓ STRONGEST prior (reuse>invent)         │
 │     agent_scope ▓▓▓ seed     semantic_sim ▓ weak glue           │
 │                        │                                        │
 │  ② COMMUNITY DETECTION (Leiden, coarse)   ← seeded by tags/     │
 │                        │                     agents/schema      │
 │                        ▼        coarse communities = DOMAINS    │
 │        ┌───────────────┴───────────────┐                        │
 │        ▼                               ▼                        │
 │  ③ RECURSIVE SPLIT (finer resolution, per-domain subgraph)      │
 │     Domain "Commercial"          Domain "Finance"               │
 │        ├─ Sub: Sales              ├─ Sub: Tax                   │
 │        ├─ Sub: Marketing          └─ Sub: Audit                 │
 │        └─ Sub: Partnerships                                     │
 │                        │                                        │
 │  ④ CENTRALITY (lineage) → anchor chip  +  LLM NAMING (company   │
 │     prior + Context Pack)  +  BIND to L3 verdict → REUSE/CREATE │
 └────────────────────────┬────────────────────────────────────────┘
                          │  Domain → Sub-Domain tree
                          │  (each node: evidence chips + reuse/create)
            ┌─────────────┴─────────────┐
            ▼                           ▼
   L5 PROPOSERS                 L8 API → 17.0b taxonomy tree
   (mine measures per            17.0d Domain draft
    sub-domain → Pages)
```

**Worked example.** `finance.sales.{orders, order_items, order_revenue}` are
tied tightly by lineage + join_key; `co_query` shows sales and marketing tables
answered together often; a `Commercial` tag already sits on some of them. Coarse
detection + that tag prior pull sales *and* marketing into **one Domain**; the
recursive split separates the dense `finance.sales.*` lineage from
`marketing.campaigns.*` into **Sub-Domains Sales and Marketing**; `orders` has
the highest betweenness → the anchor chip; the LLM names it `Commercial`; and
L3 says a `Commercial` tag already exists → **REUSE**.

**Honest gap.** Community detection is still sensitive to the resolution knob
(CPM `γ`: too low → one mega-domain; too high → singletons) and to per-layer
weight calibration; the algorithm is now fixed (Leiden / CPM, §11.1) but `γ` and
the layer weights are exposed as tunable constants, with the soft tag/Agent seeding
(`initial_membership`) plus the reassignment ledger as the stabiliser until real
estate graphs are available to tune against. `τ_reassign` is likewise a tunable
constant — set conservatively so only strong tag disagreements surface for review.

### The Page miners (L5), in building blocks

L4 hands L5 the sub-domain communities; L5 turns each sub-domain's **measures,
coded columns, and conflicts** into archetype-tagged Page candidates. It is
deterministic **detectors first, LLM second**: one detector per archetype emits
a `PageCandidate(archetype, evidence, confidence)` from signals only, then an
LLM drafts the body, then it is validated, ranked, and persisted. Per §10 the
miners are **largely a re-point of the MV-advisor's existing fingerprint /
scoring shapes onto the estate graph** — not new machinery.

Four miners, one per archetype — the deterministic signal is the load-bearing
part; the LLM only writes prose over it:

| Archetype | Deterministic detector signal | What the Page pins | Certify |
|---|---|---|---|
| `[Routing]` | each metric-view **measure** in the sub-domain (`data_sources.metric_views` + measures) | NL question → the governed **view + measure** (one canonical answer) | Yes |
| `[Disambiguation]` | one term → **several measures** across MVs (CONFLICT candidates + same-concept-different-expression fingerprints) | which **grain / count / role** the phrase means | Yes |
| `[Guardrail]` | **ratio / percentage-format** measures + **AVG-of-rate** shapes | never average a rate — recompute from **numerator / denominator** | Yes |
| `[Taxonomy]` | **low-cardinality coded columns** (profiling distinct values + column comments) | decode **codes / buckets / glossary** | No¹ |

¹ `[Taxonomy]` certifies only when it is a governed code list; the other three
certify Yes (formulas are authoritative). Any Recent-context overlay is always
`certify-no`.

The pipeline inside L5, in building blocks:

1. **Detect** — per-archetype detectors emit `PageCandidate`s from deterministic
   signals only (no LLM), reusing `mv_fingerprint` / `mv_scoring` shapes and the
   CONFLICT state. `[Routing]` fans out one candidate per measure; the others
   fire only when their signal is present.
2. **Draft** — an LLM writes the body (description, definition, rules, synonyms)
   from the candidate + its evidence. The Context Pack (§6) supplies **vocabulary
   only**: `lexicon` fills the four synonym classes, `financial_context` becomes
   a labeled Recent-context overlay, `regulatory_notes` a `[Guardrail]` *context*
   sentence — never the rule itself (the rule derives from an internal measure).
3. **Validate** — routing Pages are checked with **`ask_genie`** (does the NL
   question actually resolve to the intended measure?); MV bodies must parse (the
   existing MV rails). A detected contradiction is **downgraded to CONFLICT for
   human adjudication, never auto-resolved** (MV-D35).
4. **Dedupe (best-effort)** — Pages have **no read API**, so Page-vs-Page dedupe
   is name/synonym heuristics only — the flagged asymmetry from §5.
5. **Rank + persist** — L6 ranks evidence-first, L7 persists; each surfaces as a
   17.0e Page draft filed under its sub-domain, with Sources / Related pre-filled
   (`[Routing]` → the MV + measure as Sources, the Agent as Related; `[Guardrail]`
   → the numerator / denominator measures as Sources).

```
   L4 OUT:  a sub-domain community  (measures · coded columns · conflicts)
                        │
                        ▼
 ┌──────────────────────── L5  PAGE MINERS ───────────────────────┐
 │  ① DETECT (deterministic — one detector per archetype)          │
 │     each measure ─────────────► [Routing]   candidate           │
 │     term → many measures ─────► [Disambiguation] (from CONFLICT) │
 │     rate / AVG-of-rate ───────► [Guardrail]  candidate          │
 │     low-card coded column ────► [Taxonomy]   candidate          │
 │                        │  PageCandidate(archetype, evidence, conf)│
 │                        ▼                                         │
 │  ② DRAFT (LLM writes body)  ← Context Pack: synonyms / overlay   │
 │                        │                    (vocabulary only)    │
 │                        ▼                                         │
 │  ③ VALIDATE ── ask_genie (routing resolves?) · MV body parses    │
 │                        │  contradiction → CONFLICT (human)       │
 │                        ▼                                         │
 │  ④ DEDUPE best-effort (no Page read API) → name / synonym only   │
 └────────────────────────┬────────────────────────────────────────┘
                          │ ranked Page drafts (L6 → L7)
                          ▼
                 L8 API → 17.0e Page draft (under its sub-domain)
```

**Worked example.** Sub-Domain *Sales* holds the MV
`finance.sales.order_revenue` with measures `total_revenue` and `discount_rate`.
The `[Routing]` detector emits one candidate per measure (revenue → the governed
`total_revenue`). `discount_rate` is percentage-format → the `[Guardrail]`
detector fires ("never average `discount_rate`; recompute from the numerator /
denominator"). Two Agents define "revenue" over different expressions → a
CONFLICT fingerprint → a `[Disambiguation]` candidate. The LLM drafts each body,
`ask_genie` confirms "how much did we make?" routes to `total_revenue`, and the
drafts surface as 17.0e Pages under *Sales*.

**Honest gap.** Two, both already flagged: Page dedupe is best-effort because
Pages have no read API, and CONFLICT candidates are surfaced for human
adjudication rather than auto-merged — L5 deliberately *proposes* the conflict,
it never resolves it.

### The rank & trust gate (L6), in building blocks

Everything L4 and L5 produce — domain/sub-domain proposals, Page candidates, MV
and Agent suggestions — funnels into L6, which does two jobs: **rank** so the
reviewer reads the strongest first, and **firewall** anything unsafe or
structurally illegitimate before it is persisted or served. The governing rule
is **MV-D35: facts lead, the score ranks.** Quality is already binary-gated
upstream (validated body, dedupe non-overlap), so the score is a
**demand / importance ranking signal, never a "confidence" in correctness** —
and a bare percent is never displayed as confidence.

**① The ranking score — `usage × lineage-centrality × governance`.** Generalizes
the MV-advisor's LYDS blend (`mv_scoring`) from single-MV proposals to every
estate candidate. Three factors:

1. **usage** — demand from `query.history` / `billing.usage`: how much the
   underlying assets are actually asked about (and what they cost). The "is
   anyone using this?" signal.
2. **lineage-centrality** — degree / betweenness on the lineage subgraph,
   precomputed in L2/L4: "how load-bearing" — the spine everything joins to
   ranks above a leaf.
3. **governance status** — governed > curated > ungoverned, the traffic-light
   ladder (governed = success · curated = warning · ungoverned = danger): a
   proposal anchored on governed assets outranks one anchored on ungoverned ones.

The blend maps to a tier — **HIGH / MEDIUM / LOW with a sub-threshold suppress**
(reuse the MV-advisor thresholds + the **evidence-coverage cap** so a
single-signal opinion cannot outrank a corroborated finding).

**② The trust firewalls (a candidate must pass ALL to surface):**

- **PII firewall on tag names** — `leakage.LeakageOracle`, extended to tag
  names. Governed tag names replicate globally in plaintext, so a Domain /
  Sub-Domain proposal whose *name* would leak PII (an email, an id) is blocked,
  not surfaced.
- **Policy conformance** — propose-only is the default; the one writable thing is
  the consented `SET TAG` membership apply. Any candidate implying an
  instruction / card / Page write is rejected at the gate.
- **Provenance ladder (MV-D38)** — higher tiers always win: T0 internal-verified
  (system tables) > T1 company-official > T2 industry-canonical > T3
  web-inferred. External context can raise or lower a **vocabulary** rank but can
  never outrank graph-backed structural evidence — a T3 naming hint never beats a
  T0 lineage fact.

**③ The display contract (MV-D35).** Lead with the proven facts — evidence chips
(shared spine, N co-queries, cost share, governance rung) — and let the **tier**
order the list. The blended number ranks; it is never rendered as "NN%
confidence."

| Building block | Reuse anchor |
|---|---|
| `usage × centrality × governance` blend + tiering | `mv_scoring` blend / tier shapes (re-point to estate candidates) |
| Coverage cap (corroboration beats single signal) | `mv_scoring` evidence-coverage cap |
| PII firewall on tag names | `leakage.LeakageOracle` (extend to tag names) |
| Governance traffic-light (governed/curated/ungoverned) | the mockup ladder (17.0b evidence chips) |
| Provenance-ladder enforcement | MV-D38 tiers (§6) |
| Persist ranked + suppressed | `genie_ont_*` (L7) |

```
   L4 + L5 candidates  (domains · sub-domains · Pages · MV/Agent fixes)
                        │
                        ▼
 ┌──────────────────────── L6  RANK & TRUST ──────────────────────┐
 │  ① SCORE   usage × lineage-centrality × governance             │
 │            │  → tier: HIGH / MEDIUM / LOW  (coverage-capped)    │
 │            ▼                                                     │
 │  ② FIREWALLS (must pass ALL)                                    │
 │     PII firewall on tag names (LeakageOracle) ─┐               │
 │     policy conformance (propose-only)          ├─► pass / BLOCK │
 │     provenance ladder T0>T1>T2>T3 (MV-D38) ────┘               │
 │            │                                                     │
 │  ③ DISPLAY: facts lead (evidence chips), tier ranks — no "NN%"  │
 └────────────────────────┬────────────────────────────────────────┘
                 pass      │      block / sub-threshold
                          ├──────────────► suppress (run report only)
                          ▼
                 L7 persist (ranked) → L8 API → 17.0b / 17.0d / 17.0e
```

**Worked example.** A Domain proposal anchored on the governed `finance.sales`
spine (high centrality) with 41% query share (high usage) and governed status →
scores **HIGH**, surfaced facts-first. A speculative "Web3" sub-domain suggested
only from a T3 web hint, with no lineage or usage, ranks below every graph-backed
proposal and — if it never clears threshold — is **suppressed** (returned in the
run report, not shown). A tag proposal whose name echoes a column full of emails
is **blocked outright** by the PII firewall, regardless of score.

**Honest gap.** The factor weights and tier thresholds need calibration on real
estates (the same open-tuning caveat as L4), and the governance rung depends on
tag / certification coverage that is often sparse early — so the ladder leans
`ungoverned` until an estate matures, which the ranking must not over-penalise.

### The persistence layer (L7), in building blocks

L6 emits two streams: ranked candidates that passed, and suppressed ones that
did not. L7 durably records both so (a) the thin page reads fast and (b) a re-run
never re-surfaces what a curator already resolved. It is the **`genie_opt_mv_*`
pattern reused verbatim** — Delta written by the batch job, mirrored to Lakebase
for sub-second page reads — with the full schema in §7. The building blocks are
the tables, grouped by role:

- **Proposals** — `genie_ont_domains` (domain / sub-domain + `tag_decision` +
  evidence JSON), `genie_ont_members` (the `SET TAG` targets),
  `genie_ont_pages` (archetype, body, synonyms, Related / Sources, certify).
- **Governance snapshot** — `genie_ont_tag_graph` (existing governed tags +
  assignment counts + dedupe verdicts) → backs the 17.0c Tags lens.
- **Memory — the load-bearing part** — `genie_ont_consents` /
  `genie_ont_suppressions`: durable "applied / dismissed," so a re-run **skips
  what a curator resolved** (MV-D26). This is the same "never resurrect a
  rejected candidate" guarantee the MV-advisor enforces, and it is what makes the
  engine idempotent across runs.
- **External cache** — `genie_ont_context_pack` (+ the `genie_ont_context_sources`
  citation index) — the versioned, pinned MV-D38 pack (§6).
- **Audit** — `genie_ont_applied`: every executed `SET TAG` (who, when, dry-run
  vs real).

| Building block | Reuse anchor |
|---|---|
| Delta system-of-record (job-written) | `genie_opt_mv_*` table pattern |
| Lakebase mirror (page fast-reads) | `gso_lakebase.py` + `mv_state.py` |
| Suppression / consent ledger (idempotent re-runs) | MV-advisor suppression grain (MV-D26) |
| Derived candidate ids (stable across runs) | `sug_<fingerprint>` id scheme |

```
   L6  ranked  ─┐                        ┌──────────────────────────┐
               ├──►  L7  Delta (SoR) ───►│ Lakebase mirror (headers)│──► L8 reads
   L6 suppressed┘        genie_ont_*     └──────────────────────────┘
                              │
                              ▼  consent / suppression
                    feeds NEXT run's L3/L5 (don't re-surface resolved items)
```

**Idempotency in one line:** derived ids + the suppression ledger mean a re-run
produces the *same* candidate ids and filters out everything a curator already
dismissed — so nobody re-reviews a proposal they killed last week.

**Honest gap.** Pages have no read API, so `genie_ont_pages` is our *only* record
of a proposed Page — if a curator edits or deletes the published Page in
Discover, the engine can't reconcile that drift, so consent / suppression is
best-effort for Pages specifically (the same no-read-API asymmetry from §5).

### Serving + apply (L8 / L9), in building blocks

The page is deliberately **thin** — the §2 load-bearing decision. All heavy work
is materialized by the batch job; L8 only reads Lakebase and renders, and L9 is
the single optional write.

**L8 — serving (`/api/ontology/*`, new, thin):**

- A new FastAPI router that reads the **Lakebase mirror** (never recomputes), with
  endpoints mapping 1:1 to the mockup frames: **preflight** (17.0a permission
  banner), **taxonomy** (17.0b), **tags-lens** (17.0c), **drafts** (17.0d / e),
  **apply**.
- **Preflight** resolves the permission tiers — which readers ran, which are
  grant-blocked — and drives the tiered banner; missing grants **degrade, never
  hang**.
- **Fast-path exception** (§2): the OBO **inventory-only** tier (metric-view +
  tag counts via `information_schema`) is cheap enough to run in-request, so the
  page renders something instantly on first load while the full signal-weighted
  run fills in behind it.
- **Identity** (§8): OBO for user-scoped reads (inventory, `ask_genie` routing
  validation); the SP-only system-table signals were already materialized by the
  job, so the page itself never waits on them.

**L9 — apply (optional, the only write):**

- The one mutation the whole engine offers: the consented **`SET TAG` membership
  apply** (MV-D37), **default OFF**.
- Flow: **dry-run → preview diff → explicit consent → execute → audit**
  (`genie_ont_applied`), reusing the **MV consent rails** + the Databricks
  Automate-tag-assignment dry-run precedent.
- **Identity** (§8): **OBO** — the write is attributed to the consenting human and
  gated on their `MANAGE DISCOVERY` + `ASSIGN` grants (the write tier in the
  banner).
- Everything else — Pages, the Discover card, Agent instructions — stays
  **copy-ready**; no API writes. To the curator, L9 is just the "Apply for me →
  Preview changes" button on 17.0d; the DDL lives behind the scenes (the
  zero-burden contract).

```
 Browser ◄──► /api/ontology/*  (thin FastAPI, OBO) ──► Lakebase reads ──► frames
                    │
                    └─ apply path (optional):
                       draft → DRY-RUN → preview diff → CONSENT → SET TAG (OBO)
                                                                    └─► genie_ont_applied (audit)
```

**Closing the walkthrough.** L9 is the *only* place the engine mutates the
workspace, and only for tag membership — optional, off by default, dry-run-first.
An install that never grants the write tier still gets the entire L0 → L8
read-only advisory: the full estate-wide ontology, proposed and copy-ready, with
zero writes. That is the whole layer stack, end to end.

## 6. External context / enrichment tier (MV-D38)

External context makes the ontology domain-aware instead of pure schema
clustering — but it must never poison structure. The governing rule:

> **External context is a naming, description, and hypothesis layer — never a
> source of structural truth.** The graph (system tables) decides *membership*;
> the outside world only decides *vocabulary* and *informational overlay*.

This is the Page "Recent context" contract (MV-D28) generalized to the whole
engine: labeled, sourced, dated, `certify-no`, quarantined from authoritative
formulas.

### What the user sees (and what they don't)

> The engine does the heavy lifting; the user gets **clear suggestions for
> Domains, Sub-Domains, and Pages — nothing else.** The Context Pack, provenance
> tiers, validation, and caching are entirely internal. The Pack is a cache, not
> a UI.

The external-context feature collapses, on the user's side, to **two
touch-points**:

1. **One opt-in toggle** — "Use industry context to improve naming" (the Context
   Sources tier already in the banner; **disabled with a plain reason** when no
   context source is available, MV-D46/D47).
2. **An optional low-confidence confirm** — only when industry confidence is low:
   "You look like *Food Retail* — correct?" Skippable; skipping just runs the
   gap-check suppressed.

The full user-facing surface is: the ranked Domain / Sub-Domain / Page drafts
(the seven frames), those two touch-points, and an optional per-suggestion
evidence chip (e.g. *"named via industry model + 3 governed-tag matches"* —
informational, not actionable). **Never surfaced:** pack versions, tiers,
approval states, source lists (those live only behind the Recent-context
disclaimer on Pages), or any "manage context" screen. There is no per-section
sign-off — the engine self-validates and surfaces only what passed.

### Provenance ladder (higher tiers always win — MV-D35)

| Tier | Source | May influence | May NEVER touch |
|---|---|---|---|
| **T0 Internal-verified** | system tables, `information_schema`, `serialized_space` | membership, measures, lineage, certification | — (ground truth) |
| **T1 Company-official** | their own filings/10-K, internal docs they provide | descriptions, synonyms, domain names, Recent-context | membership, measure definitions |
| **T2 Industry-canonical** | Databricks industry data models, GICS/NAICS, standards bodies | cluster naming, gap *hypotheses*, synonym expansion | anything structural, certification |
| **T3 Web-inferred** | competitor sites, news, general web | weak naming hints, dated Recent-context | names on their own, any number, membership |

### Where external context plugs in

| External input | Stage | What it does | Guardrail |
|---|---|---|---|
| Company name → industry resolution (NAICS/GICS) | before L1 (Context Pack) | seeds the run's vocabulary | human-confirmable in Settings |
| Databricks industry data models / reference architectures | L4→L5 naming + L5 gap check | business-language cluster names; "industry has domain X, estate has none" | gap = hypothesis ranked below evidence-backed proposals, never auto-created |
| Competitor context | L5 | broaden synonyms, sanity-check naming | T3 — hints only |
| Wall Street filings / financials | Page enrichment (L5) | Recent-context on financial-metric Pages | must cite filing URL + as-of date, or dropped; `certify-no` |
| Regulatory context | L5 + guardrail Pages | inform `[Guardrail]`/`[Rule]` Page *context* | informational only — the rule derives from an internal measure/column |
| Industry acronyms / jargon | L5 (synonyms) | retrieval-critical synonym classes | PII firewall on tag names |

### The provenance envelope (internal atomic unit)

External context is resolved **once per company**, into a versioned, cached
Context Pack, passed as a read-only prior into L4/L5 — never sprinkled as inline
calls, and never edited by the user. Every external leaf is wrapped so L6
ranking treats pack facts and graph facts uniformly:

```
Provenanced<T> {
  value:        T,
  tier:         "T0" | "T1" | "T2" | "T3",     // internal → web-inferred
  source_url:   string | null,                 // required for T1–T3 numbers
  source_kind:  "system_table" | "filing" | "industry_model" | "standards_body" | "web" | "llm_synthesis",
  as_of:        date,
  confidence:   float,                          // 0–1
  decay_weight: float                           // recency-adjusted at read time
}
```

### The Context Pack (internal artifact — the user never edits this)

```
ContextPack {
  # envelope — identity, versioning, pinning (all internal)
  pack_id, company_key, version, content_hash, generated_at,
  generated_by:  { model, egress_log_ref },
  status:        "active" | "stale" | "superseded",   // internal, auto-managed
  refresh_policy:{ ttl_days, financials_ttl_days },

  # industry resolution — the ONLY field with an optional user confirm
  industry: {
    codes:           Provenanced<{ naics, gics_sector }>,
    label:           Provenanced<string>,       // "Consumer Staples — Food Retail"
    user_confirmed:  bool,                        // set only if the user confirmed
    gate_confidence: float                        // < τ ⇒ suppress gap-check + prompt confirm
  },

  # advisory template — "is_template: true" ⇒ NEVER structural
  canonical_domains: [ { name: Provenanced<string>, description: Provenanced<string>,
                         typical_subdomains: Provenanced<string>[], is_template: true } ],

  # retrieval-critical synonyms (the 4 classes)
  lexicon:  [ { term, synonyms: Provenanced<string>[],
                synonym_class: "acronym"|"casual"|"jargon"|"abbrev", pii_scanned: true } ],

  # Page Recent-context only — certify-no; number dropped if no URL+date
  financial_context: [ { concept, segment?, period, value: Provenanced<string> } ],
  regulatory_notes:  [ { regime, scope: Provenanced<string>, applies_to_hint: string[] } ],
  competitors:       [ { name, public_segments: Provenanced<string>[] } ],

  # safety / audit (internal)
  egress_log:   [ { url, fetched_at, sha256 } ],
  pii_findings: [ { field_path, action: "redacted"|"blocked" } ]
}
```

Why an artifact, not inline calls: **reproducibility** (a run is deterministic
given a pinned pack — web volatility can't silently rename domains between runs);
**cost/latency** (web + LLM synthesis is slow and rate-limited → batch only,
cached, slow refresh); **auditability** (the egress log + citation index record
every external claim). It is NOT a review surface — the engine self-validates and
the user never approves or manages a pack (see the user-facing contract above).

### Storage (`genie_ont_context_pack` + a queryable citation index)

Same header-columns + JSON-payload pattern as `genie_opt_mv_*`, plus a normalized
index so the egress audit is a `SELECT`:

- **`genie_ont_context_pack`** — one immutable row per `(company_key, version)`:
  header columns (`pack_id, company_key, version, content_hash, status,
  industry_code, gate_confidence`) + the full pack as JSON.
- **`genie_ont_context_sources`** — one row per provenanced leaf with a
  `source_url`: `(pack_id, field_path, tier, source_url, source_kind, as_of,
  sha256)` — powers the egress audit and lets a dead source invalidate a claim.
- Lakebase mirrors only the header for the page's sub-second reads.

### Lifecycle (fully automatic — no user workflow)

```
(opt-in on) → generate → self-validate (firewall · no-unsourced-numbers · PII scan · confidence-gate)
            → active → [stale after TTL] → regenerate → supersede
```

No `unreviewed/approved` states, no per-section sign-off. The engine validates
and either uses a field or drops it. The **only** human interrupt is the optional
low-confidence industry confirm; skipping it runs the gap-check suppressed.

### Consumer binding (which section feeds which layer — and its leash)

| Pack section | Consumer | Effect | Leash |
|---|---|---|---|
| `industry` | L4 naming + L5 gap-check | vocabulary; enables gap hypotheses | `gate_confidence` < τ ⇒ gap-check off + prompt confirm |
| `canonical_domains` | L4 naming; L5 gap hypotheses | names clusters; "industry has X, estate doesn't" | `is_template`; gap ranked below graph-backed proposals |
| `lexicon` | L5 Page synonyms | fills the 4 synonym classes | PII-scanned before any becomes a name |
| `financial_context` | L5 Page Recent-context | informational overlay | `certify-no`; dropped if no URL+date |
| `regulatory_notes` | L5 guardrail Page context | context sentence, not the rule | rule derives from an internal measure/column |
| `competitors` | L5 naming | weak hint | T3 — never a name alone |

**No pack section binds to L2/L3 or the membership/measure writers** — the
structural firewall is the binding table itself.

### The rails

1. **Structural firewall** — external context is passed only as a prior to
   naming/description/synonym/gap functions; it is architecturally unable to
   reach membership or measure-definition writers (separate inputs/paths).
2. **No unsourced numbers** — any financial/regulatory figure without a citable
   URL + as-of date is dropped, not guessed (LeakageOracle firewall extended).
3. **Labeled + dated everywhere** — anything T1–T3 in an artifact carries the
   disclaimer *"Informational context summarised by an LLM from public sources
   as of DATE. Not certified operational data."* + a Sources list; `certify-no`.
4. **Opt-in Context Sources tier (MV-D46/D47)** — external context is a fifth
   capability tier in the permission banner, surfaced as a **Context Sources
   panel** of opt-in **AI Gateway MCP** services (`system.ai.web_search`, You.com,
   Confluence/Drive/M365, internal Genie/SQL), each `EXECUTE`-granted and
   policy-governed (§6.1), not raw egress. No source available / disabled → the
   toggle is disabled with a plain reason and the engine degrades to estate-only
   naming; nothing breaks.
5. **Recency decay** — T3 web facts decay by age; T2 canonical models are
   stable; T0/T1 don't decay.
6. **Confidence gating** — a low-confidence company→industry resolution
   suppresses the canonical-domain gap-check entirely (don't hallucinate a
   missing domain off a bad sector guess).
7. **Zero user burden** — the Context Pack is an internal cache. The user's
   entire surface is the ranked Domain/Sub-Domain/Page suggestions plus one
   opt-in toggle and one optional low-confidence confirm. Provenance and
   validation are internal; the engine surfaces only what passed. No pack
   versions, tiers, approval states, or "manage context" screen are ever shown.

### 6.1 Context sources — a governed AI Gateway MCP registry (MV-D46 / MV-D47)

Enrichment does **not** reach the internet directly. It calls an **opt-in registry
of Unity AI Gateway MCP Services** — each a **Unity Catalog securable** invoked
through the Gateway (`https://<host>/ai-gateway/mcp-services/<fqn>`, JSON-RPC
`tools/call`; managed MCP servers use `…/api/2.0/mcp/…` with OAuth scopes). The
Gateway proxies **managed credentials**, so no tokens ever live in app code, and
the platform enforces the firewall (built-in write-block policy; optional
PII-block / human-approval, `ON CALL`/`ON RESULT`). Every registry entry carries
`{class, provenance_tier, influence}` so the MV-D38 structural firewall holds:

| Source (lead registry) | Class | Tier | May influence |
|---|---|---|---|
| `system.ai.web_search`, You.com (`myyoumcp`) | external | **T3** web | naming / synonyms / gap-hypothesis / Recent-context |
| Confluence (Atlassian), Google Drive, Microsoft 365 | external | **T1** company docs | naming / description / synonyms |
| GitHub | external | T1–T2 | naming / description |
| Genie One (`…/mcp/genie`), Databricks SQL (`…/mcp/sql`), UC functions | **internal** | **T0** verified | **structural signal + validation** |
| Gmail, Slack, Calendar | external | — | **default OFF / excluded** (PII-heavy, low-signal) |

- **Web search (MV-D46)** — primary is the managed **`system.ai.web_search`** MCP;
  the **fallback ladder** (probe in order, degrade-not-hang) is You.com MCP → a
  Model-Serving-native web tool (Gemini `google_search` / OpenAI `web_search`) →
  **estate-only**. Capability probe = `EXECUTE` on the service; **HIPAA/BAA**
  workspaces stay hard-off.
- **Firewall by class** — external/overlay MCPs reach **only** naming/description/
  synonym/gap/Recent-context; they never touch membership or structure. The
  internal UC-backed MCPs (Genie/SQL) may add **structural signal** (verified UC)
  and **validation** — `ask_genie` checks a routing Page, and the reflexive loop is
  that the Genie MCP *consumes* the Genie Ontology this engine populates.
- **Governance is Databricks-native (MV-D45)** — `EXECUTE` grants + service policies
  + `system.ai_gateway.usage` (`service_type='MCP_SERVICE'`) / `system.access.audit`
  (`mcpCall`), read via the GenieWatch SP pattern. No app-built egress controls.
- Results cached in `genie_ont_context_pack` (Delta + Lakebase), pinned by version
  to the run that consumed it; the MS-Learn / Databricks-docs MCP is the T2 template.

Refs: [managed MCP servers](https://docs.databricks.com/aws/en/agents/mcp-tools/managed-mcp),
[MCP Services](https://docs.databricks.com/aws/en/agents/mcp-tools/mcp-services),
[govern an MCP service](https://docs.databricks.com/aws/en/ai-gateway/govern-mcp-service).

## 7. The data model (extends the GSO precedent)

**Grain: the METASTORE, not the workspace (MV-D49).** Domains/Sub-Domains are
governed tags and Pages are Discover artifacts — both metastore-scoped — and the
substrate (metric views, tables, lineage, `system.tags.governed_tags`) is Unity
Catalog, also metastore-level and read with no workspace filter (bounded only by
the MV-D42 catalog allowlist). So every `genie_ont_*` table is keyed by
**`metastore_id`**, the idempotent MERGE's `WHEN NOT MATCHED BY SOURCE DELETE` is
**metastore-scoped**, and the batch runs **once per metastore** (the idempotent
MERGE makes a single scheduled runner safe; duplicate installs converge).
`workspace_id` is retained only as **provenance** (which install triggered a run;
which workspace an Agent lives in) — never as a partition key. The shipped 17d/17e
code is still `workspace_id`-keyed and is reconciled to this grain by the dedicated
re-grain phase (`ontology-regrain-build.md`) before 17f.

Named `genie_ont_*` to sit beside `genie_opt_mv_*` in the same GSO
catalog/schema, written by the job, mirrored to Lakebase for the page:

- `genie_ont_domains` — proposed domain/sub-domain, parent ref, name,
  description, **tag_decision** (reuse `Finance/Tax` vs create), evidence JSON.
- `genie_ont_members` — (domain_id, asset_fqn, asset_type) the `SET TAG` targets.
- `genie_ont_pages` — archetype, title, body, synonyms, related/sources FQNs,
  certify recommendation.
- `genie_ont_tag_graph` — snapshot of existing governed tags + assignment counts
  + dedupe verdicts (backs the Tags lens).
- `genie_ont_context_pack` — the versioned external Context Pack (MV-D38).
- `genie_ont_consents` / `genie_ont_suppressions` — durable "applied /
  dismissed", so re-runs don't re-surface what a curator resolved (MV-D26).
- `genie_ont_applied` — audit of executed `SET TAG` writes (who, when, dry-run
  vs real).

## 8. Identity map (which client per stage)

| Stage | Reads/writes | Identity | Why |
|---|---|---|---|
| Inventory (tables, tag assignments) | `system.information_schema.*` | **OBO** | auto-filtered; shows what the user may see, no grant |
| Usage/lineage/cost | `system.access.*`, `billing.usage`, `query.history` | **SP** | not OBO-readable — GenieWatch pattern |
| Governed-tag catalog | `system.tags.governed_tags` | **SP** | account-level read |
| External enrichment (MV-D38/D46/D47) | AI Gateway MCP context sources (`system.ai.web_search`, You.com, Confluence/Drive/M365, internal Genie/SQL) | **Batch** | `EXECUTE`-granted UC securables; Gateway proxies managed creds; service-policy firewall; usage/audit in `ai_gateway.usage`; cached to the Context Pack |
| Routing validation | `ask_genie` | **OBO** | validate as the user would experience it |
| `SET TAG` apply | `manage_uc_tags` / DDL | **OBO** | write attributed to the consenting human |

## 9. Pipeline sequence (one run)

1. **Preflight** resolves tiers → decides which readers can run (degrade if SP
   grants missing; degrade if no context source has `EXECUTE`, MV-D46/D47).
2. **Context Pack** (MV-D38) resolved/loaded if the enrichment tier is on and at
   least one AI Gateway MCP context source is available.
3. **Readers** (L1) pull signal frames, cached.
4. **Graph build** (L2) fuses them.
5. **Embed + ER** (L3) — dedupe **first**, so clustering operates on canonical
   entities and we never grow tag sprawl.
6. **Cluster** (L4) → domains/sub-domains with evidence; named with the Context
   Pack prior.
7. **Propose** (L5) domains, Pages, MV fixes, Agent assignments; **validate**
   (MV bodies parse; routing Pages resolve via `ask_genie`).
8. **Rank + firewall** (L6) — evidence-first; provenance ladder enforced.
9. **Persist** (L7) to Delta + Lakebase; suppressed items filtered.
10. **Serve** (L8); curator reviews.
11. **Apply** (L9) — optional, dry-run → consent → `SET TAG`, audited.

## 10. What's genuinely new vs. reused

- **Reuse nearly wholesale:** the SP system-table reader + TTL cache, the grant
  preflight list, the `genie_opt_mv_*` persistence + Lakebase-mirror pattern,
  `llm_utils`, the leakage firewall, the GSO job packaging, the MV consent rails,
  the MV-body validation, the Page "Recent context" enrichment contract.
- **Build new:** the estate-wide graph builder (L2), the ER/dedupe engine (L3),
  the community-detection clustering (L4), the domain/Agent proposers (L5), the
  Context Pack resolver (MV-D38), and the thin `/api/ontology/*` router + page
  (L8/L9). The Page/MV miners are largely a re-point of existing
  fingerprint/scoring shapes onto the estate graph.

## 11. Architectural decisions (resolved → MV-D39–D47)

These were the open build decisions; all are now **DECIDED** and recorded in the
playbook register (`mv-advisor-playbook.md`, MV-D39–D47). The original six §11
leans became MV-D39–D44; three follow-on component decisions (MV-D45 install
footprint, MV-D46 governed web-search MCP, MV-D47 AI Gateway MCP context-source
registry) fold in the Lakebase Search + AI Gateway MCP choices. The leans became
the decisions:

1. **Compute engine + algorithm for the graph** → **MV-D39: in-job Leiden**
   (`leidenalg` over `python-igraph`, comfortable to ~10⁵ nodes) inside the GSO
   wheel — multiplex layers + soft-seeded (`initial_membership`) + CPM objective,
   with soft-seed disagreements surfaced as human-adjudicated `reassign` proposals
   (17g), never auto-switches. Leiden is
   chosen over Louvain (which can emit disconnected communities under the
   recursive split) and over Spark **GraphFrames** (LPA-only — no Leiden — and a
   Scala JAR awkward on the serverless job, `environment_version 4`, where the
   `leidenalg`/`igraph` manylinux wheels install via pip with zero cluster
   libraries). GraphFrames stays the documented escalation-only path if an
   account's fused graph overflows job memory. The L2 builder hides the algorithm
   behind one interface so the swap stays local.
2. **Similarity substrate** → **MV-D40: Lakebase Search** (`lakebase_vector` ANN
   + `lakebase_text` BM25) on the **already-installed** Lakebase — not a separate
   managed Vector Search service; embeddings reuse the existing `databricks-gte-
   large-en` FMAPI endpoint; introduced in Phase 3 behind an interface that
   degrades to in-process cosine (MV-D45). This removes a net-new managed service
   from the install footprint.
3. **Refresh model** → **MV-D41: nightly materialize + on-demand override**, both
   writing `genie_ont_*` through the same idempotent path.
4. **Scope boundary** → **MV-D42: an opt-in catalog allowlist in Settings** (not
   account-wide by default), bounding cost and blast radius.
5. **First-render latency** → **MV-D43: the OBO inventory-only fast path** renders
   in-request while the full run is backgrounded — never block (degrade-not-hang).
6. **External enrichment default** → **MV-D44: OFF by default**, opt-in per
   workspace; the Context Pack **self-validates** (never a user approval, per the
   MV-D38 zero-burden contract) before it steers naming. Mechanism is
   **MV-D46: a governed web-search MCP** (`system.ai.web_search` via Unity AI
   Gateway) with a You.com / Model-Serving fallback ladder — see item 8 and §6.1.

Three further component decisions bound the install footprint and enrichment:

7. **Install footprint** → **MV-D45: no net-new managed service**; the engine
   reuses Lakebase (persistence + Lakebase Search), Model Serving (LLM +
   embeddings), AI Gateway MCP context sources (web search + enrichment), the GSO
   job, system tables, and UC governed tags. Every external tier is
   capability-probed and degrades, never blocks; Phase 1 adds zero services.
8. **Web search** → **MV-D46: the managed `system.ai.web_search` MCP** through
   Unity AI Gateway (`EXECUTE`-granted UC securable, built-in write-block policy,
   usage-tracked), with a You.com / Model-Serving-native / estate-only fallback
   ladder. Not raw egress; not a per-provider adapter.
9. **Enrichment substrate** → **MV-D47: an opt-in registry of AI Gateway MCP
   context sources**, classified `{class, provenance_tier, influence}` and
   firewalled by class (external = naming-only, internal Genie/SQL = structural +
   validation). Governance is Databricks-native (`EXECUTE` + service policies +
   `ai_gateway.usage`/`audit`), per §6.1.

**Build sequencing.** With these closed, the engine is buildable in phases. The
first Goal-Mode slice is the read-only spine (preflight → OBO inventory →
tag/lineage taxonomy → serve 17.0a/b/c), specified in
`ontology-phase1-build.md`; clustering sophistication (MV-D39 Leiden via `leidenalg`),
Lakebase Search dedupe (MV-D40), the nightly batch (MV-D41), external enrichment
(MV-D44/D46), and the `SET TAG` apply (L9) land in later phases.
