# Ontology — Curation Redesign: Domains, Sub-Domains, Pages (signals-first, build spec)

**Status:** build-ready by stage (Stage 1 first; offline slice + deploy-gated
verification) · **Owner directive:** MV-D51–MV-D60 (see `mv-advisor-playbook.md`,
**Decisions register**), inheriting MV-D35 / MV-D37 / MV-D38 / MV-D39 / MV-D43 /
MV-D45 / MV-D49 / MV-D50 unchanged. **Design source of truth:**
`ontology-engine-architecture.md` (§3–§6). **Builds on:** the shipped 17d (signal
graph + ER), 17e (Leiden clustering), the metastore re-grain (MV-D49), 17f (Pages),
17g (rank/serve/drafts) — all on the `ontology` branch.

> **The one-line contract:** the engine today *defines a Domain as a governed tag*
> and ignores the estate's real structure. This redesign makes Domains **business
> areas discovered from strong, explainable signals** (foreign keys, joins, metric
> views, naming), demotes governed tags to corroboration, separates **facets** from
> **aboutness**, and makes every suggestion **show its reason and honest
> confidence**. It changes **no grain** (MV-D49), **no identity model** (MV-D50),
> and adds **no new dependency** (MV-D45).

---

## 1. Why (the live evidence)

An Appendix-A signal inventory was run against the live airline estate
(`serverless_stable_6t92c3_catalog`, 2026-08-31) — full numbers in **§Appendix A**.
The headline:

- **Every populated "Domain" is a governed tag** (17/17 `tag_decision=reuse`, all
  `score=100.0`). The top members are **facet / label / demo tags**: `Contains
  Synthetic` (112 members), `Data Tier`, `Certification`, `Controlled Placeholder`,
  `Governance`, a tag literally named `Domain`, `Demo Domain`, `Demos`,
  `Techsummit-fy27`, `Reference`. These are attributes of data, not business areas.
- **Zero airline business domains** (Revenue, Maintenance, Loyalty, Reservation,
  Route, Fleet, Passenger) surfaced — because those schemas carry no governed tag,
  and tags are the only seed.
- Meanwhile the estate is **structurally rich and unused**: **200 foreign keys +
  95 primary keys**, **99 cross-schema FK edges** (vs 94 same-schema — the FK graph
  fuses schemas into bounded contexts), **40 metric views** + 12 materialized views,
  **10 `airline_demo_mvm_*` schemas literally named as business areas**, and dozens
  of shared join columns (`aircraft_id`×14, `fare_id`×9, `route_id`×8, `pnr_id`×8,
  `ffp_member_id`×8).

Root cause is a **definitional inversion** (`cluster.py` seeds `tag_assignment` at
weight 5.0; `_bind_level` marks every top-level tag `reuse`) compounded by ignoring
the strongest signals and by a coverage-cap **tier** (`rank.py`) users can't read.

## 2. Goals & principles

**Goals, in priority order:** (1) **meaningful** — suggestions look like real
business domains; (2) **explainable** — every card shows WHY + HOW SURE in plain
language; (3) **simple & manageable** — deterministic rules + visible thresholds,
one repo serving many enterprises; (4) **multi-enterprise** — sane defaults, config,
graceful degradation on sparse estates.

**Principles.** Four plain questions that double as the card's explanation:
1. Business area, or a data label? (facet vs aboutness — MV-D51)
2. Big & connected enough? (legitimacy bar — MV-D57)
3. Why this, how sure? (evidence + honest confidence — MV-D56)
4. Sound like a real domain? (industry-name alignment — MV-D58)

Cross-cutting: **signals-first** (fix grouping before gates/presentation); **rules
where decisive, clustering for the remainder** (MV-D53); **align, don't conform**
(MV-D58); **map, don't merge** across contexts (MV-D60); **degrade, never block**
(MV-D43); reads default to **OBO** (MV-D50); **metastore grain** throughout
(MV-D49); **no new dependency** (MV-D45).

**Non-goals:** replacing Leiden (retained); a universal canonical model; any change
to grain, the OBO/`run_as` identity model, or the single-writer apply path (17i).

## 3. Scope

**In:** the wheel engine (`ontology/{graph,cluster,pages,rank,transforms,
materialize,ddl,er}.py`), the job reader (`jobs/run_ontology_materialize.py`), the
backend serve/settings seam (`backend/ontology/services/{mirror,refresh,ont_settings,
drafts}.py`, `routers/{taxonomy,tags,drafts}.py`), and the drafts UI
(`frontend/src/ontology/**`). **Out:** the apply path (17i), the Estate Graph
(17k / MV-D48), and the skipped-refresh banner (Appendix B — independent).

## 4. Architecture overview

```
reader signals ─▶ facet/aboutness split ─▶ rules-first grouping ─▶ Leiden remainder
                                                                        │
   sub-domain boundaries ◀───────────────────────────────────────────┘
        │
        ▼
   Page mining ─▶ rank + honest "why" ─▶ industry name alignment ─▶ MERGE (metastore) ─▶ serve
```

Every stage attaches **evidence**; the serve layer renders that evidence as the
explanation (MV-D56). An offline **evaluation harness** (MV-D59) scores each run
against the aligned reference. Grain/OBO/firewall invariants hold at every stage.

---

## 5. Stage 1 — Signals + rules-first grouping (root cause) · MV-D51/52/53/60

This is the **build-ready** stage. Everything here is **offline** in the wheel +
job reader; **no backend/route/TS contract changes**, **no new settings column**
(config surface lands in Stage 3), **no new dependency**.

### 5.1 Signals (MV-D52)
`build_signal_graph` today carries `tag_assignment`, `lineage_adjacency`,
`co_query`, `agent_scope`, `semantic_sim`; `join_key` is declared but **empty**. Add
these edge kinds (all readable via existing `information_schema` / MV YAML — no new
dependency), each stamped with `source` + `as_of` like the existing edges:

| New signal | Source (read as OBO/`run_as`) | Edge kind | Strength |
|---|---|---|---|
| **FK / PK** | `information_schema.{referential_constraints,key_column_usage,constraint_column_usage}` | `join_key` (populate the empty layer) | decisive |
| **Shared join-column** | `information_schema.columns` (same `*_id/_key/_code` name across tables) | `join_key` (FK proxy, lower weight) | strong |
| **Metric-view membership** | MV YAML via existing `estate_metric_view_yamls` (reused from 17f) | `mv_membership` (MV → source tables) | curated |
| **Naming / schema** | `information_schema.{schemata,tables}` (shared schema + tokenized name stems) | `schema_affinity` | strong |
| **Query-history JOIN** (follow-on) | `system.query.history` JOIN parse | upgrade `co_query` | medium |

Governed **tags demote from seed to corroboration**: `tag_assignment` weight drops
from 5.0 to ≈ lineage/2; tags no longer *create* a candidate on their own.

### 5.2 Facet vs aboutness (MV-D51)
Before any tag becomes a domain candidate, `transforms.py` classifies each governed
tag as **ABOUTNESS** (business area → feeds domains as evidence) or **FACET**
(sensitivity / tier / quality / lifecycle / PII / synthetic / certification / status
/ team / demo). Heuristic-first, deterministic:
- A shipped **facet pattern list** (default constants in `transforms.py`; lifted to
  `OntologySettings.domain_facet_denylist` in Stage 3). Seed from the live evidence:
  `contains_synthetic`, `data_tier`, `certification`, `controlled_placeholder`,
  `governance`, `open_reference`, `reference`, `demo`/`demos`/`demo_domain`,
  `techsummit-*`, a bare `domain`, `*_team`.
- Signal backstops: very high assignment cardinality with low structural cohesion, or
  a value set that looks enumerated (tier/level/yes-no), leans facet.
- Optional **injected LLM tiebreaker** for genuine ambiguity only — degrades to the
  heuristic verdict if unavailable (MV-D43). Never scores every tag.
Facet tags route to a facet catalog (recorded in `evidence` JSON; an optional
`genie_ont_facets` table is Stage 3) and **out of domain candidacy**.

### 5.3 Rules-first grouping (MV-D53)
`cluster.py` groups **deterministically where a signal is decisive**, in precedence:
1. **Curated domain tag / UC data domain** — authoritative, immovable (aboutness tags only).
2. **FK-connected component** (+ shared-join-column proxy) — the primary structural rule.
3. **Metric-view membership** — an MV's source tables form a curated group.
4. **Shared schema** — assets in one schema (esp. named business areas) group.

Assets **no rule resolves** fall through to **Leiden/CPM** (`leidenalg`, MV-D39 —
**retained, not replaced**) as the remainder pass. Each rule stamps a machine +
human-readable reason into `evidence` (e.g. `"grouped by foreign key: fact_revenue →
dim_route"`, `"same schema: airline_demo_mvm_revenue"`). Deterministic, offline,
fixed-seed.

### 5.4 Identity map-not-merge (MV-D60)
`er.py` continues to merge exact/near-duplicate tags **within** a context, but
records cross-context correspondences (`same-as` / `role-of` / `related`) instead of
collapsing them; cross-context homonyms become `[Disambiguation]` Page candidates in
Stage 4. No change to the canonical-id scheme.

### 5.5 Stage-1 acceptance (offline)
`./scripts/test.sh` green, including: FK/PK & shared-join extraction from a fixture
`information_schema`; `join_key` layer populated; facet classifier routes the seed
denylist to FACET and keeps `Revenue`/`Maintenance` as ABOUTNESS; rules-first
grouping produces FK-connected + schema-named domains **without** any tag; tag-only
input no longer creates a single-asset domain; Leiden runs only on the unresolved
remainder; evidence carries a plain reason per assignment; map-not-merge keeps two
contexts distinct; contract-frozen (routes + `OntologyRefreshStatus` / taxonomy /
tag-lens byte-identical); firewall unchanged (no SET/UNSET/CREATE TAG, no
`web_search`); `uv.lock` untouched. `npm run lint` + `tsc` clean.

---

## 6. Stage 2 — Sub-domains from explicit boundaries · MV-D54
Per Domain, derive sub-domains from **explicit boundaries first**, in precedence:
sub-tags (`Domain/Sub` convention) → **value sub-tag** (a distinct `tag_value` of a
value-carrying tag on the Domain's assets, e.g. `mvm_subdomain=fare_pricing` — the 14
values proven in findings §3; the one genuinely-new read, threaded additively through
`transforms.assemble_tag_graph`) → schema-within-domain → MV/FK components; the finer
Leiden split is the **fallback only** when no explicit boundary exists (today
`cluster.py` *always* re-clusters). Each sub-domain carries its boundary reason.
Offline; grain unchanged (sub-domains stay `genie_ont_domains` rows with `parent_id` +
`Domain/Sub` `tag_value`, MV-D49).

## 7. Stage 3 — Gates + explainable why/confidence + config · MV-D56/57
- **Facet filter** (§5.2) as the front gate.
- **Legitimacy bar** (config; moderate defaults **≥3 tables / ≥2 schemas / ≥1
  connection**): below bar → an "add to existing domain" hint, not a new domain.
- **Evidence chips** surfaced from `evidence` (anchor / FK / MV / schema / tag prior
  / agents).
- **Honest confidence** replaces the opaque tier: a readable band + the signals
  present + the gap (`"Medium — FK-connected + shared schema; connect query history
  to rank by usage"`). Score still orders the queue but never renders bare.
- **Config surface** (additive, defaulted, idempotent `ADD COLUMN IF NOT EXISTS` —
  the MV-D50 pattern; mirrored in `types.ts`; exposed in `SettingsForm`):
  `domain_facet_denylist`, `domain_min_tables=3`, `domain_min_schemas=2`,
  `domain_require_connection=true`, `industry_alignment={enabled,reference_model}`.
- **Name dedup / qualification (Stage-1 live finding — see §Appendix A.2).** Two
  domains must never share a rendered `name`. When the FK-component rule and the
  shared-schema rule (or two structural components) yield the same label, qualify by
  the distinguishing scope (`Cost Attribution (finance)` vs `Cost Attribution
  (ops)`) or, when they cover the same asset set, collapse to one. The `domain_id`
  is already a member-fingerprint (distinct sets → distinct ids); this gate only
  fixes the human-facing **label** collision. Applies to sub-domains too (the raw
  duplicated `Dev … Metrics` sub-labels seen live collapse once Stage 2's explicit
  boundaries name them).
- **Curated-tag absorbs its structural component (Stage-1 live finding — see
  §Appendix A.2).** When a curated-domain-tag domain (e.g. `Alaska Airlines
  Maintenance and Engineering`, 19) and an FK-component domain (e.g.
  `airline_demo_mvm_maintenance`, 46) overlap in members or share a governed home,
  they are the **same** business area surfaced by two rules — do not emit both. The
  curated tag wins the identity (name + `reuse` decision) and **absorbs** the FK
  component's members; the structural signal becomes corroborating `evidence`
  (raising confidence), not a rival domain. Reconciliation is deterministic
  (member-overlap ≥ threshold OR shared parent tag) and precedes ranking.

## 8. Stage 4 — Pages · MV-D55
`pages.py` keeps concept-anchoring (17d `canonical_id`) + the eight archetypes, but:
- **Triggers broaden** beyond MV measures to coded columns, business terms in
  table/column comments, and recurring Genie-history disambiguations — all
  corroboration-gated (≥2 independent artifacts, MV-D35).
- **Attachment**: to the domain of the **majority of the Page's source tables**
  (not a single pre-populated signal).
- **Asset tagging**: Sources = backing MVs/tables; Related = serving agents; each
  with a one-line "why this asset."
- Read-only; no Agent-instruction write (MV-D27); `page_id` stays concept-anchored.

## 9. Industry-reference alignment · MV-D58
Load the matching **Vibe industry model** (domain taxonomy + ontology JSON); hybrid
match (string + embedding seed anchors → structural propagation → semantic sanity)
emitting **typed** correspondences (`exact` / `narrower` / `broader` / `derived` /
`not-equivalent`) + gap-domain hypotheses. **Provenance-gated** (T2/T3): it can
suggest and name but **never outranks** a T0/curated fact (`rank.py` ladder). Folds
into the Phase-4 / 17h Context Pack + firewall (MV-D38); toggle off by default
(MV-D44).

## 10. Evaluation & trust harness · MV-D59
Offline harness reporting, per run: gold-standard precision/recall/F of discovered
domains vs the aligned reference; structural health (singleton/orphan rate, depth,
branching — the metrics the Vibe repo itself reports); a cheap reference-free LLM
sanity monitor; and a human spot-review queue. Gates every subsequent
signal/threshold change.

## 11. Data-model impact
Prefer the existing `evidence` JSON for new fields (rule reasons, facet class,
confidence band, alignment relations) to avoid DDL churn (MV-D45/49). Optional
additive tables, metastore-keyed, CDF-on, no retired columns: `genie_ont_facets`
(Stage 3), `genie_ont_alignment` (Stage-4/Phase-4). No key or grain change.

## 12. Guardrails / invariants (every stage)
Metastore grain (MV-D49); OBO-default reads (MV-D50); no new dependency (MV-D45,
`uv.lock` untouched); degrade-not-hang (MV-D43); **read-only foundations** — no
SET/UNSET/CREATE GOVERNED TAG, no `manage_uc_tags`, no `web_search` here (writes stay
carved to `apply.py`, 17i); frozen earlier route/response contracts; deterministic +
offline wheel logic; fixed Leiden seed.

## 13. Phasing & sequencing (signals-first)
Stage 1 (signals + facet split + rules-first grouping + map-not-merge) → Stage 2
(sub-domains) → Stage 3 (gates + why/confidence + config) → Stage 4 (Pages) → §9
alignment folds into Phase 4. Each stage: **offline code + tests first**, then a
**deploy-gated** live run + review before the next. Stage 1 ships first because it
fixes the root cause the live inventory exposes.

## 14. Definition of Done (per stage)
- `./scripts/test.sh` green incl. the stage's new tests; `npm run lint` + `tsc` +
  vitest green; `uv.lock` / `package-lock.json` untouched (MV-D45).
- **Stage 1 specifically:** facet tags no longer appear as Domains; FK/MV/naming
  signals drive at least the airline business domains (Revenue, Maintenance,
  Loyalty, Reservation, Route, Fleet, Passenger) on the live estate; single-asset
  tag "domains" gone; every assignment carries a plain reason.
- Grain / OBO / firewall invariants asserted (§12).

## 15. Risks & mitigations
- Slower feedback (touches reader/wheel) → offline extractor tests + this live
  inventory before building.
- FK/MV sparse on some estates → naming/schema + tags backstop; honest confidence
  states the gap.
- Config sprawl → moderate defaults, small surface, all inspectable.
- Reference-alignment overreach → typed relations + provenance firewall (never
  overwrites), off by default.

---

## Appendix A — Live signal inventory (evidence for the thresholds)

Run 2026-08-31 against `serverless_stable_6t92c3_catalog` (warehouse
`41cfe645e10807a4`), as the OBO admin. This is the empirical basis for §1 and the
Stage-1/Stage-3 defaults.

**Current engine output (tag-only) — the problem:**
- 17 "Domains", **100% `tag_decision=reuse`, all `score=100.0`**.
- By member count: `Contains Synthetic` **112**, `Product` 33, `Demo Domain` 19,
  `Domain` 15, `Northwind Domain` 9, `Ep Domain` 9, `Data Tier` 8, `Treasury Domain`
  7, `Demos` 3, `Reference` 2, `Controlled Placeholder`/`Flight School Team`/
  `Governance`/`Techsummit-fy27`/`Certification` 1 each.
- **No airline business domain surfaced** (Revenue/Maintenance/Loyalty/Reservation/
  Route/Fleet/Passenger/Procurement) — those schemas carry no governed tag.

**Unused structural signals present — the opportunity:**
- **FK/PK:** 200 FOREIGN KEY + 95 PRIMARY KEY catalog-wide. Airline schemas:
  `revenue` 84 FK / 18 PK, `maintenance` 52 / 27, `airline_demo_ext` 32 / 16,
  `reservation` 10 / 3, `procurement` 7 / 3, `loyalty` 3 / 3, `route` 2 / 2,
  `flight` 2 / 1, `passenger` 1 / 1, `airport` 0 / 1, `fleet` 0 / 1.
- **FK graph fuses schemas:** 94 same-schema + **99 cross-schema** FK edges → domains
  = FK-connected components spanning schemas (decisive, explainable, ignored today).
- **Metric views:** 40 `METRIC_VIEW` (`airline_demo_metrics`) + 12 materialized views
  (`silver`) + 60 MANAGED "mvm" tables across **10 `airline_demo_mvm_*` schemas**
  literally named as business areas.
- **Schema naming:** the 10 mvm schemas (airport, fleet, flight, loyalty,
  maintenance, passenger, procurement, reservation, revenue, route) + a medallion
  bronze/silver/metrics/ext set — naming already encodes the taxonomy.
- **Shared join columns (FK proxy):** `aircraft_id` 14 tables, `fare_id` 9,
  `work_order_id` 9, `route_id` 8, `pnr_id` 8, `ffp_member_id` 8, `passenger_type_code`
  8, `fare_class_id` 7, `bsp_settlement_id` 7 (`currency_code` 17 — a facet-ish unit,
  weight down).

**Conclusion:** coverage for the four new signals is **high**; the moderate
legitimacy bar (≥3 tables / ≥2 schemas / ≥1 connection) is comfortably met by real
business areas and excludes one-asset facet tags. **Primary rule = FK-connected
component + schema-name grouping; MV membership = sub-domain boundary; tags demoted
to corroboration + facet routing.**

### A.2 — Stage-1 live verification (2026-08-31, deployed app)

`./scripts/deploy.sh --update` (app RUNNING) → `ontology_materialize` run scoped to
`catalog_allowlist=["serverless_stable_6t92c3_catalog"]` → **SUCCEEDED** (1925 tags,
207 domains, 45 ungrouped). Results:

- **Facets eliminated from Domains (the fix, proven):** an explicit check for every
  known facet key (`certified`/`contains_synthetic`/`confidential`/`restricted`/
  `reconciled`/`semantic_layer`/`pii_*`/`data_tier`/`certification`/`governance`…)
  as a top-level domain returned **zero rows**.
- **Governed taxonomy surfaces with reasons:** `Alaska Airlines Commercial` (reuse,
  48, *"grouped by curated domain tag"*), `Maintenance and Engineering` (reuse, 19),
  `IFEC` (reuse, 15), `Operations` (reassign, 24). Rule split of top-level domains:
  **32 FK-component, 26 shared-schema, 3 curated-tag** — every domain carries a plain
  `evidence.reason`.
- **Two refinements this run exposed (folded into §7):**
  1. **Duplicate labels** — the same schema emitted both an FK-component and a
     shared-schema domain (`Cost Attribution` ×2, `Northpeak` ×2, `Default` ×2), and
     the structural sub-split fragmented one schema into many identically-named
     sub-domains (`Dev … Airline Demo Metrics` ×14). → §7 name-dedup/qualification;
     Stage 2's explicit boundaries also collapse the sub-label duplication.
  2. **Concept split across rules** — `Alaska Airlines Maintenance and Engineering`
     (curated tag, 19) and `Airline Demo Mvm Maintenance` (FK, 46) are the same area
     via two rules. → §7 curated-tag-absorbs-structural-component.
- **Expected pre-Stage-3 noise:** scoping to the *whole* shared catalog (dozens of
  unrelated `e2e_real_*`/`bakehouse`/`migration`/`cost_attribution` schemas) inflated
  the count via the shared-schema rule; Stage-3 `domain_require_connection=true` +
  the legitimacy bar prune schema-only groups, and a Settings allowlist scoped to the
  airline catalogs avoids it. (The bare button run also correctly `skipped` on an
  empty allowlist — the snapshot guard held.)

**Verdict:** Stage 1's core thesis is validated live; remaining items are Stage-2
(sub-domain boundaries) and Stage-3 (legitimacy bar + the two §7 gates) scope.

### A.3 — Stage-3 live verification (2026-09-01, deployed app)

`./scripts/deploy.sh --update` (offline path — `SKIP_FRONTEND_BUILD=1 UV_OFFLINE=1`, app
RUNNING) → `ontology_materialize` run scoped to `catalog_allowlist=
["serverless_stable_6t92c3_catalog"]`, `run_as` = the user (MV-D50) → **SUCCEEDED**
(143 rows: 61 top-level, 82 sub). Stage-3 mechanics that **passed**:

- **Name dedup / qualification (§7):** zero duplicate rendered names *within the
  top-level namespace*; the raw duplicates that remain (`Cost Attribution (…N)`,
  `Prashanth Wanderbricks Silver ×8`) are **cross-parent sub-domains** — namespace-legal
  by design (`dedupe_domain_names` deduplicates per parent).
- **Honest confidence (MV-D56):** `evidence.rank.confidence.band` renders `Low/Medium/
  High` + `signals_present` + `gap`, never a percent.
- **Legitimacy bar prunes junk (MV-D57):** 58 / 61 top-level domains gated
  (`surfaced=false`) — the shared-catalog noise (`e2e_real_*`, `bakehouse`,
  `wanderbricks_*`, `cost_attribution` fragments) no longer stands up as Domains.

**Two real defects the gate exposed (→ Stage-3.1 follow-up, NOT yet fixed):**

1. **Trust inversion — curated governed-tag Domains are wrongly gated.** All three
   curated `reuse` Domains (`Alaska Airlines Commercial` / `IFEC` / `Maintenance and
   Engineering`) came back `surfaced=false` with `legitimacy_reason = "no structural
   connection between the assets"`, while ad-hoc FK-component Domains (`Airline Demo Mvm
   Airport` / `… Maintenance`) and the `reassign` `Operations` surfaced. Root cause:
   `rank._is_connected` only accepts a *structural* keyword (`foreign key` / `metric
   view` / `community detection` / co-query / shared-spine); a curated Domain's reason
   is `"grouped by curated domain tag: …"`, which matches none, so `require_connection`
   fails the bar. This **inverts MV-D53 precedence** (a human-asserted governed tag is
   the highest-trust signal and must be legitimate by fiat — never gated below a
   structural heuristic; its `uncapped_tier` was already `high`, capped only by sparse
   coverage). **Fix:** treat a curated governed-tag grouping as connected/legitimate
   (or exempt curated Domains from the bar entirely).
2. **`absorb_curated_into_structural` did not fire for maintenance.** The curated
   `Alaska Airlines Maintenance and Engineering` and the FK `Airline Demo Mvm
   Maintenance` (same schema `airline_demo_mvm_maintenance`) stayed as two separate
   Domains (`corroborating` empty) — the ≥ 0.5 member-overlap wasn't met because the
   curated tag sits on a different asset set than the FK/MV-derived component. Worst
   outcome: the FK twin surfaces (`create`) while the curated original is gated.
   **Fix:** widen the §7 reconciliation to also merge on a **shared governed home /
   schema anchor** (the "OR shared parent tag" branch), not member-overlap alone, so the
   curated Domain wins identity and the FK becomes corroboration.

**Verdict:** Stage-3 plumbing (dedup, honest bands, junk-gating) is validated live, but
the legitimacy bar must not suppress curated governed-tag Domains — that is a correctness
bug, tracked as **Stage-3.1** (small `rank.py` + `cluster.py` wheel patch; offline-tested,
then deploy-verified via the now-fast offline path). Independent of Stage 4 (Pages).

## Appendix B — Skipped-refresh banner (independent follow-up)
Surface a distinct `skipped` refresh state (empty-allowlist run, per the materializer
guard) with plain messaging and a Settings deep link. Small and decoupled — tracked
here so it isn't lost, not part of this redesign.
