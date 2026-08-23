# Automated Metric View Suggestion Engine for Databricks AI/BI Genie — Point of View & Architectural Solution Design

*Prepared from the perspective of a Principal Data Architect / AI Product Strategist for the Databricks ecosystem. Scope: an in-app intelligence feature inside Genie Workbench (`databricks-solutions/databricks-genie-workbench`) that analyzes Genie Spaces and auto-recommends, maps, and generates Unity Catalog Metric Views — delivered as a toggleable task inside the existing Genie Space Optimizer job, and extended to curate Discover domains, subdomains, and Pages from the same profiling output. Product status as of mid-2026 is flagged throughout as GA / Public Preview / Beta / Private Preview / Undocumented / API-Ask.*

---

## TL;DR

- **Build it as a read-first "advisor" service inside Genie Workbench** that fuses three signal layers — Unity Catalog lineage (deterministic), vector-embedding semantic match, and sqlglot SQL-AST fingerprinting (syntactic) — into a 0–100 confidence score, then emits governed Metric View YAML proposals. Every primitive it needs to *read* exists today, and the picture has improved materially: **Genie's own benchmark evaluation APIs are now in Beta** (create eval run, get eval run, list and get evaluation results), so accuracy measurement no longer needs a bespoke judge harness. The remaining gap is that **no Metric View create/update REST endpoint exists**, so views are created via SQL DDL.
- **The single highest-precision seed is an already-optimized Genie Space.** Genie Workbench's Auto-Optimize (Genie Space Optimizer, "GSO") pipeline already produces governed joins, metric views, instructions, and trusted-asset SQL as `field_path` + `new_value` patches to `serialized_space`. Harvesting those artifacts is a near-deterministic path to Metric View proposals and should be Phase 1 before any probabilistic matching. *(Patch shape superseded — see [Appendix A · Delta 3](#delta-3--dict-patches-and-new-patch_types).)*
- **Ship it as a task inside the existing GSO job, not a separate pipeline.** The user experience is a single toggle in the run-config panel — "Suggest metric views" — which, when checked, asks a second question: *where* may the Workbench create them. If the user consents and holds the Unity Catalog privileges, the metric view is **created, attached to the Genie Agent, and then the space is optimized on top of it**. If consent is withheld or the privileges are missing, the run degrades cleanly to suggest-only and renders copy-ready DDL in the optimization output. Consent is scoped to one run and one schema, recorded with the run, and re-verified at preflight. *(Now a two-run flow — see [Appendix A · Delta 1](#delta-1--two-run-consent-model); consent scoping and re-verification are unchanged.)*
- **The same profiling output curates Discover.** Domains, subdomains, and Pages are the human-modeled layer of the Genie Ontology, and a Genie Space is already a curated business scope — which makes it the strongest available domain seed. GSO's co-usage, lineage, join-key, and description-embedding signals map onto domain and subdomain proposals; its instruction text, metric-view synonyms, and benchmark evidence map onto Page drafts. Domain *assignment* is automatable today via governed tags and tag automations; domain, subdomain, and Page *creation* is UI-only and is the largest new API ask.
- **Ground everything in Unity Catalog governance and On-Behalf-Of (OBO) auth.** Suggestions must be computed under the signed-in user's OBO token so row filters, column masks, and BROWSE/SELECT boundaries are respected automatically at query time; the app service principal is a fallback only for org-wide background scans of system tables, with results re-filtered to the viewer.

---

## Key Findings

1. **All read-side metadata primitives exist today.** Metric View definitions are fully retrievable via `DESCRIBE TABLE EXTENDED <mv> AS JSON` — the `view_text` field returns the complete YAML (source, joins, fields, measures) and each column carries a `metadata` field holding agent metadata (synonyms, display_name, format). The JSON output has been available since Databricks Runtime 16.2 and Databricks commits to a *stable* JSON schema, making it safe for automation. Genie Space config is retrievable via `get_space(..., include_serialized_space=True)`, returning `data_sources.tables[].identifier`, `data_sources.metric_views[]`, `instructions.text_instructions`, `instructions.example_question_sqls`, and `benchmarks`. Lineage system tables carry `genie_space_id` inside `entity_metadata`, and `source_type` includes a dedicated `METRIC_VIEW` value — enabling direct graph joins between spaces, physical tables, and metric views.

2. **Accuracy measurement now runs on Genie's own benchmark evaluation, and the APIs for it are Beta.** Genie Workbench no longer scores optimization runs with a separate bank of MLflow judges; it invokes **Genie's native Benchmark Eval runs** and reads their results. Databricks shipped the matching REST surface in 2026: *Create eval run for benchmarks*, *Get benchmark evaluation run*, *List all evaluation runs in the space*, *List benchmark evaluation results*, and *Get benchmark evaluation result details*, all labelled **Beta**. This is the single most consequential change for this design. Grading is now the platform's job, using the platform's own definition of correctness, which means the advisor does not need to defend a bespoke scoring methodology to a customer — it reports the same number the customer sees in the Genie **Evaluations** tab. GSO's remaining job is orchestration, patching, versioning, and the audit trail (still persisted across roughly 15 Delta tables plus Lakebase), not grading. *(Table count superseded — see [Appendix A · Delta 6](#delta-6--six-delta-tables-extended-additively).)*

   The mechanics matter for how lift is attributed. **Chat mode** grading is deterministic: Genie runs the generated SQL and compares the result set against the benchmark question's SQL Answer, and failure is reported with structured assessment reasons — `EMPTY_RESULT`, `RESULT_MISSING_ROWS`, `RESULT_EXTRA_ROWS`, `RESULT_MISSING_COLUMNS`. **Agent mode** grading is LLM-judge based, with an optional evaluation note to steer it. Questions Genie cannot assess, or that lack a SQL Answer, come back flagged **Manual review needed**. An eval run reports `num_questions`, `num_correct`, `num_needs_review`, and `num_done`, with status values including `RUNNING`, `DONE`, `EVALUATION_FAILED`, `EVALUATION_CANCELLED`, and `EVALUATION_TIMEOUT`.

3. **AST fingerprinting is the right mechanism to detect un-governed measures.** sqlglot parses the Databricks/Spark dialect, canonicalizes queries (alias renaming by first appearance, flattening and sorting of AND predicates, literal normalization, whitespace and case stripping), and lets you fingerprint recurring `SUM(CASE WHEN … THEN … END)` formulas, GROUP BY sets, filter predicates, and join keys across query history and benchmarks. Recurrent fingerprints with no governed Metric View equivalent are the highest-value proposals.

4. **The enterprise text-to-SQL cliff justifies the whole effort.** Per Lei et al., "Spider 2.0" (arXiv:2411.07763, ICLR 2025), an o1-preview code-agent framework solves only 21.3% of tasks, against 91.2% on Spider 1.0 and 73.0% on BIRD. Governed semantic layers are the documented remedy: Databricks' Genie best-practices guidance states that metric views are particularly effective for Genie Agents because they pre-define metrics, dimensions, and aggregations, and thereby improve response accuracy — precisely by removing the measure ambiguity that drives most benchmark failures.

5. **Materialization cost is now predictable and must be governed by policy.** Metric View materialization builds a managed Lakeflow Spark Declarative pipeline. The `REFRESH POLICY` clause offers four values — `AUTO` (cost-model default), `INCREMENTAL` (soft, falls back to full), `INCREMENTAL STRICT` (fails rather than silently full-recomputing), and `FULL` — and `EXPLAIN CREATE MATERIALIZED VIEW` previews incremental eligibility (naming blockers like `ROW_TRACKING_NOT_ENABLED`, `WINDOW_WITHOUT_PARTITION_BY`, `EXPRESSION_NOT_DETERMINISTIC`) before you commit. Proposals should ship with a pre-checked eligibility guarantee and a safe default policy.

6. **Lakeflow Jobs already supports everything the "single toggle" needs.** Job parameters (`{{job.parameters.<name>}}`), task values (`{{tasks.<task>.values.<name>}}`), the **If/else condition task**, **Run if** dependency modes, and task disablement are all documented GA control-flow primitives. A conditional optimizer task requires no new platform capability — only a job-definition change in the bundle.

7. **Discover, Domains, and Subdomains reached Public Preview in July 2026; Pages are in Beta.** Domains are an organization layer built on **governed tags**; subdomains follow a required `{parentDomainTag}/{subdomainName}` convention with exactly one level of nesting, one parent per subdomain, and many-to-many asset membership. A Page is a governed, authoritative definition of a business concept, organized under a domain or subdomain. Genie One prioritizes a Page's definition over context it infers automatically and cites the Page in its answer — which makes Page curation a direct, measurable accuracy lever, not just documentation hygiene.

8. **Domain membership is automatable; domain creation is not.** Assigning an asset to a domain means applying the corresponding governed tag, which is fully scriptable via `ALTER … SET TAGS` (DBR 13.3+) or `SET TAG` (DBR 16.1+), subject to `ASSIGN` on the governed tag. **Tag automations** (Beta) can assign or remove governed tags on tables and volumes at scale from declarative rules, with a dry-run preview. But creating a domain, subdomain, Page, or custom Discover section is a UI action with no documented public REST API — the single largest gap in the Discover half of this design.

---

## Details

### Part 1 — System Tables & API Ecosystem Analysis

#### 1a. Metric View metadata

| Source | Type | Key fields | Status | Permissions | Limitations / notes |
|---|---|---|---|---|---|
| `DESCRIBE TABLE EXTENDED <catalog.schema.mv> AS JSON` | SQL | `view_text` (full YAML: source, joins, fields, measures), per-column `metadata` (synonyms, display_name, format) | **GA** (JSON since DBR 16.2; schema stable) | SELECT on MV; USE CATALOG + USE SCHEMA | Create/manage requires DBR 17.2+; measures/dimensions only decomposed via the YAML in `view_text` |
| `system.access.table_lineage` / `system.access.column_lineage` | System table | `source_table_full_name`, `source_type` (incl. `METRIC_VIEW`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`), `entity_metadata.genie_space_id` | **GA** | Admin, or per-user dynamic view; BROWSE/SELECT on objects | Lineage system tables retain a rolling 1-year window; Catalog Explorer/lineage API retain indefinitely after 2024-09-01 |
| `system.information_schema.*` | System table | tables, columns, views, `table_tags`, `column_tags` | **GA** | UC object privileges | Does not decompose metric-view semantics; `table_tags` is the reconstruction surface for domain membership (see Part 8) |
| Unity Catalog Tables API (`GET /api/2.1/unity-catalog/tables`) | REST/SDK | `table_type`, `view_definition`, comment, properties, owner | **GA** | SELECT/BROWSE | Doesn't expand YAML measures; use `DESCRIBE … AS JSON` |
| Table Insights / column-level popularity | Catalog Explorer UI (derived signal) | relative column popularity | **Preview / UI** | UC object privileges | **API-Ask:** no documented stable queryable system table; approximate via `column_lineage` counts |

#### 1b. Genie Space configuration

| Source | Type | Key fields | Status | Permissions | Limitations / notes |
|---|---|---|---|---|---|
| Genie `get_space(include_serialized_space=True)` | REST/SDK | `data_sources.tables[].identifier`/`description`/`column_configs`, `data_sources.metric_views[]`, `instructions.text_instructions`, `instructions.example_question_sqls`, `benchmarks`, `config.sample_questions` | **GA** | CAN VIEW/RUN on space; SELECT on objects | `serialized_space` is an escaped JSON string, schema `version: 2`; round-trippable via Create/Update |
| Genie Conversation API | REST/SDK | generated SQL, result set, status | **GA** | CAN RUN; DBSQL entitlement; warehouse CAN USE | Async — poll every 1–5s, cap ~10 min |
| Genie Management/CRUD | REST/SDK | `serialized_space` round-trip | **GA** | CAN MANAGE | **30 tables or views per Genie Agent** (older AWS setup page lists 25 — validate per workspace). This limit is a capacity lever, see Part 7 |
| Genie throughput limits | Platform | — | **GA / Public Preview (API tier)** | — | ~20 questions/min/workspace across spaces; API free tier ~5/min; 10,000 conversations/space |

#### 1c. Genie Workbench evaluation runs

| Source | Type | Key fields | Status | Notes |
|---|---|---|---|---|
| **Genie Benchmark Eval APIs** — create eval run for benchmarks; get eval run; list eval runs in space; list evaluation results; get evaluation result details | REST/SDK | Request: `space_id` + optional list of benchmark question IDs (**all questions if omitted**). Run: `eval_run_id`, `eval_run_status`, `run_by_user`, `created_timestamp`, `num_questions`, `num_correct`, `num_needs_review`, `num_done`. Result detail: benchmark question ID, assessment score, `manually_assessed`, structured assessment reasons | **Beta** | **The accuracy backbone.** Requires CAN EDIT on the space. Chat mode is deterministic result-set comparison; Agent mode is LLM-judge graded with an optional evaluation note. Statuses include `RUNNING`, `DONE`, `NOT_STARTED`, `EVALUATION_FAILED`, `EVALUATION_CANCELLED`, `EVALUATION_TIMEOUT`. Paginated via `next_page_token`. Beta means the contract can change — wrap it behind an adapter |
| Genie benchmark questions (in `serialized_space.benchmarks`) | REST/SDK | question text, ground-truth SQL answer | **GA** | CAN VIEW. ≤500 benchmark questions per space; each benchmark answer must have exactly one answer with `format = SQL` |
| GSO run artifacts (`/api/auto-optimize/runs/{run_id}/…`) | Workbench FastAPI + Lakebase/Delta | patch = `field_path` + `new_value`; per-iteration eval-run references; per-question results; strategist suggestions | **Workbench-internal** | OBO for auth, SP for job submission; audit trail across ~15 Delta tables. Role has changed: GSO now **records and correlates** eval-run results rather than producing its own accuracy verdicts. *Endpoint, patch shape and table count superseded — see [Appendix A](#appendix-a--implementation-deltas), Deltas 3, 5 and 6* |
| MLflow tracing / experiments | Managed MLflow 3 | run lineage, patch history, parameter and metric logging | **GA** | Still the home for **run provenance and versioning**, not grading. `mlflow.genai.evaluate()` remains available for non-Genie surfaces but is no longer the accuracy source for a Genie Space. *Superseded — see [Appendix A · Delta 7](#delta-7--mlflow-is-tracing-only)* |
| `system.query.history` | System table | `statement_text`, `query_source.genie_space_id`, duration/cost, `executed_by` | **Public Preview** | `statement_text` and `error_message` empty under customer-managed keys |

**Explicit API asks (do not exist today):**
1. Metric View **create/update REST endpoint** (creation is SQL DDL only).
2. Genie **field-attribution API** ("which measures, dimensions, or Pages answered question X"). The eval-run APIs tell you *whether* an answer was right; they do not tell you *which context produced it*, which is what separates an ontology failure from a generation failure.
3. **Column popularity / table insights** as a stable system table.
4. **Domains / subdomains / Pages / Discover sections CRUD API** (see Part 8).
5. **Eval-run promotion to GA**, plus a documented retention window for eval-run history — trend analysis across engagements depends on it.

*Resolved since the first draft:* the Genie **benchmark results read API** was the top ask; it now exists in Beta (create eval run, get eval run, list runs, list results, get result details), which removes the need for a bespoke judge harness inside Genie Workbench.

---

### Part 2 — Multi-Modal Signal Extraction Engine

| Signal class | Source(s) | Concrete extraction technique | Output artifact |
|---|---|---|---|
| **Lineage & graph** | `table_lineage`, `column_lineage` | Join Genie-space source tables to MV source tables on `source_table_full_name`; use `source_type = 'METRIC_VIEW'` to detect existing coverage; discover multi-hop join paths; flag orphans | Coverage/overlap graph + orphan list |
| **NL intent** | space `text_instructions`, `example_question_sqls`, `benchmarks[].question`, `config.sample_questions` | Embed with `databricks-gte-large-en` (1024-dim, 8192-token window; the GTE endpoint does not normalize, `bge-large-en` does) | Intent vectors |
| **SQL AST** | Genie generated SQL, benchmark answer SQL, GSO iteration SQL, `query.history` filtered by `genie_space_id` | sqlglot `parse_one(dialect="databricks")` → canonicalize → fingerprint recurring `SUM(CASE WHEN…)`, GROUP BY sets, filter predicates, window functions, join keys | Measure/dimension fingerprints with recurrence counts |
| **Query-history patterns** | `system.query.history` | Rank by frequency × cost; top filter columns; recurring aggregations; distinct-user breadth | Ranked demand list |
| **Optimized-space harvest** (highest value) | GSO `patches` + applied `serialized_space` | Extract the joins, `metric_views` entries, instructions, and trusted-asset SQL the optimizer already produced | High-confidence seed proposals |

---

### Part 3 — Matching Algorithm & Scoring Logic

**Three tiers, blended:** (a) determinism via lineage/schema overlap; (b) semantic similarity between NL intent and MV field text (Databricks Vector Search Delta-Sync index with `embedding_model_endpoint_name`, or direct FMAPI embedding calls for small corpora); (c) syntactic AST parsing to detect un-governed measures.

```
Score = 100 × (0.35·L + 0.30·Y + 0.20·S + 0.15·D)
```

| Component | Meaning | How computed |
|---|---|---|
| **L** — Lineage/schema determinism | Source-table and column overlap | Jaccard of column sets from `column_lineage` |
| **Y** — Syntactic AST match | Fingerprint recurrence × structural equivalence | (normalized recurrence) × (AST-equivalence flag) |
| **S** — Semantic similarity | Max cosine of intent text vs a reference set | FMAPI cosine against one of two reference kinds (see `MV-D12`): governed **MV field text** where the candidate acts on an existing view, else the **source columns' names and comments**. Read as MV-field-only, S would be structurally 0.0 for every `NEW_METRIC_VIEW` — including the worked example below, which asserts 0.40 |
| **D** — Demand/recency | Frequency × cost × distinct users, decayed | Normalized, then staleness-decayed (see `MV-D11` — each factor saturates, then they combine as a **geometric mean**, not a literal product, which could not reach the 0.80 the worked example below asserts) |

**Decay:** `D_effective = D × 0.5^(age_days / H)`, half-life `H ≈ 30 days`. Fingerprints unseen within the query-history retention window are dropped.

**Worked example — un-governed revenue measure.** Genie repeatedly emits `SUM(l_extendedprice * (1 - l_discount))` with no MV equivalent: L = 0.90, Y = 0.95 (identical canonical fingerprint seen 60×), S = 0.40, D = 0.80.
`Score = 100 × (0.315 + 0.285 + 0.080 + 0.120) = 80.0` → **High**.

**Worked example — raw table → existing MV at onboarding.** L = 0.95, Y = 0.20, S = 0.75, D = 0.30.
`Score = 100 × (0.3325 + 0.06 + 0.15 + 0.045) = 58.75` → **Medium** (propose, require confirm).

**Thresholds:** High ≥ 75, Medium 50–74, Low 25–49, suppress < 25.

**Conflicting or partial matches:** if two MVs partially match one fingerprint, emit the higher-**L** candidate as primary and attach the other under `alternatives[]`. Never auto-apply below High. If a fingerprint matches an existing MV measure *and* an instruction that defines it differently, downgrade to a `conflict` state (Part 5).

---

### Part 4 — In-App Integration & UX Touchpoints

| Touchpoint | Trigger | UI element | Mock interaction copy |
|---|---|---|---|
| **Space Onboarding** | Raw table added that maps to an existing governed MV | Inline banner + diff view | *"Governed metric available. Replace **samples.tpch.orders** with Metric View **finance.sales.orders_metrics** (4 measures, 6 dimensions already governed)?"* **[Preview diff] [Replace] [Keep raw table] [Dismiss]** |
| **Workbench Evaluation Loop** | Benchmark question fails on ambiguous/missing measure | Diagnostic card on the failed question | *"Failed: 'What was discounted revenue by region last quarter?' Diagnosis: **revenue** is computed 3 different ways across generated SQL. Governing it as a Metric View measure will stabilize this answer."* **[Generate draft YAML] [View evidence (3 SQL variants)] [Ignore]** |
| **Semantic Layer Governance** | Recurring unmapped fingerprint | Proposal queue with confidence badges | *"High (80%) — 12 benchmark queries and 48 history queries reuse an un-governed **discounted_revenue** formula."* **[Review proposed Metric View] [Accept] [Edit YAML] [Reject]** |
| **Optimizer run settings** (pre-run) | User configures an Auto-Optimize run | Toggle, then a target picker and an inline entitlement check | *"Suggest metric views — the optimizer will propose governed metric views from the profiling it already does."* → expands to *"Where should metric views be created? `finance.sales` — ✅ You can create metric views here."* → **○ Suggest only ● Create and attach, then optimize** (See Part 7.3) |
| **Permission denied** (pre-run) | Entitlement probe fails on the chosen schema | Inline warning, non-blocking | *"You don't have permission to create metric views in `finance.sales`. Missing: CREATE TABLE on the schema. The run will continue in Suggest only mode."* **[Copy grant request] [Choose a different schema] [Continue in suggest-only mode]** |
| **Optimization output screen** | Run completes in suggest-only mode | DDL panel with evidence | *"3 metric views proposed, none created. Lift not measured — these were not attached during this run."* **[Copy DDL] [Copy GRANT] [Re-run with this metric view]** (See Part 7.5) |

Behavioral rules: nothing is written until the user confirms. **Accept** on a materialized proposal first runs `EXPLAIN CREATE MATERIALIZED VIEW` and surfaces any incremental blocker inline; only then does it run `CREATE VIEW … WITH METRICS`. **Reject** records a negative signal that suppresses the fingerprint for a decay period.

**Sample proposed Metric View YAML:**

```yaml
version: "1.1"
source: samples.tpch.lineitem
comment: >
  PURPOSE: Governed discounted revenue for TPC-H line items.
  BEST FOR: Discounted revenue by market segment | Revenue by order status | Line item volume trends
  NOT FOR: List-price revenue before discounts (query l_extendedprice directly)
  DIMENSIONS: order_status, market_segment
  MEASURES: discounted_revenue, line_item_count
  SOURCE: samples.tpch.lineitem (sales domain)
  JOINS: orders (order attributes), customer (nested under orders)
  NOTE: Auto-proposed by Genie Workbench from 60 recurring generated-SQL fingerprints.
joins:
  - name: orders
    source: samples.tpch.orders
    'on': source.l_orderkey = orders.o_orderkey
    rely:
      at_most_one_match: true   # set ONLY because profiling proved o_orderkey unique; unvalidated at runtime
    joins:                       # customer NESTED under orders (snowflake) — a flat sibling join whose
      - name: customer           # 'on' references the orders alias is TRANSITIVE and fails or silently
        source: samples.tpch.customer   # returns wrong grain. Nested joins require DBR 17.1+.
        'on': orders.o_custkey = customer.c_custkey
dimensions:
  - name: order_status
    expr: orders.o_orderstatus
    comment: Order lifecycle status flag from the orders table
    display_name: Order Status
    synonyms: [status, state, order state]
  - name: market_segment
    expr: orders.customer.c_mktsegment   # nested columns are parent_join.child_join.column
    comment: Customer market segment from the customer dimension
    display_name: Market Segment
    synonyms: [segment, customer segment, mktsegment]
measures:
  - name: discounted_revenue
    expr: SUM(l_extendedprice * (1 - l_discount))
    display_name: Discounted Revenue
    format: {type: currency, currency_code: USD, decimal_places: {type: exact, places: 2}}
    synonyms: [revenue, net revenue, discounted sales]
  - name: line_item_count
    expr: COUNT(1)
    display_name: Line Item Count
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: revenue_by_segment_status
      type: aggregated
      dimensions: [order_status, market_segment]
      measures: [discounted_revenue, line_item_count]
```

**The generation quality standard (normative for every emitted YAML).** A suggestion engine that emits invalid or silently-wrong YAML is worse than no engine, because its output arrives pre-trusted. Every generated definition must pass these gates, drawn from field-hardened metric-view practice, before it reaches a proposal card:

| Gate | Rule | Failure it prevents |
|---|---|---|
| Syntax | `WITH METRICS LANGUAGE YAML`, `AS $$…$$`, `version: "1.1"` quoted; never emit `name`, `time_dimension`, top-level `window_measures`, `join_type`, or `table` in joins | Creation errors; TBLPROPERTIES look-alikes that create a regular VIEW |
| Multi-hop ladder | For any second-hop dimension: (1) prefer a denormalized column on the first dimension; (2) else nested joins **only if** DBR ≥ 17.1 **and** profiling proves the intermediate key 1:1; (3) else a subquery-`source` that pre-joins with explicit uniqueness guards. **Never** emit a flat sibling join whose `on` references another join alias | Transitive joins: `UNRESOLVED_COLUMN` on ≥17.1, silent wrong grain below it |
| Cardinality | `rely.at_most_one_match: true` only when profiling has proven the dimension key unique — it is **not validated at runtime** and a fan-out inflates every SUM/COUNT. GSO enrichment already computes join-key uniqueness; consume it | Fan-out cartesians producing confidently wrong totals |
| SCD2 | Any dimension with an `is_current`-style column gets `AND {dim}.is_current = true` in the join (profiling detects the column) | Duplicate dimension rows inflating measures |
| Source selection | Additive measures must aggregate columns from the fact `source`; a measure aggregating a joined dimension's column is a defect the dedup/conflict path flags, not a proposal | Revenue-from-dimension under/over-reporting |
| Composability | When the corpus shows a ratio of two recurring aggregates, emit atomic measures plus a `MEASURE()`-composed derived measure; `SUM(CASE WHEN c THEN 1 END)` shapes become `COUNT(1) FILTER (WHERE c)`. Percent-of-total is **never** `MEASURE()/MEASURE()` (always 1.0) — use a Fixed-LOD dimension (`SUM(x) OVER ()`) read with `ANY_VALUE()` | Duplicated aggregation logic; ratio measures that are always 1.0 |
| Formats | Only `byte`, `currency`, `date`, `date_time`, `number`, `percentage`; `percent` and `decimal` are invalid | Creation errors from plausible-looking format types |
| Agent metadata | Structured comment (PURPOSE / BEST FOR / NOT FOR / DIMENSIONS / MEASURES / SOURCE / JOINS / NOTE); 3–10 synonyms per field (max 10, 255 chars each); `NOT FOR` cross-references the adjacent MV the dedup gate found | Weak Genie routing; duplicate-metric usage |
| Benchmark contamination | `BEST FOR` lines are **paraphrased intents, never verbatim benchmark question text**. An attached MV's comment is context Genie reads; verbatim benchmark text in it invalidates the benchmark it will be measured by | The engine grading itself on questions it memorized |
| Capability | Warehouse/runtime supports what the YAML uses: DBR 17.3+ to create/edit; 17.1+ for nested joins; 18.1+ for `fields:`, `agg()`, window `offset`. The entitlement probe (§7.3.1) gains these capability rows | Grant-holding users blocked by runtime, not permissions |
| Post-create | `DESCRIBE EXTENDED` must show `Type: METRIC_VIEW`; semantic validation queries use `MEASURE(\`name\`)` with GROUP BY (never `SELECT *`, never a re-typed aggregate); fan-out smoke test: row count unchanged after each join | Regular-VIEW impostors; validation queries that mask the exact defects being checked |
| Update path | Subsequent edits to an engine-created MV use `ALTER VIEW … AS $$…$$`, never `CREATE OR REPLACE` or drop+create — replace deletes UC grants and cascading metadata | A metrics iteration silently revoking every consumer's access |
| Grants | The creating user owns the view; other space users need SELECT or their answers silently degrade. The apply flow surfaces a copy-ready `GRANT SELECT` checklist for the space's audience (never auto-granted) | Per-user accuracy divergence nobody can reproduce |

**Sample proposal payload:**

```json
{
  "suggestion_id": "sug_9f2a",
  "type": "NEW_METRIC_VIEW",
  "confidence_score": 80,
  "tier": "HIGH",
  "target_space_id": "01ef_genie",
  "proposed_object": "finance.sales.discounted_revenue_metrics",
  "score_components": { "L": 0.90, "Y": 0.95, "S": 0.40, "D": 0.80,
    "weights": { "L": 0.35, "Y": 0.30, "S": 0.20, "D": 0.15 } },
  "evidence": {
    "ast_fingerprint_recurrence": 60,
    "benchmark_questions": ["bmk_12", "bmk_31"],
    "query_history_statement_ids": ["stmt_a1", "stmt_b7"],
    "lineage_source_tables": ["samples.tpch.lineitem", "samples.tpch.orders"],
    "semantic_top_match": { "field": "samples.tpch.lineitem.l_extendedprice",
      "cosine": 0.40, "reference_kind": "SOURCE_COLUMN_METADATA" }
  },
  "provenance": {
    "generated_by": "gwb-mv-advisor@1.0",
    "auth_identity": "OBO",
    "gso_run_id": "run_5521",
    "gso_task_key": "metric_view_advisor",
    "generated_at": "2026-08-22T14:03:00Z"
  },
  "dedup_fingerprint": "9b7f1c0a4e8d2b6f35a1c8e04d7b29f6a3c5e18b0d4f7a2c6e9b3d5f81a4c7e0",
  "alternatives": [],
  "conflicts": []
}
```

**`dedup_fingerprint` is a bare hex digest** (amended under `MV-D10`; the digest above is
illustrative). It is `sha256(space_id | canonical_measure_expr | sorted_source_set)` —
`mv_state.mv_candidate_fingerprint`, the `MV-D7` upsert key for
`genie_opt_mv_candidates` and the `genie_opt_artifacts.content_hash` cross-reference.
An earlier draft of this payload carried a readable composite string
(`"sha256:sum(...)|group:order_status,market_segment"`); it predated the shipped key, was
never implemented, and should not be resurrected — despite its prefix it was not a digest,
it omitted the space id, and it keyed on the grouping set rather than the source set.
The expression-grained fingerprint in `optimization/mv_fingerprint.py` is a
*different* value that is deliberately never persisted here: it collides across spaces and
across source sets, which is exactly what this column must not do.

**`semantic_top_match` carries its `reference_kind`** (amended under `MV-D12`). An earlier
draft of this payload reported `{"field": null, "cosine": 0.40}` — a cosine against nothing,
which no reader could interpret and no implementation could reproduce. The 0.40 is real: this
candidate has no governed metric view, so **S** scored its intent text against the source
columns' own names and comments, and the field naming that column makes the number legible.
The kind is recorded rather than inferred because 0.40 against a curated metric-view field
and 0.40 against a column comment are different strengths of evidence.

---

### Part 5 — Security, Governance & Conflict Resolution

**Permission boundaries.** Compute suggestions under **OBO** using the `X-Forwarded-Access-Token` header with least-privilege scopes. Because Genie and metric views resolve data access via each end user's own Unity Catalog permissions, row filters and column masks are enforced per user at query time; the advisor must never surface a suggestion referencing a securable the viewing user cannot BROWSE or SELECT. Lineage graphs share the UC permission model, so lineage-derived evidence is already scoped.

**Service-principal fallback.** Use the app SP only for org-wide background scans of system tables where admin access is required, then re-filter every candidate to the viewing user's grants before display. Record structured audit logs (identity, action, target, status) for every OBO action. **The SP is never a write path for metric views.** If the signed-in user cannot create the object, the answer is a `GRANT` statement for their admin, not a fallback identity with broader rights — an app that writes what its user could not write has quietly become a privilege-escalation vector.

**Entitlement is not authorization.** These are two separate checks and the design keeps them separate (Part 7.3). *Entitlement* is what Unity Catalog will permit, discovered by probing effective privileges under OBO. *Authorization* is the user's explicit, scoped, recorded decision to let the Workbench act. Holding `CREATE TABLE` on a schema does not mean a user wants an optimization run writing to it. Consent is therefore requested before the run, scoped to one run and one `catalog.schema`, carried with the run as a parameter rather than read from mutable app state, and re-verified at preflight. Writes fail closed on any mismatch.

**Conflict resolution workflow** (proposed MV contradicts an instruction or trusted asset):
1. **Detect** — if a fingerprint matches an existing MV/measure *and* an instruction or trusted-asset SQL defines the same concept differently, set state = `CONFLICT` instead of emitting a suggestion.
2. **Surface, don't overwrite** — render existing instruction / trusted asset beside the proposed measure, with divergent expressions highlighted.
3. **Default to the trusted asset** — trusted assets provide exact, curated answers; the MV proposal is marked "requires human adjudication."
4. **Adjudicate and log** — the reviewer chooses (keep instruction / adopt MV / reconcile both); the decision is written to the audit trail and, if adopted, applied as a reviewed patch. Never auto-resolve.

---

### Part 6 — Phased Architectural Implementation Strategy

| Phase | Scope | Dependencies | Effort | Risks | Success metrics |
|---|---|---|---|---|---|
| **Phase 1 — Heuristic harvest & deterministic match** | Harvest optimized-space artifacts; deterministic lineage/schema match; sqlglot AST fingerprinting; read-only suggestions with L/Y/D scoring; ship as the gated `metric_view_advisor` GSO task in propose-only mode | Lineage system tables, `DESCRIBE…AS JSON`, `serialized_space`, GSO artifacts, sqlglot, Lakeflow If/else task | ~6–8 weeks | Lineage capture gaps; `statement_text` empty under CMK; 1-yr lineage window | ≥70% precision on High tier; ≥N proposals accepted per space |
| **Phase 2 — Embedding-based semantic matching + Discover proposals** | Add the **S** component; raw-table→MV swaps at onboarding; add the `discover_curator` task emitting domain/subdomain proposals and Page drafts (propose-only) | Vector Search or FMAPI `gte-large-en`; governed tags; Phase 1 pipeline | ~6–8 weeks | Embedding drift; synonym sparsity; domain over-fragmentation | Recall lift with no High-tier precision regression; ≥1 domain proposal accepted per estate scan |
| **Phase 3 — Create, attach, verify, and publish** | LLM-drafted MV YAML validated with `EXPLAIN`; `CREATE VIEW … WITH METRICS`; patch `data_sources.metric_views[]`; re-benchmark and auto-rollback on regression; governed-tag application via tag automations for domain membership; curator publish flow for Pages | FMAPI chat; REFRESH POLICY; DABs; tag automations (Beta); Phase 2 | ~8–10 weeks | Hallucinated measures; surprise full-refresh cost; conflict with trusted assets; UI-only domain/Page creation | ≥90% generated YAML valid on first `EXPLAIN`; measurable benchmark accuracy lift post-attach |

> **Delivery mechanism superseded — see [Appendix A](#appendix-a--implementation-deltas).** The phase scopes, effort, risks and success metrics stand; the "gated task + Lakeflow If/else" framing does not.

---

### Part 7 — Native GSO Job Integration: Metric View Suggestions as an Optimizer Task

The design goal is that a user never runs a second tool. They open the Auto-Optimize run configuration, flip **Enable metric view suggestions**, and the optimizer does the rest — because by the time the optimizer reaches its lever loop it has already paid for all the expensive profiling the advisor needs.

#### 7.1 What GSO already profiles, and what the advisor reuses

GSO's Create agent profiles the data, builds metadata, generates grounding instructions, creates benchmark questions, and deploys a configured space — its output is a space with joins, metric views, instructions, and validated example SQL. The IQ Scanner then evaluates 12 checks (table descriptions, column annotations, sample queries, and so on). The Auto-Optimize job executes Genie against every benchmark, compares generated SQL to expected answers, diagnoses failures, and tunes across five lever types: tables/columns, metric views, TVFs, join specs, and instructions/example SQL.

Every one of those artifacts is an input the advisor would otherwise have to recompute:

| GSO artifact (already computed) | Produced in | Reused as | Metric View YAML target |
|---|---|---|---|
| Table and column inventory for the space scope | preflight | Candidate `source` and join table set | `source`, `joins[].source` |
| Column descriptions / annotations (generated or repaired) | enrichment | Dimension `display_name`, `comment` | `dimensions[].display_name`, `comment` |
| Synonyms and agent metadata | enrichment | Measure and dimension synonyms | `measures[].synonyms` |
| Inferred join keys and cardinality (join-spec lever) | enrichment / lever loop | `joins[].on` predicates | `joins[].on` |
| Numeric vs categorical vs temporal column typing | preflight profiling | Measure vs dimension classification; time-grain candidates | `measures[]` vs `dimensions[]` |
| Query-history mining and column popularity | preflight | Demand ranking (**D**) and materialization dimension set | `materialization.materialized_views[].dimensions` |
| Generated SQL per benchmark question, per iteration | baseline + lever loop | AST fingerprints (**Y**) — the core un-governed-measure signal | `measures[].expr` |
| Eval-run results and structured assessment reasons | baseline + lever loop (via the Benchmark Eval APIs) | Failure-attributed prioritization: which measure, if governed, flips the most failing questions. `RESULT_MISSING_COLUMNS` and `RESULT_EXTRA_ROWS` in particular point at grain and filter defects a metric view fixes directly | proposal ranking |
| Existing `data_sources.metric_views[]` | preflight | Dedup baseline — never propose a duplicate | `dedup_fingerprint` |

The practical consequence: the advisor task is cheap. It performs no table scans of its own beyond an optional `EXPLAIN` and a bounded validation query; everything else is a read of Delta tables the run has already written.

#### 7.2 Where the tasks sit in the DAG

> **Superseded — see [Appendix A · Delta 2](#delta-2--gated-phases-inside-optimize).** The placement rationale below stands and is preserved by the phase ordering; the DAG does not.

The sequencing follows from a product decision: if the user grants write permission, the metric view is **created and attached before the space is optimized**, so the lever loop tunes on top of the governed foundation rather than around it. A metric view is not a config tweak like an instruction edit; it changes the substrate the agent reasons over. Optimizing first and then swapping the substrate would invalidate the tuning you just paid for.

```
preflight ─→ baseline ─→ enrichment ─→ mv_gate (If/else)
                                            │
                              ┌──── true ───┴──── false ────┐
                              ▼                             │
                     metric_view_advisor                    │
                              │                             │
                    consent_granted?                        │
                    ┌─── yes ─┴─ no ───┐                    │
                    ▼                  ▼                    │
              metric_view_apply    (DDL to output)          │
              create → attach          │                    │
                    │                  │                    │
              mv_baseline (eval run)   │                    │
                    │                  │                    │
                    └──────────┬───────┴────────────────────┘
                               ▼
                          lever_loop  (run_if: ALL_DONE)
                               ▼
                       finalize ─→ deploy
```

Rationale for that placement:

- **After enrichment**, because enrichment is where descriptions, synonyms, and join specs are settled — the advisor wants the enriched metadata, not the raw estate.
- **Before the lever loop**, because the metric view is a foundation change. Once it is attached, the loop's instruction, join-spec, and column-metadata levers are tuned against the governed measure, which is what the customer will actually run in production.
- **With its own eval run in between.** `mv_baseline` calls the Benchmark Eval API over the same question set the `baseline` task used, so the delta attributable to *the metric view alone* is isolated before the loop starts moving other variables. Without that intermediate run the metric view's contribution is unrecoverable from the final number, and the first question any reviewer asks is "how much of the lift was the metric view?"
- **`lever_loop` uses `run_if: ALL_DONE`** on the advisor and apply edges, so a failed, skipped, or permission-denied metric-view path never fails the optimization run. Metric view suggestions are an enhancement, not a dependency.

#### 7.3 The pre-run consent gate

> **Partly superseded — see [Appendix A · Delta 1](#delta-1--two-run-consent-model).** Both questions, the scoping and the re-verification survive intact; the gate now sits at the trigger of a second run.

The toggle alone is not sufficient, because selecting it implies a write to Unity Catalog that the user has not yet authorized and may not be entitled to make. The run-configuration step therefore asks two separate questions, and conflating them is the most common way this kind of feature goes wrong:

| Question | What it establishes | Failure mode if skipped |
|---|---|---|
| **Do you consent** to the Workbench creating a metric view on your behalf, in this run, in this schema? | Authorization — the user's explicit, scoped, recorded intent | The app writes UC objects the user did not expect, under their own OBO identity, with their name on the audit record |
| **Do you hold the privileges** to create it there? | Entitlement — what Unity Catalog will actually permit | The run proceeds for forty minutes and fails at the write step, wasting the whole optimization |

Consent is asked once, before the run starts, and is **scoped to a single run and a single target schema**. It is not a persistent setting, and it is never inferred from the suggestions toggle being on.

**Run-configuration flow.**

1. User opens Auto-Optimize run config and checks **Suggest metric views**.
2. The panel expands to reveal a target picker: **Where should metric views be created?** — a `catalog.schema` selector defaulting to nothing, plus explanatory copy.
3. On selection, the app runs a **pre-flight entitlement probe** under OBO against the chosen schema (see 7.3.1) and renders the result inline, before the user commits.
4. The user chooses one of two modes, and the choice is recorded with identity and timestamp:
   - **Suggest only** — produce DDL, write nothing.
   - **Create and attach** — create the metric view, add it to the Genie Space, then optimize. Requires a passing entitlement probe and an explicit confirmation.
5. The run starts. Preflight re-verifies entitlement, because grants can change between configuration and execution.

Mock copy for the expanded panel:

> **Suggest metric views** ☑
> The optimizer will look for un-governed measures in this space's generated SQL and propose metric views for them.
>
> **Where should metric views be created?** `[ finance ▾ ] . [ sales ▾ ]`
>
> ✅ You can create metric views in `finance.sales`. *(Checked as prashanth@example.com)*
>
> ○ **Suggest only.** Show me the DDL in the run output. Nothing is created.
> ● **Create and attach.** Create approved metric views in `finance.sales`, add them to this Genie Agent, then optimize the space with them in place.
> ☐ Also materialize (starts a Lakeflow pipeline and incurs ongoing refresh cost). *Off by default.*
>
> **[ Start optimization ]**

And the denial case:

> ⚠️ **You don't have permission to create metric views in `finance.sales`.**
> Missing: `CREATE TABLE` on the schema. The run will continue in **Suggest only** mode and show you the DDL at the end.
> **[ Copy grant request ]** — sends your admin the exact `GRANT` statement needed.
> **[ Choose a different schema ]  [ Continue in suggest-only mode ]**

Note that the Workbench cannot grant the missing privilege, and should not offer to. It can generate the `GRANT` statement, name the principal, and get out of the way. Anything that looks like the app escalating its own access is a trust problem, not a convenience feature.

##### 7.3.1 The entitlement probe

Creating a metric view is a UC write. Verify, under the signed-in user's OBO token, that they hold:

| Privilege | On | Why |
|---|---|---|
| `USE CATALOG` | target catalog | Traverse to the schema |
| `USE SCHEMA` | target schema | Traverse to the object |
| `CREATE TABLE` | target schema | Unity Catalog's create privilege for view-class objects. **Verify the exact privilege name against your target runtime before shipping** — metric views are newer than the general view path and the requirement should be confirmed rather than assumed |
| `SELECT` | every source and join table in the proposal | The view resolves against them at query time |
| `CAN MANAGE` | the Genie Agent | Required to patch `data_sources.metric_views[]` |

The probe also carries **capability rows**, not just privilege rows: the target warehouse/runtime must support what generated YAML will use — DBR 17.3+ to create or edit metric views, 17.1+ for nested (snowflake) joins, 18.1+ for `fields:`, `agg()`, and window `offset`. A user with every grant but the wrong runtime gets a capability denial with the same clarity as a permission denial, and the generator downgrades its join strategy (nested → subquery-source) rather than emitting YAML the runtime cannot plan.

Probe with a `dry_run`-style check rather than a trial write: read effective privileges from the UC permissions surface, and confirm the schema exists. Do **not** attempt a speculative `CREATE VIEW` and catch the exception — a partial create leaves debris and an audit entry the user did not authorize.

The probe emits a structured result the UI renders and the run records:

```json
{
  "probe_id": "probe_7f21",
  "checked_as": "prashanth@example.com",
  "auth_identity": "OBO",
  "target": "finance.sales",
  "checked_at": "2026-08-23T09:14:00Z",
  "results": {
    "USE CATALOG on finance": "GRANTED",
    "USE SCHEMA on finance.sales": "GRANTED",
    "CREATE TABLE on finance.sales": "DENIED",
    "CAN MANAGE on space 01ef_genie": "GRANTED"
  },
  "verdict": "INSUFFICIENT",
  "missing": ["CREATE TABLE on finance.sales"],
  "remediation_sql": "GRANT CREATE TABLE ON SCHEMA finance.sales TO `prashanth@example.com`;",
  "fallback_mode": "suggest_only"
}
```

#### 7.4 Modes, and what each one does

> **Partly superseded — see [Appendix A · Delta 1](#delta-1--two-run-consent-model).** All three modes survive; `create_and_attach` becomes the second run of a two-run flow rather than a branch within one run.

The consent decision collapses into a single parameter the job reads. Three modes, and the middle one is the one most engagements should use:

| `mv_action_mode` | Behavior | When |
|---|---|---|
| `suggest_only` | Advisor produces proposals and **renders copy-ready DDL in the optimization output screen**. No UC write, no space patch. Optimization proceeds on the unmodified space. This is also the automatic fallback when the entitlement probe fails or consent is withheld | Default. Any run where the user lacks privileges, or a first pass in an unfamiliar estate |
| `create_and_attach` | Create the approved metric view in the consented schema, patch `data_sources.metric_views[]`, run `mv_baseline` to isolate its lift, then optimize the space with it in place | Consent granted and probe passed |
| `sandbox` | Create in a scratch schema, attach to a **cloned** space, measure, report, tear down. Leaves the real space and catalog untouched | Evaluating whether the feature is worth enabling, or demoing without estate impact |

**Materialization stays off unless separately checked**, in either write mode. Attaching a metric view is a metadata change; materializing one starts a managed Lakeflow pipeline with ongoing refresh cost. Bundling those two consents together is how a customer ends up with a surprise bill from a feature they thought was advisory.

#### 7.5 What "suggest only" actually renders

> **Not superseded — this section is binding.** Under [Delta 1](#delta-1--two-run-consent-model) it becomes the output contract of *every* first run, so it carries more weight than the original design gave it.

The fallback path is not a consolation prize, and it should not look like one. It is the mode most first runs will use, so the output has to be directly actionable:

- The full `CREATE VIEW … WITH METRICS LANGUAGE YAML` statement, syntax-validated, with a copy button.
- The `GRANT` statement needed to unblock the write path next time, with the principal filled in.
- The Genie Agent patch that *would* have been applied, shown as a diff against current `data_sources.metric_views[]`.
- The evidence block: recurrence count, contributing benchmark question IDs, source tables, confidence score.
- An explicit, honest label: **"Lift not measured — this metric view was not created or attached during this run."** Never present a projected accuracy gain for a view that was never evaluated. The whole credibility of the feature rests on the fact that its numbers come from real eval runs.
- A one-click **[Re-run with this metric view]** action that pre-fills the next run config in `create_and_attach` mode against the same target, so the user is one grant away from closing the loop.

#### 7.6 The toggle and consent, end to end

> **Superseded below the UI row — see [Appendix A · Deltas 1, 2 and 5](#appendix-a--implementation-deltas).** The UI layer and the consent-travels-with-the-run rule stand; the endpoint, the If/else operands and the task chain do not.

| Layer | Mechanism | Value |
|---|---|---|
| UI | Checkbox + target picker + mode radio in the Auto-Optimize run-config panel | "Suggest metric views" / `finance.sales` / "Create and attach" |
| Entitlement probe | OBO read of effective privileges, before submit | `verdict: SUFFICIENT \| INSUFFICIENT` |
| App API | `POST /api/auto-optimize/runs` body | `{"enable_metric_view_suggestions": true, "mv_target_catalog": "finance", "mv_target_schema": "sales", "mv_action_mode": "create_and_attach", "mv_materialize": false, "mv_consent": {"granted_by": "prashanth@example.com", "granted_at": "2026-08-23T09:14:22Z", "probe_id": "probe_7f21"}}` |
| Job trigger | `jobs.run_now(job_parameters={...})` | All of the above, serialized as strings |
| Job | If/else condition task operands | `{{job.parameters.enable_metric_view_suggestions}} == "true"`, then `{{job.parameters.mv_action_mode}} == "create_and_attach"` |
| Tasks | `metric_view_advisor` → `metric_view_apply` → `mv_baseline` | Gated in sequence |

Note the documented If/else semantics: `==` and `!=` perform **string** comparison, and boolean task values are serialized to `"true"`/`"false"`. Pass the flag as a string and compare against the string, or the condition silently fails closed — which, for a write-gating condition, is the correct direction to fail.

**The consent object travels with the run.** It is not read from app state at write time, because app state can change mid-run and because a consent record that lives outside the run artifact is not auditable after the fact. `mv_consent` is a job parameter, it is persisted with the run, and `metric_view_apply` refuses to write if it is absent, malformed, or references a `probe_id` whose re-verification at preflight failed.

**Downgrade, never upgrade.** If preflight re-verification finds the grant has been revoked since configuration, the run silently downgrades to `suggest_only` and records the reason in the output screen. It never escalates in the other direction — a run configured as `suggest_only` cannot become a writing run because a privilege happened to be available.

#### 7.7 Bundle definition

> **Superseded in full — see [Appendix A · Delta 2](#delta-2--gated-phases-inside-optimize).** Retained to show the intended gating semantics; do not implement this YAML — condition tasks, wheel entry points and extra tasks are forbidden by `test_phase7_job_dag.py`.

```yaml
resources:
  jobs:
    genie_space_optimizer:
      name: genie-space-optimizer
      parameters:
        - name: space_id
          default: ""
        - name: enable_metric_view_suggestions
          default: "false"
        - name: mv_action_mode
          default: "suggest_only"     # suggest_only | create_and_attach | sandbox
        - name: mv_target_catalog
          default: ""
        - name: mv_target_schema
          default: ""
        - name: mv_materialize
          default: "false"            # separate consent; never bundled with attach
        - name: mv_consent
          default: ""                 # JSON: granted_by, granted_at, probe_id
        - name: mv_min_confidence
          default: "75"               # High tier only, by default

      tasks:
        - task_key: preflight
          # ... existing definition, plus: re-verify mv_consent + entitlement probe,
          # and set task value mv_effective_mode (downgrade to suggest_only on failure)

        - task_key: baseline
          depends_on: [{ task_key: preflight }]

        - task_key: enrichment
          depends_on: [{ task_key: baseline }]

        # ---- gate 1: is the feature on at all? ----
        - task_key: mv_gate
          depends_on: [{ task_key: enrichment }]
          condition_task:
            op: EQUAL_TO
            left: "{{job.parameters.enable_metric_view_suggestions}}"
            right: "true"

        # ---- advisor: always propose-only, regardless of mode ----
        - task_key: metric_view_advisor
          depends_on:
            - task_key: mv_gate
              outcome: "true"
          max_retries: 1
          python_wheel_task:
            package_name: genie_space_optimizer
            entry_point: metric_view_advisor
            parameters:
              - "--space-id={{job.parameters.space_id}}"
              - "--run-id={{job.id}}-{{job.run_id}}"
              - "--min-confidence={{job.parameters.mv_min_confidence}}"
              - "--profile-table={{tasks.enrichment.values.profile_table}}"
              - "--baseline-eval-run-id={{tasks.baseline.values.eval_run_id}}"
              - "--target-catalog={{job.parameters.mv_target_catalog}}"
              - "--target-schema={{job.parameters.mv_target_schema}}"

        # ---- gate 2: did the user consent AND survive re-verification? ----
        # reads preflight's effective mode, not the raw parameter, so a revoked
        # grant downgrades the run instead of failing it
        - task_key: mv_write_gate
          depends_on: [{ task_key: metric_view_advisor }]
          condition_task:
            op: EQUAL_TO
            left: "{{tasks.preflight.values.mv_effective_mode}}"
            right: "create_and_attach"

        # ---- create in the consented schema, then attach to the space ----
        - task_key: metric_view_apply
          depends_on:
            - task_key: mv_write_gate
              outcome: "true"
          python_wheel_task:
            package_name: genie_space_optimizer
            entry_point: metric_view_apply
            parameters:
              - "--candidates={{tasks.metric_view_advisor.values.candidate_table}}"
              - "--target-catalog={{job.parameters.mv_target_catalog}}"
              - "--target-schema={{job.parameters.mv_target_schema}}"
              - "--materialize={{job.parameters.mv_materialize}}"
              - "--consent={{job.parameters.mv_consent}}"

        # ---- isolate the metric view's own contribution before tuning starts ----
        - task_key: mv_baseline
          depends_on: [{ task_key: metric_view_apply }]
          python_wheel_task:
            package_name: genie_space_optimizer
            entry_point: mv_baseline
            parameters:
              - "--space-id={{job.parameters.space_id}}"
              - "--pre-eval-run-id={{tasks.baseline.values.eval_run_id}}"
              - "--attached-views={{tasks.metric_view_apply.values.created_metric_views}}"

        # ---- optimization runs on top of whatever foundation now exists ----
        - task_key: lever_loop
          depends_on:
            - task_key: enrichment
            - task_key: metric_view_advisor
            - task_key: mv_baseline
          run_if: ALL_DONE            # tolerates skip, failure, or suggest-only
          python_wheel_task:
            package_name: genie_space_optimizer
            entry_point: lever_loop
            parameters:
              - "--mv-candidates={{tasks.metric_view_advisor.values.candidate_table}}"

        - task_key: finalize
          depends_on: [{ task_key: lever_loop }]

        - task_key: deploy
          depends_on: [{ task_key: finalize }]
```

Three details in that spec are doing real work. `mv_write_gate` reads **preflight's re-verified effective mode**, not the raw job parameter, so a grant revoked between configuration and execution downgrades the run rather than erroring at the write. `metric_view_advisor` runs identically in every mode — it always only proposes — which means the expensive analysis is never wasted when consent is absent, and `suggest_only` output is exactly what `create_and_attach` would have written. And `lever_loop` depends on all three upstream metric-view nodes with `run_if: ALL_DONE`, so optimization proceeds whether the foundation was changed, left alone, or failed to change.

#### 7.7.1 Task-values contract

> **Mechanism superseded, payload retained — see [Appendix A · Deltas 2 and 4](#delta-2--gated-phases-inside-optimize).** Every field below survives as an artifact payload; the task-value transport does not.

The advisor publishes a small, typed surface so downstream tasks and the app can branch without reading Delta:

```json
{
  "candidate_table": "main.genie_workbench.mv_candidates",
  "candidate_count": 7,
  "high_confidence_count": 3,
  "requested_mode": "create_and_attach",
  "effective_mode": "suggest_only",
  "downgrade_reason": "CREATE TABLE on finance.sales revoked between config and preflight",
  "consent_probe_id": "probe_7f21",
  "baseline_eval_run_id": "e1ef34712a29169db030324fd0e1df5f",
  "created_metric_views": [],
  "ddl_artifact_path": "/Volumes/main/genie_workbench/runs/5521/metric_views.sql",
  "space_patch_ids": [],
  "tables_freed": 0,
  "advisor_status": "COMPLETED_WITH_CANDIDATES"
}
```

Only numeric, string, and boolean values are usable inside If/else operands, so keep list-valued fields out of any condition expression and branch on `high_confidence_count` or `advisor_status` instead.

#### 7.8 Create-and-attach flow (`mv_action_mode: create_and_attach`)

> **Step ownership and step 6 superseded — see [Appendix A · Deltas 1 and 3](#delta-1--two-run-consent-model).** All seven steps survive as behavior; steps 1–4 move to the backend under OBO and steps 5–7 become phases in `optimize`.

This runs **before** the lever loop. Steps 1–4 happen in `metric_view_apply`; steps 5–7 in `mv_baseline`.

1. **Re-verify consent.** Confirm `mv_consent` is present and well-formed, and that preflight's re-run of the entitlement probe still returns `SUFFICIENT` for the same `catalog.schema`. Abort to `suggest_only` on any mismatch — never write against a stale authorization.
2. **Rank** candidates by confidence, then by the count of failing benchmark questions each would plausibly repair. Failure attribution comes from the eval-run result details: cluster failing questions by assessment reason, and prefer candidates targeting `RESULT_MISSING_COLUMNS`, `RESULT_MISSING_ROWS`, and `RESULT_EXTRA_ROWS` clusters, which are grain, join, and filter defects a governed measure resolves. Exclude questions flagged **Manual review needed** — they carry no verdict to improve on. Cap creations per run (default 3) so a single run cannot flood a schema.
3. **Validate statically** — parse the generated YAML. Only if `mv_materialize` was separately consented and a `materialization` block is present, run `EXPLAIN CREATE MATERIALIZED VIEW` and abort the candidate on any incremental blocker rather than shipping a silent full-recompute. Default to `REFRESH POLICY INCREMENTAL`; use `INCREMENTAL STRICT` for high-cost or regulated metrics so the pipeline fails loudly instead of billing quietly.
4. **Create** with `CREATE VIEW … WITH METRICS LANGUAGE YAML` on the run's SQL warehouse, **under OBO, in the consented `catalog.schema` and nowhere else**. The target is the one the user picked at configuration; the task must not fall back to another schema, a default schema, or the app service principal's own schema if the write fails. A failed write is a downgrade to `suggest_only`, not a retry somewhere more permissive. Immediately after: `DESCRIBE EXTENDED` and assert `Type: METRIC_VIEW` — a syntax slip creates a regular VIEW that passes every later check while behaving as neither. Any later edit to this object uses `ALTER VIEW … AS $$…$$`, never `CREATE OR REPLACE`, which deletes the view's UC grants.
5. **Validate semantically** — query the new measure via `MEASURE(\`name\`)` with GROUP BY over a frozen sample window and diff result sets against the originating aggregation, not SQL text (`SELECT *` is unsupported on metric views, and re-typing the aggregate would mask the defect being checked). Run the fan-out smoke test: row count over the join must equal the pre-join count. Drop the view and reject the candidate on any difference outside tolerance. Surface the `GRANT SELECT` checklist for the space's audience alongside the created object — the creator's own answers validating proves nothing about other users'.
6. **Attach to the space** — express the attachment as a standard GSO patch against `data_sources.metric_views[]`, plus optional removal of the raw tables the metric view now covers, applied via the Genie Update Space API. Requires CAN MANAGE, verified in the probe. Because it is an ordinary patch, it inherits the existing versioning, diff, and rollback machinery.
7. **Measure the foundation change** (`mv_baseline`) — call *create eval run for benchmarks* over the same question set the `baseline` task used, poll to `DONE`, and read the results. This is the isolated metric-view delta, recorded before the lever loop changes anything else. **Optimization then proceeds from here**, with the metric view in place as the new foundation.

**Rollback semantics differ for the two artifacts, and conflating them causes damage.** The space patch is fully reversible and should be rolled back automatically if the metric view regresses accuracy. The metric view itself is a Unity Catalog object the user may now reference from a dashboard, a query, or another space within minutes of creation. So: **detach automatically, but never auto-drop.** On regression, revert the space patch, mark the view as unattached in the run output, and offer an explicit one-click drop with a warning that other consumers may already depend on it. In `sandbox` mode, where the scratch schema exists only for the run, auto-drop is correct and expected.

**Capacity as a second-order benefit.** A Genie Agent is capped at 30 tables or views. A metric view that pre-joins a fact and three dimensions replaces four entries with one, freeing three slots. Report `tables_freed` in the run summary — for spaces at the ceiling, this is often a more compelling reason to accept the proposal than the accuracy delta.

```json
{
  "patch_id": "patch_88c1",
  "field_path": "data_sources.metric_views",
  "operation": "append",
  "new_value": [{ "identifier": "finance.sales.discounted_revenue_metrics" }],
  "companion_patch": {
    "field_path": "data_sources.tables",
    "operation": "remove",
    "removed": ["samples.tpch.lineitem", "samples.tpch.orders", "samples.tpch.customer"]
  },
  "gate": { "mode": "create_and_attach",
            "consent": { "granted_by": "prashanth@example.com",
                         "granted_at": "2026-08-23T09:14:22Z",
                         "target": "finance.sales",
                         "probe_id": "probe_7f21",
                         "reverified_at_trigger": "2026-08-23T09:41:07Z",
                         "materialize_consented": false },
            "created_object": "finance.sales.discounted_revenue_metrics",
            "baseline_eval_run_id": "e1ef34712a29169db030324fd0e1df5f",
            "post_attach_eval_run_id": "a77c02be41d3907fb1194ce0aa2b8c14",
            "affected_question_ids": ["bmk_12", "bmk_31", "bmk_44"],
            "baseline_accuracy": 0.73, "post_attach_accuracy": 0.81,
            "regressed_questions": 0, "needs_review_questions": 1, "tables_freed": 2,
            "on_regression": "DETACH_ONLY_NEVER_DROP" }
}
```

**`reverified_at_trigger` is a timestamp, not a boolean** (amended under `MV-D7`; the field
was `reverified_at_preflight: true` before [Delta 1](#delta-1--two-run-consent-model) moved
the gate to trigger time). A boolean cannot distinguish "re-verified moments before this
write" from "re-verified once, three weeks ago" — and staleness is the whole risk the check
exists to close. It is persisted as a `TIMESTAMP` column on `genie_opt_mv_consents`, where
NULL means never re-verified and the job refuses to attach.

#### 7.9 Idempotency, cost, and failure isolation

> **Failure-isolation mechanism superseded — see [Appendix A · Delta 2](#delta-2--gated-phases-inside-optimize).** All four requirements hold; `max_retries`/`run_if`/task-value status become per-phase `try/except` plus a Delta status row.

- **Idempotency.** Key every candidate on `sha256(space_id | canonical_measure_expr | sorted_source_set)`. Re-running the job upserts rather than duplicating, and a candidate already rejected by a human stays suppressed until its decay window expires. On the write path, check for an existing object at the target name before creating: a re-run must not produce `discounted_revenue_metrics_2`.
- **Cost.** In `suggest_only` the task adds a few Delta reads, one embedding batch, and no table scans — negligible against the benchmark executions the run already performs. In `create_and_attach` it adds one validation query per candidate plus one extra eval run (`mv_baseline`), which is the real cost given the ~20 questions/min workspace ceiling. Materialization, if separately consented, adds ongoing pipeline cost that outlives the run — surface an estimate before the user checks that box, not after.
- **Failure isolation.** `max_retries: 1` on the advisor, `run_if: ALL_DONE` on every downstream edge, and an `advisor_status` task value the app renders as a non-blocking warning. Three distinct outcomes must all leave the optimization intact: advisor failure, consent withheld, and write failure. A metric-view path that cannot complete must degrade to `suggest_only` and let the run finish.
- **Auditability.** Every run records the requested mode, the effective mode, the downgrade reason if any, the consent object, the probe result, and the identity under which each write executed. If a customer asks six months later why a metric view exists in their catalog, the run should answer that question without anyone reading code.

---

### Part 8 — Curating Discover: Domains, Subdomains and Pages from Optimizer Profiling

The clustering, co-usage, and description work the optimizer performs to scope one Genie Space is the same work a curator does to scope a domain. Doing it twice is waste. This section designs a second gated task, `discover_curator`, that turns GSO profiling into Discover artifacts.

#### 8.1 What the platform gives you (status and mechanics)

| Capability | What it is | Status | Programmatic surface |
|---|---|---|---|
| **Discover page** | Curated internal marketplace over tables, dashboards, notebooks, apps, metric views, and Genie Agents; curator and consumer personas; AI-powered recommendations | **Public Preview** (Beta earlier in 2026) | UI only |
| **Domains** | Business-aligned grouping layer built on governed tags | **Public Preview** | Membership scriptable via tags; creation UI only |
| **Subdomains** | Second level, `{parentDomainTag}/{subdomainName}`, one level of nesting, one parent, many-to-many assets, subdomain tag independent of parent tag | **Public Preview** | Same as domains; must be managed from a workspace-level Manage Discover page |
| **Pages** | Governed, authoritative definition of a business concept, scoped to exactly one domain or subdomain; Genie One prioritizes and cites them | **Beta** (account preview toggle) | UI + Genie Code drafting and bulk import; no REST API |
| **Governed tags** | Account-level tags with an enforcing tag policy; applied to UC objects and to workspace objects including dashboards, apps, notebooks and Genie Agents; requires `ASSIGN` on the tag plus `APPLY TAG` on the object | **Public Preview**; Tag Policies API documented | Assignment via `ALTER … SET TAGS` (DBR 13.3+). **No public REST endpoint for tag assignment on securables** — Databricks has confirmed the endpoint the UI uses is internal by design, so automation must issue SQL through the Statement Execution API |
| **Tag automations** | Condition/action rules that assign or remove governed tags on Unity Catalog **tables and volumes** at scale, keeping them accurate as data changes | **Beta** | The practical bulk-assignment mechanism. Requires `USE CATALOG`, `USE SCHEMA`, `APPLY TAG` **and `MANAGE` on the catalog in scope** (checked at catalog level regardless of schema narrowing), plus `ASSIGN` on every tag involved |
| **AI-driven domain suggestions** | Native proposal of domains | **Coming soon in preview** | — |
| **Certification** | `system.certification_status` and related system governed tags; steers Genie One toward vouched assets | **Available** | Scriptable as a governed tag |

Two constraints shape everything below. First, **enablement is two-sided**: an account admin must turn on "Domains and Discover Page" at the account level and "Discover Page" per workspace. Second, **`MANAGE DISCOVERY` is the curator permission**, grantable at account, domain, or subdomain scope — so the workbench can and should scope its proposals to the domains the signed-in user actually curates.

**Reconstructing current state.** Domain membership is queryable today without any Discover API:

```sql
SELECT catalog_name, schema_name, table_name, tag_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'Finance' OR tag_name LIKE 'Finance/%';
```

That query is the baseline the curator task diffs against, so it proposes only what is missing.

#### 8.2 Signal-to-artifact mapping

| GSO profiling signal | Derived Discover artifact | Why it is the right signal |
|---|---|---|
| Genie Space membership (`data_sources.tables[]`) | **Domain seed** | A space is already a human-curated business scope with an owner; it is the single strongest prior available |
| Table co-usage in `query.history` (co-occurrence in the same statement or session) | Domain cluster cohesion | Assets people query together belong together |
| Lineage adjacency (`table_lineage` edges) | Domain cluster cohesion; subdomain splits along pipeline stages | Transformation proximity is business proximity |
| Inferred join keys (join-spec lever) | Domain connectivity | A shared join key is a hard structural link, not a soft one |
| Naming prefixes and existing tags | Naming/tag consistency term | Cheap, high-precision, and matches how teams already think |
| Embeddings of table and column descriptions | Semantic cohesion; subdomain separation | Catches business relatedness that names and lineage miss |
| Enriched table/column descriptions | Domain **Description** and **Subtitle** | Already written, already reviewed |
| Metric view measures + synonyms + display names | **Page** candidates (one per business concept) | A governed measure *is* a business concept with a definition |
| Space `text_instructions` | Page body "Business use" section | Instructions are prose definitions of business rules |
| Benchmark questions | Page "Examples of usage"; Discover section search queries | Real questions, in business language |
| Certification status and freshness | Section ranking; Page trust signals | Steers both people and Genie One toward vouched assets |
| Table owner / space owner | Domain **Technical owner** / **Business owner** | Required fields, already known |

#### 8.3 Domain proposal scoring

Mirror the metric-view model so reviewers learn one mental model:

```
DomainScore = 100 × (0.30·C + 0.25·J + 0.20·E + 0.15·N + 0.10·M)
```

| Component | Meaning | How computed |
|---|---|---|
| **C** — Co-usage cohesion | Do these tables get queried together? | Normalized modularity of the co-occurrence graph from `query.history` |
| **J** — Join connectivity | Are they structurally linked? | Fraction of cluster members reachable via inferred join keys or lineage edges |
| **E** — Semantic cohesion | Do their descriptions cluster? | Mean intra-cluster cosine minus mean inter-cluster cosine (silhouette-style) |
| **N** — Naming/tag consistency | Do names and existing tags agree? | Longest common prefix coverage plus existing-tag agreement rate |
| **M** — Space membership | Does an existing Genie Space already scope this? | Jaccard of the cluster against the space's table set |

Thresholds match Part 3 (High ≥ 75, Medium 50–74, Low 25–49). Two extra rules matter in practice:

- **Subdomain rule.** Run a second clustering pass *within* an accepted domain. Propose a subdomain only if the sub-cluster's internal cohesion exceeds the parent's by a clear margin and it holds at least a minimum asset count (default 5) — otherwise you fragment a domain into noise. Enforce the one-level nesting limit and the `{parent}/{child}` naming convention at proposal time, and reject any proposed subdomain name containing a slash.
- **Overlap is fine.** Assets are many-to-many across domains, so do not force a hard partition. Emit overlapping memberships where the evidence supports both, and let the curator prune.

```json
{
  "proposal_id": "dom_31a7",
  "type": "NEW_DOMAIN",
  "domain_score": 82,
  "tier": "HIGH",
  "governed_tag": "Loyalty",
  "display": {
    "subtitle": "Frequent-flyer program data and partner economics",
    "description": "Tables, metric views and agents supporting loyalty revenue, membership and the co-brand card partnership.",
    "technical_owner": "data-eng-loyalty@example.com",
    "business_owner": "loyalty-analytics@example.com"
  },
  "score_components": { "C": 0.84, "J": 0.79, "E": 0.71, "N": 0.90, "M": 1.00 },
  "evidence": {
    "seed_genie_space_id": "01ef_genie",
    "co_usage_edges": 212,
    "join_key_coverage": 0.79,
    "existing_tag_agreement": 0.63
  },
  "proposed_members": [
    { "asset": "main.loyalty.fct_member_activity", "type": "TABLE", "member_score": 0.94 },
    { "asset": "main.loyalty.revenue_metrics", "type": "METRIC_VIEW", "member_score": 0.91 },
    { "asset": "01ef_genie", "type": "GENIE_AGENT", "member_score": 1.00 }
  ],
  "proposed_subdomains": [
    { "governed_tag": "Loyalty/Co-brand", "domain_score": 77, "member_count": 9 },
    { "governed_tag": "Loyalty/Membership", "domain_score": 71, "member_count": 14 }
  ],
  "apply_plan": {
    "tag_ddl": [
      "ALTER TABLE main.loyalty.fct_member_activity SET TAGS ('Loyalty' = '')",
      "ALTER VIEW main.loyalty.revenue_metrics SET TAGS ('Loyalty' = '')"
    ],
    "manual_steps": ["Create governed tag 'Loyalty'", "Create domain in Discover UI", "Publish domain"]
  }
}
```

The `manual_steps` array is deliberate. Until a Domains API exists, the workbench should be explicit that it can prepare and validate everything and apply membership, but a human completes creation in the UI. Pretending otherwise produces a proposal that silently does nothing.

#### 8.4 Page drafting

A Page carries Domain, Owner, Synonyms, Description, Page body, Related assets, and Sources. Every one of those has a GSO source:

| Page field | Drafted from |
|---|---|
| Domain | The accepted domain or subdomain proposal |
| Owner | Metric view owner, or the space owner |
| Synonyms | Metric view `synonyms` and `display_name`; alias clusters from AST fingerprints |
| Description | Measure `comment`, or the enriched column description |
| Page body | Definition (canonical `expr` rendered in prose and code), Business use (from space `text_instructions`), Examples (benchmark *questions*, never their result values) |
| Related assets | The metric view, its source tables, the Genie Agent |
| Sources | The metric view definition, the certified dashboard or query the definition derives from |

```json
{
  "proposal_id": "page_5d90",
  "type": "NEW_PAGE",
  "domain": "Loyalty/Co-brand",
  "title": "Discounted Revenue",
  "owner": "loyalty-analytics@example.com",
  "synonyms": ["net revenue", "revenue after discount", "discounted sales"],
  "description": "Gross line-item revenue net of promotional discount, before tax and before partner settlement.",
  "body_sections": {
    "definition": "SUM(l_extendedprice * (1 - l_discount)), computed at line-item grain.",
    "business_use": "Used in the monthly Revenue Review. Excludes accrual adjustments uploaded after book close.",
    "examples": ["What was discounted revenue by market segment last quarter?"]
  },
  "related_assets": ["finance.sales.discounted_revenue_metrics", "samples.tpch.lineitem", "01ef_genie"],
  "sources": ["finance.sales.discounted_revenue_metrics"],
  "state": "DRAFT",
  "firewall_check": { "pii_scan": "PASS", "literal_scan": "PASS", "sample_values_included": false }
}
```

Two behaviors are worth calling out because they change how Pages should be used. First, **Genie One prioritizes a Page's definition over context it infers automatically and cites the Page in its answer** — so a Page is a direct accuracy lever, measurable with the same eval-run API used for metric views: take the benchmark questions whose answers depend on the term, run an eval before and after publication, and compare `num_correct` on that subset. Second, **draft Pages are visible only to the Page owner in Genie One conversations, while published Pages are available in all of them**. That gives a clean staging model, with one operational consequence: an eval run triggered by any other identity will not see a draft Page, so the before/after test must either be run by the Page owner or deferred until publication.

Where an estate already documents terms elsewhere, prefer the native **bulk import** path — Genie Code reads attached documents, extracts terms, deduplicates, and returns proposed Pages with conflict, duplicate, and low-confidence flags for review. The workbench's job in that case is to supply the source material and the domain assignment, not to re-implement the extractor.

#### 8.5 Governance workflow and the firewall

```
profile → cluster → propose → dry-run → curator approve → apply membership → create + publish (UI) → re-benchmark
```

- **Draft by default.** Domains, subdomains, and Pages all have native draft states; draft domains are visible only to users holding `MANAGE DISCOVERY`. Never publish from an automated task.
- **Dry run before tagging.** Where tag automations are used for bulk membership, use the built-in dry-run preview and show the curator the exact asset list before enabling the rule.
- **Scope to the curator.** Only propose against domains where the signed-in user holds `MANAGE DISCOVERY`, and only include assets they can BROWSE or SELECT. Apply tags under OBO; the `ASSIGN` permission on the governed tag is checked by the platform, so a proposal the user cannot apply should be rendered as "requires a curator" rather than failing at execution.
- **The firewall is non-negotiable here, more than anywhere else in this design.** Tag data, domain metadata, custom section titles and subtitles, and Page content are all stored as plain text and replicated globally, and **Page data does not support customer-managed key encryption**. No sample values, no query literals, no PII, no regulated data may enter a tag name, tag value, domain description, section title, or Page body. Run an automated scan on every draft and block on failure — the `firewall_check` block above is a required field, not a decoration.
- **Naming validation at proposal time.** Reject top-level domain tags containing `/`, reject subdomain names containing `/`, and enforce the `{parent}/{child}` prefix. Also check the plan against the documented ceilings before emitting it: Databricks caps the **combined number of domains and subdomains per account** and the **number of tags that can be applied**, both published on the Resource limits page rather than in the domains documentation. Read those limits at runtime rather than hard-coding them — a taxonomy proposal that exceeds the account ceiling fails halfway through and leaves membership half-applied.
- **Remember that subdomain tags are independent.** Tagging an asset `Loyalty/Co-brand` does **not** tag it `Loyalty`. If the intent is for an asset to appear under both, the apply plan must emit both statements. Subdomain assets do still surface in the parent domain's browse and search, so this matters for tag-driven automation and ABAC, not for findability.

#### 8.6 Wiring it into the job

> **Superseded — see [Appendix A · Delta 2](#delta-2--gated-phases-inside-optimize).** The curator is a gated phase inside `optimize` on the same terms as the advisor, not a sibling task; the signal-to-artifact mapping in 8.2–8.5 is unaffected.

The curator task is a sibling of the advisor, gated by its own parameter and dependent on the same enrichment output:

```yaml
        - task_key: discover_gate
          depends_on: [{ task_key: enrichment }]
          condition_task:
            op: EQUAL_TO
            left: "{{job.parameters.enable_discover_curation}}"
            right: "true"

        - task_key: discover_curator
          depends_on:
            - task_key: discover_gate
              outcome: "true"
          run_if: ALL_DONE
          python_wheel_task:
            package_name: genie_space_optimizer
            entry_point: discover_curator
            parameters:
              - "--space-id={{job.parameters.space_id}}"
              - "--profile-table={{tasks.enrichment.values.profile_table}}"
              - "--mv-candidates={{tasks.metric_view_advisor.values.candidate_table}}"
              - "--mode=propose"
```

It consumes the advisor's candidate table because a *proposed* metric view is itself a Page candidate — which means enabling both toggles produces a coherent bundle: a governed measure, a domain to file it under, and a Page defining it in the language of the business. Because Domains and Pages feed Unity Catalog Semantics and therefore the Genie Ontology, the same optimizer run that raises one space's benchmark score also strengthens the shared context every other agent draws on.

#### 8.7 API asks specific to Discover

1. **Domain and subdomain CRUD API** — create, update, publish, delete, with the `MANAGE DISCOVERY` grant enforced. Without it, the workbench can only prepare and stage.
2. **Pages CRUD API** — create draft, update, publish, unpublish; read published Pages for evaluation.
3. **Discover sections API** — create and publish custom sections, including search-query sections and pinning.
4. **Tag automations API** — create, dry-run, and enable automations programmatically rather than through Catalog Explorer.
5. **AI domain suggestions API** — when native suggestions ship, a read path so the workbench reconciles with them rather than competing.
6. **Domain-scoped read for evaluation** — the ability to ask "which Pages and domain context did Genie One use for this answer," which is the Discover analogue of the field-attribution ask in Part 1.

---

## Recommendations

1. **Ship Phase 1 as a strictly read-only advisor task, gated by a job parameter, harvesting already-optimized spaces first.** Highest precision, lowest risk, and no new platform capability required. Promote to broader heuristic matching once High-tier precision reaches 70%.
2. **Grade with Genie's native benchmark eval runs, and store the `eval_run_id`, not a copied score.** The Beta eval-run APIs (create, get, list runs, list results, get result details) removed the need for a parallel judge stack. Keep MLflow for run provenance, patch history, and versioning; stop using it as the accuracy source for a Genie Space. Wrap the Beta endpoints behind a thin adapter so a contract change costs you one file. *(The MLflow sentence is superseded — see [Appendix A · Delta 7](#delta-7--mlflow-is-tracing-only); the adapter already exists as the `EvalRunner` seam.)*
3. **Make the metric view a hypothesis Genie's own evaluation falsifies, not an assertion the advisor makes.** Feeding candidates into the lever loop and re-running the affected benchmark subset is what converts a plausible suggestion into a measured accuracy delta — and because the grader is the platform's, the number needs no defending.
4. **Gate every write behind a pre-run consent gate, not a mid-run prompt.** Ask for the target `catalog.schema` and probe entitlement *before* the run starts, so nobody discovers a permission problem forty minutes in. Default to `suggest_only`, downgrade automatically when the probe fails, and never upgrade. Keep materialization as a separately checked consent, and on regression **detach the metric view from the space but never auto-drop the Unity Catalog object** — someone may already be pointing a dashboard at it.
5. **Treat trusted assets and custom instructions as authoritative in conflicts** — surface, adjudicate, log; never overwrite.
6. **For Discover, apply membership automatically and leave creation to a human until the API exists.** Governed tags and tag automations cover assignment today; domain, subdomain, and Page creation do not have an API, and a proposal that cannot be applied should say so plainly.
7. **Run the PII and literal firewall scan as a blocking gate on every domain and Page draft.** Page data has no CMK support and replicates globally; this is the one control in the design where a single miss is unrecoverable.
8. **Escalate to Phase 3 auto-generation only when** High-tier precision reaches 85% and accept rate reaches 50%. Below that, auto-generation erodes trust faster than it adds coverage.

---

## Caveats

- **Verify the exact create privilege and runtime floor for metric views on your target workspace.** The design assumes Unity Catalog's standard view-creation privilege set (`USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on the schema, `SELECT` on sources). Current docs put creation/edit at **DBR 17.3+** (v1.1 YAML features at 17.2+; nested joins at 17.1+; `fields:`/`agg()`/window `offset` at 18.1+) — the probe's capability rows encode these, but confirm both the privilege name and the runtime floor against the target workspace before the probe ships. A probe that checks the wrong privilege or version will either block entitled users or promise a write that then fails.
- **Materialization consent is separable from attach consent, and must stay that way.** Attaching a metric view is a metadata change; materializing one starts a managed Lakeflow pipeline that bills after the run ends. Any UI that collapses these into one checkbox will eventually produce a surprise invoice traced back to a feature the customer understood as advisory.
- **Undocumented internals.** The "five versus six levers" naming discrepancy between the repo glossary and third-party write-ups, and internal schema acronyms in Genie Workbench, are not defined in public sources. Confirm against source code before building against them. Note also that public write-ups describing a bank of automated MLflow judges reflect an earlier architecture; grading now runs on Genie's native benchmark eval runs.
- **The Genie benchmark eval APIs are Beta, not GA.** Create eval run, get eval run, list runs, list results, and get result details are all labelled Beta, so field names and enum values can change without a deprecation window. Isolate them behind an adapter, and record the API version alongside every stored `eval_run_id`.
- **Grading semantics differ by benchmark mode, and this changes what a delta means.** Chat mode compares result sets deterministically against the question's SQL Answer; Agent mode uses an LLM judge with an optional evaluation note. Do not pool accuracy across modes in a single headline number, and treat Agent-mode deltas with the caution any LLM-graded metric deserves — the judge-bias literature has not stopped applying just because the judge now ships with the platform.
- **"Manual review needed" is a third outcome, not a failure.** Questions Genie cannot assess, and questions without a SQL Answer, return unassessed. Exclude them from both the numerator and denominator of any lift calculation, and surface `num_needs_review` in the run summary — a rising count usually means the benchmark suite is degrading, not the space.
- **Benchmark questions and answer SQL remain evaluation-only.** They should not be copied into space configuration; a benchmark that leaks into instructions stops being a test.
- **Data-availability limits.** `statement_text` and `error_message` are empty under customer-managed keys; lineage system tables retain a rolling 1-year window; `system.query.history` is Public Preview with its own retention. All three bound historical demand signals.
- **Materialization** is flagged Experimental in parts of the YAML reference even though `REFRESH POLICY` is GA. Validate on the target workspace and runtime before enabling it in proposals.
- **Throughput ceilings** (~20 questions/min/workspace; API free tier ~5/min) constrain live replay. Prefer harvesting persisted GSO and query-history data.
- **The 30-table-per-agent limit** (25 on some older AWS docs) makes metric view consolidation a capacity lever as well as an accuracy one. Validate the exact number per workspace.
- **Discover, Domains, and Subdomains are Public Preview; Pages and tag automations are Beta; AI-driven domain suggestions are not yet in preview.** Preview features are not covered by compliance certifications, and in workspaces with the compliance security profile enabled some previews may not be available at all. Confirm availability before committing a customer to this workflow.
- **Domain and Page creation being UI-only is a hard architectural constraint**, not a temporary inconvenience. Design the curator task to produce a reviewed, validated, copy-ready plan, and treat full automation as contingent on the API asks in section 8.7.

---

## Appendix A — Implementation deltas

This appendix supersedes the conflicting parts of Part 7. It exists because Parts 6–8 were
drafted against a Genie Space Optimizer job that no longer exists: `main` replaced the
multi-stage wheel DAG with a **linear four-task notebook DAG**, and that replacement is
enforced by tests that pass today. The original sections are deliberately left in place —
their reasoning is still the best statement of *why* the feature is shaped this way, and
most of it survives the mechanical changes untouched.

**Authority order.** Repository code and its tests win over this appendix — per `MV-D4`,
the rules file and this document are amended and the tests are not; this appendix wins
over the body of the POV; the body of the POV wins where this appendix is silent. The full
evidence base is `docs/design/mv-advisor-gap-report.md`, which quotes the current code
verbatim with line citations.

**Nothing in Part 7's product behavior is negotiated away here.** Every requirement —
two-question consent, run-and-schema scoping, preflight re-verification,
downgrade-never-upgrade, the suggest-only output contract, detach-never-drop, the leakage
firewall, and the "Lift not measured" label — survives intact. What changes is *where the
code runs and what it writes to*. See [Preserved product behavior](#preserved-product-behavior)
for the explicit checklist.

### A note on decision numbering

Three separate `D`-numbered decision sets are in circulation, and conflating them will
cause real errors:

| Namespace | Where it is recorded | Example |
|---|---|---|
| **MV advisor playbook** `MV-D1`–`MV-D10` | `docs/design/mv-advisor-playbook.md`, "Decisions register" — **the defining source**, which this appendix cites | `MV-D1` = two-run consent model; `MV-D3` = phases inside `optimize`, not new tasks; `MV-D7` = three metric-view Delta tables; `MV-D8` = the generation quality standard; `MV-D10` = two permanently distinct fingerprint levels |
| **GSO v2 playbook** `GSO v2 D1`–`GSO v2 D9` | Cited throughout the optimizer code; reconstructed in gap report §4. The defining file `GSO_OPTIMIZER_V2_TODO.md` is **not checked in** | `GSO v2 D1` = the native Benchmark Eval API is the sole eval runner; `GSO v2 D9` = linear DAG, no condition tasks, no task values |
| **Baseline-eval-fix plan** `applier.py D1`–`applier.py D3` | `optimization/applier.py:358` | Quality-instruction policies (`mv_preference`, `column_ordering`, …) |

Every decision citation in this appendix carries its namespace prefix — `MV-D2`,
`GSO v2 D9`, `applier.py D2` — and a bare `D<number>` appears nowhere outside the table
above. The collision is not hypothetical: `GSO v2 D1` and `MV-D1` are different decisions
about different subjects, and gap report §4 records `GSO v2 D4`, `GSO v2 D5` and
`GSO v2 D6` as having **zero citations anywhere in the repository** — they are unrelated to
`MV-D4`–`MV-D10`, which are defined in the MV advisor playbook and are used here.

Deltas 1 through 3 carry `MV-D` identifiers and between them cover `MV-D1`, `MV-D2`,
`MV-D3`, `MV-D5` and `MV-D6`; `MV-D4` governs this appendix's existence and is cited in the
authority note above. Deltas 5 and 7 record repo facts established by the gap report rather
than decisions taken in the playbook, so they cite the gap report instead — that is a
difference in provenance, not an omission. Deltas 4 and 6 are mixed: each states a repo fact
from the gap report *and* records `MV-D7`, the decision that resolved gap report §3 item 7
by giving the metric-view feature three Delta tables of its own. Delta 8 carries `MV-D8`
alone — the generation quality standard, adopted after the other deltas were written, which
is why it amends rather than supersedes the section it touches.

---

#### Delta 1 — Two-run consent model

*(`MV-D1`. Supersedes: TL;DR bullet 3, 7.3 step 4–5, 7.4, 7.6, 7.8 steps 1–7 ownership.)*

**What changes.** Part 7 assumed one run that creates a metric view mid-flight, attaches it,
and then optimizes on top. That is not implementable: **the GSO job runs as the service
principal**, so the user's OBO token does not exist inside it, and Part 5's rule that "the
SP is never a write path for metric views" would be violated the moment the job issued
`CREATE VIEW … WITH METRICS`. Create-and-attach therefore splits across two runs, with the
UC write hoisted into the FastAPI backend where the OBO token actually lives.

**Run 1 — advise.** The user enables suggestions. The run is *always* effectively
`suggest_only` regardless of entitlement, because nothing can be created from inside the
job. The advisor phase proposes candidates, and the run output renders the full §7.5
contract: validated DDL, the `GRANT`, the would-be patch as a diff, the evidence block,
and **"Lift not measured"**. No UC write, no space patch.

**Between runs — consent.** The user reviews candidates in the run output, selects which to
adopt, picks the target `catalog.schema`, and consents. The entitlement probe runs under
OBO via `require_obo_workspace_client` (`backend/services/auth.py:117-127`), which — unlike
`get_workspace_client` — never falls back to the service principal. The probe is **new OBO
code**; `GET /api/auto-optimize/permissions/{space_id}` must not be reused for it, because
it probes the SP's privileges rather than the user's.

**Run 2 — adopt.** At `POST /api/auto-optimize/trigger`, *before the job is submitted*, the
backend re-verifies the probe and executes `CREATE VIEW … WITH METRICS LANGUAGE YAML`
under OBO in the consented schema and nowhere else. Only on success does it call
`run_now`, passing the created identifiers and the consent record as job parameters. The
job — as SP — then attaches the view by patch, measures, and reverts on regression: all
writes it already performs today.

| Step | Identity | Where |
|---|---|---|
| Propose candidates, render DDL | SP (job) | `optimize` task, advisor phase |
| Entitlement probe | **OBO** | FastAPI, new route |
| Record consent | OBO | FastAPI, persisted with the run |
| `CREATE VIEW … WITH METRICS` | **OBO** | FastAPI, at trigger time, before `run_now` |
| Attach to space (patch) | SP (job) | `optimize` task, attach phase |
| Isolated lift eval | SP (job) | `optimize` task, eval phase |
| Detach on regression | SP (job) | snapshot revert |
| Drop the UC object | **OBO** | explicit backend endpoint, user-initiated only |

**Why this is better than it looks.** §7.5 already specified a one-click
**[Re-run with this metric view]** action that pre-fills the next run in `create_and_attach`
mode. The two-run model makes that the primary path rather than a fallback, and it gives
the user something the single-run design could not: a review step between seeing a proposal
and having an object in their catalog.

**Lift attribution is preserved without `mv_baseline`.** Run 2 evaluates iteration 0 with
the view created but **not yet attached**, applies the attach patch, then evaluates again
before any lever fires. That delta is the isolated metric-view contribution — exactly what
§7.2 and §7.8 step 7 required, obtained from two in-process eval runs rather than a
separate task. Both go through the `EvalRunner` seam (`optimization/eval_runner.py`); no
second adapter, no raw SDK eval calls.

**Downgrade, never upgrade — unchanged and now easier to enforce.** If re-verification at
trigger time fails, the backend does not create, and submits the job in `suggest_only`.
A run configured as `suggest_only` can never become a writing run. Because the write
decision is made in one place, before submission, there is no mid-run state in which a
downgrade can be missed.

**`sandbox` mode survives** as the one exception to detach-never-drop: the backend creates
in a scratch schema and auto-drop is correct there, because the schema exists only for the
run. Outside sandbox, the job never drops.

**Materialization stays a separate consent** (`mv_materialize`), never bundled with
create-or-attach. The `EXPLAIN CREATE MATERIALIZED VIEW` precheck runs in the backend
alongside the create, per §7.8 step 3.

---

#### Delta 2 — Gated phases inside optimize

*(`MV-D3`; `MV-D5` for the job-parameter lockstep. Supersedes: 7.2 DAG, 7.6 rows "Job"/"Tasks", 7.7 bundle YAML, 7.7.1 transport, 7.9 failure isolation, 8.6.)*

**What changes.** `mv_gate`, `metric_view_advisor`, `mv_write_gate`, `metric_view_apply`,
`mv_baseline` and `discover_gate` are **not tasks**. Metric-view work runs as gated phases
inside the existing `optimize` task. The job DAG is unchanged:

```
intake_and_snapshot → benchmark_qc_and_repair → optimize → publish_and_audit
```

Three mechanisms Part 7 relies on are each independently forbidden by a passing test in
`packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py`. Per `MV-D4` the feature
conforms to those tests; they are not weakened or deleted:

| Part 7 mechanism | Prohibiting test |
|---|---|
| `condition_task` gates (`mv_gate`, `mv_write_gate`, `discover_gate`) | `test_no_condition_tasks` — also asserts every task is a `notebook_task` |
| `{{tasks.X.values.Y}}` handoff | `test_no_dbutils_notebook_run_or_task_values_in_new_notebooks` |
| Extra tasks / `run_if: ALL_DONE` edges | `test_dependencies_form_a_linear_chain`, `test_deploy_and_legacy_tasks_removed` |

`python_wheel_task` is equally unavailable: all four tasks are notebooks and the package
declares no `[project.scripts]` entry points.

**Gating.** Each phase begins with an ordinary `if` on a job parameter read via
`dbutils.widgets`. A string comparison against `"true"` is still the right test — Part 7's
fail-closed reasoning about string operands carries over unchanged; it is simply an `if`
in Python rather than an If/else task.

**Failure isolation.** Every phase is wrapped in `try/except`. On failure the phase
persists a status row to Delta and **the optimization continues**; nothing raises across a
phase boundary. This delivers §7.9's requirement — advisor failure, consent withheld, and
write failure must all leave the optimization intact — without `max_retries` or
`run_if: ALL_DONE`.

**Phase order inside `optimize`**, preserving §7.2's rationale that the foundation changes
before tuning begins:

```
[enrichment / lever 0, existing]
   → mv_advise      (propose; always runs when enabled, in every mode)
   → mv_attach      (run 2 only; attach patch for views the backend created)
   → mv_isolate     (eval; the isolated metric-view delta — replaces mv_baseline)
   → [unified lever loop, existing]
   → mv_revert      (detach on regression; snapshot revert, never a drop)
```

`mv_advise` runs identically in every mode, so — exactly as §7.7 intended — the expensive
analysis is never wasted when consent is absent, and `suggest_only` output is precisely
what run 2 would have written.

**Job parameters (`MV-D5`).** Any new parameter must be added in **lockstep to all four** of
`databricks.yml`, `packages/genie-space-optimizer/databricks.yml`,
`scripts/deploy_lib/gso_job.py`, and the `run_now` map in
`packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py`, plus a
`dbutils.widgets` declaration in each consuming notebook. `run_now` rejects undeclared
keys, so the launcher set must stay a subset of the three job definitions. Because §7.7
proposes seven scalars and each costs roughly twelve coordinated edits, **prefer a single
JSON-encoded `mv_config` parameter** parsed and validated in the notebook — this also suits
`mv_consent`, which is already JSON.

---

#### Delta 3 — Dict patches and new PATCH_TYPES

*(`MV-D2`; `MV-D6` for rollback. Supersedes: 7.8 step 6 and the patch JSON at the end of 7.8; TL;DR bullet 2.)*

**What changes.** There is no `field_path` + `new_value` patch model in this repository —
`field_path` does not appear anywhere in the optimizer source. Patches are dicts:

| Part 7 field | Repo equivalent |
|---|---|
| `field_path` | `type` (a key in `PATCH_TYPES`) **plus** `target` (the asset identifier) |
| `new_value` | `new_text`, with `old_text` carrying the prior value |
| `operation` | `op` on the rendered command: `add` / `update` / `remove` / `update_section` / `rewrite` |

The §7.8 patch becomes, in shape:

```json
{
  "type": "attach_metric_view",
  "target": "finance.sales.discounted_revenue_metrics",
  "new_text": "finance.sales.discounted_revenue_metrics",
  "old_text": "",
  "lever": 2,
  "risk_level": "…",
  "grounded_in": ["bmk_12", "bmk_31", "bmk_44"]
}
```

The rich `gate` block from §7.8 — consent, probe id, both eval-run ids, accuracies,
`tables_freed`, `on_regression` — is retained as patch provenance and run state, not
discarded.

**This delta fixes a live bug, not just a naming mismatch.** The Lever-2 metric-view patch
types already exist in `PATCH_TYPES` and render commands, but `_apply_action_to_config`
treats `mv_measures`, `mv_dimensions` and `mv_yaml` as **config-level no-ops**, and nothing
in the codebase attaches a metric view to a space at all: `add_table`/`remove_table` mutate
`data_sources.tables` only. A new MV patch type must therefore ship **a real applier that
mutates `data_sources.metric_views`**.

Four registration points, all required:

1. `PATCH_TYPES` in `common/config.py` — declare the type.
2. `_ALLOWED_PATCH_TYPES` in `optimization/unified_loop.py` — the live loop allowlist, which
   today contains eleven types and no metric-view type. A type absent here is silently
   never applied.
3. `_apply_action_to_config` in `optimization/applier.py` — the real applier. The companion
   raw-table removal from §7.8 can reuse the existing `remove_table` branch, which must also
   be added to the allowlist.
4. `_PATCH_TEXT_FIELDS` in `optimization/leakage.py` — **mandatory**. MV proposals carry
   `comment`, `display_name` and `synonyms`, all free text, and the firewall currently
   covers only the two example-SQL patch types. Extend the existing scanner; do not write a
   second one.

**Rollback (`MV-D6`).** Detach-on-regression is implemented as whole-snapshot revert
(`integration/revert.py`), which is how rollback works throughout this codebase. The UC
object is never auto-dropped outside sandbox mode; a drop is an explicit backend OBO
endpoint, user-initiated. Note the consequence Part 7 did not anticipate: a snapshot revert
also reverts unrelated patches from the same iteration. If per-patch detach is required,
that is new machinery and needs its own decision.

---

#### Delta 4 — Artifacts in genie_opt_artifacts

*(No `MV-D` — repo fact per gap report §1.6; `GSO v2 D9` for Delta-by-`run_id` handoff; scope narrowed by `MV-D7`. Supersedes: 7.7.1 transport and `ddl_artifact_path`.)*

**What changes.** There is no Volumes convention for run artifacts — `/Volumes` appears
nowhere in the optimizer source, and the only real Volumes use in the repo is the GSO wheel
upload path. Cross-phase and cross-task state goes to the `genie_opt_artifacts` Delta
table, keyed by `run_id`.

The entire §7.7.1 payload survives **as a payload**; only the transport changes. Write it
with `write_required_artifact` under a new `artifact_kind` (proposed: `mv_advisor`), and
read it downstream with `load_latest_artifact_record` behind a missing-record gate. The
`benchmark_qc` handoff is the worked precedent, and its read ordering — load, then
missing-record gate, then payload, then eligibility gate — is itself pinned by
`test_benchmark_qc_is_a_required_verified_handoff`. Follow that shape.

Two fields map onto columns that already exist rather than needing invention:

- `ddl_artifact_path` → the DDL text lives in the artifact payload; there is no path.
- The §7.9 idempotency key `sha256(space_id | canonical_measure_expr | sorted_source_set)`
  → `genie_opt_artifacts.content_hash`, which exists for exactly this purpose.

**`MV-D7` narrows what belongs here.** The *rendered DDL text* is a stage output and stays
an artifact row, written under a new `artifact_kind` exactly as described above. The
*candidate itself* is not: it is a stateful entity with a human decision attached and a
lifetime longer than the run that produced it, so it lives in `genie_opt_mv_candidates`
(see [Delta 6](#delta-6--nine-delta-tables-six-run-scoped-plus-three-metric-view-stores)).
The idempotency key is the bridge between the two stores: the artifact's `content_hash`
is set to the candidate's `dedup_fingerprint`, so the DDL text for any candidate is
reachable by that key and neither store needs a foreign key the other cannot supply.

Part 7's closing note that "only numeric, string, and boolean values are usable inside
If/else operands" is moot: there are no If/else tasks, so list-valued fields such as
`created_metric_views` and `space_patch_ids` need no special handling.

---

#### Delta 5 — The run-start endpoint

*(No `MV-D` — repo fact per gap report §1.7. Supersedes: 7.6 "App API" row; the §1c GSO run-artifacts row.)*

**What changes.** The route is **`POST /api/auto-optimize/trigger`**, not
`POST /api/auto-optimize/runs`. Extend its inline `TriggerRequest` model in
`backend/routers/auto_optimize.py` with the metric-view fields; **never add a parallel
start endpoint**. All routes stay under the existing `/api/auto-optimize` router prefix.

The §7.6 request body is otherwise correct as a payload. Under [Delta 1](#delta-1--two-run-consent-model)
this same endpoint also performs the OBO create before submitting the job, which makes it
the single place where the write decision is made.

Two genuinely new OBO-only routes are required — the entitlement probe and the explicit
drop. Both are new code rather than extensions of anything existing, and **their names are
not yet decided**: this repository's convention is to not invent endpoint names in a design
doc. See gap report §3 for the open naming decisions.

---

#### Delta 6 — Nine Delta tables: six run-scoped plus three metric-view stores

*(Repo fact per gap report §1.6; the metric-view stores are `MV-D7`. Supersedes: Key Finding 2 and the §1c row, both of which say "roughly 15 Delta tables".)*

**What changes.** A GSO run writes **six** `genie_opt_*` tables, defined in one place
(`optimization/ddl.py`): `genie_opt_runs`, `genie_opt_stages`, `genie_opt_iterations`,
`genie_opt_patches`, `genie_opt_benchmark_mutations`, `genie_opt_artifacts`. Two more exist
outside that registry — `genie_opt_scan_snapshots` and the per-domain
`genie_benchmarks_{domain}`. They live in `{GSO_CATALOG}.{GSO_SCHEMA}`, so Part 7's
`main.genie_workbench.mv_candidates` is wrong in both prefix and location.

**`MV-D7` — the metric-view feature adds three tables to that registry**, not columns to
the existing six: `genie_opt_mv_candidates`, `genie_opt_mv_consents`,
`genie_opt_mv_created_objects`. Gap report §3 item 7 left the choice open between a
candidates table and an `artifact_kind` row; the three-table shape resolves it, because all
three hold **stateful entities rather than stage-handoff blobs**:

- A **consent** is created by the entitlement probe *before any run exists*, so its
  `run_id` is NULL at insert and filled at trigger time. It cannot be a column on
  `genie_opt_runs`, which has no row yet, and it cannot be partitioned by `run_id`.
- A **candidate** deliberately **outlives the run that proposed it** — `create_and_attach`
  acts on proposals approved from an earlier run ([Delta 1](#delta-1--two-run-consent-model)),
  and carries a mutable human decision plus a rejection decay window. A `run_id`-keyed
  artifact row is the wrong grain and the wrong lifetime.
- A **created object's** `status` mutates `CREATED → ATTACHED → DETACHED → DROPPED`, the
  last transition potentially months later via the explicit drop endpoint. Artifacts are
  append-only by design.

What survives from the artifacts-first instinct is the split in
[Delta 4](#delta-4--artifacts-in-genie_opt_artifacts): the *rendered DDL text* is a stage
output and stays an artifact row, cross-referenced to its candidate by setting the
artifact's `content_hash` to the candidate's `dedup_fingerprint`. Requested mode, effective
mode and downgrade reason are recorded on the metric-view rows that own them
(`genie_opt_mv_candidates` and `genie_opt_mv_consents` respectively) rather than widening
`genie_opt_runs`, so a run with the feature off carries no metric-view columns at all.

**Schema evolution is additive and never automatic-destructive.** Adding a column means
editing the `CREATE` DDL string, appending to `ADDITIVE_COLUMN_MIGRATIONS`, wiring the
writer, extending the Workbench API model, and adding tests — all in one commit. Adding a
table means a name constant, a DDL string, and registration in `_ALL_DDL`; column
migrations only apply to tables already registered there. Never drop or rename a
historical table or column.

Lakebase is a separate store — the app's own Postgres, holding scan results, starred
spaces and watch caches. It is not where GSO run state lives, and the synced-table readers
for GSO Delta tables are currently disabled behind a flag.

---

#### Delta 7 — MLflow is tracing-only

*(No `MV-D` — repo fact per gap report §4, `GSO v2 D3`. Supersedes: the §1c MLflow row and the MLflow half of Recommendation 2.)*

**What changes.** MLflow is **decommissioned in GSO except for tracing**. There are no
experiments, no registry, no judges. Part 7's claim that MLflow is "still the home for run
provenance and versioning" is not true of this codebase: tracking and versioning are
Delta-only. There is no MLflow `LoggedModel` snapshot, no UC Model Registry version, and no
per-mutation MLflow run; the champion iteration is selected from `genie_opt_iterations` and
marked in Delta.

Metric-view provenance therefore goes to Delta — `genie_opt_patches` for the patch and its
provenance chain, `genie_opt_runs` for consent and mode, `genie_opt_artifacts` for
candidates and DDL. The POV's own Caveat that "public write-ups describing a bank of
automated MLflow judges reflect an earlier architecture" is correct, and this is the
decision behind it.

Recommendation 2's other half stands and is already implemented: grade with native
benchmark eval runs, store the `eval_run_id`, and keep the Beta endpoints behind a thin
adapter. That adapter exists as the `EvalRunner` seam, and `eval_run_id` is already a
persisted column on `genie_opt_iterations`.

---

#### Delta 8 — Generation quality gates on the trigger-time create (MV-D8)

*(`MV-D8` — MV advisor playbook. Amends: [Delta 1](#delta-1--two-run-consent-model)'s Run 2 create step. Shares ownership with §7.8 steps 4–5, which stay current for these gates.)*

**What changes.** [Delta 1](#delta-1--two-run-consent-model) hoisted the UC write into the
backend at trigger time, and its restatement of that flow predates `MV-D8`. Because this
appendix wins over the body, an appendix-only reader would otherwise get the create
without the generation gates — and an engine that emits invalid or silently wrong YAML is
worse than no engine, because its output arrives pre-trusted. Six gates bind the
trigger-time create:

1. **One renderer, one validator.** All YAML is rendered and validated exclusively by
   `mv_yaml` (playbook Prompt 5.5). The backend create path never renders inline, so the
   proposal the user approved and the object actually created cannot drift apart.
2. **Type assertion before status.** Post-create, `DESCRIBE EXTENDED` must assert
   `Type: METRIC_VIEW` before `genie_opt_mv_created_objects` records `status=CREATED`.
   A syntax slip produces a regular VIEW that passes every later check while behaving as
   neither.
3. **Validation that cannot mask the defect.** Semantic validation queries use
   `MEASURE(\`name\`)` with GROUP BY, plus the fan-out row-count smoke test — never
   `SELECT *`, which is unsupported on metric views, and never a re-typed aggregate,
   which would re-implement the very expression under test.
4. **Grants-preserving edits.** Any subsequent edit uses `ALTER VIEW … AS $$…$$`, never
   `CREATE OR REPLACE` or drop+create, either of which deletes the view's UC grants and
   cascading metadata — a metrics iteration silently revoking every consumer's access.
5. **Grants surfaced, never applied.** A copy-ready `GRANT SELECT` checklist for the
   space's audience is surfaced on the run record and never auto-applied. The creating
   user owns the view; without SELECT, other users' Genie answers degrade silently and
   per-user accuracy diverges in a way nobody can reproduce.
6. **Capability, not just privilege.** The entitlement probe's capability rows — DBR
   17.3+ to create or edit, 17.1+ for nested joins, 18.1+ for `fields:`, `agg()`, and
   window `offset` — feed `mv_yaml.validate`, which downgrades the join strategy
   (nested → subquery-`source`) rather than emitting YAML the runtime cannot plan.

**Shared ownership with the body.** Unlike every other delta here, this one does not
supersede the section it touches: **§7.8 steps 4–5 are current** for these gates, having
been amended under `MV-D8` in the same change that added this delta. A body reader and an
appendix reader therefore converge on the same requirements. If the two ever diverge
again, that is a defect in this document, not a decision.

---

### Preserved product behavior

Every item below is a requirement of the original design that survives these deltas
unchanged. Implementation must satisfy all of them.

| Requirement | Source | How it is preserved |
|---|---|---|
| Two separate questions: consent (authorization) and privileges (entitlement) | 7.3 | Both asked before run 2; probe under OBO |
| Consent scoped to one run and one `catalog.schema` | 7.3, Part 5 | Recorded with the run, carried as a job parameter, not read from app state |
| Consent re-verified before any write | 7.6, 7.8 step 1 | Re-verified at trigger time, immediately before the OBO create |
| **Downgrade, never upgrade** | 7.6, Recommendation 4 | Probe failure ⇒ backend does not create and submits `suggest_only`; no path upgrades a run |
| Write to the consented schema **and nowhere else** | 7.8 step 4 | Backend create targets the recorded schema; a failed write is a downgrade, never a retry somewhere more permissive |
| The SP is never a write path for metric views | Part 5 | The UC create is OBO-only in the backend; the job never issues MV DDL |
| Suggest-only renders DDL, `GRANT`, patch diff, evidence | 7.5 | Unchanged, and now the output contract of every first run |
| **"Lift not measured"** label | 7.5, Part 4 | Mandatory on every run-1 output; never present a projected gain for an unevaluated view |
| Isolated metric-view lift, measured before tuning | 7.2, 7.8 step 7 | Two in-process eval runs in run 2, pre- and post-attach, before the lever loop |
| **Detach automatically, never auto-drop** | 7.8, Recommendation 4 | Regression ⇒ snapshot revert; drop is an explicit user-initiated OBO endpoint |
| Sandbox mode may auto-drop | 7.4 | The one exception; scratch schema exists only for the run |
| Materialization is a separate consent | 7.4, Caveats | `mv_materialize` never bundled with create or attach |
| `EXPLAIN` precheck before materializing | 7.8 step 3 | Runs in the backend alongside the create |
| Leakage / PII firewall on everything shipped | 7.8, 8.5, Recommendation 7 | Extend `optimization/leakage.py`; new text-carrying patch types must register in it |
| Benchmark questions stay evaluation-only | Caveats | Firewall covers MV comments and Page bodies alike |
| Metric-view path never fails the optimization | 7.9 | Per-phase `try/except` plus a Delta status row |
| Cap creations per run; idempotent candidates | 7.8 step 2, 7.9 | Unchanged; `content_hash` supplies the dedup key |
| Full auditability of mode, consent, probe, identity | 7.9 | Persisted to `genie_opt_runs` and `genie_opt_patches` |
| Conflicts surfaced, adjudicated, never auto-resolved | Part 5, Recommendation 5 | Unaffected by these deltas |
| `DESCRIBE EXTENDED` asserts `Type: METRIC_VIEW` before `status=CREATED` | 7.8 step 4, Delta 8 | The type gate runs before the created-object row is written, so a regular-VIEW impostor never reaches ATTACHED |
| Semantic validation uses `MEASURE()` with GROUP BY plus a fan-out smoke test | 7.8 step 5, Delta 8 | Never `SELECT *`, never a re-typed aggregate — both mask the defect being checked |
| Edits use `ALTER VIEW … AS $$…$$`, never `CREATE OR REPLACE` | 7.8 step 4, Delta 8 | Replace and drop+create delete the view's UC grants and cascading metadata |
| `GRANT SELECT` checklist surfaced, never auto-applied | 7.8 step 5, Part 4, Delta 8 | Carried on the run record; the creator's own validation proves nothing about other users' answers |
| Capability rows gate the join strategy, not just the write | 7.3.1, Part 4, Delta 8 | DBR 17.3/17.1/18.1 floors feed `mv_yaml.validate`, which downgrades nested → subquery-`source` rather than emitting unplannable YAML |
| `BEST FOR` lines paraphrased, never verbatim benchmark text | Part 4, Delta 8 | Firewall class, same as literals and PII: an attached view's comment is context Genie reads, so verbatim text contaminates the benchmark grading it |

### What this appendix does not decide

Open items, deferred to `docs/design/mv-advisor-gap-report.md` §3: names for the two new
OBO routes and the new patch types; whether metric-view parameters ship as one JSON blob or
several scalars; whether run 2's isolated lift eval is worth its cost against the
~20 questions/min workspace ceiling; and whether per-patch detach is built or whole-snapshot
revert is accepted. Per repository convention, none of these names should be invented in a
design document — decide them, then implement.
