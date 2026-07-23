# GSO Wide-Schema Support Specification

Status: Ready for implementation review  
Scope: Genie Space Optimizer (GSO) only  
Out of scope: Create Agent

## 1. Problem

GSO currently collects the complete Unity Catalog schema and profiles every
profilable column for each eligible asset: all regular table/view columns and
all metric-view dimensions. A regular table profile builds one aggregate
statement containing `COUNT(DISTINCT ...)` for every column plus `MIN` and
`MAX` for numeric and date columns. Wide assets therefore produce very large
SQL statements. The same complete column set is also serialized into
benchmark-generation and repair prompts.

Assets with hundreds or thousands of columns can consequently hit SQL
compilation or execution timeouts and model-context limits.

GSO must keep the complete schema for deterministic validation while placing
separate hard limits on:

1. columns actively used by profiling and prompt construction;
2. distinct columns value-profiled during the complete run;
3. expressions in an individual profiling statement;
4. total profiling work submitted by a run; and
5. the complete serialized size of every LLM request.

## 2. Required outcomes

- Support assets with thousands of columns without profiling every column.
- Retain an immutable, complete UC inventory for deterministic validation.
- Rank columns before value profiling.
- Maintain no more than 50 active working-set columns per asset.
- Value-profile no more than 50 distinct columns per asset during a run.
- Reserve capacity for columns discovered by benchmark repair or optimization.
- Use real query behavior when the GSO service principal can access it.
- Continue successfully when no query-history source is available.
- Prevent GSO-generated traffic from influencing later rankings.
- Bound every profiling statement, the total profiling stage, and every LLM
  request independently of schema width and asset count.
- Preserve current table, view, and metric-view behavior where it is safe.

## 3. Non-goals

- Changing Create Agent discovery, profiling, or prompt behavior.
- Requiring query-history access for installation or optimization.
- Profiling every column in the complete UC inventory.
- Sending raw historical SQL to an LLM.
- Persisting raw historical SQL or user identities.
- Harvesting Genie conversation history. Existing Genie configuration,
  instructions, sample questions, and benchmarks remain ranking inputs, but
  conversation collection is not part of this design.
- Replacing GenieWatch's system-table behavior.
- Inferring value distributions for metric-view measures. Measures are retained
  as typed metadata; only dimensions receive value profiles in this version.

## 4. Definitions and hard invariants

### 4.1 Representations

GSO maintains three separate representations. They must not be collapsed into
one list.

1. **Full inventory**: every column for every eligible UC asset, including its
   canonical asset identifier, name, type, description, constraint roles, and
   metric-view role. The full inventory is used by deterministic validation and
   local ranking only. It is never serialized wholesale into an LLM request.
2. **Active working set**: columns currently available to profiling, benchmark
   context, or optimization context. An asset has at most 50 active columns.
   Plan revisions may replace active columns.
3. **Cumulative value-profiled set**: distinct columns for which the run has
   submitted value-profile SQL. Once a column enters this set it remains in the
   cumulative count even if a later plan revision removes it from the active
   working set. An asset has at most 50 cumulatively value-profiled columns.

An omitted column remains valid and available to deterministic validation. It
is not assumed to be missing from the asset.

### 4.2 Default budgets

| Setting | Default | Hard behavior |
|---|---:|---|
| Maximum active columns per asset | 50 | Never exceeded by a plan revision |
| Maximum cumulative value-profiled columns per asset | 50 | Never exceeded during the run |
| Initial active target | 45 | Leaves adaptive capacity when direct requirements permit |
| Adaptive reserve | 5 | Used for columns discovered after initial profiling |
| Exploration target | 5 | Included within the initial active target, not added to it |
| Minimum initial target for a non-empty asset | 8 | Limited by inventory size |
| Maximum eligible assets | 20 | Existing GSO profiling limit |
| Maximum aggregate expressions per statement | 10 | Includes cardinality, minimum, and maximum expressions |
| Maximum value-list columns per asset | 10 | Prevents unbounded follow-up queries |
| Concurrent profiling statements | 3 | Run-wide |
| Maximum profiling statements per asset | 30 | Includes row count and value-list queries |
| Maximum profiling statements per run | 600 | 20 assets × 30 statements |
| Profiling stage wall-clock deadline | 30 minutes | Remaining work becomes `metadata_only` |
| Statement execution deadline | 50 seconds | Timed-out statements are cancelled |
| Maximum complete LLM request size | 60,000 characters | System and user messages combined |

The column budgets and prompt budget are independent. Selecting 50 columns for
each of 20 assets does not authorize a 1,000-column prompt if the serialized
request would exceed 60,000 characters.

### 4.3 Initial allocation

For each asset:

1. Add directly required columns in deterministic priority order.
2. If fewer than 40 columns are selected, add observed-use, semantic, and
   structural columns until reaching 40 or exhausting candidates.
3. Add up to five exploration columns, without exceeding an initial total of
   45.
4. If directly required columns alone exceed 45, they may consume some or all
   of the five adaptive slots, up to the hard maximum of 50.
5. If more than 50 directly required columns exist, retain the top 50 and
   record `required_overflow_count`. Overflow columns remain valid in the full
   inventory and can be activated by a later operation-specific revision.
6. Fill to the minimum target of eight when the inventory contains at least
   eight columns.

## 5. Durable state contracts

The existing `genie_opt_artifacts` table remains the durable notebook handoff.
Implementation must add the following artifact kinds to the authoritative
artifact-kind list.

### 5.1 `wide_schema_inventory`

Required, immutable, and written once by Notebook 1.

```json
{
  "contract_version": 1,
  "captured_at": "timestamp",
  "inventory_hash": "sha256",
  "assets": [
    {
      "asset_key": ["catalog", "schema", "asset"],
      "asset_id": "`catalog`.`schema`.`asset`",
      "asset_type": "table",
      "columns": [
        {
          "column_key": ["catalog", "schema", "asset", "column"],
          "column_id": "`catalog`.`schema`.`asset`.`column`",
          "name": "column",
          "data_type": "string",
          "description": "...",
          "constraint_roles": ["primary_key"],
          "metric_role": "dimension",
          "ordinal": 1
        }
      ]
    }
  ]
}
```

Allowed asset types are `table`, `view`, and `metric_view`. Allowed constraint
roles are `primary_key`, `foreign_key`, and `join_key`. Metric role is
`dimension`, `measure`, or null.

Notebook 1 must verify that the artifact can be read back. Failure to persist a
valid inventory is fatal because later deterministic validation depends on it.
The rollback `config_snapshot` remains separate and must not be repurposed as
the inventory artifact.

### 5.2 `wide_schema_evidence`

Best-effort and written by Notebook 1. It contains normalized aggregate
evidence only:

- canonical asset and column IDs;
- reason codes;
- distinct-query counts by SQL role and recency bucket;
- last-used timestamp;
- source mode and source scope;
- query-history coverage and degradation counts; and
- hashes of normalized query shapes when needed for deduplication.

It must not contain raw SQL, literals, usernames, user IDs, or tokens.

### 5.3 `wide_schema_selection_plan`

Required and append-only. Notebook 2 writes revision 1; later notebooks append
new revisions rather than overwriting prior plans.

Each revision contains:

- `contract_version` and `selector_version`;
- `run_id`, `revision`, `parent_plan_hash`, and `inventory_hash`;
- source mode and evidence coverage;
- canonical asset and column IDs;
- stable rank, priority, evidence score, and reason codes;
- active status and the reason for activation or eviction;
- cumulative value-profiled count;
- profile status and available metrics;
- selected, omitted, overflow, and metadata-only counts.

Allowed profile statuses are:

- `pending`
- `profiled`
- `partial`
- `timed_out`
- `metadata_only`
- `not_selected`

Every required plan write must be read back and hash-verified before the next
notebook consumes it.

Plan changes are state transitions. Activation or eviction is one appended
revision; completion, timeout, or failure of the resulting profile work is a
subsequent appended revision. Existing revisions are never updated in place.

### 5.4 Existing `space_metadata` artifact

`space_metadata` must no longer copy the complete UC column inventory. It may
contain the active prompt-matching projection, profile summaries, RLS verdicts,
asset semantics, and references to the inventory and plan hashes. Code that
needs deterministic validation must load `wide_schema_inventory` directly.

## 6. End-to-end flow

```text
Full UC inventory -------------------------------> deterministic validation
       |
       + Genie configuration and SQL
       + existing benchmark SQL
       + optional query-history aggregates
       + metadata and structural heuristics
       |
       v
Deterministic ranked plan, revision 1
       |
       +--> active working set, normally <=45 initially
       |       |
       |       +--> bounded value profiling
       |       +--> bounded benchmark prompts
       |       +--> bounded optimization prompts
       |
       +--> adaptive slots for newly required columns
                   |
                   v
             append plan revision
```

Benchmark repair and optimization failures are not inputs to the initial plan
because they do not yet exist. They create later plan revisions.

## 7. Evidence collection

### 7.1 Always-available evidence

Notebook 1 extracts locally from the Genie configuration and existing
benchmarks:

- SQL snippets and expressions;
- filters and join specifications;
- existing benchmark SQL;
- metric-view measures and dimensions explicitly referenced by the Space;
- sample questions, title, description, and instructions;
- primary keys, foreign keys, and configured join keys; and
- UC names, descriptions, types, ordinals, and metric-view roles.

Raw configuration text stays in the normal Genie snapshot. Only normalized
column evidence is copied to `wide_schema_evidence`.

### 7.2 Query-history source hierarchy

GSO uses at most one query-history source per run.

1. Probe `system.query.history` using the GSO service principal.
2. If the probe and a bounded read succeed, use `system_table` exclusively.
3. Otherwise probe the Query History REST API for the workload warehouse IDs
   supplied with the optimization request.
4. Use every accessible configured workload warehouse and record inaccessible
   warehouses.
5. If neither source yields usable history, record `none` and continue.

The source mode is one of:

- `system_table`
- `warehouse_api`
- `none`

Required optional grants for the system-table path are:

```sql
GRANT USE CATALOG ON CATALOG system TO `<gso-service-principal>`;
GRANT USE SCHEMA ON SCHEMA system.query TO `<gso-service-principal>`;
GRANT SELECT ON TABLE system.query.history TO `<gso-service-principal>`;
```

The REST path uses `CAN VIEW` on each configured workload warehouse. Existing
`CAN MONITOR`, `CAN MANAGE`, or ownership permissions also satisfy access.
These permissions remain optional and are never installation prerequisites.

The authoritative warehouse list for a run is
`run_manifest.workload_warehouse_ids`. The Optimize request supplies this list;
it is not inferred from the profiling warehouse and is not an `app.yaml`
resource.

### 7.3 Query-history collection bounds

| Setting | Default |
|---|---:|
| Lookback | 30 days |
| Maximum system-table statements | 10,000 |
| Maximum REST statements | 5,000 |
| Maximum accepted statement size | 256 KiB |
| Maximum raw SQL processed per run | 50 MiB |
| Maximum history parsing time | 120 seconds |

Statements are processed newest first. Collection stops when any applicable
count, byte, or time limit is reached. Oversized or unparsed statements are
skipped and counted; they are not truncated and reparsed as potentially invalid
SQL.

Source-side filters retain only successful SQL in the lookback window and, for
the REST path, configured workload warehouses. Where supported, source-side
filters also exclude the GSO service principal.

### 7.4 GSO traffic exclusion

Client-side filtering excludes a statement when any of the following apply:

- `query_source.job_info.job_id == GSO_JOB_ID`;
- query tags identify Genie Workbench or GSO;
- the executing identity is the GSO service principal; or
- a versioned fingerprint matches a known legacy GSO profiling or validation
  query shape.

Every GSO-controlled Statement Execution request must carry these tags:

- `application=genie_workbench`
- `component=gso`
- `purpose=profiling|benchmark_validation|optimization|history_collection`
- `run_id=<run-id>`

Tags are the preferred exclusion mechanism for new traffic. Job ID, SP
identity, and fingerprints cover historical or untagged traffic.

### 7.5 Canonical SQL resolution

SQL is parsed with the pinned Databricks dialect of `sqlglot`.

- Identity comparisons use normalized component tuples, never a joined string
  split on `.`. An asset key is `(catalog, schema, asset)` and a column key is
  `(catalog, schema, asset, column)`.
- Display IDs render every component with escaped backticks. A literal dot
  inside a component therefore cannot collide with a multipart name.
- Quoted identifiers retain their semantic component value and are
  case-normalized using the same function as UC inventory construction.
- Fully qualified assets must match the inventory exactly.
- A partially qualified asset is resolved only when exactly one Space asset
  matches after applying source-provided catalog or schema context.
- An unqualified asset is resolved only when exactly one Space asset has that
  leaf name.
- Aliased columns are attributed through the resolved alias target.
- An unqualified column is attributed only when exactly one resolved base asset
  contains that column.
- Ambiguous asset or column evidence is discarded at column level and counted.
  It may contribute table-level usage only when the table itself is unambiguous.
- CTE output columns are traced to base columns when the AST provides an
  unambiguous lineage path.
- `SELECT *` contributes table-level evidence only. It never selects all
  columns.

Query shapes are deduplicated using a SHA-256 hash of normalized AST SQL with
literals replaced. The hash may be persisted; the normalized or original SQL
may not.

## 8. Deterministic ranking

The selector implementation is versioned as `wide_schema_selector_v1`.

### 8.1 Priority order

Each column may have multiple reason codes. Its primary priority is the lowest
number below.

| Priority | Evidence | Reason codes |
|---:|---|---|
| 0 | Active benchmark failure or repair target | `REPAIR_FAILURE` |
| 1 | Existing benchmark SQL | `BENCHMARK_SQL` |
| 2 | Genie SQL, expression, filter, join, key, user pin, or explicitly used metric field | `CONFIG_SQL`, `JOIN_KEY`, `USER_PIN`, `METRIC_FIELD` |
| 3 | Human query-history evidence | `QUERY_HISTORY` |
| 4 | Local semantic match | `SEMANTIC_MATCH` |
| 5 | Type and structural coverage | `STRUCTURAL_COVERAGE` |
| 6 | Reserved exploration | `EXPLORATION` |

Within a priority, columns sort by descending evidence score and then ascending
canonical column ID. No iteration over an unordered set or map may affect rank.

### 8.2 Query-history score

For each distinct normalized query shape referencing a column, take the highest
applicable SQL-role weight:

| SQL role | Weight |
|---|---:|
| Join predicate | 6 |
| Filter predicate | 5 |
| Grouping | 4 |
| Aggregate argument | 4 |
| Ordering | 2 |
| Projection | 1 |

Multiply that weight by the recency multiplier and sum across distinct query
shapes:

| Age | Multiplier |
|---|---:|
| 0–7 days | 1.00 |
| 8–14 days | 0.50 |
| 15–30 days | 0.25 |

One query contributes at most one weighted score per column. Automated GSO
traffic contributes zero.

### 8.3 Semantic score

Semantic ranking is local and does not call an LLM. Names and text are
lowercased, snake/camel-case split, punctuation-normalized, and stripped of a
versioned stop-word list.

For each column:

- add 8 when its complete normalized name appears in Space title,
  description, instructions, or benchmark questions;
- add 2 per distinct column-name token appearing in those Space texts, capped
  at 8;
- add 1 per distinct overlap between column-description tokens and Space-text
  tokens, capped at 4; and
- add 2 when the data type and name match a versioned business concept rule
  such as date, amount, quantity, status, geography, category, or identifier.

### 8.4 Structural coverage and exploration

Structural scoring prefers, in order:

1. remaining entity identifiers;
2. dates and timestamps;
3. numeric measures;
4. categorical dimensions; and
5. other scalar columns.

Exploration fills missing type categories first. Remaining exploration ties
sort by SHA-256 of `run_id + canonical_column_id`, giving deterministic results
within a run while allowing later runs to explore different columns.

## 9. Adaptive plan revisions

A new benchmark, repaired SQL statement, or optimization failure may reference
an omitted but valid inventory column.

1. Validate the reference against the full inventory.
2. Append a plan revision activating the required column.
3. If the active set is full, evict the lowest-ranked active column that is not
   required by the current operation, not user-pinned, and not an active join
   key.
4. If the cumulative value-profiled set contains fewer than 50 columns, submit
   a bounded profile for the promoted column and add it to the cumulative set.
5. If the cumulative value-profiled set already contains 50 columns, activate
   the promoted column as `metadata_only`. Do not submit profile SQL for it.
6. Never decrement the cumulative value-profiled count when an active column is
   evicted.
7. Append a second revision after profiling records the resulting `profiled`,
   `partial`, or `timed_out` status.

This rule guarantees both active and cumulative limits. Rebalancing never means
that a 51st distinct column may be value-profiled.

## 10. Profiling execution

### 10.1 Regular tables and views

Profiling uses the configured GSO SQL warehouse through Statement Execution.
There is no automatic Spark fallback for value profiling.

For each selected column, construct the applicable expressions:

- `approx_count_distinct(column)` for scalar columns;
- `min(column)` and `max(column)` for numeric and date/time columns; and
- no value metrics for unsupported complex types.

Pack whole-column metric groups into statements with at most ten aggregate
expressions. A column's expressions stay together unless that single column
would independently exceed the limit.

Every profiling subquery must explicitly project only the columns referenced by
that statement. `SELECT *` is forbidden in profiling SQL.

Example:

```sql
SELECT
  approx_count_distinct(`status`) AS `status__cardinality`,
  approx_count_distinct(`amount`) AS `amount__cardinality`,
  min(`amount`) AS `amount__min`,
  max(`amount`) AS `amount__max`
FROM (
  SELECT `status`, `amount`
  FROM `catalog`.`schema`.`table` TABLESAMPLE (100 ROWS)
)
```

Row count is a separate best-effort statement. After cardinality results are
available, at most ten selected low-cardinality string columns per asset may
receive a bounded value-list query. Returned values remain subject to the
existing low-cardinality threshold and output-size limits.

If `TABLESAMPLE` is unsupported for a regular view, GSO may retry in the same
warehouse with an explicit selected-column projection and `LIMIT 100`. This
shape fallback counts against statement budgets and is not allowed after a
timeout.

### 10.2 Metric views

- Selected dimensions use metric-view-legal `GROUP BY` profiling.
- Dimension queries obey the same statement deadline and run-wide concurrency
  controls.
- Selected measures retain name, type, description, and measure expression but
  receive `metadata_only` status. GSO does not calculate generic measure
  cardinality, minimum, maximum, or distinct values.
- A metric view with no resolvable dimensions remains valid and continues with
  metadata-only context.

### 10.3 Timeout and retry semantics

Statement Execution uses a maximum 50-second wait. If the accepted statement is
still pending or running at the deadline:

1. call the Statement Execution cancellation API;
2. record `timed_out` for the affected metrics;
3. do not submit the same statement through Spark or another execution path;
4. split the group once and retry only when the run and asset statement budgets
   still allow it; and
5. after the single split retry, mark remaining metrics unavailable.

Submission, authorization, or warehouse-availability failures do not trigger
unbounded fallback. The asset continues as `metadata_only` or `partial`.

### 10.4 Run scheduling

Profiling groups are scheduled round-robin across assets so the run deadline
does not consistently starve later assets. No more than three statements run
concurrently.

When the asset statement limit, run statement limit, or 30-minute stage deadline
is reached, GSO stops submitting work, cancels accepted in-flight profiling
statements where appropriate, and marks remaining selected columns
`metadata_only`. The optimization run continues.

## 11. Prompt construction

### 11.1 Global request budget

Every GSO LLM call must use the shared wide-schema prompt packer. The final
serialized `messages` array, including system prompts, templates, failure
payloads, and user context, must be at most 60,000 characters.

The packer must be used by:

- benchmark generation;
- curated-question SQL generation;
- benchmark correction and repair;
- benchmark semantic quality review;
- optimization patch generation; and
- any prompt-matching or description-generation call that includes schema.

No call may rely only on the per-asset column cap.

### 11.2 Prompt projection

- Include asset identifiers and types before column detail.
- Include current repair or optimization targets first.
- Include active columns in stable rank order, distributed round-robin across
  relevant assets.
- Truncate asset descriptions to 1,000 characters and selected column
  descriptions to 300 characters before packing.
- Include profile metrics only for active columns.
- Include an omitted-context summary containing counts, never the full omitted
  name list.
- Never serialize `wide_schema_inventory` directly.
- Build valid structured JSON; never enforce the limit by slicing serialized
  JSON or SQL at an arbitrary character boundary.

If required context still cannot fit, batch the operation by benchmark or
connected asset group. Each batch independently obeys the request limit.
Cross-asset batches retain the join specifications connecting their assets.

The packer records final request size, included counts, omitted counts, plan
hash, and inventory hash for every call.

## 12. Deterministic validation

All SQL and configuration validation loads the complete
`wide_schema_inventory`, not the active prompt projection.

- An omitted inventory column is valid.
- A column absent from the inventory is invalid even if an LLM proposes it.
- Table, view, and metric-view roles are checked against the inventory.
- SQL validation may activate a valid omitted column through a plan revision,
  but activation is not required merely to establish validity.
- Prompt allowlists and deterministic validation allowlists are separate
  objects with different contracts.

## 13. Four-notebook integration

### Notebook 1: intake and snapshot

- Preserve the original rollback snapshot.
- Load and canonicalize the complete UC inventory.
- Persist and verify `wide_schema_inventory`.
- Probe one query-history source.
- Harvest bounded query history and normalize it in memory.
- Extract configuration, benchmark, metadata, key, and metric-role evidence.
- Persist `wide_schema_evidence` without raw SQL or identities.
- Record source capability and degradation counts.

### Notebook 2: benchmark QC and repair

- Load and verify the inventory artifact. Load evidence when present; missing
  optional evidence becomes an empty evidence set rather than a failure.
- Build and persist selection-plan revision 1.
- Profile the initial working set within cumulative, statement, and time
  budgets.
- Append and verify a profiling-outcome revision before using the plan for
  benchmark prompts.
- Use the shared prompt packer for benchmark generation and quality review.
- Validate all SQL against the full inventory.
- When validation or repair identifies an omitted valid column, append a plan
  revision before constructing the repair prompt.
- Profile promoted columns only when cumulative capacity remains.
- Persist bounded `space_metadata` referencing inventory and plan hashes.

### Notebook 3: optimize

- Load and verify the full inventory and latest selection plan.
- Reuse the bounded active projection and profile context.
- Apply the shared 60,000-character request limit.
- Validate patches and SQL against the full inventory.
- Append operation-specific plan revisions when a failure requires an omitted
  valid column.
- Never value-profile a 51st distinct column for an asset.

### Notebook 4: publish and audit

- Read the latest plan and all plan revisions.
- Record source mode, coverage, selected counts, cumulative profiled counts,
  metadata-only counts, reason distributions, ambiguity counts, prompt sizes,
  statement counts, cancellations, retries, and timeouts.
- Do not publish or persist raw historical SQL or user identities.

## 14. Optional permission experience

Optimize exposes an optional **Query usage signal** section.

The UI shows:

- GSO service-principal identifier;
- `System query history available`;
- `Warehouse query history available`;
- `Partially available`, including inaccessible configured warehouse names; or
- `Query history unavailable`.

For the system-table option, the UI provides copyable UC grants. For the REST
fallback, the user selects representative workload warehouses and receives
copyable `CAN VIEW` instructions.

The application never grants warehouse permissions automatically. A grant
operation requires an explicit user action and the acting principal must
already be authorized to manage that warehouse.

Missing permissions never block installation or optimization.

## 15. Privacy and retention

- Raw query text is processed in memory only.
- Raw query text is never sent to an LLM, persisted, or logged.
- Query literals and comments are removed before query-shape hashing.
- Usernames and user IDs are used only for in-memory SP exclusion and are not
  persisted or logged.
- Persisted evidence contains only canonical asset and column IDs, aggregate
  role counts, recency buckets, last-used timestamps, source scope, and query
  shape hashes.
- Reusable aggregate caches have a maximum 24-hour TTL and use the same evidence
  contract.
- Inventory descriptions remain UC metadata and are stored only in the
  inventory artifact; prompt projections apply field-size limits.

## 16. Failure behavior

### 16.1 Fatal

- The complete UC inventory cannot be collected.
- The required inventory artifact cannot be written, read back, or hash
  verified.
- Selection-plan revision 1 cannot be persisted or verified.
- A downstream notebook detects an inventory-hash mismatch.

### 16.2 Non-fatal degradation

- Missing `system.query.history` grants.
- Missing `CAN VIEW` on one or all configured workload warehouses.
- Query History API authorization or availability errors.
- Empty query history.
- Oversized, ambiguous, or individually unparseable historical statements.
- History count, byte, or parsing-time limits.
- Profiling submission failures or individual statement timeouts.
- Profiling asset, run, or wall-clock budget exhaustion.
- Missing metric-view dimensions.
- An adaptively activated column that must remain metadata-only because the
  cumulative profile budget is exhausted.

GSO records degradation and continues using remaining evidence and deterministic
validation.

## 17. Acceptance criteria

### Inventory and selection

- A 5,000-column asset produces a complete inventory artifact.
- Its active working set never exceeds 50 columns.
- Its cumulative value-profiled set never exceeds 50 columns.
- Its initial active set does not exceed 45 unless directly required columns
  consume adaptive capacity.
- A repair discovered after 50 columns were already profiled activates the new
  column as metadata-only and does not submit profile SQL for it.
- Evicting an active column does not decrement cumulative profiled count.
- Omitted inventory columns remain valid during SQL and patch validation.

### SQL execution

- No profiling SQL contains `SELECT *`.
- No regular profiling statement contains more than ten aggregate expressions.
- No asset submits more than 30 profiling statements.
- No run submits more than 600 profiling statements or continues submissions
  after the 30-minute profiling deadline.
- A timed-out accepted warehouse statement is cancelled and is not retried via
  Spark.
- A failed group is split at most once.
- Partial profiling does not fail the optimization run.

### Prompt construction

- Every complete LLM `messages` payload is at most 60,000 characters.
- A test with 20 assets, 50 active columns per asset, maximum-length names, and
  long descriptions remains within the request limit through deterministic
  packing or batching.
- The full inventory is never serialized into an LLM request.
- Current repair and optimization targets outrank generic metadata context.
- Prompt omission does not alter deterministic validation results.

### Ranking and history

- Ranking is reproducible for identical inputs, selector version, and run ID.
- Config, benchmark, join, user-pinned, and repair columns outrank generic
  semantic matches.
- Frequently used human-query columns outrank unrelated columns within the
  query-history priority.
- Ambiguous unqualified references never attach column evidence to an arbitrary
  asset.
- `SELECT *` never marks every column as used.
- GSO-generated profiling and validation traffic contributes no score.
- Both history sources produce the same normalized evidence contract.
- Missing history permissions do not fail installation or optimization.

### Asset and privacy coverage

- Wide table, view, and metric-view tests cover selection, profiling, timeout,
  prompt packing, validation, and adaptive revisions.
- Metric-view measures remain metadata-only and dimensions use legal queries.
- Raw query text, literals, usernames, and user IDs are absent from artifacts,
  logs, MLflow prompt inputs, and LLM requests.

## 18. Rollout

1. Add the three artifact contracts, deterministic selector without query
   history, initial/adaptive budget enforcement, bounded profiling SQL,
   cancellation semantics, global prompt packing, and full-inventory
   validation. This phase resolves the wide-schema timeout problem without new
   optional permissions.
2. Add `system.query.history`, AST normalization, canonical attribution, query
   tags, and GSO traffic exclusion.
3. Add the Query History REST fallback, workload-warehouse request settings,
   and optional permission UI.
4. Add 24-hour aggregate caching and publish/audit UI for selection-plan and
   profiling telemetry.

Every phase preserves the hard SQL, cumulative profiling, and LLM-request
bounds established in phase 1.
