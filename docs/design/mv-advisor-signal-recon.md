# Metric view advisor — Prompt 6 signal recon

**Scope: recon only.** No code was changed, no existing document was edited, nothing
was committed. Measured against HEAD `ee1327ad` with a clean working tree.

**Evidence standard.** Every claim carries `file:line` for code, read fresh in this
session. Every fenced block is a verbatim quote. Every *absence* claim states the
search that was run and a positive control — a search for a symbol known to exist,
issued from the same working directory, proving the search would have found a hit.
The repo-root positive control for all of them is:

```
$ ls databricks.yml
databricks.yml  9.9K
```

**On document line numbers.** Playbook claims are cited by MV-D identifier and prompt
heading rather than line number, so this file does not become a new MV-D9 staleness
surface. POV and code citations use line numbers.

---

## Summary table

| Q | Signal / input | Verdict |
|---|---|---|
| Q1 | Per-question generated SQL corpus | **PRESENT AND PERSISTED.** `genie_opt_iterations.rows_json`, key `generated_sql`, iteration 0 included. One caveat: empty on a failed eval-run. |
| Q2 | **L** — lineage overlap | **NO PRODUCER.** Type exists as an input contract. Column-level lineage needs a new system-table grant, so it is *not* a code-only change. |
| Q3 | **D** — demand | **PARTIAL.** Frequency exists at the wrong grain. Cost and distinct-user counts do not exist at all and need a new query. |
| Q4 | **S** — semantic | **DEGRADES SILENTLY TO 0.0.** By explicit design and pinned by tests — but with no reason an operator can read afterwards. `preflight_embedding_endpoint` has zero callers. |
| Q5 | The leakage oracle | **VACUOUS TODAY, CODE-ONLY TO FIX.** The corpus is already loaded from Delta and already wrapped as a `BenchmarkCorpus` in the optimize path. |
| Q6 | Artifact kind + `content_hash` | **ADDING A KIND IS CODE-ONLY.** But `write_artifact` cannot accept a `content_hash`, so MV-D7's cross-reference does **not** hold today. |
| Q7 | Widget / parameter ordering | **SILENT DEFAULT, NOT A RAISE.** Prompt 8 does **not** need to move ahead of Prompt 6. |

Three claims in committed code and DDL are contradicted by this recon; they are
collected in [Findings that contradict committed text](#findings-that-contradict-committed-text)
rather than fixed here.

---

## Q1 — THE CORPUS

**Verdict: yes, per-question generated SQL is persisted to Delta, keyed by `run_id`.**
Prompt 6's premise holds. It is *not* an artifact and *not* a dedicated column — it is
a key inside a JSON blob column.

**Table and column.** `genie_opt_iterations.rows_json`, keyed by `run_id` +
`iteration`:

```90:90:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
    rows_json           STRING                 COMMENT 'JSON: per-question evaluation detail rows',
```

**Producer.** The official eval runner extracts the model's SQL from the eval detail
and writes it into the per-question row under two keys — the nested `response` shape
and a flat alias:

```419:419:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py
    actual_sql = _first_response_text(getattr(detail, "actual_response", None))
```

```454:456:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py
        "outputs/response": actual_sql,
        "inputs/expected_response": expected_sql,
        "generated_sql": actual_sql,
```

**It survives the run.** The writer strips exactly two keys, neither of them the SQL:

```866:866:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py
        _STRIP_COLS = {"trace", "trace_id"}
```

and inserts the remainder as `rows_json`:

```933:933:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py
        "thresholds_met, rows_json, reflection_json, "
```

Unlike `genie_opt_artifacts.artifact_json`, `rows_json` is **not** base64-encoded — it
is plain escaped JSON (`_opt_json`, `state.py:872-875`) and is read back with
`json.loads` (`state.py:1638-1642`). A reader needs no decode step.

**Iterations covered, including iteration 0.** There are exactly two `write_iteration`
call sites in the package, and the first is the baseline:

```
$ rg -n "write_iteration\(" packages/genie-space-optimizer/src/genie_space_optimizer/
.../optimization/unified_loop.py:2886:    write_iteration(
.../optimization/unified_loop.py:3323:        write_iteration(
.../optimization/state.py:822:def write_iteration(
```

Iteration 0's eval is produced immediately above that first call:

```2854:2854:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py
    baseline_eval = _native_eval(
```

So iteration 0 is covered, and so is every later full eval.

**Readers Prompt 6 can reuse** rather than re-implementing a query — all four already
`json.loads` the `rows_json` column: `load_iterations` (`state.py:1562`),
`load_latest_full_iteration` (`state.py:1601`), `load_all_full_iterations`
(`state.py:1692`), `load_all_scored_iterations` (`state.py:1721`). `publish.py:424`
is a working precedent for consuming the rows.

**The one caveat, and it matters.** On a non-success terminal eval status the runner
returns an *empty* row list, and an iteration row is still written:

```595:596:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py
        rows: list[dict] = []
        if status == _SUCCESS_STATUS:
```

So "an iteration-0 row exists for this `run_id`" does not imply "a corpus exists."
Prompt 6 must treat an empty or SQL-less `rows_json` as a first-class skip with a
recorded reason, not as an empty result set that scores to nothing.

**Smallest change to persist it:** none. Persistence already exists. The only
adjustment Prompt 6's own text needs is to read the corpus through one of the four
existing loaders keyed by `run_id`, which satisfies its "from Delta, NOT from
in-memory state" constraint directly.

---

## Q2 — L (lineage overlap)

**Verdict: nothing in GSO produces `LineageOverlap`. The type exists purely as an
input contract.** And the cheapest credible producer is *not* a code-only change,
because column-level lineage is not granted to the service principal.

**The type and its scorer exist.** `LineageOverlap` is defined at
`mv_scoring.py:150`, defaulted into the input bundle at `:368`, consumed by
`lineage_overlap_score` at `:388`, and exported at `:1101`. The module states plainly
that it does not compute the data:

```153:153:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/mv_scoring.py
    Both sets are supplied by the caller from ``column_lineage``; this module
```

**Absence of a producer.** Every construction of `LineageOverlap` outside the module's
own default is in tests:

```
$ rg -n "LineageOverlap" --glob "*.py" .
./packages/.../tests/unit/test_mv_scoring.py:25,162,228,239,427,732,891
./packages/.../optimization/mv_scoring.py:48,150,368,388,1101
```

Seven test sites, five definition/wiring sites, zero production callers.

**Absence of column-level lineage anywhere in the repo.** `column_lineage` appears
exactly once in Python, and it is the docstring quoted above — not a query:

```
$ rg -n "column_lineage" --glob "*.py" .
./packages/.../optimization/mv_scoring.py:153:    Both sets are supplied by the caller from ``column_lineage``; this module
```

Positive control, same cwd, same search family — table-level lineage *is* found, so
the search was sound:

```
$ rg -c "table_lineage" --glob "*.py" .
./backend/services/create_agent_tools.py:2
./backend/watch/services/system_tables.py:6
./scripts/deploy_lib/uc.py:1
```

**Does GenieWatch's reader generalize?** Partly. It is the right SQL and the wrong
module. `backend/watch/services/system_tables.py` reads table-level lineage
(`_EXECUTED_RESOURCES_SQL` at `:746`, plus rollup/spaces/graph variants at `:767`,
`:789`, `:806`), which is the closest thing to a producer that exists.

**SP-only by design, not by accident.** The module says so in its own header, and the
identity is hard-wired rather than resolved:

```9:11:backend/watch/services/system_tables.py
All queries run as the *service principal* — system tables are not OBO-readable.
SP must hold `USE CATALOG system`, `USE SCHEMA system.{query,billing,access}`,
and SELECT on each table above. The deploy script grants these automatically.
```

```115:116:backend/watch/services/system_tables.py
def _client() -> WorkspaceClient:
    return get_service_principal_client()
```

There is no OBO path in the file — absence check, with the SP helper as its own
positive control:

```
$ rg -n "get_workspace_client" backend/watch/services/system_tables.py
# exit 1, no hits
$ rg -n "get_service_principal_client" backend/watch/services/system_tables.py
28:from backend.services.auth import get_service_principal_client
116:    return get_service_principal_client()
```

**It is not importable from the job.** Line 28 above ties the module to
`backend.services.auth`, i.e. to the Apps backend package, and it additionally
requires the app's `SQL_WAREHOUSE_ID` env var (`:108-112`). The job should reuse the
*SQL pattern*, not the module. The job already has both an SP-identity client
(`_workspace_client.make_workspace_client`) and a warehouse execution helper
(`common/warehouse.sql_warehouse_query`), so an in-job lineage read is a
straightforward new query on existing rails.

**Cost of an in-job read.** Cheap relative to the exact-count probe MV-D14's marker
worries about: `system.access.table_lineage` filtered by `entity_metadata.genie_space_id`
and an `event_time` window, aggregated by source table — a bounded scan of a system
table, not a scan of customer fact tables. Table grain is affordable.

**Column grain is the blocker, and it is a grant, not code.** The SP grant list does
not include `column_lineage`:

```30:43:scripts/deploy_lib/uc.py
WATCH_SYSTEM_GRANTS: list[tuple[str, str, str]] = [
    # (securable_type, fully_qualified_name, privilege)
    ("CATALOG", "system",                         "USE_CATALOG"),
    ("SCHEMA",  "system.query",                   "USE_SCHEMA"),
    ("SCHEMA",  "system.billing",                 "USE_SCHEMA"),
    ("SCHEMA",  "system.access",                  "USE_SCHEMA"),
    ("TABLE",   "system.query.history",           "SELECT"),
    ("TABLE",   "system.billing.usage",           "SELECT"),
    ("TABLE",   "system.billing.list_prices",     "SELECT"),
    ("TABLE",   "system.access.audit",            "SELECT"),
    ("TABLE",   "system.access.table_lineage",    "SELECT"),
    # workspaces_latest is optional / newer; absence is handled in code.
    ("TABLE",   "system.access.workspaces_latest", "SELECT"),
]
```

So the honest statement is: **table-level overlap is available today; column-level
overlap — which is what `LineageOverlap`'s docstring describes and what a column-set
Jaccard requires — needs a new grant on an existing deployment.** That crosses out of
code-only and into deploy territory, and existing installs would need re-granting.

**The POV Part 5 conflict is real.** The POV requires OBO computation and a
per-user visibility guarantee:

```266:266:docs/design/metric-view-suggestion-engine-pov.md
**Permission boundaries.** Compute suggestions under **OBO** using the `X-Forwarded-Access-Token` header with least-privilege scopes. Because Genie and metric views resolve data access via each end user's own Unity Catalog permissions, row filters and column masks are enforced per user at query time; the advisor must never surface a suggestion referencing a securable the viewing user cannot BROWSE or SELECT. Lineage graphs share the UC permission model, so lineage-derived evidence is already scoped.
```

and notes that lineage reads need elevated or per-user-scoped access:

```48:48:docs/design/metric-view-suggestion-engine-pov.md
| `system.access.table_lineage` / `system.access.column_lineage` | System table | `source_table_full_name`, `source_type` (incl. `METRIC_VIEW`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`), `entity_metadata.genie_space_id` | **GA** | Admin, or per-user dynamic view; BROWSE/SELECT on objects | Lineage system tables retain a rolling 1-year window; Catalog Explorer/lineage API retain indefinitely after 2024-09-01 |
```

An in-job lineage read is SP-scoped by construction, so the final sentence of POV:266
— "lineage-derived evidence is already scoped" — is false for in-job computation. The
SP sees lineage for tables a given viewer may not be able to BROWSE, and a candidate
scored on that evidence is later shown to that viewer.

This is resolvable without reopening MV-D1, and the existing architecture points at
the resolution: MV-D1 already routes creation through the backend under OBO, so the
job can compute SP-scoped evidence and the **presentation** path can filter per user
at read time. That keeps the POV's actual guarantee (never *surface* an unviewable
securable) while dropping its incidental claim (that scoping comes for free). Prompt 6
should state which of the two it is honouring, because computing under SP and
presenting unfiltered would violate POV:266 outright.

---

## Q3 — D (demand)

**Verdict: `wide_schema_history` gives frequency at the wrong grain, and gives neither
cost nor distinct users. A new query is required.**

**What it actually aggregates.** The SQL does no aggregation at all — it selects raw
finished `SELECT` statements and aggregates in Python on the driver:

```238:257:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/wide_schema_history.py
    return f"""
        SELECT
          statement_id,
          statement_text,
          statement_type,
          start_time,
          executed_by,
          executed_as,
          query_source,
          query_tags
        FROM system.query.history
        WHERE start_time >= current_timestamp() - INTERVAL {int(lookback_days)} DAYS
          AND execution_status = 'FINISHED'
          AND statement_type = 'SELECT'
          AND workspace_id = {int(workspace_id)}
          AND ({' OR '.join(relevance)})
          AND {' AND '.join(exclusions)}
        ORDER BY {order_by}
        LIMIT {MAX_SYSTEM_STATEMENTS}
    """
```

Those eight columns are the entire input. Its Python output is per *column*, keyed by
`column_key`, carrying `evidence_score`, `distinct_query_counts` (role × recency
buckets), `query_occurrence_count`, `last_used_timestamp` and `query_shape_hashes`.

**Retention window.** 30 days by default, widening to 90 when the corpus is too thin,
capped at 10 000 statements:

```28:31:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/wide_schema_history.py
HISTORY_LOOKBACK_DAYS = 30
EXTENDED_HISTORY_LOOKBACK_DAYS = 90
MIN_USEFUL_QUERY_SHAPES = 20
MAX_SYSTEM_STATEMENTS = 10_000
```

**Cost and distinct users are absent.** Absence check with a positive control from the
same file:

```
$ rg -n "total_duration_ms|total_task_duration_ms|COUNT\(DISTINCT|billing" \
    packages/.../optimization/wide_schema_history.py
# exit 1, no hits
$ rg -n "HISTORY_LOOKBACK_DAYS" packages/.../optimization/wide_schema_history.py
28:HISTORY_LOOKBACK_DAYS = 30
# search sound
```

`executed_by` is selected, but it is used to *exclude* service-principal traffic, not
to count distinct humans. There is no duration column, so there is no cost proxy of
any kind in this module.

**Three answers, separately:**

- **Frequency** — exists, but grained per *column × normalized query shape*, not per
  *measure*. `DemandSignal` wants per-measure. Mapping column-grain frequency onto a
  measure expression is new logic even though the counts exist.
- **Cost** — does not exist. Needs `total_task_duration_ms` (or billing apportionment
  as GenieWatch does at `system_tables.py:263-276`) added to the SELECT, or a new
  query.
- **Distinct users** — does not exist. Needs a `COUNT(DISTINCT executed_by)`
  aggregate, which also raises a question the current module never had to answer:
  distinct-user counts on query history are a people-signal, and the module currently
  filters SP traffic rather than attributing human traffic.

**CMK is not handled by name, and degrades into a second dead end.** There is no CMK
branch:

```
$ rg -n -i "cmk|customer.managed" packages/.../optimization/wide_schema_history.py
# exit 1, no hits
```

It degrades *implicitly*: `statement_text` is read as `str(row.get("statement_text") or "")`
(`:572`), so redacted text yields an unparseable statement, zero accepted statements,
and the zero-accepted condition triggers the same warehouse Query History REST
fallback as an outright failure (`_rest_history_rows`, `:326-391`). The problem is
that the REST fallback has no privileged access to statement text either — under CMK
it is redacted there too. So under customer-managed keys, **D has no source at all**,
and the failure presents as "no usable rows" rather than as "text is unavailable on
this workspace." Prompt 6 should record which of the two it hit; they call for
different operator responses.

---

## Q4 — S (semantic)

**Verdict: an unconfigured or unreachable endpoint degrades silently to `S = 0.0`, by
explicit design and pinned by tests. What is missing is not the degradation — it is a
recorded reason. `preflight_embedding_endpoint` exists and is never called.**

**The full chain, step by step.**

1. **"Unconfigured" does not present as empty.** Both endpoint constants default to
   real endpoint names (`leakage.py:51`, `EMBEDDING_ENDPOINT` → `databricks-bge-large-en`;
   `common/config.py:2496`, `MV_EMBEDDING_ENDPOINT` → `databricks-gte-large-en`). So an
   operator who configures nothing gets a call to a plausible endpoint that may not
   exist in their workspace — the *unreachable* path, not a distinguishable
   "unconfigured" path.

2. **`get_embedding` returns `None` on everything.** `leakage.py:189`. Two `except
   Exception` blocks, both logging at `debug`, both falling through to `return None` —
   covering query failure, timeout, and unparseable response alike. It never raises and
   never returns a zero vector.

3. **The production adapter converts `None` to `[]` silently.**
   `FoundationModelEmbeddingClient.embed` (`mv_scoring.py:300-326`) appends
   `_l2_normalize(raw or [])`, so a dead endpoint becomes an empty vector with no
   signal to the caller.

4. **`semantic_score` returns a default match.** `mv_scoring.py:456`. When the client
   is `None` or there is nothing to compare it returns `SemanticMatch()` immediately;
   when `embed` raises it logs a warning and returns the same. Its docstring states the
   intent explicitly:

```469:472:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/mv_scoring.py
    Returns a zero match when there is nothing to compare or no client: a
    missing embedding endpoint costs the advisor one signal out of four rather
    than the whole run, matching how the firewall degrades. A negative maximum is
    reported as 0.0 — opposed vectors mean no semantic match, and a negative
```

5. **`S = 0.0` flows into the blend with its weight intact** (`MV_SCORE_WEIGHT_S = 0.20`,
   `common/config.py:2445`). The signal is zeroed, not dropped, so the achievable
   ceiling drops by 20 points.

**Nothing distinguishable is recorded.** The result object carries three fields:

```339:341:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/mv_scoring.py
    field: str | None = None
    cosine: float = 0.0
    reference_kind: str = SEMANTIC_REF_NONE
```

On the degraded path all three are defaults — which is byte-identical to the result
for a candidate that legitimately had no references to compare. An operator reading
`evidence.semantic_top_match` afterwards **cannot tell a dead endpoint from an absent
reference set.** Only the exception path leaves a `logger.warning`; the two commoner
paths (client `None`, embedding `None`) leave a `debug` line or nothing.

**`preflight_embedding_endpoint` is dead code.** Absence of callers, with a positive
control on a sibling symbol from the same module:

```
$ rg -n "preflight_embedding_endpoint" --glob "*.py" .
./packages/.../optimization/leakage.py:248:def preflight_embedding_endpoint(w: Any, endpoint: str | None = None) -> bool:
$ rg -c "get_embedding" --glob "*.py" .
./packages/.../tests/unit/test_mv_scoring.py:4
./packages/.../optimization/mv_scoring.py:3
./packages/.../optimization/leakage.py:7
```

One hit, the definition itself. It returns `bool` — it neither raises nor returns
`None`. Its docstring instructs callers to set `firewall_embedding_disabled=True`, a
flag that does not exist in the codebase either.

**Existing tests that pin the degradation** (so this behaviour is deliberate, not
drift): `test_mv_scoring.py:326` (no client), `:334` (embed raises), `:345` (wrong
vector count), `:422` (no client ⇒ score 80), `:489` (evidence shows null field),
`:1207` (adapter default endpoint).

**Answering the decision you posed.** `S = 0.0` is defensible — the design intent is
stated in the code and tested, and refusing to score would let one optional signal
veto the whole advisor. But it is only defensible **with a recorded reason**, and that
reason does not exist today. This is precisely the vacuous-check hole B4 closed for
the comment echo, and the same remedy applies: a status field on `SemanticMatch`
mirroring the `MV_ECHO_CHECK_COMPARED` / `MV_ECHO_CHECK_NOT_COMPARED` pair, so
"compared and found nothing" and "never compared" stop being the same payload. Given
B4's precedent, I would treat that as in-scope wherever S first ships rather than as a
later hardening pass.

---

## Q5 — THE ORACLE

**Verdict: confirmed vacuous without an oracle. Everything needed to build one is
already in scope at optimize time, and wiring it is code-only.**

**The vacuity is already legible**, because B4 made it so: `generate()` sets
`echo_check` to `MV_ECHO_CHECK_NOT_COMPARED` when no oracle is supplied, and both
result types carry the field. So this is a known-and-marked gap rather than a silent
one — the marker exists, the producer does not.

**What an oracle needs.** `BenchmarkCorpus.from_benchmarks` (`leakage.py:147`, on the
dataclass at `:127`)
consumes an iterable of dicts and reads `question`, `expected_sql`, and `id` or
`benchmark_id`. `LeakageOracle.__init__` (`leakage.py:703`) takes one or more
`BenchmarkCorpus` instances, not raw dicts.

**What the run already has.** The optimize task loads the corpus from Delta:

```203:206:packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_optimize.py
    all_benchmarks = load_benchmark_corpus(spark, uc_schema, domain)
    loaded_benchmarks = benchmark_corpus_for_optimization(all_benchmarks)
    if not loaded_benchmarks:
        raise RuntimeError(
```

and the loop already wraps it:

```2852:2852:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py
    benchmark_corpus = BenchmarkCorpus.from_benchmarks(benchmarks)
```

**No oracle is held anywhere.** The only two constructions are transient, inside
`leakage.py` itself:

```
$ rg -n "LeakageOracle\(" --glob "*.py" packages/genie-space-optimizer/src
.../optimization/leakage.py:956:    return LeakageOracle(benchmark_corpus).is_scored_benchmark_qa(
.../optimization/leakage.py:988:    decision = LeakageOracle(benchmark_corpus).evaluate_example_sql(
```

Absence in the two files that would need one, with a positive control proving the
search reaches them:

```
$ rg -n "LeakageOracle" .../optimization/unified_loop.py .../jobs/run_optimize.py
# exit 1, no hits
$ rg -c "BenchmarkCorpus" .../optimization/unified_loop.py
# hits — search sound
```

**Persisted question text exists**, so the advisor does not depend on loop-local
state. The full corpus lives in `genie_benchmarks_{domain}` with `inputs.question` and
`inputs.expected_sql` (schema declared in `benchmarks.py:1308-1317`), and mutations
keep before/after JSON:

```162:163:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
    before              STRING                 COMMENT 'JSON {question, sql} prior state (NULL for added)',
    after               STRING                 COMMENT 'JSON {question, sql} resulting state (unchanged for excluded; NULL for removed / prune_recommended)',
```

Note that `genie_benchmarks_{domain}` is not declared in `ddl.py` — absence check with
positive control:

```
$ rg -n "genie_benchmarks_" packages/.../optimization/ddl.py
# exit 1, no hits
$ rg -n "genie_opt_benchmark_mutations" packages/.../optimization/ddl.py
158:CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_opt_benchmark_mutations (
```

**Verdict for Prompt 6.** Code-only: `LeakageOracle(BenchmarkCorpus.from_benchmarks(benchmarks))`.
One caveat that follows from Prompt 6's own placement decision — the advisor runs
*after* the unified loop, so it must build its own oracle from the corpus it loads
from Delta rather than reaching for `unified_loop.py:2852`'s local variable. That is
the same "consume from Delta, not from in-memory state" rule Prompt 6 already applies
to the SQL corpus, so it is consistent rather than an extra burden.

---

## Q6 — ARTIFACT KIND

**Verdict: adding an `ARTIFACT_KINDS` entry is code-only. But `write_artifact` cannot
accept a `content_hash`, so MV-D7's cross-reference does not hold today.**

**`write_artifact` has no `content_hash` parameter:**

```1312:1324:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py
def write_artifact(
    spark: SparkSession,
    run_id: str,
    artifact_kind: str,
    payload: dict | list | str | None,
    *,
    catalog: str,
    schema: str,
    stage_name: str | None = None,
    iteration: int | None = None,
    source_notebook: str | None = None,
    parent_artifact_id: str | None = None,
) -> str | None:
```

It computes the hash internally from the serialized payload —
`hashlib.sha256(artifact_json.encode("utf-8")).hexdigest()` — so the written value is
always a hash *of the artifact JSON*, never a caller-supplied fingerprint.

**MV-D7 requires the opposite, and the DDL asserts it as fact:**

```200:200:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
    dedup_fingerprint   STRING        NOT NULL COMMENT 'MV-D7 idempotency key: sha256(space_id | canonical_measure_expr | sorted_source_set). Upsert key together with target_space_id; also the content_hash of the rendered-DDL genie_opt_artifacts row for this candidate',
```

"also the `content_hash` of the rendered-DDL `genie_opt_artifacts` row" is not
achievable through the current writer. The fix is small and additive — a
`content_hash: str | None = None` keyword that overrides the computed value when
supplied — and because it is keyword-with-default, every existing call site keeps its
present behaviour. It is a signature change to a shared writer, not a migration.

**Adding a kind is code-only.** The tuple is the only gate, and it warns rather than
raises:

```1335:1339:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py
    if artifact_kind not in ARTIFACT_KINDS:
        logger.warning(
            "write_artifact: unknown artifact_kind=%r (expected one of %s)",
            artifact_kind, ARTIFACT_KINDS,
        )
```

There is no `CHECK` constraint in the DDL (absence check: no `CHECK (`, `CONSTRAINT`
or `ENFORCED` in `ddl.py`; positive control: `ADDITIVE_COLUMN_MIGRATIONS` and the
artifacts DDL both resolve in the same file). Readers filter on an exact kind string,
so an unrecognised kind is ignored rather than fatal — `state.load_artifacts`,
`backend/routers/auto_optimize.py:437-453`, `integration/revert.py:510`. The frontend
never references `artifact_kind` at all.

`ADDITIVE_COLUMN_MIGRATIONS` (`ddl.py:292`) is a tuple of `(table, column, col_def)`
triples for **column** additions. A new value in an existing `STRING` column needs no
entry there. The only optional hygiene item is the `artifact_kind` COMMENT, which
`CREATE TABLE IF NOT EXISTS` will not rewrite on an existing table anyway.

---

## Q7 — PARAMETER ORDERING

**Verdict: a widget read for an undeclared job parameter returns the notebook's own
default. It does not raise. Prompt 8 does not need to move ahead of Prompt 6.**

**Why it does not raise.** The notebook declares every widget with `dbutils.widgets.text`
before reading it, so the widget always exists in the notebook's own context
regardless of what the job definition passes. Verified mechanically rather than by
eye:

```
$ python3 <parse run_optimize.py>
declared via .text : 12 ['apply_mode', 'benchmark_policy', 'catalog', 'domain', 'levers',
                         'llm_model', 'max_attempts', 'run_id', 'schema', 'space_id',
                         'target_accuracy', 'warehouse_id']
read via .get      : 12 [ ...same 12... ]
READ BUT UNDECLARED: (none)
DECLARED NOT READ  : (none)
```

There is no exception handling around any widget read anywhere in the package:

```
$ rg -n "InputWidgetNotDefined|except.*[Ww]idget" --glob "*.py" \
    packages/genie-space-optimizer/src backend
# exit 1, no hits
```

So the mechanism is: `.text(name, default)` registers the default → a job parameter
overrides it when present → `.get` returns the default when absent. A *bare* `.get`
without a prior `.text` **would** raise `InputWidgetNotDefined`, but that pattern is
not used here. The order of declaration matters, not the job definition.

**What this means for Prompt 6.** Its gating parameter,
`enable_metric_view_suggestions`, can be added to `run_optimize.py` following the
pattern Prompt 6 already points at (`run_intake_and_snapshot.py:81-95`, which is the
same declare-then-read block) and read safely **before any mirror declares it** — as
long as the declared default is the off value and the comparison is a STRING compare
as the playbook specifies. Off-by-default means the phase is a no-op until the mirrors
catch up, which is exactly the zero-cost-when-off behaviour Prompt 6 asks for.

**The risk runs the other way.** `run_now` rejects undeclared job-parameter keys, so
the moment the *backend* wants to pass the flag, all mirrors must already declare it:

```98:105:packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py
            # Only send parameters the 4-task job declares. Every key here must
            # be a job parameter on the runner (run_now rejects undeclared
            # keys), so this set MUST stay a subset of the declared params in
            # BOTH job definitions: the root bundle
            # (databricks.yml resources.jobs.gso-optimization-runner) and the
            # notebook installer (scripts/deploy_lib/gso_job.py) — which in turn
            # mirror the package bundle. `benchmark_repair_max_tries` is declared
            # by the job but not overridden here, so it uses the job default.
```

That is a Prompt 8 concern for the *trigger* path, not a blocker for the job's own
default-off gate. The two can land in either order provided Prompt 6's default is off.

**Mirror state today.** `llm_model` is declared in three of four mirrors and
deliberately absent from the package bundle:

```
databricks.yml                                   llm_model=11
packages/genie-space-optimizer/databricks.yml    llm_model=0
scripts/deploy_lib/gso_job.py                    llm_model=12
.../backend/job_launcher.py                      llm_model=2
```

That asymmetry is sanctioned and asserted as the *only* permitted difference:

```513:517:backend/tests/test_deploy_lib.py
    # Declared params identical except llm_model.
    gso_params = {p["name"] for p in settings["parameters"]}
    pkg_params = {p["name"] for p in pkg_job["parameters"]}
    assert gso_params - pkg_params == {"llm_model"}
    assert pkg_params - gso_params == set()
```

**Correction to a claim worth stating precisely.** A four-way mirror test does not
exist, but it is wrong to say the mirrors are untested. A *pairwise* test does exist —
`test_gso_job_settings_mirror_package_bundle_4task` (`backend/tests/test_deploy_lib.py:488`)
pins `gso_job.py` against the package bundle on task keys, order, entrypoints,
declared params and per-task `base_parameters`. `test_phase7_job_dag.py:19` pins the
package bundle on its own. What is genuinely unpinned is the **root `databricks.yml`**
and the **launcher map** — no test reads either as a mirror. Absence check, with its
own positive control:

```
$ rg -n "\"databricks.yml\"|'databricks.yml'" --glob "*.py" \
    packages/genie-space-optimizer/tests backend/tests
backend/tests/test_deploy_lib.py:496:    bundle_path = repo_root / "packages" / "genie-space-optimizer" / "databricks.yml"
packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py:19:_BUNDLE = _PKG_ROOT / "databricks.yml"
```

Two tests read *a* `databricks.yml`; both read the package one. So two of four mirrors
are pinned, and the root bundle is the mirror where drift lands silently. Whoever adds
the next parameter should know that.

---

## Findings that contradict committed text

Recorded, not fixed — this pass is recon-only. Each is a correct-when-written claim
that a later finding has undermined, which is the failure mode MV-D14's marker exists
to prevent.

1. **`mv_scoring.py:196` asserts a cost proxy that does not exist.** The `DemandSignal`
   docstring reads "the repo's available cost proxy (``wide_schema_history`` reads
   duration, not DBUs)". `wide_schema_history` reads *neither* — its SELECT list has no
   duration column (Q3 absence check). The parenthetical is false as shipped, and it is
   load-bearing: it tells a future implementer that a cost signal is already available
   when it is not.

2. **`ddl.py:200` asserts a cross-reference the writer cannot produce.** The
   `dedup_fingerprint` COMMENT states the fingerprint is "also the `content_hash` of the
   rendered-DDL `genie_opt_artifacts` row." `write_artifact` always computes
   `content_hash` from the payload and accepts no override (Q6). MV-D7 carries the same
   claim.

3. **POV:266's closing sentence does not hold for in-job computation.** "Lineage graphs
   share the UC permission model, so lineage-derived evidence is already scoped" is true
   under OBO and false under the SP identity the job runs as (Q2).

Item 1 belongs wherever D is first implemented; item 2 wherever the DDL artifact is
first written; item 3 in whichever prompt settles the compute-vs-present split.

---

## RECOMMENDATION

**Prompt 6 is executable as written, and I recommend splitting it anyway.** The reason
is not feasibility — it is that Prompt 6's output would be uninterpretable on its first
real run.

**The arithmetic that drives this.** The blend weights are
`L=0.35, Y=0.30, S=0.20, D=0.15` (`common/config.py:2443-2446`). Mapping the recon onto
them:

| Signal | Weight | State today |
|---|---|---|
| **Y** | 0.30 | Present. Corpus persisted (Q1), `corpus_scan` shipped in Prompt 3. |
| **L** | 0.35 | No producer. Column grain needs a new SP grant (Q2). |
| **S** | 0.20 | Degrades to 0.0 silently; indistinguishable from "no references" (Q4). |
| **D** | 0.15 | Frequency at wrong grain; cost and distinct users absent (Q3). |

Ship Prompt 6 as written against today's inputs and the achievable confidence ceiling
is **50** with S working, and **30** without it. The largest single weight is the one
with no producer at all. Worse, the failure is quiet in exactly the way the S gap is
quiet: a top-ranked candidate scoring 45 looks like a cautious advisor rather than an
advisor missing 55% of its evidence, and the ranking would be driven almost entirely
by Y — which means the advisor's *ordering*, not just its absolute scores, would be an
artifact of missing inputs.

**Proposed boundary, if you split.**

- **6a — signal producers.** The items that are not code-only or not present:
  (i) an in-job SP lineage read populating `LineageOverlap` at table grain via
  `common/warehouse.sql_warehouse_query`, plus an explicit decision on the
  `column_lineage` grant and on the compute-vs-present filtering that POV:266 forces;
  (ii) a demand query supplying cost and distinct users, plus the column→measure grain
  mapping. Fold in the two small correctness items the advisor depends on: the
  `write_artifact` `content_hash` passthrough (Q6) and a recorded reason for a
  degraded S (Q4).
- **6b — the advisor phase.** Everything in Prompt 6's current text: the gating widget,
  placement after the loop, reading `rows_json` by `run_id`, `corpus_scan` → estate MV
  index → score → dedup → persist → `mv_yaml.generate`/`validate` → DDL artifact,
  try/except isolation, tests.

The split is unusually cheap here because `mv_scoring` already takes `LineageOverlap`,
`DemandSignal` and `EmbeddingClient` as **injected** inputs. 6b can be written and
tested in full against fixtures, and 6a swaps fixtures for producers without touching
the advisor. That is the same seam Prompt 5.5's `PROVEN` fixture exploits — and it
carries the same caveat MV-D14's marker already records: fixtures prove the consumer,
so 6a owes an integration assertion that a real producer's output reaches the scorer in
the shape the scorer expects.

**What a first useful version can ship without.** S, comfortably — the code says so
and tests pin it. Cost and distinct-user D, if frequency-only D at mapped column grain
is accepted. Column-level L, if table-level overlap is accepted and the scoring
normalization is restated for table grain. What I would not ship without is L
*entirely*, because zeroing the heaviest weight distorts ranking rather than merely
lowering scores.

**The tradeoff, stated both ways.** Splitting costs a commit boundary and defers the
first visible candidate; the advisor's logic sits unexercised against real signal until
6a lands, and there is a real risk 6a's grant question stalls on a deployment decision
that has nothing to do with the advisor. Not splitting gets a working end-to-end phase
sooner and lets the signals fill in incrementally, at the cost of a first run whose
ranking is driven by one signal and whose scores look like caution rather than absence.

**A middle path worth weighing before choosing either.** Keep Prompt 6 whole, but
require it to record per-candidate signal availability — which of L/Y/S/D were computed
versus defaulted — using the vacuous-marker pattern B4 established for the echo check.
That converts the dangerous failure (invisible missing evidence) into a visible one,
lets 6b ship now, and turns 6a into a follow-up that raises scores rather than a
prerequisite that gates them. It does not fix the ranking distortion; it makes the
distortion legible, which may be enough to justify shipping whole.

I am not deciding this. My own lean is the middle path, because it preserves Prompt 6's
delivery while removing the specific failure mode that makes the unsplit version risky
— but it depends on whether you would rather have a candidate on screen sooner or a
score you can trust the first time you read one.
