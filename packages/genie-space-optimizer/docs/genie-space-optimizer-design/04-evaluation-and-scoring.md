# 04 -- Evaluation and Scoring

[Back to Index](00-index.md) | Previous: [03 Optimization Pipeline](03-optimization-pipeline.md) | Next: [05 Optimization Levers](05-optimization-levers.md)

---

## Overview

The evaluation system is the optimizer's "eyes" -- it measures Genie Space quality by running benchmark questions through Genie and scoring the responses with 9 specialized judges across 7 quality dimensions. The scores determine whether optimization is needed, whether patches helped or hurt, and when to stop iterating.

---

## Benchmark Generation

### Generation Strategy

Benchmarks are the evaluation questions used to measure Genie Space quality. They are generated (or loaded) during preflight:

| Source | Priority | When Used |
|--------|----------|-----------|
| UC table (`genie_benchmarks_{domain}`) | 1st | If >= 5 valid benchmarks exist, reuse them |
| Genie Space curated (`example_question_sqls`, `sample_questions`) | Extracted | Always extracted and merged |
| LLM generation | Fallback | When < 5 valid benchmarks exist |

### LLM Generation

The `BENCHMARK_GENERATION_PROMPT` receives:
- Full UC metadata: tables, columns (with types and descriptions), metric views, TVFs
- Column allowlist (Extract-Over-Generate principle: the LLM must only use columns that actually exist)
- Genie Space instructions and sample questions as context
- Target count: `TARGET_BENCHMARK_COUNT` (20)

Each generated benchmark includes:

| Field | Description |
|-------|-------------|
| `question` | Natural language question a business user would ask |
| `expected_sql` | Correct SQL using fully-qualified table names |
| `expected_asset` | Asset type: `TABLE`, `MV` (metric view), or `TVF` |
| `category` | One of 7 categories |
| `required_tables` | Tables the SQL should reference |
| `required_columns` | Columns the SQL should reference |
| `expected_facts` | 1-2 facts the answer should contain |

### Categories

Benchmarks cover 7 categories to ensure diverse evaluation:

| Category | What It Tests |
|----------|---------------|
| `aggregation` | SUM, COUNT, AVG, GROUP BY |
| `ranking` | ORDER BY, LIMIT, window functions |
| `time-series` | Date filters, temporal ranges, YTD |
| `comparison` | WHERE clauses, HAVING, multi-condition filters |
| `detail` | Single-row lookups, specific entity queries |
| `list` | Multi-row results, IN clauses |
| `threshold` | Boundary conditions, >= / <= filters |

### Coverage Gap Fill

After initial generation, the optimizer checks for uncovered assets (tables, metric views, TVFs that have no benchmark questions). A `BENCHMARK_COVERAGE_GAP_PROMPT` generates additional questions specifically targeting these gaps, up to `TARGET_BENCHMARK_COUNT * COVERAGE_GAP_SOFT_CAP_FACTOR` (30) total.

### Validation

Every benchmark undergoes `SQL EXPLAIN` validation before use. Invalid benchmarks (those that fail to parse or reference non-existent objects) are dropped. A minimum of 5 valid benchmarks is required to proceed.

---

## Temporal Date Resolution

Benchmark questions often contain relative time references like "this year" or "last quarter" that become stale as time passes. The optimizer automatically detects and rewrites these.

### Detection

The `_detect_temporal_intent()` function scans question text using regex patterns:

| Pattern | Regex | Example Match |
|---------|-------|---------------|
| This year / YTD | `this year\|year.to.date\|ytd` | "What are YTD revenues?" |
| This month | `this month\|current month` | "Show this month's sales" |
| This quarter | `this quarter\|current quarter` | "Current quarter performance" |
| Last quarter | `last quarter\|previous quarter` | "Last quarter revenue" |
| Last year | `last year\|previous year` | "Compare to last year" |
| Last N months | `last (\d+) months?` | "Revenue for last 6 months" |
| Last N days | `last (\d+) days?` | "Orders in last 30 days" |

### Rewriting

`_rewrite_temporal_dates()` updates date literals in the ground-truth SQL:

1. Detects the temporal intent from the question text
2. Computes the correct date range based on today's date
3. Replaces date literals in the `expected_sql` (e.g., `'2025-01-01'` -> `'2026-01-01'`)
4. Attaches a `temporal_note` to the evaluation context so judges know dates were auto-adjusted

Explicit years in the question text (e.g., "for 2024") are **not** rewritten -- these are assumed to be intentional.

### Temporal-Stale Benchmark Flagging

After temporal date rewriting, the optimizer runs a second pass: `_flag_stale_temporal_benchmarks()` executes each temporal benchmark's GT SQL via Spark and checks whether it returns any rows. Benchmarks whose GT SQL returns 0 rows (e.g., because the underlying data doesn't extend to the rewritten date range) are flagged `temporal_stale=True`. These benchmarks are excluded from the accuracy denominator entirely, preventing false failures from data gaps that are not the Genie Space's fault.

The log message `Flagged N/M benchmarks as temporal-stale (excluded from accuracy)` is emitted when flagging occurs.

---

## Result Comparison

### Comparison Types

When comparing GT and Genie SQL results, the evaluation produces one of several comparison outcomes:

| Comparison Type | Condition | Accuracy Impact |
|----------------|-----------|-----------------|
| `match` | Results match (exact, subset, or fuzzy) | Counted as correct |
| `mismatch` | Results differ | Counted as failure |
| `genie_result_unavailable` | Genie produced no result | Excluded from denominator |
| `both_empty` | Both GT and Genie SQL returned 0 rows | Excluded from denominator |
| `gt_infra_error` | GT SQL hit infrastructure error | Excluded from denominator |

The `both_empty` comparison type was introduced to handle cases where neither query returns data (e.g., both use date filters that no longer match any records). Rather than treating this as a failure, it is excluded from scoring.

### `fetch_genie_result_df()` Retry Behavior

The helper that retrieves Genie's query results via the Statement Execution API retries up to 3 times with linear backoff (2s base delay) when the statement is still `PENDING` or `RUNNING`, or when results are transiently unavailable. This prevents premature `genie_result_unavailable` comparisons caused by slow-executing statements.

---

## The 9-Judge Scoring Framework

### Judge Overview

| # | Judge | Type | Target | Scoring |
|---|-------|------|--------|---------|
| 1 | Syntax Validity | Deterministic | 98% | Binary: SQL parses via Spark `EXPLAIN` |
| 2 | Schema Accuracy | Deterministic | 95% | Binary: all referenced tables/columns exist |
| 3 | Logical Accuracy | LLM | 90% | 0-100: correct filters, aggregations, GROUP BY, ORDER BY |
| 4 | Semantic Equivalence | LLM | 90% | 0-100: same business metric as expected answer |
| 5 | Completeness | LLM | 90% | 0-100: all requested dimensions/measures present |
| 6 | Response Quality | LLM | Advisory | 0-100: NL analysis text accurately describes the SQL |
| 7 | Result Correctness | Deterministic | 85% | Binary: result values match expected |
| 8 | Asset Routing | Deterministic | 95% | Binary: correct asset type (TABLE/MV/TVF) selected |
| 9 | Arbiter | LLM | Advisory | Tiebreaker verdict when results disagree |

### Deterministic Judges

**Syntax Validity:** Runs `EXPLAIN {sql}` via Spark. If Spark can parse the SQL without error, it passes.

**Schema Accuracy:** Extracts all table and column references from the SQL and verifies they exist in UC metadata.

**Result Correctness:** Compares the actual query results against the expected results. Uses value matching with tolerance for floating-point differences and ordering variations.

**Asset Routing:** Detects what asset type Genie used (metric view with MEASURE() syntax, TVF with function call, or regular table) and compares against the benchmark's `expected_asset`.

### LLM Judges

LLM judges receive the question, expected SQL, Genie's SQL, result samples, and any temporal notes. They return structured JSON:

```json
{
  "score": 85,
  "pass": true,
  "failure_type": "wrong_aggregation",
  "blame_set": ["table_x.column_y"],
  "counterfactual_fix": "Change SUM to COUNT DISTINCT",
  "wrong_clause": "SELECT SUM(amount)",
  "rationale": "The query uses SUM instead of COUNT DISTINCT..."
}
```

These structured fields flow directly into ASI metadata and provenance -- there is no separate rationale-pattern extraction step.

**Response Quality** evaluates whether Genie's natural language analysis (the text explanation that accompanies the SQL) accurately describes what the query does. It returns `unknown` when no analysis text is available (Genie doesn't always include it), and never penalizes for missing NL responses.

### The Arbiter

The arbiter is a conditional judge that runs only when result correctness reports a mismatch (Genie's results differ from expected). Its purpose is to determine who is actually correct.

**When results match:** Returns `both_correct` without an LLM call (fast path).

**When ground truth has 0 rows and Genie's result is unavailable:** Returns `both_correct` (null-result defense).

**When infra/permission/execution errors occurred:** Returns `skipped`.

**Otherwise:** Makes an LLM call with both SQL statements, result samples, and context. Returns one of:

| Verdict | Meaning | Downstream Impact |
|---------|---------|-------------------|
| `genie_correct` | Genie is right, benchmark expected SQL is wrong | Auto-correct benchmark after `GENIE_CORRECT_CONFIRMATION_THRESHOLD` (2) confirmations |
| `ground_truth_correct` | Expected SQL is right, Genie is wrong | Hard failure, drives optimization |
| `both_correct` | Both are valid, different approaches | Excluded from hard failure clusters |
| `neither_correct` | Both are wrong | GT repair attempted, eventually quarantined |

### Arbiter-Adjusted Accuracy

The overall accuracy calculation uses arbiter verdicts to correct the raw scores:

- `genie_correct` and `both_correct` verdicts are counted as **correct** (overriding individual judge failures)
- `ground_truth_correct` and `neither_correct` verdicts use the individual judge scores as-is
- This prevents the optimizer from chasing false failures caused by stale benchmark SQL

The following are **excluded from the denominator** entirely (they neither help nor hurt the score):

- GT infrastructure failures (`result_correctness == "excluded"`)
- Quarantined questions (repeated `neither_correct` with failed GT repair)
- Temporal-stale benchmarks (GT SQL returns 0 rows due to date drift)
- `both_empty` comparisons (both GT and Genie returned 0 rows)
- `genie_result_unavailable` comparisons (Genie produced no result)

---

## Scoring Aggregation

### Per-Question Scoring

Each question receives a pass/fail verdict from each judge. The overall question result is the **minimum** across non-advisory judges (response quality and arbiter are advisory -- they inform but don't fail).

### Per-Dimension Scoring

Each dimension's score is the percentage of questions that passed that judge:

```
dimension_score = (passing_questions / total_questions) * 100
```

### Overall Accuracy

The overall accuracy is the **mean** of all non-advisory dimension scores:

```
overall = mean(syntax, schema, logical, semantic, completeness, result, routing)
```

### Threshold Check

`all_thresholds_met()` returns `True` when every dimension meets or exceeds its target (see [01 -- Introduction](01-introduction.md) for the target table). If this returns `True` after baseline, the lever loop is skipped.

---

## MLflow Integration

### Experiment Structure

Each optimization run creates an MLflow experiment at:

```
/Shared/genie-space-optimizer/{space_id}/{domain}
```

### Tracking Hierarchy

| MLflow Concept | Maps To | What It Contains |
|----------------|---------|------------------|
| Experiment | Genie Space | All runs for a space |
| Run | Evaluation pass | One set of benchmark evaluations |
| LoggedModel | Configuration version | Scores + config snapshot |
| Trace | Single question evaluation | Question, SQL, judge verdicts |
| Assessment | Judge feedback | Score, failure metadata |

### Tags on Traces

Every evaluation trace is tagged for traceability:

| Tag | Value | Purpose |
|-----|-------|---------|
| `genie.optimization_run_id` | UUID | Links to optimizer run |
| `genie.iteration` | Integer | Iteration number (0 = baseline) |
| `genie.eval_scope` | `full`, `slice`, `p0` | Evaluation scope |
| `genie.lever` | Integer or null | Which lever was being evaluated |

### Assessments on Traces

| Assessment Type | Source | Content |
|-----------------|--------|---------|
| `Expectation` | Benchmark | Expected SQL logged for comparison |
| `asi_{judge}` | ASI | Structured failure metadata per judge |
| `gate_{type}` | Gate evaluation | Pass/fail + regression details |

### LoggedModel Versions

Each accepted iteration creates a new LoggedModel version with:
- Overall accuracy and per-dimension scores
- Config snapshot (instruction text, table descriptions, etc.)
- Link to the evaluation MLflow run

The best-performing version is promoted during finalize.

---

## Evaluation Scopes

The optimizer uses different evaluation scopes for efficiency:

| Scope | Questions Evaluated | When Used |
|-------|--------------------|-----------|
| `full` | All benchmarks | Baseline, start of each iteration, final gate, repeatability |
| `slice` | Only questions affected by the current patch | Slice gate (quick regression check) |
| `p0` | Priority-0 questions (highest-impact failures) | P0 gate (intermediate check) |
| `held_out` | Reserved subset not seen during optimization | Optional, for unbiased evaluation |

---

## Retry and Error Handling

| Error | Handling |
|-------|----------|
| MLflow harness `AttributeError` | Up to 4 retries with exponential backoff (10s, 20s, 30s, 40s); falls back to single-worker mode on retry 2+ |
| Genie API HTTP 429 (rate limit) | Exponential backoff with `GENIE_RATE_LIMIT_RETRIES` (3) retries, starting at `GENIE_RATE_LIMIT_BASE_DELAY` (30s) |
| Infrastructure SQL errors | Configurable via `FAIL_ON_INFRA_EVAL_ERRORS` (default: `true` = fail fast) |
| gRPC/Spark Connect timeout | Automatic retry with backoff |
| LLM JSON parse failure | Robust `_extract_json()` handles markdown fences and prose-wrapped responses |

---

Next: [05 -- Optimization Levers](05-optimization-levers.md)
