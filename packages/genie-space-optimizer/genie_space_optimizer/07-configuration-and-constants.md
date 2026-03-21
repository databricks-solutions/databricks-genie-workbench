# Configuration & Constants

All configurable values for the Genie Space Optimizer. These live in `common/config.py` as module-level constants with sensible defaults. They can be overridden via environment variables, job parameters, or the `thresholds` argument to `optimize_genie_space()`.

---

## 1. Quality Thresholds

Per-judge thresholds on a 0-100 scale. The harness considers optimization complete when all thresholds are met.

```python
DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "result_correctness": 85.0,
    "asset_routing": 95.0,
}

REPEATABILITY_TARGET = 90.0
```

MLflow GenAI reports thresholds with `/mean` suffix in metric names:

```python
MLFLOW_THRESHOLDS = {
    "syntax_validity/mean": 0.98,
    "schema_accuracy/mean": 0.95,
    "logical_accuracy/mean": 0.90,
    "semantic_equivalence/mean": 0.90,
    "completeness/mean": 0.90,
    "result_correctness/mean": 0.85,
    "asset_routing/mean": 0.95,
}
```

---

## 2. Rate Limits and Timing

```python
RATE_LIMIT_SECONDS = 12
"""Delay between consecutive Genie API calls.
The Genie API has a rate limit; this prevents HTTP 429 errors.
Applied in genie_predict_fn before each start_conversation call."""

PROPAGATION_WAIT_SECONDS = 30
"""Seconds to wait after applying patches before running evaluation.
Genie Space config changes need time to propagate to the query engine."""

GENIE_POLL_INITIAL = 3
"""Initial poll interval (seconds) when waiting for a Genie query to complete."""

GENIE_POLL_MAX = 10
"""Maximum poll interval (seconds). Adaptive backoff: poll_interval = min(poll_interval + 1, GENIE_POLL_MAX)."""

GENIE_MAX_WAIT = 120
"""Maximum wait (seconds) for a single Genie query before timing out."""

JOB_POLL_INTERVAL = 30
"""Seconds between polls when waiting for an evaluation job to complete."""

JOB_MAX_WAIT = 3600
"""Maximum wait (seconds) for an evaluation job. 1 hour default."""

UI_POLL_INTERVAL = 5
"""Seconds between UI status polls (React SPA polling interval)."""

SQL_STATEMENT_POLL_LIMIT = 30
"""Maximum number of polls when waiting for a SQL statement execution to complete."""

SQL_STATEMENT_POLL_INTERVAL = 2
"""Seconds between polls for SQL statement execution status."""

INLINE_EVAL_DELAY = 12
"""Seconds between questions in inline (non-job) evaluation mode."""
```

---

## 3. Iteration and Convergence

```python
MAX_ITERATIONS = 5
"""Maximum number of lever iterations before stopping.
Does not count the baseline evaluation (iteration 0)."""

SLICE_GATE_TOLERANCE = 5.0
"""Percentage points. If slice accuracy drops more than this below the
previous accuracy, the lever is rolled back. E.g., if prev=80%, gate=75%."""

REGRESSION_THRESHOLD = 2.0
"""Percentage points. If any individual judge score drops more than this
between iterations, the lever is considered regressive and rolled back."""

PLATEAU_ITERATIONS = 2
"""Number of consecutive iterations with no improvement before declaring STALLED.
Used in the legacy loop; lever-aware loop checks per-lever instead."""

ARBITER_CORRECTION_TRIGGER = 3
"""Minimum number of 'genie_correct' arbiter verdicts before automatically
correcting benchmark expected_sql values in the MLflow evaluation dataset."""

REPEATABILITY_EXTRA_QUERIES = 2
"""Number of extra Genie queries per question during the Cell 9c repeatability test.
Total samples per question = 1 (original) + REPEATABILITY_EXTRA_QUERIES."""
```

---

## 4. LLM Configuration

```python
LLM_ENDPOINT = "databricks-claude-opus-4-6"
"""Databricks model serving endpoint for ALL LLM calls:
  - LLM judge scoring (_call_llm_for_scoring)
  - Benchmark question generation (generate_benchmarks)
  - Metadata proposal generation (generate_metadata_proposals)
Must be a chat completion endpoint."""

LLM_TEMPERATURE = 0
"""Temperature for LLM calls. 0 for maximum determinism."""

LLM_MAX_RETRIES = 3
"""Maximum retry attempts for LLM calls. Uses exponential backoff: 2^attempt seconds."""
```

---

## 5. Benchmark Generation

```python
TARGET_BENCHMARK_COUNT = 20
"""Target number of benchmark questions to generate per domain."""

BENCHMARK_CATEGORIES = [
    "aggregation", "ranking", "time-series", "comparison",
    "detail", "list", "threshold",
]
"""Categories to diversify benchmark questions across."""

TEMPLATE_VARIABLES = {
    "${catalog}": "catalog",
    "${gold_schema}": "gold_schema",
}
"""Template variables used in benchmark expected_sql.
resolve_sql() substitutes these before spark.sql() execution."""

BENCHMARK_GENERATION_PROMPT = """You are a Databricks Genie Space evaluation expert.

Given the following schema context for domain "{domain}":

## Tables and Columns
{tables_context}

## Metric Views
{metric_views_context}

## Table-Valued Functions
{tvfs_context}

## Genie Space Instructions
{instructions_context}

## Sample Questions (from Genie Space config)
{sample_questions_context}

Generate exactly {target_count} diverse benchmark questions that a business user would ask.

For each question, provide:
1. "question": The natural language question
2. "expected_sql": The correct SQL using ${{catalog}}.${{gold_schema}} template variables
   - For metric views: use MEASURE() syntax
   - For TVFs: use function call syntax
   - For tables: use standard SQL
3. "expected_asset": "MV" | "TVF" | "TABLE"
4. "category": one of {categories}
5. "required_tables": list of table names referenced
6. "required_columns": list of column names referenced
7. "expected_facts": list of 1-2 facts the answer should contain

Ensure diversity: at least 2 questions per category. Include edge cases
(filters, multi-table joins, temporal ranges, NULL handling).

Return a JSON array of question objects. No markdown, just JSON."""
```

---

## 5b. Proposal Generation Prompts

```python
PROPOSAL_GENERATION_PROMPT = """You are a Databricks metadata optimization expert.

Given these evaluation failures and the current metadata state, generate a concrete
metadata change to fix the failures.

## Failure Cluster
- Failure type: {failure_type}
- Blamed objects: {blame_set}
- Affected questions: {affected_questions}
- Counterfactual fixes suggested by judges: {counterfactual_fixes}
- Severity: {severity}

## Current Metadata for Blamed Objects
{current_metadata}

## Target Change Type
{patch_type_description}

Generate the exact new value for the metadata field. Be specific and actionable.
For column descriptions: include business definition, data type context, and usage hints.

Return JSON: {{"proposed_value": "...", "rationale": "..."}}"""

LEVER_4_JOIN_SPEC_PROMPT = """You are a Databricks Genie Space join optimization expert.

Given these join-related failures and the current join_specs configuration,
generate a join specification that would help Genie correctly join the relevant tables.

## Failures
{failures_context}

## Current Join Specs
{current_join_specs}

## Table Relationships
{table_relationships}

## Available Tables
{table_names}

Generate a valid join_spec JSON object. Include:
- left_table_name and right_table_name (fully qualified)
- join_columns array with left_column and right_column pairs
- relationship_type annotation (e.g., "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--")

Return JSON: {{"join_spec": {{...}}, "rationale": "..."}}"""

LEVER_5_DISCOVERY_PROMPT = """You are a Databricks Genie Space column discovery expert.

Given these column discovery failures and the current column_configs,
recommend which columns should have format assistance (get_example_values),
entity matching (build_value_dictionary), or synonyms.

## Failures
{failures_context}

## Current Column Configs
{current_column_configs}

## Constraints
- Maximum {max_value_dictionary_cols} string columns can have build_value_dictionary enabled
- Currently {string_column_count} string columns exist, {current_dictionary_count} have build_value_dictionary enabled

Generate specific column-level recommendations.

Return JSON: {{"recommendations": [{{"table": "...", "column": "...", "action": "enable_example_values|enable_value_dictionary|add_synonym", "value": "..."|null}}], "rationale": "..."}}"""

LEVER_6_INSTRUCTION_PROMPT = """You are a Databricks Genie Space instruction expert.

Given these routing/disambiguation failures and the current Genie Space instructions,
generate a new instruction that would help Genie route to the correct asset or
disambiguate the user's question.

## Failures
{failures_context}

## Current Instructions
{current_instructions}

## Available Assets
Tables: {table_names}
Metric Views: {mv_names}
TVFs: {tvf_names}

Generate a clear, concise instruction. Focus on:
- When to use which asset type
- How to interpret ambiguous terms
- Default behaviors for common question patterns

Return JSON: {{"instruction_text": "...", "rationale": "..."}}"""
```

---

## 6. Non-Exportable Genie Config Fields

```python
NON_EXPORTABLE_FIELDS = {
    "id",
    "title",
    "description",
    "creator",
    "creator_id",
    "updated_by",
    "updated_at",
    "created_at",
    "warehouse_id",
    "execute_as_user_id",
    "space_status",
}
"""Fields stripped from Genie Space config before PATCH requests.
These are read-only server-managed fields."""
```

---

## 7. Feature Flags

```python
USE_PATCH_DSL = True
"""When True, use the Patch DSL (render_patch + apply_patch_set) for applying changes.
When False, use the legacy apply_proposal_batch approach."""

USE_JOB_MODE = True
"""When True, run evaluations as Databricks Jobs (full 8-judge suite, ASI, UC traces).
When False, run inline evaluation (asset routing only, 1 judge)."""

USE_LEVER_AWARE = True
"""When True, use the lever-aware optimization loop (levers 1-6 in order).
When False, use the legacy optimization loop (generic proposals)."""

ENABLE_CONTINUOUS_MONITORING = False
"""When True, call scorer.start(sampling_config=...) after registration
to enable continuous monitoring on future traces. Off by default."""

APPLY_MODE = "genie_config"
"""Where patches are applied. One of:
  - "genie_config" (DEFAULT): All changes go to Genie Space config overlays.
    Users do not need UC write access. Levers 1-3 write to column_configs,
    data_sources, instructions in the serialized space.
  - "uc_artifact": Column descriptions go to UC via ALTER TABLE, metric view
    changes go to MV YAML in UC volumes, etc.
  - "both": Apply to both targets for maximum coverage.
Levers 4-6 are always Genie config native regardless of this setting."""
```

---

## 8. Risk Classification Sets

Patch types grouped by risk level. Determines auto-apply vs queue behavior.

```python
LOW_RISK_PATCHES = {
    "add_description",
    "update_description",
    "add_column_description",
    "update_column_description",
    "hide_column",
    "unhide_column",
    "add_instruction",
    # Lever 5: Column Discovery (all low risk)
    "enable_example_values",
    "disable_example_values",
    "enable_value_dictionary",
    "disable_value_dictionary",
    "add_column_synonym",
    "remove_column_synonym",
}

MEDIUM_RISK_PATCHES = {
    "update_instruction",
    "remove_instruction",
    "rename_column_alias",
    "add_default_filter",
    "remove_default_filter",
    "update_filter_condition",
    "add_tvf_parameter",
    "remove_tvf_parameter",
    "add_mv_measure",
    "update_mv_measure",
    "remove_mv_measure",
    "add_mv_dimension",
    "remove_mv_dimension",
    # Lever 4: Join Specifications (medium risk)
    "add_join_spec",
    "update_join_spec",
    "remove_join_spec",
}

HIGH_RISK_PATCHES = {
    "add_table",
    "remove_table",
    "update_tvf_sql",
    "add_tvf",
    "remove_tvf",
    "update_mv_yaml",
}
```

**Behavior:** Low and medium risk patches are auto-applied during the lever loop. High risk patches are queued for manual review and logged but not applied.

---

## 9. Repeatability Classification

```python
REPEATABILITY_CLASSIFICATIONS = {
    100: "IDENTICAL",
    70: "MINOR_VARIANCE",
    50: "SIGNIFICANT_VARIANCE",
    0: "CRITICAL_VARIANCE",
}
"""Repeatability percentage thresholds for classification.
Checked in descending order: >= 100% is IDENTICAL, >= 70% is MINOR_VARIANCE, etc."""
```

| Range | Classification | Action |
|-------|---------------|--------|
| 100% | IDENTICAL | No action |
| 70-99% | MINOR_VARIANCE | Monitor; may improve with structured metadata |
| 50-69% | SIGNIFICANT_VARIANCE | Feed to optimizer as `repeatability_issue` |
| 0-49% | CRITICAL_VARIANCE | Feed to optimizer as `repeatability_issue` (critical severity) |

---

## 10. Repeatability Fix Routing by Asset Type

```python
REPEATABILITY_FIX_BY_ASSET = {
    "TABLE": "Add structured metadata (business_definition, synonyms[], grain, join_keys[]) "
             "to column descriptions. Add UC tags: preferred_for_genie=true, domain=<value>.",
    "MV": "Add structured column metadata to metric view columns. "
          "Use synonyms[] and preferred_questions[] to constrain dimension selection.",
    "TVF": "Add instruction clarifying deterministic parameter selection. "
           "TVF signature already constrains output; focus on parameter disambiguation.",
    "NONE": "Add routing instruction to direct questions to the appropriate asset type.",
}
"""Asset-type-specific fix recommendations for repeatability issues.
TABLE/MV -> Lever 1 (structured metadata). TVF -> Lever 6 (instructions)."""
```

---

## 11. Lever Descriptions

```python
LEVER_NAMES = {
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Column Discovery Settings",
    6: "Genie Space Instructions",
}

DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5, 6]

MAX_VALUE_DICTIONARY_COLUMNS = 120
"""Maximum number of string columns per Genie Space that can have
build_value_dictionary=true. Enforced by the Lever 5 optimizer."""
```

---

## 12. Delta Table Names

```python
TABLE_RUNS = "genie_opt_runs"
TABLE_STAGES = "genie_opt_stages"
TABLE_ITERATIONS = "genie_opt_iterations"
TABLE_PATCHES = "genie_opt_patches"
TABLE_ASI = "genie_eval_asi_results"
```

Fully qualified: `{catalog}.{schema}.{TABLE_NAME}`

---

## 13. MLflow Conventions

```python
EXPERIMENT_PATH_TEMPLATE = "/Users/{user_email}/genie-optimization/{domain}"
"""Default MLflow experiment path. {user_email} and {domain} are substituted."""

RUN_NAME_TEMPLATE = "genie_eval_iter{iteration}_{timestamp}"
"""MLflow run name pattern. Used by query_latest_evaluation to find runs."""

MODEL_NAME_TEMPLATE = "genie-space-{space_id}"
"""MLflow LoggedModel name pattern."""

PROMPT_NAME_TEMPLATE = "{uc_schema}.genie_opt_{judge_name}"
"""Prompt Registry name pattern. One per judge."""

PROMPT_ALIAS = "production"
"""Default prompt alias for loading judge prompts."""
```

Prompt Registry operational requirement (Unity Catalog schema level):
- `CREATE FUNCTION`
- `EXECUTE`
- `MANAGE`

Best practice:
- Keep `STRICT_PROMPT_REGISTRATION=true` so missing permissions fail preflight early.
- Ensure deployment grants include Prompt Registry privileges for the Databricks App service principal.
- On first-time app creation, run deploy a second time so post-creation grants can be applied.

---

## 14. Patch DSL Constants

```python
MAX_PATCH_OBJECTS = 5
"""Maximum number of distinct target objects per patch set.
Exceeding this triggers a validation error."""

RISK_LEVEL_SCORE = {
    "low": 1,
    "medium": 2,
    "high": 3,
}
"""Numeric score for risk levels. Used in Pareto frontier computation."""

GENERIC_FIX_PREFIXES = (
    "review ",
    "check ",
    "verify ",
    "ensure ",
    "investigate ",
)
"""Counterfactual fix prefixes that indicate a generic, non-actionable fix.
_is_generic_counterfactual() checks these to filter out vague proposals."""
```

---

## 15. Assessment Sources

```python
CODE_SOURCE_ID = "genie-optimizer-v2"
"""AssessmentSource source_id for code-based judges (syntax_validity, asset_routing, result_correctness)."""

LLM_SOURCE_ID_TEMPLATE = "databricks:/{endpoint}"
"""AssessmentSource source_id for LLM-based judges. {endpoint} is LLM_ENDPOINT."""
```

---

## 16. Temporal Validation Patterns

```python
TEMPORAL_PHRASES = r"\b(this year|last year|last quarter|this quarter|last \d+ months?|last \d+ days?|this month|last month|year to date|ytd)\b"
"""Regex for detecting temporal phrases in benchmark questions."""

HARDCODED_DATE = r"'\d{4}-\d{2}-\d{2}'"
"""Regex for detecting hardcoded dates in expected SQL."""
```

Used by `_check_temporal_freshness()` to flag benchmarks with hardcoded dates that may become stale.
