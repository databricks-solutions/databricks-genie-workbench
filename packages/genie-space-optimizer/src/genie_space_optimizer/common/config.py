"""
All configurable constants for the Genie Space Optimizer.

Module-level constants with sensible defaults. Can be overridden via
environment variables, job parameters, or the `thresholds` argument
to `optimize_genie_space()`.
"""

from __future__ import annotations

import os
import re
from typing import Any


def format_mlflow_template(template: str, **kwargs: Any) -> str:
    """Format a template that uses MLflow's ``{ variable }`` syntax.

    Unlike Python's ``str.format()``, single braces ``{`` ``}`` are treated as
    literal characters and ``{ variable }`` is the interpolation marker.
    Missing keys are left as-is so partial formatting is safe.
    """
    def _replacer(match: re.Match) -> str:
        key = match.group(1).strip()
        if key in kwargs:
            return str(kwargs[key])
        return match.group(0)

    return re.sub(r"\{\{\s*(\w+)\s*\}\}", _replacer, template)

# ── 1. Quality Thresholds ───────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "response_quality": 0.0,
    "result_correctness": 85.0,
    "asset_routing": 95.0,
}

REPEATABILITY_TARGET = 90.0

MLFLOW_THRESHOLDS = {
    "syntax_validity/mean": 0.98,
    "schema_accuracy/mean": 0.95,
    "logical_accuracy/mean": 0.90,
    "semantic_equivalence/mean": 0.90,
    "completeness/mean": 0.90,
    "response_quality/mean": 0.0,
    "result_correctness/mean": 0.85,
    "asset_routing/mean": 0.95,
}

# ── 2. Rate Limits and Timing ──────────────────────────────────────────

RATE_LIMIT_SECONDS = 12
GENIE_RATE_LIMIT_RETRIES = 3
GENIE_RATE_LIMIT_BASE_DELAY = 30
PROPAGATION_WAIT_SECONDS = int(os.getenv("GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT", "30"))
PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING", "90")
)
GENIE_POLL_INITIAL = 3
GENIE_POLL_MAX = 10
GENIE_MAX_WAIT = 120
JOB_POLL_INTERVAL = 30
JOB_MAX_WAIT = 3600
UI_POLL_INTERVAL = 5
SQL_STATEMENT_POLL_LIMIT = 30
SQL_STATEMENT_POLL_INTERVAL = 2
INLINE_EVAL_DELAY = 12
CONNECTION_POOL_SIZE = 20

# ── 3. Iteration and Convergence ───────────────────────────────────────

MAX_ITERATIONS = 5
SLICE_GATE_TOLERANCE = 15.0
ENABLE_SLICE_GATE: bool = False
SLICE_GATE_MIN_REDUCTION = 0.5
REGRESSION_THRESHOLD = 5.0
MAX_NOISE_FLOOR = 5.0
PLATEAU_ITERATIONS = 2
CONSECUTIVE_ROLLBACK_LIMIT = 2
"""Stop the lever loop after this many consecutive rollbacks, indicating
the optimizer is stuck and further iterations are unlikely to help."""
ARBITER_CORRECTION_TRIGGER = 3  # deprecated — use per-question thresholds below
GENIE_CORRECT_CONFIRMATION_THRESHOLD = 2
"""Minimum independent evaluations where a question must receive ``genie_correct``
before the benchmark's expected SQL is auto-corrected with Genie's SQL."""
NEITHER_CORRECT_REPAIR_THRESHOLD = 2
"""After this many ``neither_correct`` verdicts across iterations for a single
question, attempt LLM-assisted ground-truth repair."""
NEITHER_CORRECT_QUARANTINE_THRESHOLD = 3
"""Quarantine a question (exclude from accuracy denominator) after this many
consecutive ``neither_correct`` verdicts AND at least one failed GT repair."""
REPEATABILITY_EXTRA_QUERIES = 2
DIMINISHING_RETURNS_EPSILON = 2.0
"""Stop the lever loop when the last DIMINISHING_RETURNS_LOOKBACK accepted
iterations each improved mean accuracy by less than this percentage."""
DIMINISHING_RETURNS_LOOKBACK = 2
REFLECTION_WINDOW_FULL = 3
"""Number of most-recent reflection entries shown in full detail inside the
adaptive strategist prompt.  Older entries are compressed to one line."""

PERSISTENCE_MIN_FAILURES = 2
"""Minimum non-passing evaluations across iterations before a question
appears in the per-question persistence summary shown to the strategist."""

TVF_REMOVAL_MIN_ITERATIONS = 2
"""Minimum consecutive failing iterations (each with 2 evals) before TVF
removal is considered.  Effectively requires >= 4 consecutive eval failures."""

TVF_REMOVAL_BLAME_THRESHOLD = 2
"""Minimum distinct iterations where the TVF was blamed in ASI provenance
for high-confidence auto-removal."""

# ── 4. LLM Configuration ──────────────────────────────────────────────

LLM_ENDPOINT = "databricks-claude-opus-4-6"
LLM_TEMPERATURE = 0
LLM_MAX_RETRIES = 3

# ── 5. Benchmark Generation ────────────────────────────────────────────

HELD_OUT_RATIO = 0.15
"""Fraction of non-curated benchmarks reserved for held-out generalization
check in Finalize.  The optimizer never sees these during the lever loop."""

TARGET_BENCHMARK_COUNT = 24
MAX_BENCHMARK_COUNT = 29
"""Hard ceiling on benchmark count.  No evaluation should ever run on more
than this many questions, regardless of how many are generated or loaded.
With HELD_OUT_RATIO=0.15 the train split contains ~20 questions (same as
the previous TARGET_BENCHMARK_COUNT) and ~4 are held out."""

FINALIZE_REPEATABILITY_PASSES = 1
"""Number of repeatability passes in Finalize.  Reduced from 2 to make room
for a held-out generalization eval without increasing total Genie API calls."""

COVERAGE_GAP_SOFT_CAP_FACTOR = 1.5

BENCHMARK_CATEGORIES = [
    "aggregation",
    "ranking",
    "time-series",
    "comparison",
    "detail",
    "list",
    "threshold",
    "multi-table",
]

TEMPLATE_VARIABLES = {
    "${catalog}": "catalog",
    "${gold_schema}": "gold_schema",
}

# ── 5b. Data Profiling ────────────────────────────────────────────────

MAX_PROFILE_TABLES = 20
"""Maximum number of tables to profile during preflight."""

PROFILE_SAMPLE_SIZE = 100
"""Number of rows sampled per table via TABLESAMPLE."""

LOW_CARDINALITY_THRESHOLD = 20
"""Columns with fewer distinct values than this threshold get their actual
distinct values collected (useful for generating realistic filter values)."""

BENCHMARK_GENERATION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space evaluation expert.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Domain: {{ domain }}\n'
    '\n'
    '## VALID Data Assets (ONLY use these in SQL)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Genie Space Instructions\n'
    '{{ instructions_context }}\n'
    '\n'
    '## Sample Questions (from Genie Space config)\n'
    '{{ sample_questions_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate exactly {{ target_count }} diverse benchmark questions that a business user would ask.\n'
    '\n'
    '## Data-Grounded Values\n'
    'Use the Data Profile to generate realistic filter values — reference actual '
    'column values (e.g. WHERE status = \'active\') rather than inventing values. '
    'For numeric columns, use values within the profiled min/max range.\n'
    '\n'
    '## Asset Constraint (Extract-Over-Generate)\n'
    'expected_sql MUST ONLY reference tables, metric views, and functions from VALID Data Assets. '
    'Do NOT invent or hallucinate table/view names. Every FROM, JOIN, and function call must '
    'reference a real asset.\n'
    'required_columns and every column in expected_sql MUST come from the Column Allowlist. '
    'Do NOT invent column names. Before writing SQL, verify every column reference appears in the allowlist.\n'
    '\n'
    '## Metric View Query Rules\n'
    'When writing SQL for metric views:\n'
    '- NEVER use SELECT * — metric views require explicit column references.\n'
    '- ALL measure columns MUST be wrapped in MEASURE() in both SELECT and ORDER BY.\n'
    '  Example: SELECT region, MEASURE(total_revenue) FROM mv_sales GROUP BY region\n'
    '- NEVER use JOINs at query time on metric views — joins are defined in the metric view YAML.\n'
    '- Dimensions (non-measure columns) are used for GROUP BY and filtering only.\n'
    '- The Metric Views section above lists which columns are measures vs dimensions.\n'
    '\n'
    '## Question-SQL Alignment\n'
    '- expected_sql MUST answer EXACTLY what the question asks — no more, no less.\n'
    '- Do NOT add WHERE filters the question does not mention.\n'
    '- Do NOT add extra columns beyond what the question asks for.\n'
    '- Do NOT add JOINs that only serve to add unrequested columns.\n'
    '- If the question is ambiguous about a filter, do NOT assume one.\n'
    '\n'
    '## Minimal SQL Principle\n'
    'Write the simplest correct SQL. Prefer fewer columns and filters. '
    'For "multi-table" category questions, JOINs are expected and encouraged.\n'
    '\n'
    '## Diversity\n'
    'At least 2 questions per category. Include edge cases '
    '(filters, temporal ranges, NULL handling).\n'
    '\n'
    '## Multi-Table Join Coverage\n'
    'At least 3 questions MUST use JOINs across 2+ tables (category: "multi-table").\n'
    'Use the Join Specifications above to determine valid join paths.\n'
    'These questions test whether Genie correctly understands the semantic model relationships.\n'
    'Note: JOINs are for TABLE queries only — metric views MUST NOT use JOINs.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of question objects. No markdown, just JSON.\n'
    '\n'
    'Each object:\n'
    '- "question": natural language question\n'
    '- "expected_sql": correct SQL using fully-qualified names from VALID Data Assets '
    '(metric views: MEASURE() syntax; TVFs: function call; tables: standard SQL)\n'
    '- "expected_asset": "MV" | "TVF" | "TABLE"\n'
    '- "category": one of {{ categories }}\n'
    '- "required_tables": list of table names\n'
    '- "required_columns": list of column names\n'
    '- "expected_facts": 1-2 facts the answer should contain\n'
    '</output_schema>'
)

BENCHMARK_CORRECTION_PROMPT = (
    '<role>\n'
    'You are a Databricks SQL expert fixing invalid benchmark questions.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## VALID Data Assets (ONLY these exist)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '\n'
    '## Benchmarks to Fix\n'
    '{{ benchmarks_to_fix }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Fix each benchmark so expected_sql is valid using ONLY the assets and columns above.\n'
    '\n'
    '- Wrong table/view name: find closest matching valid asset, rewrite SQL.\n'
    '- Field drift (e.g., property_name vs property): map to closest valid column.\n'
    '- Metric views: use MEASURE() syntax for aggregates in SELECT/ORDER BY.\n'
    '- Metric view alias collision: NEVER use ORDER BY alias when alias == source column\n'
    '  for MEASURE() expressions. Use ORDER BY MEASURE(column) directly.\n'
    '- Metric views: NEVER use SELECT * or JOINs at query time. All measures MUST use MEASURE().\n'
    '- TVFs: use correct function call signature.\n'
    '- Multi-table JOINs: use Join Specifications above for valid join paths.\n'
    '- If error says "Query returns 0 rows", the SQL is syntactically valid but\n'
    '  references impossible filter values. Use the Data Profile to pick realistic values.\n'
    '- If no valid asset can answer the question, set expected_sql to null with unfixable_reason.\n'
    '- Preserve original question text.\n'
    '- Apply MINIMAL SQL PRINCIPLE: corrected SQL answers exactly what the question asks.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of objects. No markdown, just JSON.\n'
    '\n'
    'Each object: "question", "expected_sql" (corrected or null), "expected_asset", '
    '"category", "required_tables", "required_columns", "expected_facts", '
    '"unfixable_reason" (null if fixed).\n'
    '</output_schema>'
)

BENCHMARK_ALIGNMENT_CHECK_PROMPT = (
    '<role>\n'
    'You are a Databricks SQL quality reviewer.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Benchmarks to Review\n'
    '{{ benchmarks_json }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Determine whether each benchmark SQL answers EXACTLY what the question asks.\n'
    '\n'
    '## Issue Types\n'
    '- EXTRA_FILTER: SQL adds WHERE conditions not mentioned in the question.\n'
    '- EXTRA_COLUMNS: SQL returns columns the question did not ask for.\n'
    '- MISSING_AGGREGATION: Question implies aggregation but SQL returns unaggregated rows.\n'
    '- WRONG_INTERPRETATION: SQL answers a materially different question.\n'
    '\n'
    '## Strictness\n'
    '- EXTRA_FILTER: Be strict. If question says "revenue by destination" without '
    'mentioning a status, booking_status filters are EXTRA.\n'
    '- EXTRA_COLUMNS: Be lenient. 1-2 contextual columns (e.g., name alongside ID) are OK.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array (one object per benchmark). No markdown, just JSON.\n'
    '\n'
    '{"question": "...", "aligned": true/false, '
    '"issues": ["ISSUE_TYPE: description", ...]}\n'
    '</output_schema>'
)

BENCHMARK_COVERAGE_GAP_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space evaluation expert.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Domain: {{ domain }}\n'
    '\n'
    '## VALID Data Assets (ONLY use these in SQL)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Uncovered Assets (MUST be targeted)\n'
    '{{ uncovered_assets }}\n'
    '\n'
    '## Already Covered Questions (do NOT duplicate these)\n'
    '{{ existing_questions }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '\n'
    '{{ weak_categories_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'The uncovered assets above have ZERO benchmark questions. Generate 1-2 questions '
    'PER uncovered asset. Each question MUST reference the asset in its FROM/JOIN/function call.\n'
    '\n'
    '## Data-Grounded Values\n'
    'Use the Data Profile to generate realistic filter values — reference actual '
    'column values rather than inventing values.\n'
    '\n'
    '## Asset Constraint (Extract-Over-Generate)\n'
    'expected_sql MUST ONLY reference tables, metric views, and functions from VALID Data Assets. '
    'Do NOT invent or hallucinate names.\n'
    'required_columns and every column in expected_sql MUST come from the Column Allowlist. '
    'Do NOT invent column names. Before writing SQL, verify every column reference appears in the allowlist.\n'
    '\n'
    '## Metric View Query Rules\n'
    'When writing SQL for metric views:\n'
    '- NEVER use SELECT * — metric views require explicit column references.\n'
    '- ALL measure columns MUST be wrapped in MEASURE() in both SELECT and ORDER BY.\n'
    '- NEVER use JOINs at query time on metric views.\n'
    '- Dimensions (non-measure columns) are used for GROUP BY and filtering only.\n'
    '\n'
    '## Question-SQL Alignment\n'
    '- expected_sql MUST answer EXACTLY what the question asks — no more, no less.\n'
    '- Do NOT add WHERE filters the question does not mention.\n'
    '- Do NOT add extra columns beyond what the question asks for.\n'
    '- Do NOT add JOINs that only serve to add unrequested columns.\n'
    '\n'
    '## Minimal SQL Principle\n'
    'Write the simplest correct SQL. Prefer fewer columns and filters. '
    'For JOIN PATH items, JOINs are expected.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of question objects. No markdown, just JSON.\n'
    '\n'
    'Each object:\n'
    '- "question": natural language question\n'
    '- "expected_sql": correct SQL using fully-qualified names from VALID Data Assets '
    '(metric views: MEASURE() syntax; TVFs: function call; tables: standard SQL)\n'
    '- "expected_asset": "MV" | "TVF" | "TABLE"\n'
    '- "category": one of {{ categories }}\n'
    '- "required_tables": list of table names\n'
    '- "required_columns": list of column names\n'
    '- "expected_facts": 1-2 facts the answer should contain\n'
    '</output_schema>'
)

# ── 5b. Proposal Generation Prompts ───────────────────────────────────

PROPOSAL_GENERATION_PROMPT = (
    '<role>\n'
    'You are a Databricks metadata optimization expert. Your job is to fix a Genie Space '
    'so that it generates correct SQL for user questions.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Failure Analysis\n'
    '- Root cause: {{ failure_type }}\n'
    '- Blamed objects: {{ blame_set }}\n'
    '- Affected questions ({{ severity }}): {{ affected_questions }}\n'
    '\n'
    '## SQL Diffs (Expected vs Generated)\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Current Metadata for Blamed Objects\n'
    '{{ current_metadata }}\n'
    '\n'
    '## Target Change Type\n'
    '{{ patch_type_description }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Analyze the SQL diffs. Identify EXACTLY what metadata change (column description, '
    'table description, or instruction) would guide Genie to produce the expected SQL.\n'
    '\n'
    '- Be specific — reference actual table/column names from the SQL.\n'
    '- Do NOT generate generic instructions. Generate a targeted metadata fix.\n'
    '- Instruction budget remaining: {{ instruction_char_budget }} chars. Keep additions under 500 chars.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON: {"proposed_value": "...", "rationale": "..."}\n'
    '</output_schema>'
)

LEVER_1_2_COLUMN_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to fix table '
    'and column descriptions and synonyms so that Genie generates correct SQL.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Failure Analysis\n'
    '- Root cause: {{ failure_type }}\n'
    '- Blamed objects: {{ blame_set }}\n'
    '- Affected questions: {{ affected_questions }}\n'
    '\n'
    '## SQL Diffs (Expected vs Generated)\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Full Genie Space Schema\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Structured Table Metadata\n'
    'Tables relevant to the failure. [EDITABLE] sections may be updated; '
    '[LOCKED] sections are owned by another lever — do NOT modify.\n'
    '{{ structured_table_context }}\n'
    '\n'
    '## Structured Column Metadata\n'
    'Columns relevant to the failure. [EDITABLE] may be updated; [LOCKED] must not.\n'
    '{{ structured_column_context }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: wrong_column failure — Genie selects "store_id" instead of "location_id"\n'
    'Blamed: catalog.schema.dim_store.location_id\n'
    'SQL diff: Expected "WHERE ds.location_id = 42" vs Generated "WHERE ds.store_id = 42"\n'
    '\n'
    'Output:\n'
    '{"changes": [\n'
    '  {"table": "catalog.schema.dim_store", "column": "location_id",\n'
    '    "entity_type": "column_key",\n'
    '    "sections": {"synonyms": "store id, store number, store identifier",\n'
    '                  "definition": "Unique numeric identifier for a store location"}}\n'
    '],\n'
    '"table_changes": [],\n'
    '"rationale": "Genie confused store_id (which does not exist) with location_id. '
    'Adding store id as a synonym will resolve the ambiguity."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Propose changes at TWO levels:\n'
    '\n'
    '## Column-level changes\n'
    'For each column that needs fixing, provide ONLY the sections you want to change.\n'
    'Valid section keys: definition, values, synonyms, aggregation, grain_note, '
    'purpose, best_for, grain, scd, join, important_filters.\n'
    '\n'
    '- **synonyms**: comma-separated alternative names. '
    'Existing synonyms are auto-preserved; provide only NEW terms.\n'
    '- **definition**: concise business description of the column.\n'
    '\n'
    '## Table-level changes\n'
    'Provide sections from: purpose, best_for, grain, scd, relationships.\n'
    '\n'
    '## Rules\n'
    '- Only include sections you want to CHANGE. Omit correct sections.\n'
    '- Only update [EDITABLE] sections. Never touch [LOCKED] sections.\n'
    '- AUGMENT existing content — incorporate existing info and add new details. '
    'Only rewrite from scratch if current value is empty or misleading.\n'
    '- If a column description is correct, prefer adding synonyms.\n'
    '- Do NOT repeat synonyms already in the metadata.\n'
    '- Be specific — reference actual table/column names from the SQL diffs.\n'
    '- You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Any name not in the allowlist is INVALID and will be rejected.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object. No analysis or commentary — put reasoning in "rationale".\n'
    '\n'
    '{"changes": [\n'
    '  {"table": "<fully_qualified_table>", "column": "<column_name>",\n'
    '    "entity_type": "<column_dim|column_measure|column_key>",\n'
    '    "sections": {"definition": "new value", "synonyms": "term1, term2"}}\n'
    '],\n'
    '"table_changes": [\n'
    '  {"table": "<fully_qualified_table>",\n'
    '    "sections": {"purpose": "...", "best_for": "...", "grain": "..."}}\n'
    '],\n'
    '"rationale": "..."}\n'
    '</output_schema>'
)

DESCRIPTION_ENRICHMENT_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to write '
    'concise, accurate column descriptions for columns that currently have NO '
    'description at all — neither in Unity Catalog nor in the Genie Space.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables and Columns Needing Descriptions\n'
    '{{ columns_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Data Profile (actual values from sampled data)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input:\n'
    'Table: catalog.schema.fact_orders (Purpose: Transactional order data)\n'
    '  Columns needing descriptions:\n'
    '    - order_amount (DOUBLE) [column_measure] — cardinality=4500, range=[0.01, 99999.99]\n'
    '    - customer_id (BIGINT) [column_key] — cardinality=12000\n'
    '    - fulfillment_status (STRING) [column_dim] — cardinality=5, '
    "values=['pending', 'shipped', 'delivered', 'cancelled', 'returned']\n"
    '  Sibling columns (for context): order_id, order_date, ship_date, region\n'
    '\n'
    'Output:\n'
    '{"changes": [\n'
    '  {"table": "catalog.schema.fact_orders", "column": "order_amount",\n'
    '    "entity_type": "column_measure",\n'
    '    "sections": {"definition": "Total monetary value of the order in USD",\n'
    '                  "aggregation": "SUM for revenue totals, AVG for average order value"}},\n'
    '  {"table": "catalog.schema.fact_orders", "column": "customer_id",\n'
    '    "entity_type": "column_key",\n'
    '    "sections": {"definition": "Foreign key referencing the customer dimension",\n'
    '                  "join": "Joins to dim_customer.customer_id"}},\n'
    '  {"table": "catalog.schema.fact_orders", "column": "fulfillment_status",\n'
    '    "entity_type": "column_dim",\n'
    '    "sections": {"definition": "Current fulfillment state of the order",\n'
    '                  "values": "pending, shipped, delivered, cancelled, returned"}}\n'
    '],\n'
    '"rationale": "Used data profile values for fulfillment_status and range for order_amount."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'For each column listed, generate structured description sections appropriate '
    'to its entity type:\n'
    '- column_dim: definition, values, synonyms\n'
    '- column_measure: definition, aggregation, grain_note, synonyms\n'
    '- column_key: definition, join, synonyms\n'
    '\n'
    'Rules:\n'
    '- Infer meaning from: column name, data type, table purpose, sibling columns, '
    'and data profile (cardinality, sample values, ranges).\n'
    '- Be CONCISE — one sentence per section. Do NOT repeat information across sections.\n'
    '- For "values": use actual distinct values from the data profile when available; '
    'otherwise only list if confidently inferrable from the column name '
    '(e.g. status, type, category columns). OMIT the values section when uncertain.\n'
    '- For "synonyms": provide alternative names a user might type when querying.\n'
    '- For "join": specify which table.column this key likely joins to, based on '
    'naming conventions and sibling context.\n'
    '- If you cannot confidently infer a meaningful description, write '
    '"General-purpose [data_type] column" for the definition and OMIT other sections.\n'
    '- You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Do NOT invent table or column names.\n'
    '- Do NOT include sections you are uncertain about — omit them entirely.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object:\n'
    '{"changes": [\n'
    '  {"table": "<fully_qualified_table>", "column": "<column_name>",\n'
    '    "entity_type": "<column_dim|column_measure|column_key>",\n'
    '    "sections": {"definition": "...", "values": "..."}}\n'
    '],\n'
    '"rationale": "..."}\n'
    '</output_schema>'
)

TABLE_DESCRIPTION_ENRICHMENT_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to write '
    'concise, structured table descriptions for tables that currently have NO '
    'useful description — neither in Unity Catalog nor in the Genie Space.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables Needing Descriptions\n'
    '{{ tables_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Data Profile (actual values from sampled data)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input:\n'
    'Table: catalog.schema.fact_orders\n'
    '  Current description: (none)\n'
    '  Row count: ~250000\n'
    '  Columns: order_id (BIGINT), customer_id (BIGINT), order_date (DATE), '
    'amount (DOUBLE), status (STRING), region (STRING)\n'
    '  Data profile: status: values=[\'pending\', \'shipped\', \'delivered\']; '
    'region: values=[\'US\', \'EU\', \'APAC\']\n'
    '\n'
    'Output:\n'
    '{"changes": [\n'
    '  {"table": "catalog.schema.fact_orders",\n'
    '    "sections": {\n'
    '      "purpose": "Transactional order data with one row per order",\n'
    '      "best_for": "Revenue analysis, order volume trends, fulfillment tracking",\n'
    '      "grain": "One row per order (order_id)",\n'
    '      "scd": "Append-only fact table; status column reflects latest state"}}\n'
    '],\n'
    '"rationale": "Inferred grain from ~250K rows and order_id key; used profile for context."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'For each table listed, generate structured description sections:\n'
    '- purpose: One sentence describing what data the table holds\n'
    '- best_for: Comma-separated list of analytics this table supports\n'
    '- grain: What one row represents (include the key column)\n'
    '- scd: Slowly changing dimension type or update pattern (append-only, '
    'Type 2, snapshot, etc). OMIT if not inferrable.\n'
    '\n'
    'Rules:\n'
    '- Infer meaning from: table name, column names and types, row counts, '
    'column value distributions from the data profile, and metric view context.\n'
    '- Be CONCISE — one sentence per section. Do NOT repeat information across sections.\n'
    '- If a table already has a short description, incorporate it but expand.\n'
    '- You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Do NOT invent table or column names.\n'
    '- If you cannot confidently infer the table purpose, write '
    '"General-purpose data table" for purpose and OMIT other sections.\n'
    '- Do NOT include sections you are uncertain about — omit them entirely.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object:\n'
    '{"changes": [\n'
    '  {"table": "<fully_qualified_table>",\n'
    '    "sections": {"purpose": "...", "best_for": "...", "grain": "..."}}\n'
    '],\n'
    '"rationale": "..."}\n'
    '</output_schema>'
)

# ── Proactive Space Metadata Prompts ──────────────────────────────────

SPACE_DESCRIPTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to write '
    'a concise, structured description for a Genie Space that has NO '
    'description yet. The description helps users understand what data is '
    'available and what questions they can ask.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Existing Instructions\n'
    '{{ instructions_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Write a plain-text Genie Space description (150-300 words) with these '
    'ALL-CAPS sections:\n'
    '\n'
    'DATA COVERAGE:\n'
    '- Bullet points summarising the tables and domains covered\n'
    '- Include approximate entity counts if inferrable from table names\n'
    '\n'
    'AVAILABLE ANALYTICS:\n'
    '1. Numbered categories of analyses the data supports\n'
    '\n'
    'USE CASES:\n'
    '- Role-based use case bullets (e.g. "Sales managers (regional tracking)")\n'
    '\n'
    'TIME PERIODS:\n'
    '- Temporal coverage and supported granularities\n'
    '\n'
    'Rules:\n'
    '- Infer the domain from table names, column names, and metric views.\n'
    '- Do NOT invent data that is not represented in the schema.\n'
    '- Do NOT use Markdown (no #, **, ```, etc.).\n'
    '- Use plain bullet points (- or numbered lists) only.\n'
    '- Keep it factual and concise.\n'
    '- Start with a single sentence summarising the space before the sections.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY the description text — no JSON wrapper, no code fences.\n'
    '</output_schema>'
)

PROACTIVE_INSTRUCTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space configuration expert. Your job is to '
    'write a concise set of routing instructions for a Genie Space that has '
    'NO instructions yet. These instructions help Genie correctly route '
    'user questions to the right tables and generate accurate SQL.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Join Specifications\n'
    '{{ join_specs_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Write plain-text instructions (500-1500 characters) using ALL-CAPS SECTION HEADERS with a colon.\n'
    'Use these canonical sections (omit empty ones):\n'
    '\n'
    'PURPOSE:\n'
    '- What this space does and who it serves (1 paragraph)\n'
    '\n'
    'ASSET ROUTING:\n'
    '- Which table(s) to use for which topic/entity\n'
    '\n'
    'TEMPORAL FILTERS:\n'
    '- Default date filters, fiscal year logic, time-range rules\n'
    '\n'
    'DATA QUALITY NOTES:\n'
    '- NULL handling, is_current flags, data caveats\n'
    '\n'
    'JOIN GUIDANCE:\n'
    '- Key join patterns if join specs exist\n'
    '\n'
    'Rules:\n'
    '- Be factual — infer ONLY from the schema provided.\n'
    '- Do NOT invent business rules not evident in column names or descriptions.\n'
    '- Use short, imperative sentences.\n'
    '- Do NOT use Markdown formatting (no #, **, ```, etc.).\n'
    '- Keep total output between 500 and 1500 characters.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY the instruction text — no JSON wrapper, no code fences.\n'
    '</output_schema>'
)

SAMPLE_QUESTIONS_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to '
    'generate sample questions that users can click on to quickly explore '
    'the data available in a Genie Space.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Space Description\n'
    '{{ description_context }}\n'
    '\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Existing Instructions\n'
    '{{ instructions_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate 5-8 sample questions that:\n'
    '- Cover different tables and metric views (spread across the schema)\n'
    '- Mix query patterns: aggregation, filtering, ranking, time-based trends\n'
    '- Are phrased in natural language (NOT SQL)\n'
    '- Reference real column names and concepts from the schema\n'
    '- Are answerable with the available data\n'
    '- Vary in complexity (some simple, some multi-dimensional)\n'
    '\n'
    'Rules:\n'
    '- Each question should be a single sentence.\n'
    '- Do NOT generate questions requiring data not in the schema.\n'
    '- Do NOT duplicate questions that ask the same thing differently.\n'
    '- Prefer questions that showcase the most useful analytics.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object:\n'
    '{"questions": ["question 1", "question 2", ...],\n'
    ' "rationale": "Brief explanation of coverage choices"}\n'
    '</output_schema>'
)

LEVER_4_JOIN_SPEC_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space join optimization expert.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## SQL Diffs showing join issues\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Current Join Specs\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Table Relationships\n'
    '{{ table_relationships }}\n'
    '\n'
    '## Full Schema Context (tables, columns, data types, descriptions)\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: Expected SQL joins fact_orders to dim_customer on customer_key, '
    'but Generated SQL has no join — just queries fact_orders alone.\n'
    '\n'
    'Output:\n'
    '{"join_spec": {\n'
    '  "left": {"identifier": "catalog.schema.fact_orders", "alias": "fact_orders"},\n'
    '  "right": {"identifier": "catalog.schema.dim_customer", "alias": "dim_customer"},\n'
    '  "sql": ["`fact_orders`.`customer_key` = `dim_customer`.`customer_key`", '
    '"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]\n'
    '}, "rationale": "fact_orders references dim_customer via customer_key (both BIGINT). '
    'The generated SQL could not resolve customer name without this join."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Analyze the SQL diffs to determine which tables need to be joined and how. '
    'Compare expected SQL JOIN clauses with generated SQL to identify missing or '
    'incorrect join specifications.\n'
    '\n'
    '## Data Type Rule\n'
    'Join columns MUST have compatible data types. Check column types in the schema '
    'context before proposing a join. Joining INT to STRING is invalid.\n'
    '\n'
    '## Identifier Rule\n'
    'You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Any name not in the allowlist is INVALID and will be rejected.\n'
    '\n'
    '## Join Spec Format\n'
    '- alias: unqualified table name (last segment of identifier)\n'
    '- join condition: backtick-quoted aliases, e.g. '
    '"`fact_sales`.`product_key` = `dim_product`.`product_key`"\n'
    '- relationship_type: one of FROM_RELATIONSHIP_TYPE_MANY_TO_ONE, '
    'FROM_RELATIONSHIP_TYPE_ONE_TO_MANY, FROM_RELATIONSHIP_TYPE_ONE_TO_ONE\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON:\n'
    '{"join_spec": {\n'
    '  "left": {"identifier": "<fully_qualified_table>", "alias": "<short_name>"},\n'
    '  "right": {"identifier": "<fully_qualified_table>", "alias": "<short_name>"},\n'
    '  "sql": ["<join_condition>", "--rt=<relationship_type>--"]\n'
    '}, "rationale": "..."}\n'
    '</output_schema>'
)

LEVER_4_JOIN_DISCOVERY_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space join optimization expert. '
    'Your task is to identify MISSING join relationships between tables.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Full Schema Context (tables, columns, data types, descriptions)\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Currently Defined Join Specs\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Heuristic Candidate Hints\n'
    'Table pairs flagged by automated analysis as potential join candidates. '
    'These are HINTS only — validate them using the schema context.\n'
    '{{ discovery_hints }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input hint: "fact_sales and dim_region share region_id columns (both INT)"\n'
    'Current join specs: [fact_sales↔dim_product]\n'
    '\n'
    'Output:\n'
    '{"join_specs": [\n'
    '  {"left": {"identifier": "catalog.schema.fact_sales", "alias": "fact_sales"},\n'
    '    "right": {"identifier": "catalog.schema.dim_region", "alias": "dim_region"},\n'
    '    "sql": ["`fact_sales`.`region_id` = `dim_region`.`region_id`", '
    '"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]}\n'
    '], "rationale": "Both tables have region_id (INT). fact_sales has many rows per region. '
    'This join is not already defined."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Review hints alongside the full schema context. For each hint, validate:\n'
    '1. Column data types MUST be compatible (INT=INT, BIGINT=INT, STRING=STRING). '
    'Do NOT propose incompatible type joins.\n'
    '2. Column names/descriptions suggest a foreign-key relationship.\n'
    '3. The join is not already defined in current join specs.\n'
    '\n'
    'Also look for additional missing joins NOT covered by the hints.\n'
    '\n'
    '## Identifier Rule\n'
    'You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Any table or column not in the allowlist is INVALID and will be rejected.\n'
    '\n'
    '## Join Spec Format\n'
    '- alias: unqualified table name (last segment of identifier)\n'
    '- join condition: backtick-quoted aliases\n'
    '- relationship_type: one of FROM_RELATIONSHIP_TYPE_MANY_TO_ONE, '
    'FROM_RELATIONSHIP_TYPE_ONE_TO_MANY, FROM_RELATIONSHIP_TYPE_ONE_TO_ONE\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON:\n'
    '{"join_specs": [\n'
    '  {"left": {"identifier": "<fq_table>", "alias": "<short_name>"},\n'
    '    "right": {"identifier": "<fq_table>", "alias": "<short_name>"},\n'
    '    "sql": ["<join_condition>", "--rt=<relationship_type>--"]}\n'
    '], "rationale": "..."}\n'
    '\n'
    'If no valid joins found: {"join_specs": [], "rationale": "..."}\n'
    '</output_schema>'
)

# ── Canonical Instruction Section Vocabulary ──────────────────────────
# Sections are aligned to levers so each lever's instruction_contribution
# naturally reinforces its primary fix in the corresponding section(s).

INSTRUCTION_SECTION_ORDER: list[str] = [
    "PURPOSE",
    "ASSET ROUTING",
    "BUSINESS DEFINITIONS",
    "DISAMBIGUATION",
    "AGGREGATION RULES",
    "FUNCTION ROUTING",
    "JOIN GUIDANCE",
    "QUERY RULES",
    "QUERY PATTERNS",
    "TEMPORAL FILTERS",
    "DATA QUALITY NOTES",
    "CONSTRAINTS",
]

LEVER_TO_SECTIONS: dict[int, list[str]] = {
    1: ["BUSINESS DEFINITIONS", "DISAMBIGUATION"],
    2: ["AGGREGATION RULES", "TEMPORAL FILTERS"],
    3: ["FUNCTION ROUTING"],
    4: ["JOIN GUIDANCE", "TEMPORAL FILTERS"],
    5: ["ASSET ROUTING", "QUERY RULES", "QUERY PATTERNS", "DATA QUALITY NOTES", "CONSTRAINTS"],
}

INSTRUCTION_FORMAT_RULES = (
    'The output is rendered as PLAIN TEXT, not Markdown.\n'
    'Use ALL-CAPS SECTION HEADERS followed by a colon. '
    'Use - for bullet points. Use blank lines between sections.\n'
    'Do NOT use Markdown syntax (no ##, no **, no backticks, no code fences).\n'
    '\n'
    'Canonical sections (use ONLY these headers, in this order, omit empty ones):\n'
    'PURPOSE:             What this Genie Space does and who it serves (1 paragraph)\n'
    'ASSET ROUTING:       When user asks about [topic], use [table/TVF/MV] (Lever 5)\n'
    'BUSINESS DEFINITIONS: [term] = [column] from [table] (Lever 1)\n'
    'DISAMBIGUATION:      When [ambiguous scenario], prefer [approach] (Lever 1)\n'
    'AGGREGATION RULES:   How to aggregate measures, grain rules, avoid double-counting (Lever 2)\n'
    'FUNCTION ROUTING:    When to use TVFs/UDFs vs raw tables, parameter guidance (Lever 3)\n'
    'JOIN GUIDANCE:       Explicit join paths and conditions (Lever 4)\n'
    'QUERY RULES:         SQL-level rules — filters, ordering, limits\n'
    'QUERY PATTERNS:      Common multi-step query patterns with actual column names\n'
    'TEMPORAL FILTERS:    Date partitioning, SCD filters, time-range rules (Lever 2/4)\n'
    'DATA QUALITY NOTES:  Known nulls, is_current flags, data caveats\n'
    'CONSTRAINTS:         Cross-cutting behavioral constraints, output formatting\n'
    '\n'
    'Lever-to-section alignment (target your contribution to these sections):\n'
    '  Lever 1 -> BUSINESS DEFINITIONS, DISAMBIGUATION\n'
    '  Lever 2 -> AGGREGATION RULES, TEMPORAL FILTERS\n'
    '  Lever 3 -> FUNCTION ROUTING\n'
    '  Lever 4 -> JOIN GUIDANCE, TEMPORAL FILTERS\n'
    '  Lever 5 -> ASSET ROUTING, QUERY RULES, QUERY PATTERNS, DATA QUALITY NOTES, CONSTRAINTS\n'
    '\n'
    'Non-regressive rules:\n'
    '- INCORPORATE all existing guidance into structured sections.\n'
    '- Do NOT discard existing instructions unless factually wrong.\n'
    '- AUGMENT each section with new learnings.\n'
    '- EVERY bullet must reference a specific asset (table, column, function).\n'
    '- NEVER include generic domain guidance without referencing an actual asset.\n'
)

LEVER_5_INSTRUCTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space instruction expert.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## SQL Diffs showing routing/disambiguation issues\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Current Text Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL Queries\n'
    '{{ existing_example_sqls }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: Routing failure — Genie queries fact_bookings table instead of '
    'calling get_booking_summary TVF for "What is the booking summary?"\n'
    '\n'
    'Output:\n'
    '{"instruction_type": "example_sql",\n'
    '  "example_question": "Show me the booking summary",\n'
    '  "example_sql": "SELECT * FROM catalog.schema.get_booking_summary(:start_date, :end_date)",\n'
    '  "parameters": [{"name": "start_date", "type_hint": "DATE", "default_value": "2024-01-01"},\n'
    '                  {"name": "end_date", "type_hint": "DATE", "default_value": "2024-12-31"}],\n'
    '  "usage_guidance": "Use when user asks about booking summaries or booking overview",\n'
    '  "rationale": "Routing failure: Genie should call get_booking_summary TVF, not query fact_bookings. '
    'Example SQL teaches Genie the correct pattern."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Analyze the SQL diffs and choose the HIGHEST-PRIORITY instruction type.\n'
    '\n'
    '## Instruction Type Priority (MUST follow this hierarchy)\n'
    '1. **SQL expressions** — For business metric/filter/dimension definitions. '
    'Choose ONLY when earlier levers missed a column-level semantic definition.\n'
    '2. **Example SQL queries** — For ambiguous, multi-part, or complex question patterns. '
    'Genie pattern-matches these to learn query patterns.\n'
    '3. **Text instructions** — LAST RESORT for clarification, formatting, cross-cutting guidance.\n'
    '\n'
    '## Routing Failures MUST Use Example SQL\n'
    'When failure involves asset routing (wrong table/TVF/MV), return instruction_type '
    '"example_sql". Example SQL is far more effective for routing — Genie matches it directly.\n'
    '\n'
    '## Rules for Text Instructions\n'
    '- Use ALL-CAPS HEADERS with colon, - bullets, short lines. No Markdown (no ##, no **, no backticks).\n'
    '- Use ONLY these section headers: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '- EVERY instruction MUST reference a specific asset from Available Assets.\n'
    '- NEVER generate generic domain guidance.\n'
    '- NEVER conflict with existing instructions.\n'
    '- Budget: {{ instruction_char_budget }} chars.\n'
    '\n'
    '## Rules for Example SQL\n'
    '- Question must be a realistic user prompt matching the failure pattern.\n'
    '- SQL must be correct and executable.\n'
    '- Every FROM table, JOIN table, column reference, and function call MUST appear in the Identifier Allowlist.\n'
    '- Do NOT duplicate existing example SQL questions.\n'
    '- Use `:param_name` markers for user-variable filters. '
    'For each parameter: name, type_hint (STRING|INTEGER|DATE|DECIMAL), default_value.\n'
    '- Include usage_guidance describing when Genie should match this query.\n'
    '\n'
    '## Anti-Hallucination Guard\n'
    'You MUST ONLY use identifiers from the Identifier Allowlist. '
    'Any table, column, or function not in the allowlist is INVALID and will be rejected.\n'
    'If you cannot identify a specific asset to reference, return:\n'
    '{"instruction_type": "text_instruction", "instruction_text": "", '
    '"rationale": "No actionable fix identified"}\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON with one of these formats:\n'
    '\n'
    'example_sql:\n'
    '{"instruction_type": "example_sql", "example_question": "...", "example_sql": "...", '
    '"parameters": [{"name": "...", "type_hint": "STRING|INTEGER|DATE|DECIMAL", '
    '"default_value": "..."}], '
    '"usage_guidance": "...", "rationale": "..."}\n'
    '\n'
    'text_instruction:\n'
    '{"instruction_type": "text_instruction", "instruction_text": "...", "rationale": "..."}\n'
    '\n'
    'sql_expression:\n'
    '{"instruction_type": "sql_expression", "target_table": "...", "target_column": "...", '
    '"expression": "...", "rationale": "..."}\n'
    '</output_schema>'
)

LEVER_5_HOLISTIC_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space instruction architect. '
    'Synthesize ALL evaluation learnings into a single, coherent instruction document '
    'plus targeted example SQL queries.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Genie Space Purpose\n'
    '{{ space_description }}\n'
    '\n'
    '## Evaluation Summary\n'
    '{{ eval_summary }}\n'
    '\n'
    '## Failure Clusters from Evaluation\n'
    'Clusters group related failures by root cause and blamed objects. '
    '"Correct-but-Suboptimal" clusters produced correct results but used fragile approaches — '
    'use for best-practice guidance, not fixes.\n'
    '{{ cluster_briefs }}\n'
    '\n'
    '## Changes Already Applied by Earlier Levers\n'
    'Levers 1-4 applied these fixes. Your instructions should COMPLEMENT, not duplicate them.\n'
    '{{ lever_summary }}\n'
    '\n'
    '## Current Text Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL Queries\n'
    '{{ existing_example_sqls }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: 3 failure clusters — routing errors for booking queries going to wrong tables, '
    'missing temporal filters on date-partitioned tables.\n'
    '\n'
    'Output:\n'
    '{\n'
    '  "instruction_text": "PURPOSE:\\nThis Genie Space covers hotel booking analytics.\\n\\n'
    'ASSET ROUTING:\\n- Booking summaries: use catalog.schema.get_booking_summary TVF\\n'
    '- Detailed bookings: use catalog.schema.fact_bookings\\n\\n'
    'FUNCTION ROUTING:\\n- For booking summaries, use get_booking_summary TVF with start_date and end_date params\\n\\n'
    'TEMPORAL FILTERS:\\n- Always filter fact_bookings by booking_date for performance\\n\\n'
    'DATA QUALITY NOTES:\\n- Use is_current = true when joining dim_hotel",\n'
    '  "example_sql_proposals": [\n'
    '    {\n'
    '      "example_question": "Show me booking summary for last quarter",\n'
    '      "example_sql": "SELECT * FROM catalog.schema.get_booking_summary(:start_date, :end_date)",\n'
    '      "parameters": [{"name": "start_date", "type_hint": "DATE", "default_value": "2024-01-01"}],\n'
    '      "usage_guidance": "Use for booking summary and overview questions"\n'
    '    },\n'
    '    {\n'
    '      "example_question": "What is the average booking revenue by hotel this year?",\n'
    '      "example_sql": "SELECT dh.hotel_name, AVG(fb.revenue) AS avg_revenue '
    'FROM catalog.schema.fact_bookings fb '
    'JOIN catalog.schema.dim_hotel dh ON fb.hotel_key = dh.hotel_key '
    'WHERE dh.is_current = true AND fb.booking_date >= DATE_TRUNC(\'year\', CURRENT_DATE()) '
    'GROUP BY 1 ORDER BY 2 DESC",\n'
    '      "parameters": [],\n'
    '      "usage_guidance": "Use for hotel revenue aggregations with temporal filters"\n'
    '    },\n'
    '    {\n'
    '      "example_question": "List all bookings missing a check-in date",\n'
    '      "example_sql": "SELECT fb.booking_id, fb.guest_name, fb.booking_date '
    'FROM catalog.schema.fact_bookings fb WHERE fb.checkin_date IS NULL",\n'
    '      "parameters": [],\n'
    '      "usage_guidance": "Use for data quality queries about missing fields"\n'
    '    }\n'
    '  ],\n'
    '  "rationale": "Routing errors fixed via example SQL; temporal filter and join pattern examples added."\n'
    '}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    '## 1. Instruction Document (STRUCTURED REWRITE)\n'
    '\n'
    '### Structured Format and Non-Regressive Rewrite Rules\n'
    + INSTRUCTION_FORMAT_RULES +
    '- Target 30-80 lines. Prefer bullets over paragraphs.\n'
    '- Budget: {{ instruction_char_budget }} chars MAXIMUM.\n'
    '- Omit sections with no actionable content.\n'
    '\n'
    '## 2. Example SQL Proposals\n'
    'For any recurring failure pattern (routing, aggregation, temporal, join, filter, disambiguation), '
    'propose example SQL. Genie pattern-matches against it directly. '
    'Aim for 3-5 proposals covering distinct failure patterns.\n'
    '\n'
    '- Question must be a realistic user prompt matching a failure pattern.\n'
    '- Every FROM table, JOIN table, column reference, and function call MUST appear in the Identifier Allowlist.\n'
    '- Do NOT duplicate existing example SQL questions.\n'
    '- Use `:param_name` markers for user-variable filters.\n'
    '- Include usage_guidance for when Genie should match this query.\n'
    '- Propose at least one example SQL per distinct failure cluster when a valid SQL pattern exists.\n'
    '\n'
    '## Anti-Hallucination Guard\n'
    'You MUST ONLY use identifiers from the Identifier Allowlist. '
    'Any table, column, or function not in the allowlist is INVALID and will be rejected.\n'
    'If you cannot identify specific assets or evaluation failures to address, '
    'return empty instruction_text and no example SQL.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a single JSON object:\n'
    '{\n'
    '  "instruction_text": "PURPOSE:\\n...",\n'
    '  "example_sql_proposals": [\n'
    '    {\n'
    '      "example_question": "What is the total revenue by destination?",\n'
    '      "example_sql": "SELECT ...",\n'
    '      "parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}],\n'
    '      "usage_guidance": "Use when user asks about revenue breakdown by destination"\n'
    '    }\n'
    '  ],\n'
    '  "rationale": "Explanation of key changes made and why"\n'
    '}\n'
    '\n'
    'Always propose at least one example SQL per distinct failure cluster if a valid SQL pattern exists. '
    'Only set "example_sql_proposals" to [] when there are truly no actionable failure patterns.\n'
    'If no instruction changes needed, set "instruction_text" to "".\n'
    '</output_schema>'
)

# ── 5b. Holistic Strategist Prompt ────────────────────────────────────

STRATEGIST_PROMPT = (
    '<role>\n'
    'You are the Holistic Strategist for a Databricks Genie Space optimization framework.\n'
    '</role>\n'
    '\n'
    '<instructions>\n'
    '## Purpose\n'
    'Analyze ALL benchmark evaluation failures and produce a coordinated multi-lever '
    'action plan. Each action group addresses one root cause across multiple levers '
    'simultaneously, ensuring fixes reinforce each other rather than conflict.\n'
    '\n'
    '## When to create an action group\n'
    '- Systematic failure pattern (wrong column, missing join, wrong aggregation, etc.)\n'
    '- Correct-but-suboptimal soft signal suggesting preventive improvement\n'
    '\n'
    '## When NOT to create an action group\n'
    '- Format-only differences (extra_columns_only, select_star, format_difference)\n'
    '- Cannot identify a specific table, column, join, or instruction to change\n'
    '\n'
    '## Contract: All Instruments of Power\n'
    'For each root cause, specify EVERY lever that should act:\n'
    '- wrong_column / wrong_table / missing_synonym: Primary Lever 1, also Lever 5\n'
    '- wrong_aggregation / missing_filter: Primary Lever 2, also Lever 5\n'
    '- tvf_parameter_error: Primary Lever 3, also Lever 5\n'
    '- wrong_join / missing_join_spec: Primary Lever 4, also Lever 1 + 5\n'
    '- asset_routing_error / ambiguous_question: Primary Lever 5, also Lever 1\n'
    '\n'
    '## Contract: Structured Metadata Format\n'
    'ALL metadata changes MUST use structured sections.\n'
    'Tables: purpose, best_for, grain, scd, relationships.\n'
    'Columns by type: column_dim (definition, values, synonyms), '
    'column_measure (definition, aggregation, grain_note, synonyms), '
    'column_key (definition, join, synonyms).\n'
    'Functions: purpose, best_for, use_instead_of, parameters, example.\n'
    'Use section KEYS, not labels.\n'
    'Each section must be a SEPARATE key. Do NOT embed section headers inside another '
    "section's value — updates with embedded headers will be REJECTED.\n"
    'WRONG (rejected): {"purpose": "Fact table. BEST FOR: Duration analysis. GRAIN: One row per run"}\n'
    'CORRECT: {"purpose": "Fact table.", "best_for": "Duration analysis.", "grain": "One row per run"}\n'
    '\n'
    '## Contract: Non-Regressive / Augment-Not-Overwrite\n'
    '- INCORPORATE existing content and ADD new details. Only replace if empty or wrong.\n'
    '- Existing synonyms are auto-preserved; propose only NEW terms.\n'
    '- global_instruction_rewrite uses section-level upsert: only include sections you change.\n'
    '  Omitted sections are preserved automatically.\n'
    '\n'
    '## Contract: No Cross-Action-Group Conflicts\n'
    'Two AGs MUST NOT touch the same object. Merge overlapping failures into ONE AG.\n'
    '\n'
    '## Contract: Coordination Notes Are Mandatory\n'
    'Each AG must explain how changes across levers reference each other.\n'
    '\n'
    '## Contract: Example SQL for Routing/Ambiguity\n'
    'Routing failures MUST include example_sql in Lever 5.\n'
    '\n'
    '## Contract: Anti-Hallucination\n'
    'Every change must cite cluster_id(s). No invented root causes.\n'
    '\n'
    '## Contract: Instruction Format (Plain Text, Structured Sections)\n'
    + INSTRUCTION_FORMAT_RULES +
    '</instructions>\n'
    '\n'
    '<context>\n'
    '## Genie Space Schema\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Failure Clusters\n'
    '{{ cluster_briefs }}\n'
    '\n'
    '## Soft Signal Clusters\n'
    '{{ soft_signal_summary }}\n'
    '\n'
    '## Current Structured Metadata\n'
    '### Tables\n'
    '{{ structured_table_context }}\n'
    '### Columns\n'
    '{{ structured_column_context }}\n'
    '### Functions\n'
    '{{ structured_function_context }}\n'
    '\n'
    '## Current Join Specifications\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Current Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL\n'
    '{{ existing_example_sqls }}\n'
    '\n'
    '## Data Values for Blamed Columns\n'
    '{{ blamed_column_values }}\n'
    '</context>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON structure:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG<N>",\n'
    '      "root_cause_summary": "<one sentence>",\n'
    '      "source_cluster_ids": ["C001"],\n'
    '      "affected_questions": ["<question_id>"],\n'
    '      "priority": 1,\n'
    '      "lever_directives": {\n'
    '        "1": {"tables": [{"table": "<fq_name>", "entity_type": "table", '
    '"sections": {"<key>": "<value>"}}\n'
    '              ], "columns": [{"table": "<fq_name>", "column": "<col>", '
    '"entity_type": "<column_dim|column_measure|column_key>", '
    '"sections": {"<key>": "<value>"}}]},\n'
    '        "4": {"join_specs": [{"left_table": "<fq>", "right_table": "<fq>", '
    '"join_guidance": "<condition + type>"}]},\n'
    '        "5": {"instruction_guidance": "<text>", "example_sqls": ['
    '{"question": "<prompt>", "sql_sketch": "<SQL>", '
    '"parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}], '
    '"usage_guidance": "<when to match>"}]}\n'
    '      },\n'
    '      "coordination_notes": "<how levers reference each other>"\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_rewrite": {\n'
    '    "PURPOSE": "One paragraph describing what this Genie Space does.",\n'
    '    "ASSET ROUTING": "- When user asks about [topic], use [table/TVF/MV]",\n'
    '    "TEMPORAL FILTERS": "- Use run_date >= DATE_SUB(CURRENT_DATE(), N) for last-N-day queries"\n'
    '  },\n'
    '  "rationale": "<overall reasoning>"\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- "lever_directives" keys "1"-"5". Only include levers with work to do.\n'
    '- "sections" keys from structured metadata schema.\n'
    '- Lever 2 uses same column format as Lever 1. Lever 3: {"functions": [...]}.\n'
    '- global_instruction_rewrite: a JSON OBJECT mapping section headers to content.\n'
    '  Keys MUST be from: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '  Only include sections you want to ADD or REPLACE. Omitted sections are PRESERVED unchanged.\n'
    '  Values are plain-text bullet lists (no Markdown). Empty string means delete the section.\n'
    '- priority 1 = most impactful. Order by affected question count.\n'
    '- If no actionable improvements:\n'
    '  {"action_groups": [], "global_instruction_rewrite": {}, "rationale": "No actionable failures"}\n'
    '</output_schema>'
)

# ── 5c. Two-Phase Strategist Prompts ──────────────────────────────────

STRATEGIST_TRIAGE_PROMPT = (
    '<role>\n'
    'You are the Triage Strategist for a Databricks Genie Space optimization framework.\n'
    '</role>\n'
    '\n'
    '<instructions>\n'
    '## Purpose\n'
    'Analyze ALL failure clusters and produce action group SKELETONS. Each skeleton '
    'identifies one root cause, which clusters it covers, which levers should act, '
    'and which tables/columns are the focus. A separate detail phase produces the '
    'actual lever directives — your job is ONLY grouping and scoping.\n'
    '\n'
    '## When to create an action group\n'
    '- Systematic failure pattern (wrong column, missing join, wrong aggregation)\n'
    '- Correct-but-suboptimal soft signal\n'
    '- Merge clusters that blame the SAME objects into ONE action group\n'
    '\n'
    '## When NOT to create\n'
    '- Format-only differences (extra_columns_only, select_star, format_difference)\n'
    '- Cannot identify a specific table, column, join, or instruction to change\n'
    '\n'
    '## Lever Capabilities\n'
    '- Lever 1: Table/column descriptions, synonyms\n'
    '- Lever 2: Metric view column descriptions, synonyms\n'
    '- Lever 3: Function descriptions and parameter documentation\n'
    '- Lever 4: Join specifications between tables\n'
    '- Lever 5: Instructions + example SQL queries\n'
    '\n'
    '## Lever Mapping (All Instruments of Power)\n'
    '- wrong_column / wrong_table / missing_synonym: Primary Lever 1, also Lever 5\n'
    '- wrong_aggregation / missing_filter: Primary Lever 2, also Lever 5\n'
    '- tvf_parameter_error: Primary Lever 3, also Lever 5\n'
    '- wrong_join / missing_join_spec: Primary Lever 4, also Lever 1 + 5\n'
    '- asset_routing_error / ambiguous_question: Primary Lever 5, also Lever 1\n'
    '\n'
    '## Contracts\n'
    '- No Cross-AG Conflicts: Two AGs MUST NOT touch same table/column. Merge overlapping.\n'
    '- Anti-Hallucination: Every AG must cite cluster_ids. No invented root causes.\n'
    '- All Instruments: For each root cause, list EVERY lever that should act.\n'
    '- MV-Preference: When a Metric View covers the same measures/dimensions as a base TABLE, '
    'NEVER add instructions directing Genie to use the TABLE directly. Prefer Metric Views for '
    'aggregation queries — they define the canonical business logic. Only route to base TABLEs '
    'for lookups, filters, or columns not exposed through a Metric View.\n'
    '- TVF-Avoidance: Do NOT add ASSET ROUTING or FUNCTION ROUTING instructions directing Genie '
    'to a Table-Valued Function (TVF) when the same query can be answered by a Metric View. '
    'TVFs with hardcoded date parameters produce fragile results. '
    'Standard trend and aggregation queries MUST use Metric Views with MEASURE() syntax.\n'
    '- Temporal-Standardization: When generating TEMPORAL FILTERS or QUERY RULES instructions, '
    'include these canonical date-filter patterns:\n'
    '  "this year" -> WHERE col >= DATE_TRUNC(\'year\', CURRENT_DATE())\n'
    '  "last quarter" -> WHERE col >= ADD_MONTHS(DATE_TRUNC(\'quarter\', CURRENT_DATE()), -3) '
    'AND col < DATE_TRUNC(\'quarter\', CURRENT_DATE())\n'
    '  "last N months" -> WHERE col >= ADD_MONTHS(CURRENT_DATE(), -N)\n'
    '  Include these in the TEMPORAL FILTERS section of the instruction.\n'
    '</instructions>\n'
    '\n'
    '<context>\n'
    '## Schema Index\n'
    '{{ schema_index }}\n'
    '\n'
    '## Failure Clusters\n'
    '{{ cluster_summaries }}\n'
    '\n'
    '## Soft Signal Clusters\n'
    '{{ soft_signal_summary }}\n'
    '\n'
    '## Current Join Specs\n'
    '{{ current_join_summary }}\n'
    '\n'
    '## Current Instructions\n'
    '{{ instruction_summary }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: Two failure clusters:\n'
    '  C001: wrong_column on dim_product.product_name vs product_title (3 questions)\n'
    '  C002: missing_join between fact_sales and dim_product (2 questions, overlapping with C001)\n'
    '\n'
    'Output:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG1",\n'
    '      "root_cause_summary": "Genie selects product_title instead of product_name and '
    'misses the fact_sales→dim_product join because column synonyms and join spec are absent",\n'
    '      "source_cluster_ids": ["C001", "C002"],\n'
    '      "affected_questions": ["Q3", "Q7", "Q12"],\n'
    '      "priority": 1,\n'
    '      "levers_needed": [1, 4, 5],\n'
    '      "focus_tables": ["catalog.schema.dim_product", "catalog.schema.fact_sales"],\n'
    '      "focus_columns": ["dim_product.product_name", "fact_sales.product_key"]\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_guidance": "BUSINESS DEFINITIONS: product name disambiguation; JOIN GUIDANCE: fact_sales to dim_product join path",\n'
    '  "rationale": "C001 and C002 both blame dim_product — merged into one AG. '
    'Lever 1 fixes the synonym, Lever 4 adds the join spec, Lever 5 adds an example SQL."\n'
    '}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG<N>",\n'
    '      "root_cause_summary": "<one sentence>",\n'
    '      "source_cluster_ids": ["C001", "C003"],\n'
    '      "affected_questions": ["<question_id>"],\n'
    '      "priority": 1,\n'
    '      "levers_needed": [1, 4, 5],\n'
    '      "focus_tables": ["<fully_qualified_table_name>"],\n'
    '      "focus_columns": ["<table_short_name>.<column_name>"]\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_guidance": "<themes using canonical section names: PURPOSE, ASSET ROUTING, '
    'BUSINESS DEFINITIONS, DISAMBIGUATION, AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, '
    'QUERY RULES, QUERY PATTERNS, TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS>",\n'
    '  "rationale": "<overall reasoning>"\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- priority 1 = most impactful. Order by affected question count.\n'
    '- focus_tables/focus_columns must reference objects from Schema Index.\n'
    '- levers_needed: list of integers 1-5.\n'
    '- If no actionable improvements:\n'
    '  {"action_groups": [], "global_instruction_guidance": "", "rationale": "No actionable failures"}\n'
    '</output_schema>'
)


STRATEGIST_DETAIL_PROMPT = (
    '<role>\n'
    'You are the Detail Planner for a Databricks Genie Space optimization action group.\n'
    '</role>\n'
    '\n'
    '<instructions>\n'
    '## Purpose\n'
    'Given an action group skeleton and detailed evidence (SQL diffs, current metadata), '
    'produce the EXACT lever directives needed to fix this root cause.\n'
    '\n'
    '## Contract: Structured Metadata Format\n'
    'ALL metadata changes MUST use structured sections.\n'
    'Tables: purpose, best_for, grain, scd, relationships.\n'
    'Columns by type: column_dim (definition, values, synonyms), '
    'column_measure (definition, aggregation, grain_note, synonyms), '
    'column_key (definition, join, synonyms).\n'
    'Functions: purpose, best_for, use_instead_of, parameters, example.\n'
    'Use section KEYS, not labels.\n'
    'Each section must be a SEPARATE key. Do NOT embed section headers inside another '
    "section's value — updates with embedded headers will be REJECTED.\n"
    'WRONG (rejected): {"purpose": "Fact table. BEST FOR: Duration analysis. GRAIN: One row per run"}\n'
    'CORRECT: {"purpose": "Fact table.", "best_for": "Duration analysis.", "grain": "One row per run"}\n'
    '\n'
    '## Contract: Non-Regressive / Augment-Not-Overwrite\n'
    '[EDITABLE] sections can be updated. [LOCKED] must NOT be changed.\n'
    'INCORPORATE existing content and ADD new details. Only replace if empty or wrong.\n'
    'Existing synonyms are auto-preserved; propose only NEW terms.\n'
    'instruction_contribution uses section-level upsert: only include sections relevant to this AG.\n'
    '\n'
    '## Contract: Coordination Notes\n'
    'Explain how changes across levers reinforce each other.\n'
    '\n'
    '## Contract: Example SQL\n'
    'For any recurring failure pattern (routing, aggregation, temporal, join, filter), '
    'include example_sqls in lever 5. Propose multiple example SQLs covering distinct '
    'failure patterns — aim for 1 per affected question where a valid SQL sketch exists.\n'
    '\n'
    '## Contract: Identifier Allowlist\n'
    'You MUST ONLY reference tables, columns, and functions from the Identifier Allowlist. '
    'Any name not in the allowlist is INVALID and will be rejected.\n'
    '\n'
    '## Contract: Instruction Contribution Format\n'
    'instruction_contribution MUST use ALL-CAPS SECTION HEADERS with colon (e.g. QUERY RULES:). '
    'No Markdown (no ##, no **, no backticks). Plain text only.\n'
    'Target sections aligned with your active levers:\n'
    '  Lever 1 -> BUSINESS DEFINITIONS, DISAMBIGUATION\n'
    '  Lever 2 -> AGGREGATION RULES, TEMPORAL FILTERS\n'
    '  Lever 3 -> FUNCTION ROUTING\n'
    '  Lever 4 -> JOIN GUIDANCE, TEMPORAL FILTERS\n'
    '  Lever 5 -> ASSET ROUTING, QUERY RULES, QUERY PATTERNS, DATA QUALITY NOTES, CONSTRAINTS\n'
    'Every bullet must reference a specific asset. No generic guidance.\n'
    '\n'
    '## Contract: Improvement Proposals\n'
    'When you identify a pattern where multiple questions would benefit from '
    'a new Metric View or SQL Function that does NOT yet exist, include a "proposals" '
    'array in your output. Propose METRIC_VIEW when multiple questions need the same '
    'complex aggregation. Propose FUNCTION when multiple questions need the same '
    'date/category transformation. Only propose objects that are genuinely missing.\n'
    '\n'
    '## Contract: MV-Preference\n'
    'When a Metric View covers the same measures/dimensions as a base TABLE, '
    'NEVER add ASSET ROUTING instructions directing Genie to use the TABLE directly. '
    'Prefer Metric Views for aggregation queries. Only route to base TABLEs for lookups, '
    'filters, or columns not exposed through a Metric View.\n'
    '\n'
    '## Contract: TVF-Avoidance\n'
    'Do NOT add ASSET ROUTING or FUNCTION ROUTING instructions directing Genie '
    'to a Table-Valued Function (TVF) when the same query can be answered by a Metric View. '
    'TVFs with hardcoded date parameters produce fragile results. '
    'Standard trend and aggregation queries MUST use Metric Views with MEASURE() syntax.\n'
    '\n'
    '## Contract: Temporal-Standardization\n'
    'When generating TEMPORAL FILTERS or QUERY RULES instructions, '
    'include these canonical date-filter patterns:\n'
    '  "this year" -> WHERE col >= DATE_TRUNC(\'year\', CURRENT_DATE())\n'
    '  "last quarter" -> WHERE col >= ADD_MONTHS(DATE_TRUNC(\'quarter\', CURRENT_DATE()), -3) '
    'AND col < DATE_TRUNC(\'quarter\', CURRENT_DATE())\n'
    '  "last N months" -> WHERE col >= ADD_MONTHS(CURRENT_DATE(), -N)\n'
    'Include these in the TEMPORAL FILTERS section of the instruction.\n'
    '\n'
    '## Contract: Section Ownership\n'
    'When proposing table/column description updates, only target sections the lever can modify:\n'
    '  Lever 1: purpose, best_for, grain, scd, definition, values, synonyms\n'
    '  Lever 2: definition, values, aggregation, grain_note, important_filters, synonyms '
    '(NOT purpose/best_for/grain/scd)\n'
    '  Lever 3: purpose, best_for, use_instead_of, parameters, example\n'
    '  Lever 4: relationships, join\n'
    'Proposing sections outside the lever ownership will be rejected.\n'
    '</instructions>\n'
    '\n'
    '<context>\n'
    '## Action Group to Detail\n'
    '{{ action_group_skeleton }}\n'
    '\n'
    '## SQL Evidence\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Current Structured Metadata for Focus Objects\n'
    '### Tables\n'
    '{{ structured_table_context }}\n'
    '### Columns\n'
    '{{ structured_column_context }}\n'
    '### Functions\n'
    '{{ structured_function_context }}\n'
    '\n'
    '## Current Join Specifications\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Current Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL\n'
    '{{ existing_example_sqls }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: AG1 skeleton — root cause: "Genie uses product_title instead of product_name '
    'and misses the fact_sales→dim_product join"\n'
    'Focus: dim_product.product_name, fact_sales.product_key\n'
    'Levers needed: [1, 4, 5]\n'
    '\n'
    'Output:\n'
    '{\n'
    '  "lever_directives": {\n'
    '    "1": {\n'
    '      "tables": [],\n'
    '      "columns": [{\n'
    '        "table": "catalog.schema.dim_product",\n'
    '        "column": "product_name",\n'
    '        "entity_type": "column_dim",\n'
    '        "sections": {"synonyms": "product title, item name", '
    '"definition": "The display name of the product as shown to customers"}\n'
    '      }]\n'
    '    },\n'
    '    "4": {\n'
    '      "join_specs": [{\n'
    '        "left_table": "catalog.schema.fact_sales",\n'
    '        "right_table": "catalog.schema.dim_product",\n'
    '        "join_guidance": "fact_sales.product_key = dim_product.product_key, MANY_TO_ONE"\n'
    '      }]\n'
    '    },\n'
    '    "5": {\n'
    '      "instruction_guidance": "Add routing rule for product name lookups",\n'
    '      "example_sqls": [\n'
    '        {\n'
    '          "question": "What are the top products by revenue?",\n'
    '          "sql_sketch": "SELECT dp.product_name, SUM(fs.revenue) FROM fact_sales fs '
    'JOIN dim_product dp ON fs.product_key = dp.product_key GROUP BY 1 ORDER BY 2 DESC",\n'
    '          "parameters": [],\n'
    '          "usage_guidance": "Use when user asks about product performance or revenue by product"\n'
    '        },\n'
    '        {\n'
    '          "question": "Show me revenue by product for the last quarter",\n'
    '          "sql_sketch": "SELECT dp.product_name, SUM(fs.revenue) FROM fact_sales fs '
    'JOIN dim_product dp ON fs.product_key = dp.product_key '
    'WHERE fs.sale_date >= ADD_MONTHS(DATE_TRUNC(\'quarter\', CURRENT_DATE()), -3) '
    'AND fs.sale_date < DATE_TRUNC(\'quarter\', CURRENT_DATE()) GROUP BY 1 ORDER BY 2 DESC",\n'
    '          "parameters": [],\n'
    '          "usage_guidance": "Use for temporal product revenue queries with date filters"\n'
    '        }\n'
    '      ]\n'
    '    }\n'
    '  },\n'
    '  "coordination_notes": "Lever 1 adds synonym so Genie resolves product_title→product_name. '
    'Lever 4 defines the join so Genie can reach dim_product from fact_sales. '
    'Lever 5 example SQL demonstrates the correct join pattern.",\n'
    '  "instruction_contribution": {\n'
    '    "BUSINESS DEFINITIONS": "- product name = product_name from catalog.schema.dim_product '
    '(synonyms: product title, item name)",\n'
    '    "JOIN GUIDANCE": "- For product name lookups, JOIN fact_sales to dim_product on product_key"\n'
    '  },\n'
    '  "proposals": []\n'
    '}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON:\n'
    '{\n'
    '  "lever_directives": {\n'
    '    "1": {"tables": [{"table": "<fq_name>", "entity_type": "table", '
    '"sections": {"<key>": "<value — AUGMENT existing>"}}],\n'
    '          "columns": [{"table": "<fq_name>", "column": "<col>", '
    '"entity_type": "<column_dim|column_measure|column_key>", '
    '"sections": {"<key>": "<value>"}}]},\n'
    '    "4": {"join_specs": [{"left_table": "<fq>", "right_table": "<fq>", '
    '"join_guidance": "<condition + type>"}]},\n'
    '    "5": {"instruction_guidance": "<text>", "example_sqls": ['
    '{"question": "<prompt>", "sql_sketch": "<SQL>", '
    '"parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}], '
    '"usage_guidance": "<when to match>"}]}\n'
    '  },\n'
    '  "coordination_notes": "<how levers reference each other>",\n'
    '  "instruction_contribution": {\n'
    '    "<SECTION_HEADER>": "<plain-text content for this section>"\n'
    '  },\n'
    '  "proposals": [\n'
    '    {\n'
    '      "type": "METRIC_VIEW | FUNCTION",\n'
    '      "title": "<short name for the proposed object>",\n'
    '      "rationale": "<why this is needed — what failure pattern it fixes>",\n'
    '      "definition": "<SQL CREATE or pseudocode definition>",\n'
    '      "affected_questions": ["<question_id>", ...],\n'
    '      "estimated_impact": "<brief estimate of accuracy improvement>"\n'
    '    }\n'
    '  ]\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- Only include lever keys with work to do (from skeleton levers_needed).\n'
    '- "sections" keys from structured metadata schema.\n'
    '- instruction_contribution: a JSON OBJECT mapping section headers to content.\n'
    '  Keys MUST be from: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '  Only include sections relevant to THIS action group. Values are plain text.\n'
    '- Lever 2: same column format as Lever 1.\n'
    '- Lever 3: {"functions": [{"function": "...", "sections": {...}}]}\n'
    '- Lever 5 example_sqls: propose 1 per distinct failure pattern. Always include at least one.\n'
    '- proposals: OPTIONAL. Only include if you identify a genuinely missing Metric View or Function.\n'
    '</output_schema>'
)

# ── 5d. Adaptive Strategist Prompt (single-call, one AG) ──────────────

ADAPTIVE_STRATEGIST_PROMPT = (
    '<role>\n'
    'You are the Adaptive Strategist for a Databricks Genie Space optimization '
    'framework.  You operate in an iterative loop: after each action you receive '
    'fresh evaluation results and must decide the SINGLE best next action.\n'
    '</role>\n'
    '\n'
    '<instructions>\n'
    '## Purpose\n'
    'Analyze the CURRENT failure clusters (from the most recent evaluation) and '
    'produce exactly ONE action group — the single highest-impact fix for the '
    'remaining failures.  Prior iterations and their outcomes are provided in '
    'the reflection history so you can build on successes and avoid repeating '
    'failed approaches.\n'
    '\n'
    '## When to create an action group\n'
    '- Systematic failure pattern (wrong column, missing join, wrong aggregation, etc.)\n'
    '- Correct-but-suboptimal soft signal suggesting preventive improvement\n'
    '\n'
    '## When NOT to create an action group\n'
    '- Format-only differences (extra_columns_only, select_star, format_difference)\n'
    '- Cannot identify a specific table, column, join, or instruction to change\n'
    '- The approach was already tried and failed (see DO NOT RETRY list)\n'
    '\n'
    '## Contract: All Instruments of Power\n'
    'For the root cause you target, specify EVERY lever that should act:\n'
    '- wrong_column / wrong_table / missing_synonym: Primary Lever 1, also Lever 5\n'
    '- wrong_aggregation / missing_filter: Primary Lever 2, also Lever 5\n'
    '- tvf_parameter_error: Primary Lever 3, also Lever 5\n'
    '- wrong_join / missing_join_spec: Primary Lever 4, also Lever 1 + 5\n'
    '- asset_routing_error / ambiguous_question: Primary Lever 5, also Lever 1\n'
    '\n'
    '## Contract: Structured Metadata Format\n'
    'ALL metadata changes MUST use structured sections.\n'
    'Tables: purpose, best_for, grain, scd, relationships.\n'
    'Columns by type: column_dim (definition, values, synonyms), '
    'column_measure (definition, aggregation, grain_note, synonyms), '
    'column_key (definition, join, synonyms).\n'
    'Functions: purpose, best_for, use_instead_of, parameters, example.\n'
    'Use section KEYS, not labels.\n'
    'Each section must be a SEPARATE key. Do NOT embed section headers inside another '
    "section's value — updates with embedded headers will be REJECTED.\n"
    'WRONG (rejected): {"purpose": "Fact table. BEST FOR: Duration analysis. GRAIN: One row per run"}\n'
    'CORRECT: {"purpose": "Fact table.", "best_for": "Duration analysis.", "grain": "One row per run"}\n'
    '\n'
    '## Contract: Section Ownership\n'
    'When proposing table/column description updates, only target sections the lever can modify:\n'
    '  Lever 1: purpose, best_for, grain, scd, definition, values, synonyms\n'
    '  Lever 2: definition, values, aggregation, grain_note, important_filters, synonyms '
    '(NOT purpose/best_for/grain/scd)\n'
    '  Lever 3: purpose, best_for, use_instead_of, parameters, example\n'
    '  Lever 4: relationships, join\n'
    'Proposing sections outside the lever ownership will be rejected.\n'
    '\n'
    '## Contract: Non-Regressive / Augment-Not-Overwrite\n'
    '[EDITABLE] sections can be updated. [LOCKED] must NOT be changed.\n'
    'INCORPORATE existing content and ADD new details. Only replace if empty or wrong.\n'
    'Existing synonyms are auto-preserved; propose only NEW terms.\n'
    'global_instruction_rewrite uses section-level upsert: only include sections you change.\n'
    'Omitted sections are preserved automatically.\n'
    '\n'
    '## Contract: Example SQL\n'
    'For any recurring failure pattern (routing, aggregation, temporal, join, filter), '
    'include example_sqls in lever 5. Propose multiple example SQLs covering distinct '
    'failure patterns — aim for 1 per affected question where a valid SQL sketch exists.\n'
    '\n'
    '## Identifier Allowlist\n'
    'ONLY reference identifiers from this allowlist:\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Refinement Mode Guidance\n'
    'When the Reflection History shows a ROLLED_BACK entry:\n'
    '- If "in_plan": The lever direction was correct but caused regressions. '
    'Refine the SAME lever with narrower scope or more specific targeting. '
    'Do NOT switch to a different root cause.\n'
    '- If "out_of_plan": The approach fundamentally did not work. Switch to a '
    'different lever class or escalate. Do NOT retry the same lever type on '
    'the same target.\n'
    '\n'
    '## Escalation for Persistent Failures\n'
    'Check the Persistent Question Failures section.  If a question is marked '
    'ADDITIVE_LEVERS_EXHAUSTED, do NOT propose more add_instruction or add_example_sql '
    'patches for it — those have already been tried multiple times without effect.\n'
    'Instead, set the optional "escalation" field in your output:\n'
    '- "remove_tvf": The root cause is a misleading TVF that overrides routing.  '
    'Only TVFs may be removed — NEVER tables or metric views.  Include the TVF identifier '
    'in lever 3.  The system will assess removal confidence and either auto-apply, '
    'flag for review, or escalate to human.\n'
    '- "gt_repair": The ground-truth SQL appears incorrect (neither_correct pattern).  '
    'The system will attempt LLM-assisted GT correction.\n'
    '- "flag_for_review": No automated fix is viable.  The question will be flagged '
    'for human review in the labeling session.\n'
    'If INTERMITTENT, the question may be non-deterministic — monitor but do not '
    'escalate unless it becomes PERSISTENT.\n'
    '</instructions>\n'
    '\n'
    '<context>\n'
    '{{ context_json }}\n'
    '</context>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON structure with EXACTLY ONE action group:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG<iteration_number>",\n'
    '      "root_cause_summary": "<one sentence>",\n'
    '      "source_cluster_ids": ["C001"],\n'
    '      "affected_questions": ["<question_id>"],\n'
    '      "priority": 1,\n'
    '      "lever_directives": {\n'
    '        "1": {"tables": [{"table": "<fq_name>", "entity_type": "table", '
    '"sections": {"<key>": "<value>"}}\n'
    '              ], "columns": [{"table": "<fq_name>", "column": "<col>", '
    '"entity_type": "<column_dim|column_measure|column_key>", '
    '"sections": {"<key>": "<value>"}}]},\n'
    '        "4": {"join_specs": [{"left_table": "<fq>", "right_table": "<fq>", '
    '"join_guidance": "<condition + type>"}]},\n'
    '        "5": {"instruction_guidance": "<text>", "example_sqls": ['
    '{"question": "<prompt>", "sql_sketch": "<SQL>", '
    '"parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}], '
    '"usage_guidance": "<when to match>"}]}\n'
    '      },\n'
    '      "coordination_notes": "<how levers reference each other>",\n'
    '      "escalation": "<optional: remove_tvf | gt_repair | flag_for_review>"\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_rewrite": {\n'
    '    "PURPOSE": "One paragraph describing what this Genie Space does.",\n'
    '    "ASSET ROUTING": "- When user asks about [topic], use [table/TVF/MV]",\n'
    '    "TEMPORAL FILTERS": "- Use run_date >= DATE_SUB(CURRENT_DATE(), N) for last-N-day queries"\n'
    '  },\n'
    '  "rationale": "<why this action group is the highest-impact next step>"\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- EXACTLY one action group. Pick the single highest-impact fix.\n'
    '- "lever_directives" keys "1"-"5". Only include levers with work to do.\n'
    '- "sections" keys from structured metadata schema.\n'
    '- Lever 2 uses same column format as Lever 1. Lever 3: {"functions": [...]}.\n'
    '- global_instruction_rewrite: a JSON OBJECT mapping section headers to content.\n'
    '  Keys MUST be from: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '  Only include sections you want to ADD or REPLACE. Omitted sections are PRESERVED unchanged.\n'
    '  Values are plain-text bullet lists (no Markdown). Empty string means delete the section.\n'
    '- Do NOT repeat any approach listed in the DO NOT RETRY section.\n'
    '- If no actionable improvements remain:\n'
    '  {"action_groups": [], "global_instruction_rewrite": {}, '
    '"rationale": "No actionable failures"}\n'
    '</output_schema>'
)

# ── 5e. GT Repair Prompt ───────────────────────────────────────────────

GT_REPAIR_PROMPT = (
    'You are a SQL expert reviewing a benchmark question where BOTH the ground-truth SQL '
    'and Genie\'s generated SQL were judged incorrect by the arbiter.\n'
    '\n'
    'QUESTION: {{ question }}\n'
    '\n'
    'GROUND TRUTH SQL (judged incorrect):\n'
    '{{ expected_sql }}\n'
    '\n'
    'GENIE SQL (also judged incorrect):\n'
    '{{ genie_sql }}\n'
    '\n'
    'ARBITER RATIONALE(S):\n'
    '{{ rationale }}\n'
    '\n'
    'Your task: produce a CORRECTED ground-truth SQL that correctly answers the question.\n'
    '- Use proper Databricks SQL syntax\n'
    '- Respect temporal semantics (e.g. "this year" = DATE_TRUNC(\'year\', CURRENT_DATE()), '
    '"last 12 months" = ADD_MONTHS(CURRENT_DATE(), -12))\n'
    '- Use MEASURE() for metric view columns where appropriate\n'
    '- Return ONLY the corrected SQL, no explanation'
)


# ── 6. Non-Exportable Genie Config Fields ──────────────────────────────

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

# ── 7. Feature Flags ──────────────────────────────────────────────────

USE_PATCH_DSL = True
USE_JOB_MODE = True
USE_LEVER_AWARE = True
ENABLE_CONTINUOUS_MONITORING = False

APPLY_MODE = "genie_config"
"""Where patches are applied. One of:
  - "genie_config": All changes go to Genie Space config overlays.
  - "uc_artifact": Column descriptions go to UC via ALTER TABLE, etc.
  - "both": Apply to both targets for maximum coverage.
Levers 4-6 are always genie_config regardless of this setting."""

# ── 8. Risk Classification Sets ────────────────────────────────────────

LOW_RISK_PATCHES = {
    "add_description",
    "update_description",
    "add_column_description",
    "update_column_description",
    "hide_column",
    "unhide_column",
    "add_instruction",
    "enable_example_values",
    "disable_example_values",
    "enable_value_dictionary",
    "disable_value_dictionary",
    "add_column_synonym",
    "remove_column_synonym",
}

MEDIUM_RISK_PATCHES = {
    "update_instruction",
    "rewrite_instruction",
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

# ── 9. Repeatability Classification ────────────────────────────────────

REPEATABILITY_CLASSIFICATIONS = {
    95: "IDENTICAL",
    80: "MINOR_VARIANCE",
    60: "SIGNIFICANT_VARIANCE",
    0: "CRITICAL_VARIANCE",
}

# ── 10. Repeatability Fix Routing by Asset Type ───────────────────────

REPEATABILITY_FIX_BY_ASSET = {
    "TABLE": (
        "Add structured metadata (business_definition, synonyms[], grain, join_keys[]) "
        "to column descriptions. Add UC tags: preferred_for_genie=true, domain=<value>."
    ),
    "MV": (
        "Add structured column metadata to metric view columns. "
        "Use synonyms[] and preferred_questions[] to constrain dimension selection."
    ),
    "TVF": (
        "Add instruction clarifying deterministic parameter selection. "
        "TVF signature already constrains output; focus on parameter disambiguation."
    ),
    "NONE": "Add routing instruction to direct questions to the appropriate asset type.",
}

# ── 11. Lever Descriptions ─────────────────────────────────────────────

LEVER_NAMES = {
    0: "Proactive Enrichment",   # Always runs; NOT user-selectable
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Genie Space Instructions",
}
"""Lever ID -> display name mapping.

Lever 0 is a preparatory stage that always runs before the adaptive lever
loop. It is not included in :data:`DEFAULT_LEVER_ORDER` and should not be
shown in the UI as a toggleable option.
"""

DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5]
"""Default set of user-selectable levers, in execution order."""

MAX_VALUE_DICTIONARY_COLUMNS = 120
"""Maximum number of string columns per Genie Space that can have
enable_entity_matching=true. Enforced by auto_apply_prompt_matching()."""

ENABLE_PROMPT_MATCHING_AUTO_APPLY = True
"""When True, format assistance and entity matching are applied as a
best-practice hygiene step between baseline evaluation and the lever loop."""

CATEGORICAL_COLUMN_PATTERNS = [
    "industry", "type", "status", "state", "country", "region",
    "department", "category", "segment", "code", "tier", "level",
    "stage", "phase", "class", "group", "channel", "source", "priority",
    "currency", "unit", "role", "gender", "brand", "vendor", "supplier",
]

FREE_TEXT_COLUMN_PATTERNS = [
    "description", "comment", "notes", "address", "email", "url",
    "path", "body", "message", "content", "text", "summary", "detail",
    "narrative", "reason", "explanation",
]

NUMERIC_DATA_TYPES = {
    "DOUBLE", "FLOAT", "DECIMAL", "INT", "INTEGER", "BIGINT",
    "SMALLINT", "TINYINT", "LONG", "SHORT", "BYTE", "NUMBER",
}

MEASURE_NAME_PREFIXES = [
    "avg_", "sum_", "count_", "total_", "pct_", "ratio_",
    "min_", "max_", "num_", "mean_", "median_", "stddev_",
]

# ── 12. Delta Table Names ─────────────────────────────────────────────

TABLE_RUNS = "genie_opt_runs"
TABLE_STAGES = "genie_opt_stages"
TABLE_ITERATIONS = "genie_opt_iterations"
TABLE_PATCHES = "genie_opt_patches"
TABLE_ASI = "genie_eval_asi_results"
TABLE_PROVENANCE = "genie_opt_provenance"
TABLE_SUGGESTIONS = "genie_opt_suggestions"

# ── 13. MLflow Conventions ─────────────────────────────────────────────

EXPERIMENT_PATH_TEMPLATE = "/Shared/genie-space-optimizer/{{ space_id }}/{{ domain }}"
RUN_NAME_TEMPLATE = "iter_{{ iteration }}_eval_{{ timestamp }}"
BASELINE_RUN_NAME_TEMPLATE = "baseline_eval_{{ timestamp }}"
MODEL_NAME_TEMPLATE = "genie-space-{{ space_id }}"

UC_REGISTERED_MODEL_TEMPLATE = "{{ catalog }}.{{ schema }}.genie_space_{{ space_id }}"
"""Three-level UC registered model name. Interpolated at runtime."""

ENABLE_UC_MODEL_REGISTRATION: bool = True
"""When True, finalize registers the champion as a UC Registered Model."""

DEPLOYMENT_JOB_NAME_TEMPLATE = "genie-optimizer-deploy-{{ space_id }}"
"""Name pattern for the per-space deployment job."""

PROMPT_NAME_TEMPLATE = "{{ uc_schema }}.genie_opt_{{ judge_name }}"
PROMPT_ALIAS = "production"

INSTRUCTION_PROMPT_NAME_TEMPLATE = "{{ uc_schema }}.genie_instructions_{{ space_id }}"
INSTRUCTION_PROMPT_ALIAS = "latest"

# ── 14. Patch DSL Constants ────────────────────────────────────────────

MAX_PATCH_OBJECTS = 5
MAX_INSTRUCTION_TEXT_CHARS = 2000
MAX_HOLISTIC_INSTRUCTION_CHARS = 8000

PROMPT_TOKEN_BUDGET = 70_000
"""Token budget for LLM prompts.  Claude Opus 4.6 supports 200k tokens;
we target ~70k to stay in the quality sweet-spot while leaving headroom
for the response."""

RISK_LEVEL_SCORE = {
    "low": 1,
    "medium": 2,
    "high": 3,
}

GENERIC_FIX_PREFIXES = (
    "review ",
    "check ",
    "verify ",
    "ensure ",
    "investigate ",
)

# ── 15. Assessment Sources ─────────────────────────────────────────────

CODE_SOURCE_ID = "genie-optimizer-v2"
LLM_SOURCE_ID_TEMPLATE = "databricks:/{{ endpoint }}"

# ── 16. Temporal Validation Patterns ───────────────────────────────────

TEMPORAL_PHRASES = (
    r"\b(this year|last year|last quarter|this quarter|last \d+ months?"
    r"|last \d+ days?|this month|last month|year to date|ytd)\b"
)
HARDCODED_DATE = r"'\d{4}-\d{2}-\d{2}'"

# ── 17. Patch Types (35 entries) ───────────────────────────────────────

PATCH_TYPES = {
    # Lever 1: Tables & Columns — descriptions, visibility, aliases
    "add_description": {
        "type": "add_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["descriptions", "column_metadata"],
    },
    "update_description": {
        "type": "update_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["descriptions", "column_metadata"],
    },
    "add_column_description": {
        "type": "add_column_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_metadata", "descriptions"],
    },
    "update_column_description": {
        "type": "update_column_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_metadata", "descriptions"],
    },
    "hide_column": {
        "type": "hide_column",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_visibility", "column_metadata"],
    },
    "unhide_column": {
        "type": "unhide_column",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_visibility", "column_metadata"],
    },
    "rename_column_alias": {
        "type": "rename_column_alias",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["column_metadata", "aliases"],
    },
    "add_table": {
        "type": "add_table",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tables", "schema"],
    },
    "remove_table": {
        "type": "remove_table",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tables", "schema"],
    },
    # Lever 2: Metric Views
    "add_mv_measure": {
        "type": "add_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "update_mv_measure": {
        "type": "update_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "remove_mv_measure": {
        "type": "remove_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "add_mv_dimension": {
        "type": "add_mv_dimension",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "dimensions"],
    },
    "remove_mv_dimension": {
        "type": "remove_mv_dimension",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "dimensions"],
    },
    "update_mv_yaml": {
        "type": "update_mv_yaml",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["metric_view", "mv_yaml"],
    },
    # Lever 3: Table-Valued Functions
    "add_tvf_parameter": {
        "type": "add_tvf_parameter",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["tvf_parameters", "tvf_definition"],
    },
    "remove_tvf_parameter": {
        "type": "remove_tvf_parameter",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["tvf_parameters", "tvf_definition"],
    },
    "update_tvf_sql": {
        "type": "update_tvf_sql",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvf_definition", "tvf_sql"],
    },
    "add_tvf": {
        "type": "add_tvf",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvfs", "tvf_definition"],
    },
    "remove_tvf": {
        "type": "remove_tvf",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvfs", "tvf_definition"],
    },
    # Lever 4: Join Specifications
    "add_join_spec": {
        "type": "add_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    "update_join_spec": {
        "type": "update_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    "remove_join_spec": {
        "type": "remove_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    # Lever 5: Column Discovery Settings
    "enable_example_values": {
        "type": "enable_example_values",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "disable_example_values": {
        "type": "disable_example_values",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "enable_value_dictionary": {
        "type": "enable_value_dictionary",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "disable_value_dictionary": {
        "type": "disable_value_dictionary",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "add_column_synonym": {
        "type": "add_column_synonym",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "synonyms"],
    },
    "remove_column_synonym": {
        "type": "remove_column_synonym",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "synonyms"],
    },
    # Lever 6: Genie Space Instructions (text)
    "add_instruction": {
        "type": "add_instruction",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["instructions"],
    },
    "update_instruction": {
        "type": "update_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    "remove_instruction": {
        "type": "remove_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    "rewrite_instruction": {
        "type": "rewrite_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    # Lever 6: Genie Space Example SQL (preferred over text instructions)
    "add_example_sql": {
        "type": "add_example_sql",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["instructions", "example_question_sqls"],
    },
    "update_example_sql": {
        "type": "update_example_sql",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions", "example_question_sqls"],
    },
    "remove_example_sql": {
        "type": "remove_example_sql",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions", "example_question_sqls"],
    },
    # Shared: Filters
    "add_default_filter": {
        "type": "add_default_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    "remove_default_filter": {
        "type": "remove_default_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    "update_filter_condition": {
        "type": "update_filter_condition",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
}

# ── 18. Conflict Rules (23 pairs) ─────────────────────────────────────

CONFLICT_RULES = [
    ("add_table", "remove_table"),
    ("add_column_synonym", "remove_column_synonym"),
    ("add_instruction", "remove_instruction"),
    ("add_instruction", "update_instruction"),
    ("update_instruction", "remove_instruction"),
    ("add_join_spec", "remove_join_spec"),
    ("add_default_filter", "remove_default_filter"),
    ("add_tvf_parameter", "remove_tvf_parameter"),
    ("add_tvf", "remove_tvf"),
    ("add_mv_measure", "remove_mv_measure"),
    ("add_mv_dimension", "remove_mv_dimension"),
    ("hide_column", "unhide_column"),
    ("add_column_description", "update_column_description"),
    ("add_description", "update_description"),
    ("update_mv_measure", "remove_mv_measure"),
    ("enable_example_values", "disable_example_values"),
    ("enable_value_dictionary", "disable_value_dictionary"),
    ("update_join_spec", "remove_join_spec"),
    # Example SQL conflict pairs
    ("add_example_sql", "remove_example_sql"),
    ("add_example_sql", "update_example_sql"),
    ("update_example_sql", "remove_example_sql"),
    # Cross-type conflicts: example SQL vs text instructions on same routing
    ("add_example_sql", "add_instruction"),
    ("add_example_sql", "update_instruction"),
    # Holistic rewrite conflicts with any other instruction mutation
    ("rewrite_instruction", "add_instruction"),
    ("rewrite_instruction", "update_instruction"),
    ("rewrite_instruction", "remove_instruction"),
]

# ── 19. Failure Taxonomy (22 types) ───────────────────────────────────

FAILURE_TAXONOMY = {
    "wrong_table",
    "wrong_column",
    "wrong_join",
    "missing_filter",
    "missing_temporal_filter",
    "wrong_aggregation",
    "wrong_measure",
    "missing_instruction",
    "ambiguous_question",
    "asset_routing_error",
    "tvf_parameter_error",
    "compliance_violation",
    "performance_issue",
    "repeatability_issue",
    "missing_synonym",
    "description_mismatch",
    "stale_data",
    "data_freshness",
    "missing_join_spec",
    "wrong_join_spec",
    "missing_format_assistance",
    "missing_entity_matching",
}

# ── 19b. Cluster Priority Weights (adaptive lever loop) ───────────────

CAUSAL_WEIGHT: dict[str, float] = {
    "syntax_validity": 5.0,
    "schema_accuracy": 4.0,
    "asset_routing": 3.5,
    "logical_accuracy": 3.0,
    "semantic_equivalence": 2.0,
    "completeness": 1.5,
    "result_correctness": 1.0,
    "response_quality": 0.5,
}
"""Weight reflecting a judge's position in the causal chain.
Upstream judges (syntax, schema) cascade failures downstream, so
fixing them has higher leverage."""

SEVERITY_WEIGHT: dict[str, float] = {
    "wrong_table": 1.0,
    "wrong_column": 0.9,
    "wrong_join": 0.9,
    "missing_join_spec": 0.85,
    "wrong_join_spec": 0.85,
    "wrong_aggregation": 0.8,
    "wrong_measure": 0.8,
    "asset_routing_error": 0.9,
    "missing_filter": 0.7,
    "missing_temporal_filter": 0.7,
    "tvf_parameter_error": 0.7,
    "missing_instruction": 0.6,
    "description_mismatch": 0.4,
    "compliance_violation": 0.5,
    "performance_issue": 0.3,
    "repeatability_issue": 0.4,
    "missing_synonym": 0.3,
    "ambiguous_question": 0.3,
    "stale_data": 0.3,
    "data_freshness": 0.3,
    "missing_format_assistance": 0.3,
    "missing_entity_matching": 0.3,
}
"""Severity weight per failure type.  Higher values mean the failure
type is more impactful and should be prioritized."""

FIXABILITY_WITH_COUNTERFACTUAL = 1.0
FIXABILITY_WITHOUT_COUNTERFACTUAL = 0.4

# ── 20. Judge Prompts (5 templates) ───────────────────────────────────

JUDGE_PROMPTS = {
    "schema_accuracy": (
        "<role>\n"
        "You are a SQL schema expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the GENERATED SQL references the correct tables, columns, and joins.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: What is total revenue by region?\n"
        "Expected: SELECT region, SUM(revenue) FROM sales GROUP BY region\n"
        "Generated: SELECT region, SUM(revenue) FROM sales GROUP BY ALL\n"
        'Result: {"correct": true, "failure_type": "", "wrong_clause": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Same tables and columns; GROUP BY ALL is equivalent."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Show revenue by product\n"
        "Expected: SELECT p.name, SUM(s.revenue) FROM sales s JOIN products p ON s.product_id = p.id GROUP BY 1\n"
        "Generated: SELECT product_name, SUM(revenue) FROM orders GROUP BY 1\n"
        'Result: {"correct": false, "failure_type": "wrong_table", '
        '"wrong_clause": "FROM orders", '
        '"blame_set": ["orders"], '
        '"counterfactual_fix": "Add synonym product_name to products.name; '
        'clarify sales is the revenue table", '
        '"rationale": "Generated SQL queries orders instead of sales table."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"correct": true/false, '
        '"failure_type": "<wrong_table|wrong_column|wrong_join|missing_column>", '
        '"wrong_clause": "<problematic SQL clause>", '
        '"blame_set": ["<table_or_column>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If correct, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "logical_accuracy": (
        "<role>\n"
        "You are a SQL logic expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the GENERATED SQL applies correct aggregations, filters, "
        "GROUP BY, ORDER BY, and WHERE clauses for the business question.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: Top 5 customers by spend\n"
        "Expected: SELECT customer_id, SUM(amount) as total FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 5\n"
        "Generated: SELECT customer_id, SUM(amount) FROM orders GROUP BY ALL ORDER BY SUM(amount) DESC LIMIT 5\n"
        'Result: {"correct": true, "failure_type": "", "wrong_clause": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Same aggregation and ordering; GROUP BY ALL is equivalent."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Total revenue by country\n"
        "Expected: SELECT country, SUM(revenue) FROM sales GROUP BY country\n"
        "Generated: SELECT country, AVG(revenue) FROM sales GROUP BY country\n"
        'Result: {"correct": false, "failure_type": "wrong_aggregation", '
        '"wrong_clause": "AVG(revenue)", '
        '"blame_set": ["revenue"], '
        '"counterfactual_fix": "Update revenue column description to specify SUM aggregation", '
        '"rationale": "Question asks for total (SUM) but generated SQL uses AVG."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"correct": true/false, '
        '"failure_type": "<wrong_aggregation|wrong_filter|wrong_groupby|wrong_orderby>", '
        '"wrong_clause": "<problematic SQL clause>", '
        '"blame_set": ["<column_or_function>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If correct, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "semantic_equivalence": (
        "<role>\n"
        "You are a SQL semantics expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the two SQL queries measure the SAME business metric and would "
        "answer the same question, even if written differently.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: Monthly revenue\n"
        "Expected: SELECT month, SUM(amount) FROM sales GROUP BY month\n"
        "Generated: SELECT DATE_TRUNC('month', sale_date) AS month, SUM(amount) FROM sales GROUP BY 1\n"
        'Result: {"equivalent": true, "failure_type": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Both measure total revenue per month; just different date extraction."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Revenue by product\n"
        "Expected: SELECT product, SUM(revenue) FROM sales GROUP BY product\n"
        "Generated: SELECT product, COUNT(*) FROM sales GROUP BY product\n"
        'Result: {"equivalent": false, "failure_type": "different_metric", '
        '"blame_set": ["revenue"], '
        '"counterfactual_fix": "Clarify revenue column aggregation as SUM in description", '
        '"rationale": "Expected measures SUM(revenue) but generated counts rows."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"equivalent": true/false, '
        '"failure_type": "<different_metric|different_grain|different_scope>", '
        '"blame_set": ["<metric_or_dimension>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If equivalent, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "completeness": (
        "<role>\n"
        "You are a SQL completeness expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the GENERATED SQL fully answers the user's question without "
        "missing dimensions, measures, or filters.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: Revenue and order count by region\n"
        "Expected: SELECT region, SUM(revenue), COUNT(order_id) FROM orders GROUP BY region\n"
        "Generated: SELECT region, SUM(revenue) AS rev, COUNT(*) AS orders FROM orders GROUP BY 1\n"
        'Result: {"complete": true, "failure_type": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Both revenue and count are present; COUNT(*) vs COUNT(order_id) is equivalent."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Revenue and order count by region\n"
        "Expected: SELECT region, SUM(revenue), COUNT(order_id) FROM orders GROUP BY region\n"
        "Generated: SELECT region, SUM(revenue) FROM orders GROUP BY region\n"
        'Result: {"complete": false, "failure_type": "missing_column", '
        '"blame_set": ["order_id"], '
        '"counterfactual_fix": "Add synonym order count to order_id column description", '
        '"rationale": "User asked for order count but generated SQL omits COUNT(order_id)."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"complete": true/false, '
        '"failure_type": "<missing_column|missing_filter|missing_temporal_filter|missing_aggregation|partial_answer>", '
        '"blame_set": ["<missing_element>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If complete, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "arbiter": (
        "<role>\n"
        "You are a senior SQL arbiter for a Databricks Genie Space evaluation.\n"
        "</role>\n\n"
        "<context>\n"
        "User question and expected SQL: { inputs }\n"
        "Genie response and comparison: { outputs }\n"
        "Expected result: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Two SQL queries attempted to answer the same business question but produced "
        "different results. Analyze both and determine which is correct.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"genie_correct\">\n"
        "Question: Total active users\n"
        "Expected SQL: SELECT COUNT(*) FROM users (no filter)\n"
        "Genie SQL: SELECT COUNT(*) FROM users WHERE status = 'active'\n"
        "Genie returned 150 rows, Expected returned 300 rows.\n"
        'Result: {"verdict": "genie_correct", "failure_type": "wrong_filter", '
        '"blame_set": ["users.status"], '
        '"rationale": "User asked for active users; Genie correctly filters on status. '
        'GT SQL is missing the active filter."}\n'
        "</example>\n"
        "<example name=\"ground_truth_correct\">\n"
        "Question: Revenue by quarter\n"
        "Expected SQL: SELECT quarter, SUM(revenue) FROM sales GROUP BY quarter\n"
        "Genie SQL: SELECT quarter, AVG(revenue) FROM sales GROUP BY quarter\n"
        'Result: {"verdict": "ground_truth_correct", "failure_type": "wrong_aggregation", '
        '"blame_set": ["revenue"], '
        '"rationale": "Revenue should be summed, not averaged."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"verdict": "<genie_correct|ground_truth_correct|both_correct|neither_correct>", '
        '"failure_type": "<wrong_aggregation|wrong_filter|wrong_table|other>", '
        '"blame_set": ["<blamed_object>"], '
        '"rationale": "<brief explanation>"}\n'
        "</output_schema>"
    ),
    "response_quality": (
        "<role>\n"
        "You are evaluating the quality of a natural language response "
        "from a Databricks Genie Space AI assistant.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Genie's natural language response:\n  { outputs }\n"
        "Genie's SQL query:\n  { sql }\n"
        "Expected SQL:\n  { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Evaluate whether the natural language response:\n"
        "1. Accurately describes what the SQL query does\n"
        "2. Correctly answers the user's question\n"
        "3. Does not make claims unsupported by the query/data\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: How many orders last month?\n"
        "Genie response: There were 1,234 orders placed last month.\n"
        "SQL: SELECT COUNT(*) FROM orders WHERE order_date >= '2024-11-01'\n"
        'Result: {"accurate": true, "failure_type": "", "counterfactual_fix": "", '
        '"rationale": "Response accurately describes the count query result."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Total revenue this quarter\n"
        "Genie response: The average revenue this quarter is $50,000.\n"
        "SQL: SELECT SUM(revenue) FROM sales WHERE quarter = 'Q4'\n"
        'Result: {"accurate": false, "failure_type": "inaccurate_description", '
        '"counterfactual_fix": "Add instruction clarifying revenue is a SUM metric, not AVG", '
        '"rationale": "SQL computes SUM but response says average."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"accurate": true/false, '
        '"failure_type": "<inaccurate_description|unsupported_claim|misleading_summary>", '
        '"counterfactual_fix": "<specific change to metadata/instructions that would fix this>", '
        '"rationale": "<brief explanation>"}\n'
        'If accurate, set failure_type to "" and counterfactual_fix to "".\n'
        "</output_schema>"
    ),
}

# ── 20b. Lever Prompts (registered in MLflow for traceability) ─────────

LEVER_PROMPTS: dict[str, str] = {
    "strategist": STRATEGIST_PROMPT,
    "strategist_triage": STRATEGIST_TRIAGE_PROMPT,
    "strategist_detail": STRATEGIST_DETAIL_PROMPT,
    "adaptive_strategist": ADAPTIVE_STRATEGIST_PROMPT,
    "lever_1_2_column": LEVER_1_2_COLUMN_PROMPT,
    "lever_4_join_spec": LEVER_4_JOIN_SPEC_PROMPT,
    "lever_4_join_discovery": LEVER_4_JOIN_DISCOVERY_PROMPT,
    "lever_5_instruction": LEVER_5_INSTRUCTION_PROMPT,
    "lever_5_holistic": LEVER_5_HOLISTIC_PROMPT,
    "proposal_generation": PROPOSAL_GENERATION_PROMPT,
    "description_enrichment": DESCRIPTION_ENRICHMENT_PROMPT,
    "table_description_enrichment": TABLE_DESCRIPTION_ENRICHMENT_PROMPT,
    "proactive_instruction": PROACTIVE_INSTRUCTION_PROMPT,
    "space_description": SPACE_DESCRIPTION_PROMPT,
    "sample_questions": SAMPLE_QUESTIONS_PROMPT,
    "gt_repair": GT_REPAIR_PROMPT,
}

# ── 20c. Benchmark Prompts (registered in MLflow for traceability) ─────

BENCHMARK_PROMPTS: dict[str, str] = {
    "benchmark_generation": BENCHMARK_GENERATION_PROMPT,
    "benchmark_correction": BENCHMARK_CORRECTION_PROMPT,
    "benchmark_alignment_check": BENCHMARK_ALIGNMENT_CHECK_PROMPT,
    "benchmark_coverage_gap": BENCHMARK_COVERAGE_GAP_PROMPT,
}

# ── 21. ASI Schema (12 fields) ─────────────────────────────────────────

ASI_SCHEMA = {
    "failure_type": "str (from FAILURE_TAXONOMY)",
    "severity": "str (critical|major|minor)",
    "confidence": "float (0.0-1.0)",
    "wrong_clause": "str|None (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, MEASURE)",
    "blame_set": "list[str] (metadata fields blamed: table names, column names, instructions)",
    "quoted_metadata_text": "str|None (exact text from Genie config that caused the issue)",
    "missing_metadata": "str|None (what should exist but doesn't)",
    "ambiguity_detected": "bool",
    "expected_value": "str|None",
    "actual_value": "str|None",
    "counterfactual_fix": "str|None (suggested metadata change to fix)",
    "affected_question_pattern": "str|None (regex or description of affected questions)",
}

# ── 22. Lever-to-Patch-Type Mapping ────────────────────────────────────

_LEVER_TO_PATCH_TYPE: dict[tuple[str, int], str] = {
    # Lever 1: Tables & Columns
    ("wrong_column", 1): "update_column_description",
    ("wrong_table", 1): "update_description",
    ("description_mismatch", 1): "update_column_description",
    ("missing_synonym", 1): "add_column_synonym",
    ("select_star", 1): "update_column_description",
    ("missing_scd_filter", 1): "update_column_description",
    # Lever 2: Metric Views — route aggregation/measure issues to column descriptions
    ("wrong_aggregation", 2): "update_column_description",
    ("wrong_measure", 2): "update_column_description",
    ("missing_filter", 2): "update_mv_yaml",
    ("missing_temporal_filter", 2): "update_mv_yaml",
    ("wrong_filter_condition", 2): "update_column_description",
    # Lever 3: Table-Valued Functions
    ("tvf_parameter_error", 3): "add_tvf_parameter",
    ("repeatability_issue", 3): "add_tvf_parameter",
    # Lever 4: Join Specifications
    ("wrong_join", 4): "update_join_spec",
    ("missing_join_spec", 4): "add_join_spec",
    ("wrong_join_spec", 4): "update_join_spec",
    ("wrong_join_type", 4): "update_join_spec",
    # Lever 5: Genie Space Instructions (example SQL preferred over text)
    ("asset_routing_error", 5): "add_example_sql",
    ("missing_instruction", 5): "add_example_sql",
    ("ambiguous_question", 5): "add_example_sql",
    ("missing_filter", 5): "add_example_sql",
    # Fallback for "other" failure types — avoids falling through to add_instruction
    ("other", 1): "update_column_description",
    ("other", 2): "update_column_description",
    ("other", 3): "update_description",
    ("other", 4): "add_join_spec",
    ("other", 5): "add_example_sql",
}
