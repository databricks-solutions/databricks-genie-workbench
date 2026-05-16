---
skill_id: lever-6-sql-expression
prompt_constant_name: LEVER_6_SQL_EXPRESSION_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: true
description: Add a reusable SQL expression (measure / filter / dimension) to the knowledge store.
when_to_pick: Failure is a concrete missing measure, filter, or dimension that can be expressed as a single reusable SQL snippet at the MV level.
target_kind: metric_view
target_min_count: 0
---
<unified_rca_engine_contract>
## Unified RCA engine contract

The optimizer is a closed-loop control system. Every proposed action must
preserve this chain:

judge feedback -> RCA -> lever -> patch -> gateable outcome

Primary objective:
- Reach 100% post-arbiter accuracy, or exhaust the configured lever-loop budget.
- Hard failures are the first priority. Hard failures include arbiter verdicts
  `ground_truth_correct` and `neither_correct`.
- Soft signals may guide preventive improvements only when hard failures and
  mandatory regression debt are not being starved.

Mandatory causal fields:
- Every action group must declare `primary_cluster_id`, `source_cluster_ids`,
  and `affected_questions` using those exact JSON field names.
- Every proposal must be explainable as: this judge signal produced this RCA,
  this RCA maps to this lever, and this patch is expected to fix these target
  questions.
- If `regression_debt_qids` are present in context, they are mandatory priority
  and must be targeted before optional soft improvements.

Patch safety rules:
- A patch type must match RCA defect. A filter defect needs a filter patch,
  scoped instruction, or example SQL. Do not substitute a measure patch for a
  missing or wrong filter.
- A broad global instruction change is unsafe unless it is scoped to target
  questions or backed by explicit counterfactual dependents.
- Prefer narrow structured metadata, SQL expressions, join specs, or example SQL
  over broad prose when the root cause is structural SQL behavior.
- Preserve at least one causal patch per target question when proposing a bundle.

Regression policy awareness:
- Net post-arbiter gains can be accepted with bounded regression debt.
- Do not hide or ignore newly regressed hard questions; surface them as
  `regression_debt_qids`.
- Protected or required benchmark regressions must be treated as unbounded
  collateral risk.

Leakage boundary:
- Do not copy held-out benchmark expected SQL into Genie-visible examples.
- Use failure evidence and generated SQL to understand behavior, but output
  reusable guidance, scoped metadata, SQL expressions, or safe example patterns.

Precedence:
- If a downstream prompt provides a more specific lever map (for example a
  strategist `## Contract: All Instruments of Power` section), that map is
  authoritative for lever routing. This contract specifies the global control
  invariants only.
</unified_rca_engine_contract>


You are an expert at defining SQL Expressions for Databricks Genie Spaces.

## Context

A Genie Space is answering user questions incorrectly. Analysis of the failures
shows the root cause is: **{{ root_cause }}**

## Raw Failure Evidence
{{ raw_evidence_block }}
### Failed questions and SQL diffs
{{ cluster_context }}

### Current schema
{{ schema_context }}

### Existing SQL Expressions (do NOT duplicate these)
{{ existing_sql_snippets }}

### Strategist hints (optional — adopt, modify, or override as needed)
{{ strategist_hints }}

## Task

Based on the failure analysis, define ONE SQL Expression that would fix or
improve the identified questions.  Choose the most appropriate type:

- **measure**: A KPI or aggregation (e.g. `SUM(revenue) - SUM(cost)`).
  Use when the failure involves wrong aggregation, missing metric, or
  incorrect calculation.
- **filter**: A boolean condition (e.g. `order_total > 1000`).
  Use when the failure involves missing filters, wrong filter conditions,
  or common WHERE patterns that recur across questions.
- **expression** (dimension): A per-row derived value (e.g. `MONTH(date_col)`).
  Use when the failure involves missing grouping attributes, derived columns,
  or computed dimensions.

### Failure-type → snippet_type routing prior

{{ failure_type_routing_table }}

### Routing examples

These show the expected shape for the most common Trial-5 patterns. Each
example pairs a 1-line cluster summary with the JSON proposal that should
follow. Use them as templates, not as canon — your output should reflect
the actual cluster you receive.

**Example 1 — `plural_top_n_collapse` → `expression` (ROW_NUMBER window)**

Cluster: `failure_type=plural_top_n_collapse`, `blame_set=["RANK", "coupon_count"]`, `question_ids=["q42"]`

```json
{
  "snippet_type": "expression",
  "display_name": "TKT_COUPON Route Rank (Top N without ties)",
  "alias": "route_rank_no_ties",
  "sql": "ROW_NUMBER() OVER (ORDER BY COUNT(tkt_coupon.COUPON_SEQ_NBR) DESC)",
  "synonyms": ["top routes rank", "route ranking no ties"],
  "instruction": "Use this expression when ranking routes to select a top N. ROW_NUMBER() returns exactly N rows; RANK() returns more when ties exist.",
  "rationale": "RANK() produced 16 rows for 'top 10'; ROW_NUMBER() guarantees 10.",
  "target_table": "tkt_coupon",
  "affected_questions": ["q42"]
}
```

**Example 2 — `missing_filter` → `filter`**

Cluster: `failure_type=missing_filter`, `blame_set=["PAYMENT_STATUS_CD"]`, `question_ids=["q15", "q16"]`

```json
{
  "snippet_type": "filter",
  "display_name": "TKT_PAYMENT Settled Payments Only",
  "alias": "",
  "sql": "tkt_payment.PAYMENT_STATUS_CD = 'SETTLED'",
  "synonyms": ["settled payments", "completed payments"],
  "instruction": "Use this filter when the question concerns completed payments. Genie was including pending and reversed payments in the count.",
  "rationale": "Two questions asked for 'completed payments' but the generated SQL omitted any status filter.",
  "target_table": "tkt_payment",
  "affected_questions": ["q15", "q16"]
}
```

**Example 3 — `wrong_aggregation` → `measure`**

Cluster: `failure_type=wrong_aggregation`, `blame_set=["FARE_AMT"]`, `question_ids=["q22"]`

```json
{
  "snippet_type": "measure",
  "display_name": "TKT_DOCUMENT Distinct Tickets by Fare",
  "alias": "distinct_tickets_by_fare",
  "sql": "COUNT(DISTINCT tkt_document.DOCUMENT_NBR_TEXT)",
  "synonyms": ["distinct ticket count", "unique tickets"],
  "instruction": "Use this measure when the question asks for the number of unique tickets. The generated SQL used COUNT(*) which double-counts conjunctive tickets.",
  "rationale": "Question expects unique ticket count; SUM/COUNT(*) over fare amounts inflated the result.",
  "target_table": "tkt_document",
  "affected_questions": ["q22"]
}
```

**Example 4 — `missing_dimension` → `expression` (CASE bucket)**

Cluster: `failure_type=missing_dimension`, `blame_set=["TOTAL_FARE_USD_AMT"]`, `question_ids=["q33"]`

```json
{
  "snippet_type": "expression",
  "display_name": "TKT_DOCUMENT Fare Bucket (Low/Mid/High)",
  "alias": "fare_bucket",
  "sql": "CASE WHEN tkt_document.TOTAL_FARE_USD_AMT < 500 THEN 'Low' WHEN tkt_document.TOTAL_FARE_USD_AMT < 2000 THEN 'Mid' ELSE 'High' END",
  "synonyms": ["fare tier", "price bucket"],
  "instruction": "Use this expression to group tickets by fare tier. Genie was unable to derive the bucket boundaries without an explicit dimension.",
  "rationale": "Question asks for distribution by fare tier; no bucket dimension existed.",
  "target_table": "tkt_document",
  "affected_questions": ["q33"]
}
```

**Example 5 — no-fit case: do NOT propose**

Cluster: `failure_type=other`, `blame_set=[]`, `question_ids=["q99"]`, `counterfactual_fixes=["Genie connection timed out"]`

This cluster is an infrastructure failure, not a SQL-shape problem. No snippet
can fix it. Return the LLM equivalent of "no proposal" by emitting the JSON
with `affected_questions: []` and a `rationale` explaining why no snippet
applies — the post-validator will drop it cleanly:

```json
{
  "snippet_type": "expression",
  "display_name": "n/a",
  "alias": "",
  "sql": "",
  "synonyms": [],
  "instruction": "",
  "rationale": "Cluster reflects infrastructure failure (Genie connection timeout); no SQL snippet applies.",
  "target_table": "",
  "affected_questions": []
}
```

## Output format (strict JSON)

```json
{{
  "snippet_type": "measure" | "filter" | "expression",
  "display_name": "Human-readable name for the concept",
  "alias": "snake_case_identifier (required for measure/expression, omit for filter)",
  "sql": "The SQL expression (single string, no trailing semicolon)",
  "synonyms": ["synonym1", "synonym2"],
  "instruction": "When and how Genie should use this expression",
  "rationale": "Why this expression fixes the identified failures",
  "target_table": "primary table this expression references",
  "affected_questions": ["q1", "q2"]
}}
```

Rules:
- ``affected_questions`` MUST be a subset of the ``question_ids`` array inside the cluster JSON shown above. The cluster's ``cluster_id`` (e.g. ``"H001"``, ``"AG_PIPELINE"``) is NOT a question ID — never put it in ``affected_questions``. If the cluster shows ``"question_ids": ["q42", "q43"]``, valid values for ``affected_questions`` are subsets of ``["q42", "q43"]``. Empty array is allowed if no specific question maps to this snippet.
- ALL column references MUST use table_name.column_name syntax (e.g. `mv_sales.revenue`, NOT bare `revenue`). The Genie API rejects bare column names.
- The SQL MUST reference only tables and columns that exist in the schema.
- For measures: SQL must be a valid aggregation expression (SUM, COUNT, AVG, etc.).
- For filters: SQL must evaluate to a boolean.
- For expressions: SQL must produce a scalar value per row.
- Do NOT wrap in SELECT or WHERE — provide the raw expression only.
- Do NOT duplicate an existing SQL Expression.
- Prefer concise, reusable definitions over question-specific hacks.

Naming policy (REQUIRED — be specific, never generic):
- ``display_name`` MUST be specific enough to disambiguate the table or business domain inside this space. If two fact tables or metric views in the schema could plausibly share a concept, the name MUST encode which one it applies to.
- When the SQL references a domain-specific table such as ``mv_<domain>_fact_<entity>`` or ``mv_<domain>_dim_<entity>`` (for example ``mv_orders_fact_lines`` or ``mv_claims_dim_date``), include the compact qualifier (``ORDERS``, ``CLAIMS``, …) at the start of ``display_name`` — e.g. ``ORDERS Month-to-Date Filter``, NOT ``Month-to-Date Filter``.
- ``instruction`` MUST state when to use the expression and which table or domain it applies to.
- Avoid generic names like ``Month-to-Date Filter``, ``Total Revenue``, or ``Active Filter`` when more than one fact table or metric view in the schema could host that concept.
