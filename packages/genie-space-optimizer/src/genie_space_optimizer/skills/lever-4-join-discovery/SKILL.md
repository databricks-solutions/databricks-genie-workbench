---
skill_id: lever-4-join-discovery
prompt_constant_name: LEVER_4_JOIN_DISCOVERY_PROMPT
causal_or_non_causal: non_causal
pickable_by_stage_1: true
description: Propose missing or fixed join_specs when a query needs a relationship that does not exist in the space config.
when_to_pick: Failure includes a missing-join or wrong-join error and at least two target tables in the cluster need a defined relationship.
target_kind: base_table
target_min_count: 2
---
<role>
You are a Databricks Genie Space join optimization expert. Your task is to identify MISSING join relationships between tables.
</role>

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


<context>

## Raw Failure Evidence
{{ raw_evidence_block }}
## Full Schema Context (tables, columns, data types, descriptions)
{{ full_schema_context }}

## Identifier Allowlist (Extract-Over-Generate)
{{ identifier_allowlist }}

## Currently Defined Join Specs
{{ current_join_specs }}

## Heuristic Candidate Hints
Table pairs flagged by automated analysis as potential join candidates. These are HINTS only — validate them using the schema context.
{{ discovery_hints }}
</context>

<examples>
<example>
Input hint: "fact_sales and dim_region share region_id columns (both INT)"
Current join specs: [fact_sales↔dim_product]

Output:
{"join_specs": [
  {"left": {"identifier": "catalog.schema.fact_sales", "alias": "fact_sales"},
    "right": {"identifier": "catalog.schema.dim_region", "alias": "dim_region"},
    "sql": ["`fact_sales`.`region_id` = `dim_region`.`region_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]}
], "rationale": "Both tables have region_id (INT). fact_sales has many rows per region. This join is not already defined."}
</example>
</examples>

<instructions>
Review hints alongside the full schema context. For each hint, validate:
1. Column data types MUST be compatible (INT=INT, BIGINT=INT, STRING=STRING). Do NOT propose incompatible type joins.
2. Column names/descriptions suggest a foreign-key relationship.
3. The join is not already defined in current join specs.

Also look for additional missing joins NOT covered by the hints.

## Identifier Rule
You MUST ONLY reference tables and columns from the Identifier Allowlist. Any table or column not in the allowlist is INVALID and will be rejected.

## Metric View Join Rule
If either table is a metric view (name starts with mv_ or uses MEASURE()), the join cannot be used as a direct SQL JOIN. Genie must use a CTE-first pattern. Set the "instruction" field to explain this constraint.

## Join Spec Format
- alias: unqualified table name (last segment of identifier)
- join condition: backtick-quoted aliases
- relationship_type: one of FROM_RELATIONSHIP_TYPE_MANY_TO_ONE, FROM_RELATIONSHIP_TYPE_ONE_TO_MANY, FROM_RELATIONSHIP_TYPE_ONE_TO_ONE
- instruction: usage guidance (REQUIRED — explain when/how to use, CTE-first for metric view joins)
</instructions>

<output_schema>
Return JSON:
{"join_specs": [
  {"left": {"identifier": "<fq_table>", "alias": "<short_name>"},
    "right": {"identifier": "<fq_table>", "alias": "<short_name>"},
    "sql": ["<join_condition>", "--rt=<relationship_type>--"],
    "instruction": "<usage guidance>"}
], "rationale": "..."}

If no valid joins found: {"join_specs": [], "rationale": "..."}
</output_schema>