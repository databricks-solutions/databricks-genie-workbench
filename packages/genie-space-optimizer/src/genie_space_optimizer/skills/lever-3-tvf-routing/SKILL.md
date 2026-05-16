---
skill_id: lever-3-tvf-routing
prompt_constant_name: PROPOSAL_GENERATION_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: true
description: Fix TVF descriptions or route through the right TVF when a function call is missing or wrong.
when_to_pick: Failure stems from missing or misrouted user-defined function calls; Genie should call a TVF but doesn't, or calls the wrong one.
target_kind: function
target_min_count: 0
---
<role>
You are a Databricks metadata optimization expert. Your job is to fix a Genie Space so that it generates correct SQL for user questions.
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
## Failure Analysis
- Root cause: {{ failure_type }}
- Blamed objects: {{ blame_set }}
- Affected questions ({{ severity }}): {{ affected_questions }}

## SQL Diffs (Expected vs Generated)
{{ sql_diffs }}

## Current Metadata for Blamed Objects
{{ current_metadata }}

## Target Change Type
{{ patch_type_description }}
</context>

<instructions>
Analyze the SQL diffs. Identify EXACTLY what metadata change (column description, table description, or instruction) would guide Genie to produce the expected SQL.

- Be specific — reference actual table/column names from the SQL.
- Do NOT generate generic instructions. Generate a targeted metadata fix.
- Instruction budget remaining: {{ instruction_char_budget }} chars. Keep additions under 500 chars.
</instructions>

<output_schema>
Return JSON: {"proposed_value": "...", "rationale": "..."}
</output_schema>