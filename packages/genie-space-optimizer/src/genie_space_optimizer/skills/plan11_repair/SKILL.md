---
skill_id: plan11_repair
prompt_constant_name: PLAN11_REPAIR_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills.plan11_repair.output_schema:Plan11RepairOutput
max_tokens: 2000
abstain_supported: true
prompt_registry_name: gso_reasoning_plan11_repair
---

<role>
Your previous RepairProposal failed validation. The framework provides
the typed validator errors below. Emit a revised RepairProposal that
fixes ALL listed errors. Keep the same intent_name and patch_type
unless the errors specifically reject the patch_type (error_kind=
"patch_type_unknown"). Stay scoped to the same target_qids unless the
errors prove otherwise.
</role>

<context_inputs>
- original_patch: RepairProposal (the one that failed)
- validator_errors: list of ValidationError objects (error_kind,
                    error_detail, failing_location)
- cluster: FailureCluster (for context)
- schema_slice: same shape as Stage 3
- attempt: int (1 or 2; second attempt is the last)
</context_inputs>

<output_envelope>
{
  "result": { ...same shape as Stage 3 — one RepairProposal... } | null,
  "declined": null | <AbstainVerdict>
}
</output_envelope>

<instructions>
1. Address every error_kind in validator_errors. The most common fixes:
   - "genie_schema": add missing required fields per patch_body_shapes
   - "sql_execution": fix the SQL syntax / table references
   - "instruction_canonical": restructure to the 5-section format
   - "patch_type_unknown": pick a valid PatchType value
2. If error_kind is "patch_type_unknown" choose from:
   add_example_sql, add_instruction, add_column_description, add_join_spec,
   add_sql_snippet_expression, add_sql_snippet_filter, add_sql_snippet_measure
3. Keep patch_type, intent_name, and target_qids unchanged unless errors
   force a change.
4. If you cannot fix the errors without fundamentally changing the patch
   strategy, decline (set declined, null result).
</instructions>
