---
skill_id: plan11_narrow
prompt_constant_name: PLAN11_NARROW_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills.plan11_narrow.output_schema:Plan11NarrowOutput
max_tokens: 2000
abstain_supported: true
prompt_registry_name: gso_reasoning_plan11_narrow
---

<role>
Your RepairProposal is structurally valid but the empirical blast-radius
gate found that it breaks currently-passing benchmark questions
(collateral_qids). Emit a revised RepairProposal that fixes ONLY your
target_qids without affecting the collateral_qids.

Common narrowing techniques:
- Add a defensive WHERE clause to SQL snippets
- Qualify example SQL with a question-specific filter
- Change patch_type from broad (e.g. update_filter_condition) to surgical
  (e.g. add_example_sql for the specific question only)
- Scope instruction changes to a subsection (update_instruction_section)
  rather than rewriting the full instruction
</role>

<context_inputs>
- original_patch: RepairProposal
- target_qids: list[str] (you must still fix these)
- collateral_qids: list[str] (you must NOT break these)
- protected_sql: {qid: SQL} for each collateral_qid
- cluster: FailureCluster
- attempt: int (1 or 2; second attempt is the last)
</context_inputs>

<output_envelope>
{
  "result": { ...same shape as Stage 3 — one RepairProposal... } | null,
  "declined": null | <AbstainVerdict>
}
</output_envelope>

<instructions>
1. The revised patch_body should be strictly more specific than the original
   — narrower SQL predicates, smaller instruction scope, or a surgical
   patch_type swap. Avoid broad rewrites.
2. target_qids in the output must equal (or be a subset of) the input
   target_qids. Never add new QIDs.
3. If you cannot narrow the patch without losing coverage of target_qids,
   decline (set declined, null result).
</instructions>
