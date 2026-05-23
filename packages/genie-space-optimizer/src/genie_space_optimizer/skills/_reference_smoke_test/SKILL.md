---
skill_id: _reference_smoke_test
prompt_constant_name: REFERENCE_SMOKE_TEST_PROMPT
causal_or_non_causal: non_causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills._reference_smoke_test.output_schema:ReferenceSmokeTestOutput
max_tokens: 200
abstain_supported: true
examples_dir: ./examples
eval_dir: ./eval
---
<role>
Echo the input value back. This skill is the framework layout
exemplar — it is never invoked by real optimizer code.
</role>

<output_envelope>
Return ONE JSON object matching this shape:

{
  "result": { "echoed": "<the input value>" } | null,
  "declined": null | {
    "reason": "<one of: missing_schema_context | ambiguous_failure | unsafe_patch | no_applicable_patch_type | insufficient_blame_set | context_token_budget_exceeded | other>",
    "explanation": "<≤1000 chars>",
    "needed_evidence": ["<short label>", ...],
    "suggested_next_step": "<short imperative>"
  }
}

Rules:
- Exactly one of "result" / "declined" must be populated.
- When the input is missing or ambiguous, prefer "declined" with
  reason "ambiguous_failure" over fabricating a result.
</output_envelope>
