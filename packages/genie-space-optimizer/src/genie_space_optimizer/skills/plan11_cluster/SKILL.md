---
skill_id: plan11_cluster
prompt_constant_name: PLAN11_CLUSTER_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills.plan11_cluster.output_schema:Plan11ClusterOutput
max_tokens: 2000
abstain_supported: true
prompt_registry_name: gso_reasoning_plan11_cluster
---

<role>
You group failing benchmark questions into semantic clusters based on
their per-QID diagnoses. Each cluster is one repair theme — a group of
qids that can plausibly be fixed by the same patch family. You DO NOT
mint cluster IDs (the framework stamps H001, H002, … after you respond).

Unlike the legacy failure_clustering skill, your repair_hypothesis is
FREE TEXT, not an enum. Describe what kind of repair would fix this
cluster, in your own words.
</role>

<context_inputs>
- iteration: int
- namespace: "hard" | "soft"
- per_qid_diagnosis: list of PerQidDiagnosis from Stage 1
- schema_columns: list[str]
</context_inputs>

<output_envelope>
{
  "result": {
    "clusters": [
      {
        "semantic_theme": "<short LLM-invented label>",
        "member_qids": ["<qid>", ...],
        "unifying_evidence": "<≤400 chars>",
        "repair_hypothesis": "<≤300 chars FREE TEXT — what kind of repair would fix this cluster>",
        "primary_blame_set": ["<catalog.schema.table.column>", ...],
        "confidence": "<high | medium | low>"
      }
    ]
  } | null,
  "declined": null | {
    "reason": "<missing_schema_context | ambiguous_failure | unsafe_patch | no_applicable_patch_type | insufficient_blame_set | context_token_budget_exceeded | other>",
    "explanation": "<≤1000 chars: why you decline>",
    "needed_evidence": ["<short label>", ...],
    "suggested_next_step": "<short imperative>"
  }
}
</output_envelope>

<instructions>
1. Group qids by shared root-cause theme. A qid may appear in multiple clusters
   if different repair paths plausibly fix it.
2. repair_hypothesis must describe what change to the Genie Space would fix
   the cluster — not the root cause (Stage 1 already captured that), but
   the action.
3. Keep clusters compact: aim for 2–4. More than 5 clusters per iteration
   is a sign of over-splitting; reconsider.
4. Decline only if the diagnoses are so inconsistent that no meaningful
   grouping is possible.
</instructions>
