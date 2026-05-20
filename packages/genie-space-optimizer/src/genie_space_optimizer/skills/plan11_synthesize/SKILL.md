---
skill_id: plan11_synthesize
prompt_constant_name: PLAN11_SYNTHESIZE_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills.plan11_synthesize.output_schema:Plan11SynthesizeOutput
max_tokens: 3000
abstain_supported: true
prompt_registry_name: gso_reasoning_plan11_synthesize
---

<role>
You synthesize concrete repair patches for a single failure cluster.
You emit a list of RepairProposals — typed envelopes the GSO framework
applies through its existing dispatcher.

Patches MUST conform to the PatchType vocabulary (closed enum: the
applier dispatches on it). Within each patch_type, the patch_body is
free-form — populate the fields the applier validators expect for that
patch_type. See <patch_body_shapes> below for the per-patch_type
required fields.

You can emit 1–3 patches per cluster. Prefer fewer, surgical patches
over broad ones.
</role>

<context_inputs>
- cluster: FailureCluster (cluster_id, semantic_theme, member_qids,
                          unifying_evidence, repair_hypothesis,
                          primary_blame_set)
- member_qid_evidence: list of {qid, question_text, ground_truth_sql,
                                generated_sql, judge_rationale,
                                diagnosis (PerQidDiagnosis)}
- schema_slice: the relevant slice of the metadata snapshot —
                tables, columns, joins, instructions, example SQLs,
                snippets, MVs — that are in scope for member_qids.
                NOT the whole snapshot; the framework pre-filters.
- history: list of prior {patch, validation_outcome, applied_outcome}
           for this cluster in prior iterations (may be empty).
</context_inputs>

<patch_body_shapes>
Required fields per patch_type (missing required fields trigger repair loop):

ADD_EXAMPLE_SQL / UPDATE_EXAMPLE_SQL:
  {"example_question": str, "example_sql": str}

ADD_SQL_SNIPPET_EXPRESSION / ADD_SQL_SNIPPET_FILTER / ADD_SQL_SNIPPET_MEASURE:
  {"name": str, "sql_expression": str}

ADD_INSTRUCTION / UPDATE_INSTRUCTION_SECTION / REWRITE_INSTRUCTION:
  {"instruction_text": str (must conform to the 5-section canonical schema:
   ## KPI Definitions, ## Filter Conventions, ## Date Handling, ## Output Format, ## Notes)}

ADD_JOIN_SPEC / UPDATE_JOIN_SPEC:
  {"left": "table.column", "right": "table.column", "on": str}

ADD_COLUMN_DESCRIPTION / UPDATE_COLUMN_DESCRIPTION:
  {"table": str, "column": str, "description": str}

ADD_DESCRIPTION / UPDATE_DESCRIPTION:
  {"table": str, "description": str}

ADD_DEFAULT_FILTER / UPDATE_FILTER_CONDITION:
  {"table": str, "sql_expression": str}

HIDE_COLUMN / UNHIDE_COLUMN / RENAME_COLUMN_ALIAS:
  {"table": str, "column": str}

ADD_TABLE / REMOVE_TABLE:
  {"table": str}

ENABLE_EXAMPLE_VALUES / DISABLE_EXAMPLE_VALUES / ENABLE_VALUE_DICTIONARY / DISABLE_VALUE_DICTIONARY:
  {}  (no required fields)
</patch_body_shapes>

<output_envelope>
{
  "result": {
    "proposals": [
      {
        "intent_name": "<≤80 chars>",
        "intent_description": "<one or two sentences>",
        "repair_hypothesis": "<free text, may echo cluster's hypothesis>",
        "patch_type": "<one of PatchType.value>",
        "rationale": "<one sentence WHY this patch>",
        "confidence": "<high | medium | low>",
        "patch_body": { ...per-patch_type fields... },
        "blame_set": ["<catalog.schema.table.column>", ...],
        "target_qids": ["<qid>", ...]
      }
    ]
  } | null,
  "declined": null | <AbstainVerdict>
}
</output_envelope>

<instructions>
1. Emit 1–3 proposals. A single surgical patch is better than 3 speculative ones.
2. patch_type MUST be a valid PatchType enum value. If unsure, use add_example_sql
   (most forgiving) or add_instruction (broadest repair surface).
3. Do not repeat a patch that appears in history with a failed validation_outcome
   unless you have a concrete fix for the validator error.
4. target_qids must be a subset of the cluster's member_qids.
</instructions>
