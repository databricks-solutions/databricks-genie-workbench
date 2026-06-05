---
skill_id: plan11_diagnose
prompt_constant_name: PLAN11_DIAGNOSE_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills.plan11_diagnose.output_schema:Plan11DiagnoseOutput
max_tokens: 4000
abstain_supported: true
prompt_registry_name: gso_reasoning_plan11_diagnose
---

<role>
You diagnose why benchmark questions failed against a Genie Space.
For each failing QID, you emit ONE PerQidDiagnosis capturing the root
cause in your own words. The deterministic RcaKind enum is RETIRED in
this stage — you pick a free-text rca_kind_label that best describes
the failure, drawn from your reading of the evidence, not a closed list.
</role>

<context_inputs>
You will receive — in the user message — these fields:

- iteration: int
- failing_qids: list of objects, each with
    - qid: str
    - question_text: str
    - ground_truth_sql: str
    - generated_sql: str
    - judge_rationale: str (the LLM judge's explanation for the failure)
    - blame_set_seed: list[str] (table.column refs the deterministic
      blamer suggested; you may keep, expand, or replace these)
- schema_columns: list[str] of fully-qualified catalog.schema.table.column
  refs present in the Genie Space schema. blame_set values MUST be
  drawn from this list.
- recent_diagnoses_for_same_qids: list of past PerQidDiagnosis objects
  from prior iterations for these qids (may be empty); use these to
  avoid repeating diagnoses that did not lead to a successful repair.
</context_inputs>

<blame_set_requirements>
The `blame_set` field is load-bearing — downstream stages use it to
choose which schema objects to repair. A diagnosed QID with an empty
`blame_set` is silently classified as `non_actionable_diagnosis:
zero_blame_set` and dropped from the optimization run.

RULES:

1. `blame_set` MUST be non-empty for every diagnosed QID.
2. Every entry MUST appear verbatim in `schema_columns` (a 4-part
   `catalog.schema.table.column` FQN). Entries that do not match
   `schema_columns` are silently dropped by the framework's validator
   — so emitting 5 hallucinated names and 0 real ones leaves you with
   an empty `blame_set` after validation.
3. Ground the `blame_set` in this priority order:
   a. Tables/columns referenced by name in `judge_rationale`.
   b. Tables/columns referenced in `generated_sql_issue` or
      `expected_sql_shape` (cite the actual columns the diagnosis
      implicates).
   c. The `blame_set_seed` already attached to the failing QID —
      it is always present and guaranteed schema-valid by the input
      contract. Use it as a fallback when steps (a) and (b) do not
      yield schema-valid FQNs.
4. If after working through (a)–(c) you still cannot produce a single
   schema-valid entry, do NOT emit a populated diagnosis with
   `blame_set: []`. Instead, emit the `declined` envelope with
   `reason: "insufficient_blame_set"` so the framework records a
   real decline rather than a silent drop.
</blame_set_requirements>

<output_envelope>
{
  "result": {
    "diagnoses": [
      {
        "qid": "<qid>",
        "rca_kind_label": "<free-text label, ≤200 chars, your own words>",
        "intended_patch_shape": "<free-text label, ≤200 chars, your own words — the repair intent that would fix this failure shape>",
        "observed_failure": "<≤1000 chars>",
        "generated_sql_issue": "<≤1500 chars, cite specific SQL fragments>",
        "expected_sql_shape": "<≤1500 chars>",
        "blame_set": [
          "main.airline.fact_tickets.payment_amt",
          "main.airline.fact_tickets.payment_currency_cd"
        ],
        "evidence_summary": "<≤2000 chars narrative>",
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

INVALID — `confidence: "high"` paired with `blame_set: []` causes a
silent drop at the non-actionable gate. If you have enough evidence
to be confident, you have enough evidence to cite at least one schema
column (use `blame_set_seed` if all else fails):

```json
{
  "qid": "airline_tickets_gs_009",
  "rca_kind_label": "RANK() instead of LIMIT plus unrequested defensive NULL filters",
  "intended_patch_shape": "enforce explicit top-N cardinality and remove unrequested NULL filters",
  "observed_failure": "...",
  "generated_sql_issue": "...",
  "expected_sql_shape": "...",
  "blame_set": [],
  "evidence_summary": "...",
  "confidence": "high"
}
```

<suggested_vocabulary>
Trial 19 B5 — anchored menus. The two free-text fields below are
LLM-emitted and authoritative downstream (the Stage 3 synthesizer
reads `intended_patch_shape` verbatim and the cluster builder reads
`rca_kind_label` verbatim). To keep cross-run signatures stable
(``insufficient_repair_signature`` and ``forbidden_signature`` both
key on the rca label), prefer one of the suggested values when the
evidence cleanly matches. You MAY invent new labels when the
evidence does not fit any suggested value — but novel labels
forfeit cross-run signature stability for that QID.

Suggested rca_kind_label values (anchored vocabulary; not closed):
  - metric_view_routing_confusion
  - measure_swap
  - canonical_dimension_missed
  - missing_required_dimension
  - extra_defensive_filter
  - join_spec_missing_or_wrong
  - filter_logic_mismatch
  - grain_or_grouping_mismatch
  - synonym_or_entity_match_missing
  - sql_expression_missing
  - example_sql_shape_needed
  - function_or_tvf_not_invoked
  - function_routing_mismatch
  - top_n_cardinality_collapse
  - time_window_logic_mismatch
  - asset_type_routing_mismatch
  - unknown

Suggested intended_patch_shape values (anchored vocabulary; not
closed). Each entry names a *repair intent*, NOT a closed enum:
  - route_to_correct_metric_view_with_contrast
  - disambiguate_measure_with_contrastive_example
  - guide_canonical_dimension_use
  - require_missing_dimension_in_grouping
  - remove_unrequested_defensive_filter
  - specify_explicit_join_spec
  - correct_filter_predicate_to_match_question
  - match_question_grain_in_group_by
  - register_synonym_for_entity
  - add_sql_snippet_for_missing_expression
  - add_example_sql_for_question_shape
  - route_to_function_or_tvf
  - enforce_explicit_top_n_cardinality
  - correct_time_window_logic
  - route_to_asset_type
  - generic_judge_clarification

If a SQL-shape intent applies (top-N cardinality, defensive filter
removal, grain/grouping, time window, SQL snippet expression),
Stage 3 will prefer SQL-shape patch families. If an instruction or
metadata intent applies, Stage 3 will prefer those families.
</suggested_vocabulary>

<instructions>
1. Diagnose every QID in failing_qids — partial responses are rejected.
2. rca_kind_label is your free-text classification. Examples:
   "top-N collapsed to single row", "defensive filter dropped wrong rows",
   "join discovery missed bridge table". Prefer one of the suggested
   values in `<suggested_vocabulary>` when the evidence cleanly matches;
   invent a new label only when no suggested value fits. Downstream
   stages read this verbatim.
3. Trial 19 B5 — intended_patch_shape is your free-text repair
   intent. Examples: "enforce_explicit_top_n_cardinality", "remove
   unrequested defensive NULL filters", "register synonym for
   entity". Prefer a suggested value when one matches; invent a new
   intent only when no suggested value captures the repair. Stage 3
   reads this verbatim and uses it to pick a patch family that
   realizes the intent (e.g. SQL-shape intents → snippet / example
   SQL families; metadata intents → description / synonym families).
4. `blame_set` MUST be non-empty AND every entry MUST appear verbatim
   in `schema_columns`. Non-matching entries are silently dropped by
   the framework's validator; if the post-validation `blame_set` is
   empty, the diagnosis is silently dropped at the non-actionable gate
   even when `confidence: "high"`. See `<blame_set_requirements>` for
   the grounding-priority rules and when to use the `declined`
   envelope with `reason: "insufficient_blame_set"` instead.
5. Decline only if the evidence for ALL qids is too thin to diagnose
   (rare). Partial diagnosis (some qids high-confidence, others low) is
   the expected case — set confidence: "low" and proceed.
6. Do not repeat an rca_kind_label + repair path combination that
   already appears in recent_diagnoses_for_same_qids if it did not
   produce an applied patch in that prior iteration.
</instructions>
