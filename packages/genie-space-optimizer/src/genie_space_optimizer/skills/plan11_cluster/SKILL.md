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
- forbidden_signatures: list[str] — Trial 16.3+. Hard rejections from
  prior iterations. Patch-type/shape combinations whose typed rejection
  appears here MUST NOT be re-proposed for the same target / root family.
- insufficient_repair_signatures: list[str] — Trial 19 A2. Sibling
  channel to `forbidden_signatures`. Each entry names a
  (qid, lever, patch_type, rca_kind) quadruple that landed cleanly in
  a prior iteration via the Trial 18 `kept_insufficient` acceptance
  lane — applied without regression, but did NOT move accuracy. Treat
  each entry as forbidden as a **sole primary** repair for the same
  QID/RCA family. The cluster you propose for those QIDs MUST either
  (a) carry a reinforcement bundle (>=2 distinct levers OR >=2 distinct
  patch_types so Stage 3 can synthesize a bundled proposal) or
  (b) pivot to a different lever family. Do not produce a cluster
  whose `repair_hypothesis` is a paraphrase of an insufficient entry.
</context_inputs>

<output_envelope>
{
  "result": {
    "clusters": [
      {
        "semantic_theme": "<short LLM-invented label>",
        "member_qids": ["<qid>", ...],
        "unifying_evidence": "<≤2000 chars>",
        "repair_hypothesis": "<≤1500 chars FREE TEXT — what kind of repair would fix this cluster>",
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
5. Trial 19 A4 — anti-repeat rule. Read
   `insufficient_repair_signatures` before forming clusters. If every
   QID's plausible cluster shape paraphrases an existing insufficient
   signature, either bundle reinforcement into the
   `repair_hypothesis` (name a second lever or patch_type) or pivot to
   a different repair family. A cluster whose `repair_hypothesis` is a
   sole-primary restatement of an entry there will be rejected
   downstream by the admission gate.
6. Trial 19 A4 — prefer-SQL-shape rule. When a QID's
   `intended_patch_shape` names a concrete SQL shape (e.g.,
   `enforce_explicit_top_n_cardinality`,
   `remove_unrequested_defensive_filter`,
   `add_grouping_for_geographic_dim`), prefer SQL-shape patch families
   (lever-6 `sql_snippet_filter` / lever-5b `add_example_sql`) in the
   `repair_hypothesis` over prose-only families
   (lever-5a `add_instruction`). A prose-only sole primary for a
   named SQL-shape intent is a known failure path that has emitted
   `kept_insufficient` across multiple postmortems.
</instructions>
