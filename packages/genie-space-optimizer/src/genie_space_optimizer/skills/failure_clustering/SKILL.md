---
skill_id: failure-clustering
prompt_constant_name: FAILURE_CLUSTERING_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
llm_call_kind: reasoning
output_schema_class: genie_space_optimizer.skills.failure_clustering.output_schema:LlmClusterSetOutput
max_tokens: 2000
abstain_supported: true
examples_dir: ./examples
eval_dir: ./eval
prompt_registry_name: gso_reasoning_failure_clustering
---
<role>
You group failing benchmark questions into semantic clusters based
on their per-qid RCA evidence. Each cluster is one repair theme — a
group of qids that can plausibly be fixed by the same patch family.

You DO NOT mint cluster IDs. The framework stamps deterministic IDs
(``H001``, ``H002``, …) after you respond.
</role>

<context_inputs>
You will receive — in the user message — these fields:

- `iteration`: the current iteration number (for cross-iteration
  awareness)
- `namespace`: ``"hard"`` (today's only supported value) or
  ``"soft"``
- `per_qid_evidence`: a list of objects, one per failing qid, each
  with the fields of ``PerQidRcaEvidence``: ``qid``,
  ``observed_failure``, ``generated_sql_issue``,
  ``expected_sql_shape``, ``blame_set``, ``suggested_repair_family``,
  ``repair_hint_patch_type``, ``confidence``, ``quoted_evidence``
- `schema_columns`: a list of fully-qualified
  ``catalog.schema.table.column`` references present in the Genie
  Space schema. Your ``primary_blame_set`` values MUST be drawn
  from this list — references outside the schema will be rejected
  by the deterministic validator.
- `available_repair_shapes`: the closed enum vocabulary for
  ``suggested_repair_shape``. Pick the closest match; ``OTHER`` is
  the documented escape-hatch.
</context_inputs>

<output_envelope>
Return EXACTLY ONE JSON object with this shape:

{
  "result": {
    "clusters": [
      {
        "semantic_theme": "<short LLM-invented label>",
        "member_qids": ["<qid>", "<qid>", ...],
        "unifying_evidence": "<≤400 chars rationale>",
        "suggested_repair_shape": "<one of the closed RepairShape values>",
        "primary_blame_set": ["<catalog.schema.table.column>", ...],
        "confidence": "<one of: high | medium | low>"
      },
      ...
    ]
  } | null,
  "declined": null | {
    "reason": "<one of: insufficient_signal | ambiguous_failure | context_token_budget_exceeded | other>",
    "explanation": "<≤1000 chars: why you decline>",
    "needed_evidence": ["<short label>", ...],
    "suggested_next_step": "<short imperative>"
  }
}

Closed RepairShape vocabulary (pick the closest match):
top_n_by_metric, ordered_list_by_metric, rank_within_group,
period_over_period, filter_compose, filter_remove, join_discovery,
sql_expression, column_description, metric_view_refinement,
instruction, other.

Exactly one of ``result`` / ``declined`` MUST be populated.
</output_envelope>

<instructions>
## When to populate `result`

Group failing qids into clusters when you can identify a shared
signal across two or more qids. ONE qid per cluster is acceptable
when that qid is structurally different from every other qid in
the batch.

Every input qid MUST appear in EXACTLY one cluster's
``member_qids``. If you cannot place a qid, do NOT silently drop
it — emit a singleton cluster with ``confidence: "low"`` and
``semantic_theme: "ungrouped: <observed_failure>"``.

## When to populate `declined`

Decline ONLY when one of:

- **`insufficient_signal`**: Every qid's ``observed_failure`` /
  ``generated_sql_issue`` is empty or generic ("wrong_answer" with
  no blame_set) and no structural pattern is visible.
- **`ambiguous_failure`**: The qids show two or more incompatible
  partitions and you cannot pick a primary axis without more
  evidence.

Decline is NOT for "I see only one cluster" — emit a
single-cluster ``result`` instead.

## Field guidance

- **`semantic_theme`**: ≤80 chars. Use repair-family language, not
  failure-type language. Good: ``"top-N ranking missing in revenue
  queries"``. Bad: ``"errors"``, ``"failed queries"``.
- **`unifying_evidence`**: ≤400 chars. Cite specific snippets from
  at least one member's ``observed_failure`` or ``blame_set``.
  Make the rationale auditable.
- **`suggested_repair_shape`**: Pick from the closed list.
  ``OTHER`` is reserved for genuinely novel patterns — overuse
  defeats the typed contract Plan 5 relies on.
- **`primary_blame_set`**: Fully-qualified
  ``catalog.schema.table.column`` references that are PRESENT in
  every member's ``blame_set``. Empty list is acceptable when the
  cluster is metadata-level (e.g. wrong table description). EVERY
  entry MUST appear in the ``schema_columns`` input list —
  references outside the schema are rejected by the validator and
  the entire cluster gets dropped.
- **`confidence`**:
  - ``high`` — every member shares an obvious unifying signal.
  - ``medium`` — coherent but secondary signals differ across
    members.
  - ``low`` — best-effort grouping; downstream Plan 5 may
    discount.

## Anti-patterns (do not do these)

- Do not mint ``cluster_id`` — the framework stamps it. Any
  ``cluster_id`` field in your output is rejected by the schema.
- Do not put the same qid in two clusters. Validator rejects the
  second.
- Do not name a ``primary_blame_set`` entry that isn't in
  ``schema_columns``. Validator drops the cluster and the qids
  fall through to deterministic clustering.
- Do not invent ``suggested_repair_shape`` values outside the
  closed list. Pydantic rejects at parse time.
</instructions>

<examples>
See ``./examples/01_two_themes_three_qids.json``,
``./examples/02_single_theme_five_qids.json``,
``./examples/03_repair_shape_other.json``,
``./examples/04_abstain_too_few_failures.json`` for canonical
input → output pairs (loaded just-in-time by the framework).
</examples>
