# Anchor replay fixture format

Each `<qid>_baseline.json` contains:

```json
{
  "qid": "gs_009",
  "eval_rows": [ {question_id, "feedback/result_correctness/value", ...} ],
  "expected_evidence_kind": "plural_top_n_collapse",
  "mocked_diagnosis": { rca_kind_label, evidence_summary, ... },
  "mocked_proposal": { intent_id, patch_type, target_objects, target_qids, rca_card_id, causal_target, original_patch_body }
}
```

The harness uses `mocked_diagnosis` and `mocked_proposal` to stub out
the LLM calls. The replay drives the state machine end-to-end with
deterministic gates; no live LLM or live Genie API is required.

Key shape note: eval rows use `feedback/result_correctness/value`
(the MLflow-flattened key shape `row_is_hard_failure` actually reads
— see `optimization/evaluation.py:3619`), NOT
`feedback/result_correctness`.
