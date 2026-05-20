# Plan 10 Phase C — Anchor Replay Fixtures

Four production-failure fixtures that gate the Plan 10 deploy.

## Why these four

Two deployments before Plan 10 (`59a173d3` airline + `ab65fefe` 7now)
both landed accuracy delta **0.00** — three LLM-architecture cycles
with no needle movement. Postmortems confirmed that the failing
anchors stalled in `proposal_generation` (zero structural candidates,
zero accepted patches).

The four anchors below are exactly the four failing QIDs from those
two runs. Each is a **clean, reproducible failure** of the
Plan 9 LLM-direct dispatch path — the path Plan 10 ratifies.

| Fixture | Source run | Iter | Cluster | QID | Observed root_cause |
| --- | --- | --- | --- | --- | --- |
| `airline_gs_009_plural_top_n_collapse.json` | `59a173d3-f71f-4901-90ad-e10f1084cd7f` | 1 | H001 | `airline_ticketing_and_fare_analysis_gs_009` | `plural_top_n_collapse` |
| `airline_gs_024_missing_filter.json` | `59a173d3-f71f-4901-90ad-e10f1084cd7f` | 1 | H002 | `airline_ticketing_and_fare_analysis_gs_024` | `missing_filter` |
| `7now_gs_013_wrong_filter_condition.json` | `ab65fefe-9bb5-411c-9818-f62633ec9cfd` | 1 | H001 | `7now_delivery_analytics_space_gs_013` | `wrong_filter_condition` |
| `7now_gs_026_plural_top_n_collapse.json` | `ab65fefe-9bb5-411c-9818-f62633ec9cfd` | 1 | H002 | `7now_delivery_analytics_space_gs_026` | `plural_top_n_collapse` |

Each fixture also captures the Plan 10 spec-named **semantic failure shape**
(`failure_shape`), distinct from the deterministic `root_cause` label
the legacy RCA classifier emitted:

* `airline_gs_009` — `plural_top_n_collapse` (RANK vs LIMIT)
* `airline_gs_024` — `unrequested_filter` (over-filtering on currency)
* `7now_gs_013`   — `day_vs_mtd_grain` (CTE split needed for day-vs-MTD)
* `7now_gs_026`   — `zone_vp_collapse` (zone_combination vs zone_vp_name)

## Schema

```json
{
  "fixture_id": "<run_short>_gs_<NNN>_<failure_shape>",
  "source_run_id": "<parent_run_id_uuid>",
  "source_iteration": 1,
  "source_cluster_id": "H001",
  "source_ag_id_synthetic": "AG_REPLAY_PLAN10_<cluster_id>",
  "failing_qid": "<qid>",
  "failure_shape": "<semantic_label>",
  "observed_root_cause": "<deterministic_root_cause>",
  "production_evidence": {
    "rca_evidence_typed_per_qid_was_empty": true,
    "rca_kinds_by_qid_was_empty": true,
    "comment": "The Plan 5 LLM-direct dispatch gate closed silently because the deterministic RCA classifier returned no typed evidence for this QID."
  },
  "cluster": { /* verbatim from cluster_formation output */ },
  "llm_cluster_seed": {
    "cluster_id": "H001",
    "semantic_theme": "...",
    "member_qids": ["..."],
    "unifying_evidence": "...",
    "suggested_repair_shape": "...",
    "primary_blame_set": ["..."],
    "confidence": "medium"
  },
  "minimal_metadata_snapshot": {
    "schema_columns": [],
    "instructions": {"example_question_sqls": []},
    "data_sources": {}
  },
  "expected_dispatch_outcome": {
    "current_code_returns": "empty_list",
    "current_code_failure_mode": "leak1_silent_gate_closure_via_empty_rca_evidence_typed",
    "secondary_failure_mode_when_leak1_patched": "leak2_synthesizer_strips_target_objects",
    "post_plan10_expected": "non_empty_list_with_target_objects_populated"
  }
}
```

## How the deploy gate uses these

`tests/replay/test_plan10_anchor_replay.py` parametrizes over the
four fixtures. For each, it invokes `_dispatch_lever_5b_for_cluster`
with the fixture's cluster + `rca_evidence_typed = {}` (production
reality) and a mocked LLM that returns a known-valid Plan 9 envelope
(populated `target_objects` and `required_constructs`).

* **Current code (RED)** — dispatch returns `[]` because the gate at
  `optimizer.py:10443` requires non-empty `rca_evidence_typed`. The
  LLM mock is never called.
* **Post-Plan-10 (GREEN)** — gate is decoupled from
  `rca_evidence_typed` (see Plan 10 Phase A2 `optimizer.py` edit).
  Dispatch reaches the synthesizer, which now uses
  `RepairProposal.from_llm_output()` (Phase A1 edit) preserving
  `target_objects`. The materialized dict carries `target_objects`
  through to downstream materialization.

## Regenerating these fixtures

`_extract.py` reads the evidence bundles checked in at
`packages/genie-space-optimizer/docs/runid_analysis/<run_id>/evidence/`
and emits these four JSON files. Run from the repo root:

```bash
python packages/genie-space-optimizer/tests/replay/fixtures/plan10/_extract.py
```

If you delete a fixture and run the extractor, it is recreated
deterministically — same bundle, same JSON byte-for-byte.
