"""B5 Task 8 — 2314bb2c attribution-drift integration replay.

Synthesizes an iteration mirroring the 2314bb2c iter 1 shape (accepted
with attribution drift, decision_records contains real DecisionRecord
dataclass instances) and asserts:

1. Serialization succeeds end-to-end (no silent empty output).
2. The serialized fixture contains the iteration data including the
   coerced acceptance-decision record.
3. summarize_replay_fixture reports non-zero eval_rows.
4. The emptiness predicate the harness uses is False (so no
   GSO_REPLAY_FIXTURE_EMPTY_V1 marker would fire).
"""

from __future__ import annotations

import json


def _attribution_drift_iteration():
    """The 2314bb2c iter 1 shape — accepted with attribution drift, eval
    rows present, a real DecisionRecord in decision_records."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    acceptance_record = DecisionRecord(
        run_id="2314bb2c-95a1-4d60-8226-09e5155aee2a",
        iteration=1,
        decision_type=DecisionType.ACCEPTANCE_DECIDED,
        outcome=DecisionOutcome.ACCEPTED,
        reason_code=ReasonCode.NONE,
        ag_id="AG1",
    )
    return {
        "iteration": 1,
        "eval_rows": [
            {
                "question_id": "7now_delivery_analytics_space_gs_026",
                "result_correctness": "still_hard",
            },
            {
                "question_id": "7now_delivery_analytics_space_gs_001",
                "result_correctness": "passing",
            },
        ],
        "clusters": [
            {
                "cluster_id": "c-1",
                "root_cause": "missing_filter",
                "question_ids": [
                    "7now_delivery_analytics_space_gs_026",
                ],
            },
        ],
        "soft_clusters": [],
        "strategist_response": {
            "action_groups": [
                {
                    "id": "AG1",
                    "affected_questions": [
                        "7now_delivery_analytics_space_gs_026",
                    ],
                    "patches": [],
                },
            ],
        },
        "ag_outcomes": {"AG1": "accepted"},
        "post_eval_passing_qids": [
            "7now_delivery_analytics_space_gs_001",
        ],
        "decision_records": [
            acceptance_record,
            {
                "decision_type": "patch_applied",
                "ag_id": "AG1",
                "patch_id": "p-1",
            },
        ],
    }


def test_attribution_drift_iteration_serializes_non_empty() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iteration = _attribution_drift_iteration()
    payload = serialize_replay_fixture(
        fixture_id="2314bb2c_v1",
        iterations_data=[iteration],
    )
    fixture = json.loads(payload)
    assert len(fixture["iterations"]) == 1
    assert len(fixture["iterations"][0]["eval_rows"]) == 2
    assert len(fixture["iterations"][0]["decision_records"]) == 2
    # The coerced acceptance record survived the strip.
    types = sorted(
        r["decision_type"]
        for r in fixture["iterations"][0]["decision_records"]
    )
    assert types == ["acceptance_decided", "patch_applied"]


def test_attribution_drift_summary_reports_non_zero_eval_rows() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        summarize_replay_fixture,
    )

    summary = summarize_replay_fixture(
        iterations_data=[_attribution_drift_iteration()],
    )
    assert summary["iterations"] == 1
    assert summary["per_iter"][0]["eval_rows"] == 2
    assert summary["per_iter"][0]["decision_records"] == 2


def test_emptiness_predicate_returns_false_for_healthy_attribution_drift() -> None:
    """Mirror the predicate the harness uses to decide whether to emit the
    GSO_REPLAY_FIXTURE_EMPTY_V1 marker."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        summarize_replay_fixture,
    )

    iterations_data = [_attribution_drift_iteration()]
    summary = summarize_replay_fixture(iterations_data=iterations_data)
    fixture_iter_count = summary["iterations"]
    zero_eval = [
        per["iteration"]
        for per in summary["per_iter"]
        if int(per["eval_rows"]) == 0
    ]
    is_empty = (
        len(iterations_data) > 0
        and (fixture_iter_count == 0 or len(zero_eval) > 0)
    )
    assert is_empty is False, (
        f"healthy attribution-drift run incorrectly classified as empty: "
        f"fixture_iter_count={fixture_iter_count}, zero_eval={zero_eval}"
    )


def test_emptiness_predicate_returns_true_when_all_iterations_have_no_eval_rows() -> None:
    """The 2314bb2c iter 2-5 totality gap: iterations exist but their
    eval_rows arrays are empty (no-applied-patch iterations). The marker
    should fire."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        summarize_replay_fixture,
    )

    # Iter 1 healthy + iter 2-3 no-applied (zero eval_rows).
    healthy = _attribution_drift_iteration()
    no_applied_2 = {
        "iteration": 2,
        "eval_rows": [],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": [],
        "decision_records": [],
    }
    no_applied_3 = dict(no_applied_2)
    no_applied_3["iteration"] = 3
    iterations_data = [healthy, no_applied_2, no_applied_3]
    summary = summarize_replay_fixture(iterations_data=iterations_data)
    fixture_iter_count = summary["iterations"]
    zero_eval = [
        per["iteration"]
        for per in summary["per_iter"]
        if int(per["eval_rows"]) == 0
    ]
    is_empty = (
        len(iterations_data) > 0
        and (fixture_iter_count == 0 or len(zero_eval) > 0)
    )
    assert is_empty is True
    assert sorted(zero_eval) == [2, 3]
