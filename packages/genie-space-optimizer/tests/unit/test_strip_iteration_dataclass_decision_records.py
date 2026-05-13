"""B5 Task 1 — _strip_iteration must coerce dataclass decision_records."""

from __future__ import annotations


def _mk_record(decision_type: str = "patch_applied", ag_id: str = "AG1"):
    """Real DecisionRecord — exposes to_dict() but is NOT a Mapping."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    return DecisionRecord(
        run_id="r-1",
        iteration=1,
        decision_type=DecisionType(decision_type),
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode.NONE,
        ag_id=ag_id,
    )


def test_strip_iteration_coerces_dataclass_decision_records() -> None:
    """The 2314bb2c failure shape: an iteration contains DecisionRecord
    dataclass instances mixed into decision_records. _strip_iteration must
    coerce them via to_dict() before _strip_dict runs."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        _strip_iteration,
    )

    iteration = {
        "iteration": 1,
        "eval_rows": [
            {"question_id": "q1", "result_correctness": "passing"},
        ],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": ["q1"],
        "decision_records": [
            _mk_record(decision_type="patch_applied", ag_id="AG1"),
            {"decision_type": "control_plane_acceptance", "ag_id": "AG1"},
        ],
    }
    out = _strip_iteration(iteration)
    records = out["decision_records"]
    assert len(records) == 2
    assert records[0]["decision_type"] == "patch_applied"
    assert records[0]["ag_id"] == "AG1"
    assert records[1]["decision_type"] == "control_plane_acceptance"


def test_strip_iteration_handles_to_dict_raising_dataclass() -> None:
    """A buggy dataclass whose to_dict raises must be dropped, not bubble."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        _strip_iteration,
    )

    class _RaisingRecord:
        def to_dict(self):
            raise RuntimeError("synthesized to_dict failure")

    iteration = {
        "iteration": 1,
        "eval_rows": [],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": [],
        "decision_records": [
            _RaisingRecord(),
            {"decision_type": "control_plane_acceptance"},
        ],
    }
    out = _strip_iteration(iteration)
    records = out["decision_records"]
    # Only the dict survived.
    assert len(records) == 1
    assert records[0]["decision_type"] == "control_plane_acceptance"


def test_strip_iteration_drops_non_record_garbage_silently() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        _strip_iteration,
    )

    iteration = {
        "iteration": 1,
        "eval_rows": [],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": [],
        "decision_records": [
            "garbage_string",
            None,
            42,
            {"decision_type": "control_plane_acceptance"},
        ],
    }
    out = _strip_iteration(iteration)
    records = out["decision_records"]
    assert len(records) == 1
    assert records[0]["decision_type"] == "control_plane_acceptance"


def test_strip_iteration_legacy_dict_records_unchanged() -> None:
    """Pure-dict fixture must round-trip identically — back-compat."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        _strip_iteration,
    )

    iteration = {
        "iteration": 2,
        "eval_rows": [],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": [],
        "decision_records": [
            {"decision_type": "patch_applied", "ag_id": "AG1"},
            {"decision_type": "control_plane_acceptance", "ag_id": "AG1"},
        ],
    }
    out = _strip_iteration(iteration)
    records = out["decision_records"]
    assert len(records) == 2
    assert [r["decision_type"] for r in records] == [
        "patch_applied",
        "control_plane_acceptance",
    ]


def test_coerce_record_to_dict_branches_directly() -> None:
    """Pure-helper unit coverage for all three branches."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        _coerce_record_to_dict,
    )

    assert _coerce_record_to_dict({"a": 1}) == {"a": 1}

    record = _mk_record()
    assert _coerce_record_to_dict(record)["decision_type"] == "patch_applied"

    assert _coerce_record_to_dict("garbage") is None
    assert _coerce_record_to_dict(None) is None
    assert _coerce_record_to_dict(42) is None
