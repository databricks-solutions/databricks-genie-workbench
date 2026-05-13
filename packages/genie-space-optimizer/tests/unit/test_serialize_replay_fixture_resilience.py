"""B5 Task 3 — serialize_replay_fixture must survive one bad iteration."""

from __future__ import annotations

import json


def _good_iteration(iteration: int) -> dict:
    return {
        "iteration": iteration,
        "eval_rows": [
            {"question_id": f"q{iteration}", "result_correctness": "passing"},
        ],
        "clusters": [],
        "soft_clusters": [],
        "strategist_response": {"action_groups": []},
        "ag_outcomes": {},
        "post_eval_passing_qids": [f"q{iteration}"],
        "decision_records": [],
    }


def test_serialize_replay_fixture_survives_one_bad_iteration() -> None:
    """One malformed iteration should not bring down the rest."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    bad = {"iteration": 2, "decision_records": [object()]}  # not coercible
    good_1 = _good_iteration(1)
    good_3 = _good_iteration(3)
    payload = serialize_replay_fixture(
        fixture_id="test_v1",
        iterations_data=[good_1, bad, good_3],
    )
    fixture = json.loads(payload)
    # The good iterations survived.
    iterations = fixture["iterations"]
    iter_indices = sorted(
        int(it.get("iteration") or 0) for it in iterations
    )
    # Bad iter 2 may or may not survive (if it parsed but had no records,
    # it's still emitted as an empty iter). What matters: 1 and 3 are present
    # with their eval_rows intact.
    assert 1 in iter_indices
    assert 3 in iter_indices
    for it in iterations:
        if int(it.get("iteration") or 0) == 1:
            assert len(it["eval_rows"]) == 1
        elif int(it.get("iteration") or 0) == 3:
            assert len(it["eval_rows"]) == 1


def test_serialize_replay_fixture_handles_strip_raising() -> None:
    """If _strip_iteration raises on a non-Mapping iteration object, the
    wrapper must catch and skip, not propagate."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    payload = serialize_replay_fixture(
        fixture_id="test_v1",
        iterations_data=[
            _good_iteration(1),
            "this is not an iteration dict",  # _strip_iteration will raise
            _good_iteration(3),
        ],
    )
    fixture = json.loads(payload)
    iter_indices = sorted(
        int(it.get("iteration") or 0) for it in fixture["iterations"]
    )
    assert iter_indices == [1, 3]


def test_serialize_replay_fixture_empty_input_emits_empty_iterations() -> None:
    """Empty iterations_data must serialize to a well-formed empty fixture,
    not raise."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    payload = serialize_replay_fixture(
        fixture_id="test_v1",
        iterations_data=[],
    )
    fixture = json.loads(payload)
    assert fixture["fixture_id"] == "test_v1"
    assert fixture["iterations"] == []


def test_serialize_replay_fixture_all_good_passes_through_unchanged() -> None:
    """No regression for the healthy path."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    payload = serialize_replay_fixture(
        fixture_id="test_v1",
        iterations_data=[_good_iteration(1), _good_iteration(2)],
    )
    fixture = json.loads(payload)
    assert len(fixture["iterations"]) == 2
    assert fixture["iterations"][0]["iteration"] == 1
    assert fixture["iterations"][1]["iteration"] == 2


def test_serialize_replay_fixture_attribution_drift_with_dataclass_records() -> None:
    """The exact 2314bb2c shape: an iteration where decision_records contains
    DecisionRecord dataclass instances. Serialization must succeed and the
    fixture must contain the iteration data."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    record = DecisionRecord(
        run_id="2314bb2c",
        iteration=1,
        decision_type=DecisionType.ACCEPTANCE_DECIDED,
        outcome=DecisionOutcome.ACCEPTED,
        reason_code=ReasonCode.NONE,
        ag_id="AG1",
    )
    iteration = _good_iteration(1)
    iteration["decision_records"] = [record]

    payload = serialize_replay_fixture(
        fixture_id="2314bb2c_v1",
        iterations_data=[iteration],
    )
    fixture = json.loads(payload)
    assert len(fixture["iterations"]) == 1
    assert len(fixture["iterations"][0]["decision_records"]) == 1
    rec = fixture["iterations"][0]["decision_records"][0]
    assert rec["decision_type"] == "acceptance_decided"
    assert rec["ag_id"] == "AG1"
    assert rec["reason_code"] == "none"
