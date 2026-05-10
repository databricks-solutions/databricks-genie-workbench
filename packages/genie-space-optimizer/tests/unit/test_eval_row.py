from genie_space_optimizer.optimization.eval_row import EvalRow


def test_passing_when_result_correctness_yes() -> None:
    row = EvalRow.from_dict({"question_id": "gs_001", "result_correctness": "yes"})
    assert row.is_passing()


def test_failing_when_result_correctness_no() -> None:
    row = EvalRow.from_dict({"question_id": "gs_001", "result_correctness": "no"})
    assert not row.is_passing()


def test_passing_via_row_status_synthetic() -> None:
    # Synthetic test fixtures use ``row_status`` instead of
    # ``result_correctness``. EvalRow accepts both for backward compat,
    # but the canonical production source remains result_correctness.
    row = EvalRow.from_dict({"question_id": "gs_001", "row_status": "passing"})
    assert row.is_passing()


def test_hard_when_arbiter_hard() -> None:
    row = EvalRow.from_dict(
        {"question_id": "gs_001", "result_correctness": "no", "arbiter": "hard"}
    )
    assert row.is_hard_failure()


def test_soft_when_arbiter_soft() -> None:
    row = EvalRow.from_dict(
        {"question_id": "gs_001", "result_correctness": "no", "arbiter": "soft"}
    )
    assert not row.is_hard_failure()
    assert row.is_soft_failure()


def test_unknown_arbiter_classified_unknown() -> None:
    row = EvalRow.from_dict(
        {"question_id": "gs_001", "result_correctness": "no", "arbiter": "indeterminate"}
    )
    assert row.classification() == "unknown"


def test_round_trip_through_to_dict() -> None:
    raw = {"question_id": "gs_001", "result_correctness": "yes",
           "arbiter": "n/a", "extra_field": 42}
    row = EvalRow.from_dict(raw)
    assert row.to_dict() == raw
