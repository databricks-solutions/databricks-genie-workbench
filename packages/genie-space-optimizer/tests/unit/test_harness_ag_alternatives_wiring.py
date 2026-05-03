"""Phase D.5 Task 6 — harness wiring captures AG alternatives."""


def test_build_ag_alternatives_handles_buffered_and_filtered_ags() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_ag_alternatives_by_id,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RejectReason,
    )

    # Strategist returned 4 AGs; 1 was emitted, 1 was buffered for later
    # iteration, 1 was filtered (signature no longer matches live clusters),
    # 1 was dropped because it had no target_qids.
    strategist_returned_ags = [
        {"id": "AG_001", "rejected": False},
        {
            "id": "AG_002",
            "rejected": True,
            "reject_reason": "buffered",
            "_score": 0.42,
        },
        {
            "id": "AG_003",
            "rejected": True,
            "reject_reason": "stale_signature",
            "_score": 0.30,
        },
        {
            "id": "AG_004",
            "rejected": True,
            "reject_reason": "missing_target_qids",
        },
    ]
    emitted_ag_ids = ["AG_001"]

    result = _build_ag_alternatives_by_id(
        strategist_returned_ags=strategist_returned_ags,
        emitted_ag_ids=emitted_ag_ids,
    )
    assert set(result.keys()) == {"AG_001"}
    by_id = {opt.option_id: opt for opt in result["AG_001"]}
    assert by_id["AG_002"].reject_reason == RejectReason.BUFFERED
    assert by_id["AG_002"].score == 0.42
    assert by_id["AG_003"].reject_reason == RejectReason.OTHER
    assert by_id["AG_003"].reject_detail == "stale_signature"
    assert by_id["AG_004"].reject_reason == RejectReason.MISSING_TARGET_QIDS


def test_build_ag_alternatives_returns_empty_when_only_one_ag() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_ag_alternatives_by_id,
    )

    result = _build_ag_alternatives_by_id(
        strategist_returned_ags=[{"id": "AG_001", "rejected": False}],
        emitted_ag_ids=["AG_001"],
    )
    assert result == {"AG_001": ()}
