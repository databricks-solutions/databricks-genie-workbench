"""Phase D.5 Task 7 — harness wiring captures proposal alternatives."""


def test_build_proposal_alternatives_categorizes_dropped_proposals() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_proposal_alternatives_for_ag,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RejectReason,
    )

    # Strategist generated 5 proposals for AG_001:
    # 3 surviving, 1 malformed (failed shape validator),
    # 1 cap-dropped (5-proposal cap).
    raw_proposals = [
        {"proposal_id": "P_001"},
        {"proposal_id": "P_002"},
        {"proposal_id": "P_003"},
        {
            "proposal_id": "P_007",
            "_dropped": True,
            "_drop_reason": "malformed",
            "_drop_detail": "missing patch_type",
        },
        {
            "proposal_id": "P_011",
            "_dropped": True,
            "_drop_reason": "patch_cap_dropped",
            "_score": 0.1,
        },
    ]
    surviving_ids = ["P_001", "P_002", "P_003"]

    alts = _build_proposal_alternatives_for_ag(
        raw_proposals=raw_proposals,
        surviving_proposal_ids=surviving_ids,
    )
    assert len(alts) == 2
    by_id = {opt.option_id: opt for opt in alts}
    assert by_id["P_007"].reject_reason == RejectReason.MALFORMED
    assert by_id["P_007"].reject_detail == "missing patch_type"
    assert by_id["P_011"].reject_reason == RejectReason.PATCH_CAP_DROPPED
    assert by_id["P_011"].score == 0.1


def test_build_proposal_alternatives_returns_empty_when_no_drops() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_proposal_alternatives_for_ag,
    )

    raw_proposals = [{"proposal_id": "P_001"}]
    alts = _build_proposal_alternatives_for_ag(
        raw_proposals=raw_proposals,
        surviving_proposal_ids=["P_001"],
    )
    assert alts == ()
