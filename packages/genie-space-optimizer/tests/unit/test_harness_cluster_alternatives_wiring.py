"""Phase D.5 Task 5 — harness wiring captures cluster alternatives.

This is a focused unit test on the helper that builds
cluster_alternatives_by_id from the candidate-cluster collection that
already exists locally at the cluster-selection site.
"""


def test_build_cluster_alternatives_skips_promoted_clusters() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_cluster_alternatives_by_id,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RejectReason,
    )

    # Three candidate clusters; two were promoted to hard, one was below threshold.
    candidate_clusters = [
        {"cluster_id": "H001", "question_ids": ["q1", "q2"], "is_hard": True},
        {"cluster_id": "H002", "question_ids": ["q3", "q4"], "is_hard": True},
        {
            "cluster_id": "C_005",
            "question_ids": ["q9"],
            "is_hard": False,
            "demoted_reason": "below_hard_threshold",
        },
    ]

    result = _build_cluster_alternatives_by_id(
        candidate_clusters=candidate_clusters,
        promoted_cluster_ids=["H001", "H002"],
    )

    # Each promoted cluster gets the SAME tuple of rejected alternatives
    # (one rejection: C_005). Stamping the same tuple on each chosen
    # cluster preserves the "alternatives considered when this batch
    # was decided" semantics and is byte-stable.
    assert set(result.keys()) == {"H001", "H002"}
    for chosen_cid in ("H001", "H002"):
        alts = result[chosen_cid]
        assert len(alts) == 1
        assert alts[0].option_id == "C_005"
        assert alts[0].kind == "cluster"
        assert alts[0].reject_reason == RejectReason.BELOW_HARD_THRESHOLD


def test_build_cluster_alternatives_returns_empty_when_all_promoted() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_cluster_alternatives_by_id,
    )

    candidate_clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"], "is_hard": True},
    ]
    result = _build_cluster_alternatives_by_id(
        candidate_clusters=candidate_clusters,
        promoted_cluster_ids=["H001"],
    )
    assert result == {"H001": ()}


def test_build_cluster_alternatives_maps_demoted_reason_to_typed_enum() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_cluster_alternatives_by_id,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RejectReason,
    )

    candidate_clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"], "is_hard": True},
        {
            "cluster_id": "C_005",
            "question_ids": [],
            "is_hard": False,
            "demoted_reason": "insufficient_qids",
        },
        {
            "cluster_id": "C_007",
            "question_ids": ["q9"],
            "is_hard": False,
            "demoted_reason": "below_hard_threshold",
        },
        {
            "cluster_id": "C_011",
            "question_ids": ["q9"],
            "is_hard": False,
            # No demoted_reason → falls back to OTHER.
        },
    ]
    result = _build_cluster_alternatives_by_id(
        candidate_clusters=candidate_clusters,
        promoted_cluster_ids=["H001"],
    )
    by_id = {opt.option_id: opt for opt in result["H001"]}
    assert by_id["C_005"].reject_reason == RejectReason.INSUFFICIENT_QIDS
    assert by_id["C_007"].reject_reason == RejectReason.BELOW_HARD_THRESHOLD
    assert by_id["C_011"].reject_reason == RejectReason.OTHER
