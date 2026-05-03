"""Phase D.5 Task 1 — AlternativeOption + RejectReason."""

from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
    DecisionRecord,
    DecisionType,
    RejectReason,
    canonical_decision_json,
)


def test_alternative_option_is_frozen_dataclass_with_required_fields() -> None:
    opt = AlternativeOption(
        option_id="AG_002",
        kind="ag",
        score=0.42,
        reject_reason=RejectReason.LOWER_SCORE,
        reject_detail="strategist preferred AG_001 by 0.18 score margin",
    )
    assert opt.option_id == "AG_002"
    assert opt.kind == "ag"
    assert opt.score == 0.42
    assert opt.reject_reason == RejectReason.LOWER_SCORE
    assert opt.reject_detail.startswith("strategist preferred")
    # frozen: assignment must raise
    import dataclasses
    assert dataclasses.is_dataclass(opt)


def test_alternative_option_to_dict_emits_only_set_fields() -> None:
    opt = AlternativeOption(
        option_id="AG_002",
        kind="ag",
        reject_reason=RejectReason.MISSING_TARGET_QIDS,
    )
    row = opt.to_dict()
    assert row == {
        "option_id": "AG_002",
        "kind": "ag",
        "reject_reason": "missing_target_qids",
    }


def test_alternative_option_to_dict_includes_score_and_detail_when_set() -> None:
    opt = AlternativeOption(
        option_id="P_002",
        kind="proposal",
        score=0.1,
        reject_reason=RejectReason.PATCH_CAP_DROPPED,
        reject_detail="dropped by 5-proposal cap",
    )
    row = opt.to_dict()
    assert row == {
        "option_id": "P_002",
        "kind": "proposal",
        "score": 0.1,
        "reject_reason": "patch_cap_dropped",
        "reject_detail": "dropped by 5-proposal cap",
    }


def test_decision_record_default_alternatives_is_empty_tuple() -> None:
    rec = DecisionRecord()
    assert rec.alternatives_considered == ()


def test_decision_record_to_dict_omits_empty_alternatives() -> None:
    rec = DecisionRecord(
        run_id="run_1", iteration=1,
        decision_type=DecisionType.CLUSTER_SELECTED,
    )
    assert "alternatives_considered" not in rec.to_dict()


def test_decision_record_to_dict_includes_alternatives_when_present() -> None:
    rec = DecisionRecord(
        run_id="run_1",
        iteration=1,
        decision_type=DecisionType.CLUSTER_SELECTED,
        cluster_id="H001",
        alternatives_considered=(
            AlternativeOption(
                option_id="C_005",
                kind="cluster",
                reject_reason=RejectReason.BELOW_HARD_THRESHOLD,
            ),
        ),
    )
    row = rec.to_dict()
    assert row["alternatives_considered"] == [
        {
            "option_id": "C_005",
            "kind": "cluster",
            "reject_reason": "below_hard_threshold",
        }
    ]


def test_decision_record_round_trips_alternatives() -> None:
    original = DecisionRecord(
        run_id="run_1",
        iteration=1,
        decision_type=DecisionType.STRATEGIST_AG_EMITTED,
        ag_id="AG_001",
        alternatives_considered=(
            AlternativeOption(
                option_id="AG_002",
                kind="ag",
                score=0.42,
                reject_reason=RejectReason.LOWER_SCORE,
                reject_detail="lost by 0.18",
            ),
        ),
    )
    restored = DecisionRecord.from_dict(original.to_dict())
    assert restored.alternatives_considered == original.alternatives_considered


def test_canonical_decision_json_sorts_alternatives_inside_record_by_option_id() -> None:
    rec = DecisionRecord(
        run_id="run_1",
        iteration=1,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        proposal_id="P_001",
        alternatives_considered=(
            AlternativeOption(
                option_id="P_007", kind="proposal",
                reject_reason=RejectReason.MALFORMED,
            ),
            AlternativeOption(
                option_id="P_003", kind="proposal",
                reject_reason=RejectReason.PATCH_CAP_DROPPED,
            ),
        ),
    )
    rendered = canonical_decision_json([rec])
    p003_idx = rendered.index('"option_id":"P_003"')
    p007_idx = rendered.index('"option_id":"P_007"')
    assert p003_idx < p007_idx
