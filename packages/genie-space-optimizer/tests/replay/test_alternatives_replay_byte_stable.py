"""Phase D.5 Task 9 — byte-stable cross-projection replay test for alternatives.

A synthetic fixture exercising:
- 3 candidate clusters → 1 hard cluster H001 chosen, 2 alternatives recorded.
- 4 strategist AGs   → 1 emitted (AG_001), 3 alternatives recorded.
- 5 proposals for AG_001 → 3 surviving, 2 alternatives recorded.

The canonical decision JSON, the sorted alternatives within each
record, and the operator transcript must all be byte-stable across
runs and across re-imports.
"""

from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
    DecisionOutcome,
    DecisionRecord,
    DecisionType,
    OptimizationTrace,
    ReasonCode,
    RejectReason,
    canonical_decision_json,
    render_operator_transcript,
)


def _build_synthetic_records():
    return [
        DecisionRecord(
            run_id="syn_run", iteration=1,
            decision_type=DecisionType.CLUSTER_SELECTED,
            outcome=DecisionOutcome.INFO,
            reason_code=ReasonCode.CLUSTERED,
            cluster_id="H001",
            rca_id="rca_h001",
            evidence_refs=("cluster:H001",),
            affected_qids=("q1", "q2"),
            target_qids=("q1", "q2"),
            root_cause="missing_filter",
            alternatives_considered=(
                AlternativeOption(
                    option_id="C_007",
                    kind="cluster",
                    reject_reason=RejectReason.BELOW_HARD_THRESHOLD,
                ),
                AlternativeOption(
                    option_id="C_005",
                    kind="cluster",
                    reject_reason=RejectReason.INSUFFICIENT_QIDS,
                ),
            ),
        ),
        DecisionRecord(
            run_id="syn_run", iteration=1,
            decision_type=DecisionType.STRATEGIST_AG_EMITTED,
            outcome=DecisionOutcome.INFO,
            reason_code=ReasonCode.STRATEGIST_SELECTED,
            ag_id="AG_001",
            rca_id="rca_h001",
            target_qids=("q1", "q2"),
            source_cluster_ids=("H001",),
            alternatives_considered=(
                AlternativeOption(
                    option_id="AG_002",
                    kind="ag",
                    score=0.42,
                    reject_reason=RejectReason.LOWER_SCORE,
                ),
                AlternativeOption(
                    option_id="AG_003",
                    kind="ag",
                    reject_reason=RejectReason.BUFFERED,
                ),
                AlternativeOption(
                    option_id="AG_004",
                    kind="ag",
                    reject_reason=RejectReason.MISSING_TARGET_QIDS,
                ),
            ),
        ),
        DecisionRecord(
            run_id="syn_run", iteration=1,
            decision_type=DecisionType.PROPOSAL_GENERATED,
            outcome=DecisionOutcome.ACCEPTED,
            reason_code=ReasonCode.PROPOSAL_EMITTED,
            ag_id="AG_001",
            cluster_id="H001",
            proposal_id="P_001",
            target_qids=("q1",),
            source_cluster_ids=("H001",),
            alternatives_considered=(
                AlternativeOption(
                    option_id="P_007",
                    kind="proposal",
                    reject_reason=RejectReason.MALFORMED,
                ),
                AlternativeOption(
                    option_id="P_011",
                    kind="proposal",
                    score=0.1,
                    reject_reason=RejectReason.PATCH_CAP_DROPPED,
                ),
            ),
        ),
    ]


_EXPECTED_CANONICAL_JSON_FRAGMENT = (
    '"alternatives_considered":'
    '[{"kind":"ag","option_id":"AG_002","reject_reason":"lower_score","score":0.42},'
    '{"kind":"ag","option_id":"AG_003","reject_reason":"buffered"},'
    '{"kind":"ag","option_id":"AG_004","reject_reason":"missing_target_qids"}]'
)


def test_canonical_decision_json_alternatives_are_sorted_by_kind_then_option_id() -> None:
    records = _build_synthetic_records()
    rendered = canonical_decision_json(records)
    assert _EXPECTED_CANONICAL_JSON_FRAGMENT in rendered


def test_canonical_decision_json_is_byte_stable_across_calls() -> None:
    """Re-running canonical_decision_json on equivalent inputs produces
    identical bytes. This pins the byte-stable contract without
    importlib.reload (which would swap class identities and pollute
    other tests in the suite)."""
    records_a = _build_synthetic_records()
    json_a = canonical_decision_json(records_a)
    records_b = _build_synthetic_records()
    json_b = canonical_decision_json(records_b)
    assert json_a == json_b
    # Sanity: the alternatives_considered fragment is sorted (kind, option_id).
    assert _EXPECTED_CANONICAL_JSON_FRAGMENT in json_a


def test_round_trip_through_to_dict_from_dict_preserves_alternatives() -> None:
    """Round-tripping through ``to_dict`` -> ``from_dict`` is idempotent
    once the input is in canonical order. ``to_dict`` sorts alternatives
    by (kind, option_id) for byte-stability, so the canonical fixed-point
    is reached after the first serialization round."""
    records = _build_synthetic_records()
    # First round: original (possibly unsorted) -> canonical (sorted).
    payloads_a = [r.to_dict() for r in records]
    restored_a = [DecisionRecord.from_dict(p) for p in payloads_a]
    # Second round: canonical -> canonical (must be a fixed point).
    payloads_b = [r.to_dict() for r in restored_a]
    restored_b = [DecisionRecord.from_dict(p) for p in payloads_b]
    assert tuple(restored_b) == tuple(restored_a)
    assert payloads_b == payloads_a
    # Sanity: alternative-counts and ids are preserved across the round.
    for orig, after in zip(records, restored_a):
        assert {opt.option_id for opt in orig.alternatives_considered} == {
            opt.option_id for opt in after.alternatives_considered
        }


def test_operator_transcript_renders_alternatives_byte_stably() -> None:
    records = _build_synthetic_records()
    out_a = render_operator_transcript(
        trace=OptimizationTrace(decision_records=tuple(records)),
        iteration=1,
    )
    out_b = render_operator_transcript(
        trace=OptimizationTrace(decision_records=tuple(records)),
        iteration=1,
    )
    assert out_a == out_b
    # Sanity: the alternatives line for AG_001 shows the AG kinds sorted.
    assert "AG_002(lower_score), AG_003(buffered), AG_004(missing_target_qids)" in out_a
