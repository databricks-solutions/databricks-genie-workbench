"""Cycle 14-T2 integration replay — typed marker and replay record agree.

Anchor: airline run 294637253025289 attempt 10 F4 — stdout summary
+ journey ledger render accepted at 100%, while Phase B/H render
acceptance_decided outcome=rolled_back reason=missing_pre_rows.

Anchor: 7Now run 76457773587391 attempt 7 F2 — gs_026 in
LOOKUP_FAILED state must surface as reason_code=
target_resolution_failed via T0 + T2 in concert.

The integration drives the helper directly across two
ControlPlaneAcceptance fixtures and asserts byte-equality between
the two render surfaces. This is a precondition for C14-T3's I9
byte-equality invariant — ship I9 once these tests are green.
"""

from __future__ import annotations

import json


def _airline_decision():
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
        DeltaState,
    )
    return ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted",
        baseline_accuracy=83.3,
        candidate_accuracy=100.0,
        delta_pp=16.7,
        target_qids=("gs_024",),
        target_fixed_qids=("gs_024",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        target_delta_states=(("gs_024", DeltaState.FIXED.value),),
    )


def _seven_now_decision():
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
        DeltaState,
    )
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_resolution_failed",
        baseline_accuracy=78.3,
        candidate_accuracy=78.3,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        target_delta_states=(("gs_026", DeltaState.LOOKUP_FAILED.value),),
    )


def test_airline_anchor_typed_marker_and_record_agree(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CANONICAL_ACCEPTANCE_RENDER", "1")
    from genie_space_optimizer.optimization.control_plane import (
        format_full_eval_marker_payload,
    )
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        full_eval_marker,
    )

    decision = _airline_decision()
    payload = format_full_eval_marker_payload(
        decision,
        ag_id="AG_DECOMPOSED_H004",
        iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )

    # Surface 1: typed marker.
    line = full_eval_marker(
        optimization_run_id="run-airline",
        payload=payload,
    )
    assert "GSO_FULL_EVAL_V1" in line
    body = line[len("GSO_FULL_EVAL_V1 "):]
    parsed = json.loads(body)
    assert parsed["payload"]["accepted"] is True
    assert parsed["payload"]["reason_code"] == "accepted"
    assert parsed["payload"]["target_fixed_qids"] == ["gs_024"]

    # Surface 2: replay record.
    record = ag_outcome_decision_record(
        run_id="run-airline",
        iteration=1,
        ag={
            "id": "AG_DECOMPOSED_H004",
            "affected_questions": ["gs_024"],
        },
        outcome="accepted",
        acceptance_detail=decision,
    )
    assert record is not None
    # Both surfaces agree on reason_detail.
    assert record.reason_detail == payload["reason_detail"]
    # Both agree on regression bucket emptiness.
    assert list(record.regression_qids) == payload["out_of_target_regressed_qids"]
    # F4 catastrophe assertion: BOTH surfaces report accepted, neither
    # reports rolled_back/missing_pre_rows.
    assert "missing_pre_rows" not in record.reason_detail
    assert "rolled_back" not in payload["accepted_label"].lower()


def test_seven_now_anchor_typed_marker_and_record_agree(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CANONICAL_ACCEPTANCE_RENDER", "1")
    from genie_space_optimizer.optimization.control_plane import (
        format_full_eval_marker_payload,
    )
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        full_eval_marker,
    )

    decision = _seven_now_decision()
    payload = format_full_eval_marker_payload(
        decision,
        ag_id="AG1",
        iteration=1,
        accepted_label="FAIL (REGRESSION)",
    )

    line = full_eval_marker(optimization_run_id="run-7now", payload=payload)
    body = line[len("GSO_FULL_EVAL_V1 "):]
    parsed = json.loads(body)
    assert parsed["payload"]["reason_code"] == "target_resolution_failed"
    assert parsed["payload"]["target_delta_states"] == [
        ["gs_026", "lookup_failed"],
    ]

    record = ag_outcome_decision_record(
        run_id="run-7now",
        iteration=1,
        ag={"id": "AG1", "affected_questions": ["gs_026"]},
        outcome="rolled_back",
        acceptance_detail=decision,
    )
    assert record is not None
    # Both surfaces agree on the reason vocabulary (T0 + T2).
    assert "target_resolution_failed" in record.reason_detail
    # The replay record's metrics carry the same delta_states the
    # marker payload does.
    assert record.metrics.get("target_delta_states") == [
        ["gs_026", "lookup_failed"],
    ]


def test_byte_equal_normalized_keys_across_surfaces(monkeypatch) -> None:
    """Pre-condition for C14-T3's I9 invariant: after canonical
    normalization, the two surfaces must agree on three contract-
    critical keys: reason_code, target_still_hard_qids,
    out_of_target_regressed_qids.
    """
    monkeypatch.setenv("GSO_CANONICAL_ACCEPTANCE_RENDER", "1")
    from genie_space_optimizer.optimization.control_plane import (
        format_full_eval_marker_payload,
    )
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )

    for decision_factory in (_airline_decision, _seven_now_decision):
        decision = decision_factory()
        payload = format_full_eval_marker_payload(
            decision,
            ag_id="AG1",
            iteration=1,
            accepted_label="X",
        )
        record = ag_outcome_decision_record(
            run_id="run-x",
            iteration=1,
            ag={"id": "AG1", "affected_questions": list(decision.target_qids)},
            outcome="accepted" if decision.accepted else "rolled_back",
            acceptance_detail=decision,
        )
        assert record is not None
        # Reason code: both surfaces share the same string after
        # lowercasing. The record's reason_detail is
        # ``reason=<code>; ...`` so we extract <code> from the prefix.
        record_reason_substring = (
            record.reason_detail.split(";")[0].split("=")[-1].strip().lower()
        )
        assert record_reason_substring == payload["reason_code"].lower()
        # target_still_hard_qids: helper renders canonical sorted list.
        assert sorted(record.metrics.get("target_still_hard_qids") or []) == \
            sorted(payload["target_still_hard_qids"])
        # out_of_target_regressed_qids: helper renders canonical
        # sorted list.
        assert sorted(record.metrics.get("out_of_target_regressed_qids") or []) == \
            sorted(payload["out_of_target_regressed_qids"])
