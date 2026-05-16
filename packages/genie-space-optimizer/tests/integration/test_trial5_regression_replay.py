"""Trial-5 regression replay — these tests characterize the bugs the
contract-first hardening plan must fix. They MUST stay green for all
future runs to prove the bugs do not return.

The fixtures live at ``tests/fixtures/trial5_postmortem/``. Each test
reads its own ``replay_fixture.json``, projects the iterations, and
asserts on the projected shape.
"""

from __future__ import annotations

import json
from pathlib import Path


_FIXTURE_ROOT = Path(__file__).parent.parent / "fixtures" / "trial5_postmortem"


def _load_iterations(label: str) -> list[dict]:
    path = _FIXTURE_ROOT / label / "replay_fixture.json"
    return json.loads(path.read_text())["iterations"]


def _all_decision_records(iterations: list[dict]) -> list[dict]:
    return [
        dr
        for iter_record in iterations
        for dr in iter_record.get("decision_records", [])
    ]


def test_run_b_7now_has_narrow_skipped_no_original_patch_type():
    """Run B's proposals were dropped with
    ``reason_code == "narrow_skipped_no_original_patch_type"`` because
    Stage-2 adapters stored ``_patch_type`` (underscore-prefixed) while
    ``narrow_replacement_diagnosis`` reads ``patch_type``. The
    historical fixture must contain at least one such record — once
    Phase 1a's adapter unit tests pin the canonical shape, this
    reason_code cannot recur in a future run.
    """
    iterations = _load_iterations("run_b_7now")
    bad = [
        dr for dr in _all_decision_records(iterations)
        if dr.get("reason_code") == "narrow_skipped_no_original_patch_type"
    ]
    assert bad, (
        "Fixture must contain at least one historical occurrence — "
        "if zero, the fixture is wrong, not the test."
    )


def test_run_b_7now_proposal_failure_signature_repeats_every_iteration():
    """Run B also shows the C4 stalemate symptom: every
    ``proposal_failure_decided`` record shares the same
    (ag_id, root_cause, reason_code, next_action) signature. Phase 2b
    (signature detector) and Phase 2c (escalate branch) replace this
    loop with an ESCALATE_STALEMATE.
    """
    iterations = _load_iterations("run_b_7now")

    def _signature(dr: dict) -> tuple:
        return (
            dr.get("ag_id"),
            dr.get("root_cause"),
            dr.get("reason_code"),
            dr.get("next_action"),
        )

    failure_signatures: list[tuple] = []
    for iter_record in iterations:
        for dr in iter_record.get("decision_records", []) or []:
            if dr.get("decision_type") == "proposal_failure_decided":
                failure_signatures.append(_signature(dr))

    assert len(failure_signatures) >= 3, (
        "Historical fixture must show >= 3 proposal_failure_decided "
        "records — that is the C4 stalemate."
    )
    assert len(set(failure_signatures)) == 1, (
        "Historical fixture must show identical signatures across "
        "iterations (the loop-stalemate symptom)."
    )


def test_run_a_airline_proposal_failure_signature_repeats_every_iteration():
    """Run A emitted the same ``proposal_failure_decided`` record every
    iteration (root_cause=wrong_aggregation, ag_id=AG_PIPELINE,
    next_action=request_evidence_gathering) because the harness has no
    stalemate detector AND ``REQUEST_EVIDENCE_GATHERING`` is a no-op.
    """
    iterations = _load_iterations("run_a_airline")

    def _signature(dr: dict) -> tuple:
        return (
            dr.get("ag_id"),
            dr.get("root_cause"),
            dr.get("reason_code"),
            dr.get("next_action"),
        )

    failure_signatures: list[tuple] = []
    for iter_record in iterations:
        for dr in iter_record.get("decision_records", []) or []:
            if dr.get("decision_type") == "proposal_failure_decided":
                failure_signatures.append(_signature(dr))

    assert len(failure_signatures) >= 3, (
        "Historical fixture must show >= 3 proposal_failure_decided "
        "records — that is the C4 stalemate."
    )
    assert len(set(failure_signatures)) == 1, (
        "Historical fixture must show identical signatures across "
        "iterations (the loop-stalemate symptom)."
    )
    # The single signature must reference the wrong_aggregation root
    # cause — that is the C3 symptom (no patch-shape coverage).
    only_sig = failure_signatures[0]
    assert only_sig[1] == "wrong_aggregation", (
        f"Expected the single signature's root_cause to be "
        f"wrong_aggregation; got {only_sig!r}"
    )


def test_run_a_airline_wrong_aggregation_records_terminate_in_evidence_gathering():
    """The C3 contract break manifested as ``wrong_aggregation`` records
    that all terminated with ``next_action=request_evidence_gathering``
    (the no-op symptom). After Phase 1c's coverage matrix + Phase 3's
    SqlDiff-grounded RCA cards land, at least one lever family must
    offer a patch shape for ``wrong_aggregation`` so the iteration
    doesn't fall through to the evidence-gathering no-op."""
    iterations = _load_iterations("run_a_airline")
    wrong_agg_failures = [
        dr
        for iter_record in iterations
        for dr in iter_record.get("decision_records", []) or []
        if dr.get("root_cause") == "wrong_aggregation"
        and dr.get("decision_type") == "proposal_failure_decided"
    ]
    assert wrong_agg_failures, (
        "Fixture must record wrong_aggregation proposal_failure events"
    )
    # Every such record terminated with request_evidence_gathering
    # (the no-op next-action) — i.e. no lever family produced a
    # patch and the harness fell through.
    assert all(
        dr.get("next_action") == "request_evidence_gathering"
        for dr in wrong_agg_failures
    ), (
        "All wrong_aggregation proposal_failure records should have "
        "next_action=request_evidence_gathering (C3+C4 joint symptom)"
    )
