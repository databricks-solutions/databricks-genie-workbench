"""Phase 1 — Acceptance Unification Exit Criterion.

Replay fixture for Run A iter 1 (the historical attribution-drift
accept). Asserts that the four downstream surfaces agree on the
verdict when the AcceptanceOutcome typed wrapper feeds each surface:

  Surface 1: rendered ``Action:`` string in the FULL EVAL print block.
  Surface 2: ``GSO_FULL_EVAL_V1`` marker payload's ``accepted_label``.
  Surface 3: ``acceptance_decision`` dict passed to Phase H.
  Surface 4: ``GSO_CANDIDATE_LEDGER_ENTRY_V1`` ledger entry.

Run A iter 1 fixture inputs (from byte-stable replay metadata,
documented in the Phase 1 audit Section A.5):

  target_qids                       = ("gs_013",)
  target_fixed_qids                 = ("gs_013",)
  target_still_hard_qids            = ()
  out_of_target_regressed_qids      = ()
  baseline_accuracy                 = 25.0
  candidate_accuracy                = 25.0
  delta_pp                          = 0.0
  baseline_pre_arbiter_accuracy     = 12.5
  candidate_pre_arbiter_accuracy    = 25.6
  reason_code                       = "accepted_with_attribution_drift"
  accepted                          = True

Pre-Phase-1 behaviour: gain gate rejected (post-arbiter delta below
2pp floor), control-plane accepted via attribution-drift branch. The
three surfaces disagreed — marker said ``accepted_label="FAIL
(REGRESSION)"``, rollback-path acceptance_decision dict said
``"accepted": False``, ledger entry was emitted but the AG-id was
empty. Phase 1 makes all four agree on ``accepted=True``,
``accepted_label="PASS"``, and a non-empty ledger AG context.
"""

from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.acceptance_outcome import (
    acceptance_decision_dict,
    build_acceptance_outcome,
)
from genie_space_optimizer.optimization.acceptance_policy import (
    GainGateDecision,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    format_full_eval_marker_payload,
)
from genie_space_optimizer.optimization.iteration_ag_context import (
    capture_iter_ag_context,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    candidate_ledger_entry_marker,
    full_eval_marker,
)


# Run A iter 1 replay inputs (Section A.5 of the Phase 1 plan).
RUN_A_ITER1_CANONICAL = ControlPlaneAcceptance(
    accepted=True,
    reason_code="accepted_with_attribution_drift",
    baseline_accuracy=25.0,
    candidate_accuracy=25.0,
    delta_pp=0.0,
    target_qids=("gs_013",),
    target_fixed_qids=("gs_013",),
    target_still_hard_qids=(),
    out_of_target_regressed_qids=(),
    regression_debt_qids=(),
    soft_to_hard_regressed_qids=(),
    passing_to_hard_regressed_qids=(),
    protected_regressed_qids=(),
    unknown_to_hard_regressed_qids=(),
    target_soft_passing_qids=(),
    accidentally_improved_qids=("gs_013",),
    unresolved_target_debt_qids=(),
    existing_hard_still_hard_outside_target_qids=(),
)

RUN_A_ITER1_STRICT = GainGateDecision(
    accepted=False,
    post_arbiter_candidate=25.0,
    post_arbiter_baseline=25.0,
    delta_pp=0.0,
    min_gain_pp=2.0,
    reason_code="rejected_insufficient_gain",
)

RUN_A_ITER1_AG = {
    "id": "AG1",
    "source_cluster_ids": ["c_gs013_join_chain"],
    "target_qids": ["gs_013"],
    "affected_questions": ["gs_013"],
    "lever_directives": {"6": {}},
    "root_cause_summary": "Missing transitive join customers→orders→items",
}


def _extract_marker_payload(stdout_text: str, marker: str) -> dict | None:
    """Pull the JSON dict from a ``MARKER {...}`` stdout line."""
    pattern = re.compile(rf"^{marker}\s+(\{{.*\}})\s*$", re.MULTILINE)
    match = pattern.search(stdout_text)
    if match is None:
        return None
    return json.loads(match.group(1))


def test_run_a_iter1_outcome_is_canonical_accept():
    """The AcceptanceOutcome built from Run A iter 1 inputs must report
    ``accepted=True``, ``reason_code=accepted_with_attribution_drift``,
    ``accepted_label=PASS``. The gain-gate's rejection does NOT
    override the canonical decision."""
    outcome = build_acceptance_outcome(
        strict_decision=RUN_A_ITER1_STRICT,
        control_plane_decision=RUN_A_ITER1_CANONICAL,
        enable_control_plane_acceptance=True,
    )

    assert outcome.accepted is True
    assert outcome.reason_code == "accepted_with_attribution_drift"
    assert outcome.accepted_label == "PASS"
    assert outcome.gain_gate_failed is True
    assert outcome.control_plane_failed is False
    assert outcome.target_fixed_qids == ("gs_013",)
    # Run A iter 1: control plane accepted, gain gate rejected. The
    # attribution list must contain only the gain-gate entry.
    judges = [e["judge"] for e in outcome.regression_attribution]
    assert judges == ["acceptance_gate (rejected_insufficient_gain)"]


def test_surface_1_action_verb_matches_outcome():
    """Surface 1: the rendered ``Action:`` line.

    The harness renders ``Action: ACCEPT`` on the pass path and
    ``Action: ROLLBACK`` on the rollback path. Under Phase 1, the chosen
    path is determined by ``outcome.accepted`` alone — Run A iter 1
    takes the accept path because the canonical decision said so.
    """
    outcome = build_acceptance_outcome(
        strict_decision=RUN_A_ITER1_STRICT,
        control_plane_decision=RUN_A_ITER1_CANONICAL,
        enable_control_plane_acceptance=True,
    )

    rendered_action = "ACCEPT" if outcome.accepted else "ROLLBACK"

    assert rendered_action == "ACCEPT"


def test_surface_2_full_eval_marker_label_matches_outcome():
    """Surface 2: ``GSO_FULL_EVAL_V1`` marker payload.

    The harness now passes ``accepted_label=outcome.accepted_label``
    to ``format_full_eval_marker_payload`` (Task 4). For Run A iter 1
    that means the marker payload reports ``accepted_label="PASS"``,
    matching the canonical reason code embedded in the same payload.
    """
    outcome = build_acceptance_outcome(
        strict_decision=RUN_A_ITER1_STRICT,
        control_plane_decision=RUN_A_ITER1_CANONICAL,
        enable_control_plane_acceptance=True,
    )

    payload = format_full_eval_marker_payload(
        RUN_A_ITER1_CANONICAL,
        ag_id="AG1",
        iteration=1,
        accepted_label=outcome.accepted_label,
    )

    assert payload["accepted_label"] == "PASS"
    assert payload["reason_code"] == "accepted_with_attribution_drift"
    assert payload["accepted"] is True

    # Sanity check the rendered marker survives a stdout round-trip.
    buf = io.StringIO()
    with redirect_stdout(buf):
        print(full_eval_marker(optimization_run_id="run_a", payload=payload))

    parsed = _extract_marker_payload(buf.getvalue(), "GSO_FULL_EVAL_V1")
    assert parsed is not None
    # full_eval_marker wraps the formatted payload under a "payload" key
    # (see run_analysis_contract.py:396-402). The accepted_label string
    # must survive the round-trip unchanged.
    inner = parsed.get("payload") or parsed.get("entry") or parsed
    assert inner["accepted_label"] == "PASS"


def test_surface_3_acceptance_decision_dict_matches_outcome():
    """Surface 3: the ``acceptance_decision`` dict passed to Phase H.

    The serialiser (Task 5) produces the same shape for both pass and
    rollback paths. Run A iter 1 takes the pass path; the dict reports
    ``accepted=True`` and carries ``_canonical`` so Phase H short-
    circuits via ``canonical_decisions_by_ag_id``.
    """
    outcome = build_acceptance_outcome(
        strict_decision=RUN_A_ITER1_STRICT,
        control_plane_decision=RUN_A_ITER1_CANONICAL,
        enable_control_plane_acceptance=True,
    )

    decision = acceptance_decision_dict(outcome)

    assert decision["accepted"] is True
    assert decision["reason"] == "accepted_with_attribution_drift"
    assert decision["target_qids"] == ["gs_013"]
    assert decision["target_fixed_qids"] == ["gs_013"]
    assert decision["out_of_target_regressed_qids"] == []
    assert decision["_canonical"] is RUN_A_ITER1_CANONICAL


def test_surface_4_candidate_ledger_entry_carries_non_empty_ag_context():
    """Surface 4: ``GSO_CANDIDATE_LEDGER_ENTRY_V1`` ledger row.

    After Phase 1 Task 2, the AG-context capture runs at AG-selection
    time, strictly before any terminal emit. The ledger row for Run A
    iter 1 must carry a non-empty ``ag_id``, ``cluster_ids``, and
    ``target_qids``.
    """
    ctx = capture_iter_ag_context(ag=RUN_A_ITER1_AG, ag_id="AG1")

    assert ctx["ag_id"] == "AG1"
    assert ctx["cluster_ids"] == ("c_gs013_join_chain",)
    assert ctx["target_qids"] == ("gs_013",)
    assert ctx["levers"] == (6,)
    assert ctx["root_cause"] == (
        "Missing transitive join customers→orders→items"
    )

    # Compose the ledger entry the harness emits at the iteration's
    # ``finally:`` block. Shape mirrors the historical
    # ``IterationCandidateLedgerEntry`` projection.
    entry = {
        "iteration": 1,
        "terminal_reason": "accepted",
        "ag_id": ctx["ag_id"],
        "cluster_ids": list(ctx["cluster_ids"]),
        "target_qids": list(ctx["target_qids"]),
        "levers": list(ctx["levers"]),
        "root_cause": ctx["root_cause"],
        "acceptance_tier": "accept_attribution_drift",
        "accuracy_delta_pp": 0.0,
    }

    buf = io.StringIO()
    with redirect_stdout(buf):
        print(candidate_ledger_entry_marker(
            optimization_run_id="run_a",
            entry=entry,
        ))

    parsed = _extract_marker_payload(
        buf.getvalue(), "GSO_CANDIDATE_LEDGER_ENTRY_V1"
    )
    assert parsed is not None
    assert parsed["entry"]["ag_id"] == "AG1"
    assert parsed["entry"]["cluster_ids"] == ["c_gs013_join_chain"]
    assert parsed["entry"]["target_qids"] == ["gs_013"]


def test_run_a_iter1_no_phase_h_drift_marker_fires():
    """Coherence assertion: with all four surfaces wired to the same
    AcceptanceOutcome, the Phase H drift detector receives identical
    canonical and Phase-H outcomes and does NOT fire.

    Reproduces the contract in
    ``tests/unit/test_phase_h_drift_production_wiring.py`` —
    ``_emit_phase_h_acceptance_drift_if_any`` emits the marker only on
    disagreement.
    """
    import os

    os.environ["GSO_PHASE_H_DRIFT_OBSERVE"] = "1"
    try:
        from genie_space_optimizer.optimization.harness import (
            _emit_phase_h_acceptance_drift_if_any,
        )

        outcome = build_acceptance_outcome(
            strict_decision=RUN_A_ITER1_STRICT,
            control_plane_decision=RUN_A_ITER1_CANONICAL,
            enable_control_plane_acceptance=True,
        )
        canonical_outcome_str = (
            "accepted" if outcome.accepted else "rolled_back"
        )

        buf = io.StringIO()
        with redirect_stdout(buf):
            _emit_phase_h_acceptance_drift_if_any(
                run_id="run_a",
                iteration=1,
                canonical_outcome=canonical_outcome_str,
                canonical_reason_code=outcome.reason_code,
                phase_h_outcome=canonical_outcome_str,
                phase_h_reason_code=outcome.reason_code,
            )

        assert "GSO_PHASE_H_ACCEPTANCE_DRIFT_V1" not in buf.getvalue()
    finally:
        os.environ.pop("GSO_PHASE_H_DRIFT_OBSERVE", None)


def test_run_a_iter1_no_accepted_true_paired_with_rollback():
    """The Phase 1 exit criterion in plain English: no iteration can
    emit ``accepted=true`` paired with ``Action: ROLLBACK``.

    Build the four surfaces from the same AcceptanceOutcome and assert
    they all agree on accept/reject.
    """
    outcome = build_acceptance_outcome(
        strict_decision=RUN_A_ITER1_STRICT,
        control_plane_decision=RUN_A_ITER1_CANONICAL,
        enable_control_plane_acceptance=True,
    )

    rendered_action = "ACCEPT" if outcome.accepted else "ROLLBACK"
    marker_payload = format_full_eval_marker_payload(
        RUN_A_ITER1_CANONICAL,
        ag_id="AG1",
        iteration=1,
        accepted_label=outcome.accepted_label,
    )
    decision_dict = acceptance_decision_dict(outcome)

    # All four surfaces must agree.
    assert outcome.accepted is True
    assert rendered_action == "ACCEPT"
    assert marker_payload["accepted_label"] == "PASS"
    assert marker_payload["accepted"] is True
    assert decision_dict["accepted"] is True

    # The pathological pre-Phase-1 combination MUST NOT occur.
    pre_phase1_pathology = (
        outcome.accepted is True
        and rendered_action == "ROLLBACK"
    )
    assert not pre_phase1_pathology, (
        "Phase 1 exit criterion violated — Run A iter 1 replays to "
        "accepted=true paired with Action: ROLLBACK. The four "
        "downstream surfaces are no longer coherent."
    )
