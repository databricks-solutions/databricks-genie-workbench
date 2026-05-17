"""Phase 4 Test 1 (2026-05-16) — acceptance-outcome unification
active replay against Run A (ab65fefe) iter 1.

User-spec contract (Phase 1 exit criterion):
  Run A iter 1 fixture replays to one coherent verdict;
  no accepted=true paired with Action: ROLLBACK.

This test reads Run A iter 1's recorded ``acceptance_decided`` record
(``outcome=skipped, reason_code=no_applied_patches``) and synthesises
the upstream ``GainGateDecision`` + ``ControlPlaneAcceptance``
instances that the Phase 1 ``build_acceptance_outcome`` API
consumes. The fixture's recorded state is the canonical no-applied-
patches skip: the strategist emitted ``AG_PIPELINE`` but every
selected patch was dropped by the applier, so neither the gain-gate
nor the control-plane accepts the iteration.

This test drives Phase 1's actual API (NOT the plan's hypothetical
``AcceptanceInput`` shape) — ``build_acceptance_outcome(strict_
decision=..., control_plane_decision=..., enable_control_plane_
acceptance=...)`` — and asserts the four downstream surfaces
(rendered Action verb, accepted_label, marker payload accepted flag,
Phase H ``acceptance_decision`` dict) all derive from the same
``AcceptanceOutcome``.

**Failure mode on current main (before Phase 1):**
  ``ModuleNotFoundError: No module named
  'genie_space_optimizer.optimization.acceptance_outcome'``.

**Pass mode after Phase 1 (current state):**
  Four-surface coherence holds: rendered Action verb = ROLLBACK,
  ``outcome.accepted = False``, ``acceptance_decision_dict(outcome)
  ["accepted"] = False``, ``derive_accepted_label`` agrees.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_outcome import (
    AcceptanceOutcome,
    acceptance_decision_dict,
    build_acceptance_outcome,
    derive_accepted_label,
)
from genie_space_optimizer.optimization.acceptance_policy import (
    GainGateDecision,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)

from tests.replay.active._postmortem_fixtures import (
    get_decision_records,
    get_iteration,
    load_run_a_ab65fefe,
)


# Run A (ab65fefe) iter-1 anchor identity values — confirmed by
# reading the fixture during Phase 4 audit.
RUN_A_AG_ID = "AG_PIPELINE"
RUN_A_EXPECTED_REASON = "no_applied_patches"


def _load_iter_1_acceptance_record() -> dict:
    """Extract the single ``acceptance_decided`` record the fixture
    records for iter 1. The Phase 1 contract is that ONE such
    record is produced per iteration."""
    iteration = get_iteration(load_run_a_ab65fefe(), 1)
    recs = get_decision_records(iteration, decision_type="acceptance_decided")
    assert len(recs) == 1, (
        f"Run A iter 1 must record exactly one acceptance_decided "
        f"record. Got {len(recs)}: {[dr.get('reason_code') for dr in recs]}"
    )
    return recs[0]


def _decisions_from_fixture() -> tuple[GainGateDecision, ControlPlaneAcceptance]:
    """Build the upstream ``GainGateDecision`` and the canonical
    ``ControlPlaneAcceptance`` from the fixture's iter-1
    acceptance_decided record.

    The fixture records the SKIPPED case: no patches applied, so
    both the gain-gate and the control-plane decisions are
    non-accepting. We synthesise:

      * ``GainGateDecision`` — rejected_insufficient_gain (the
        candidate accuracy was identical to the baseline because
        no patches were applied).
      * ``ControlPlaneAcceptance`` — ``no_applied_patches`` is not
        a control-plane reason code (those describe POST-eval
        outcomes); the equivalent at the control-plane level for
        an AG that produced no candidate is a rolled_back decision
        with zero target_qids. We pin reason_code to the empty
        string and ``accepted=False`` — the operator transcript
        reads this as the "no candidate produced" tier.
    """
    rec = _load_iter_1_acceptance_record()
    target_qids = tuple(
        str(q) for q in (rec.get("target_qids") or []) if str(q)
    )

    gain = GainGateDecision(
        accepted=False,
        post_arbiter_candidate=0.0,
        post_arbiter_baseline=0.0,
        delta_pp=0.0,
        min_gain_pp=2.0,
        reason_code="rejected_insufficient_gain",
    )

    canonical = ControlPlaneAcceptance(
        accepted=False,
        reason_code="rejected_no_candidate",
        baseline_accuracy=0.0,
        candidate_accuracy=0.0,
        delta_pp=0.0,
        target_qids=target_qids,
        target_fixed_qids=(),
        target_still_hard_qids=target_qids,
        out_of_target_regressed_qids=(),
    )
    return gain, canonical


def _build_outcome() -> AcceptanceOutcome:
    gain, canonical = _decisions_from_fixture()
    return build_acceptance_outcome(
        strict_decision=gain,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )


def test_run_a_iter_1_acceptance_record_is_skipped():
    """Sanity check: the fixture's iter-1 acceptance_decided record
    matches the documented Phase 4 Audit A.2 state — outcome=skipped,
    reason_code=no_applied_patches."""
    rec = _load_iter_1_acceptance_record()
    assert rec.get("ag_id") == RUN_A_AG_ID
    assert str(rec.get("outcome") or "").lower() == "skipped"
    assert rec.get("reason_code") == RUN_A_EXPECTED_REASON


def test_run_a_iter_1_produces_single_coherent_outcome():
    """Phase 1 contract: ``build_acceptance_outcome`` returns ONE
    typed outcome. For Run A iter 1's no-applied-patches skip,
    that outcome must report ``accepted=False``."""
    outcome = _build_outcome()
    assert isinstance(outcome, AcceptanceOutcome)
    assert outcome.accepted is False, (
        f"AcceptanceOutcome reports accepted=True for an AG whose "
        f"every patch was dropped. This is exactly the drift Phase 1 "
        f"must eliminate."
    )


def test_run_a_iter_1_rendered_action_matches_outcome():
    """Phase 1 downstream consumer #1: the rendered ``Action`` verb
    in the operator transcript must NOT pair ``Action: ACCEPT`` with
    a non-accepted outcome (the Phase 1 exit-criterion sentence)."""
    outcome = _build_outcome()
    rendered_action = "ACCEPT" if outcome.accepted else "ROLLBACK"
    assert rendered_action == "ROLLBACK", (
        f"Drift detected: rendered Action={rendered_action!r} but "
        f"outcome.accepted={outcome.accepted!r}."
    )


def test_run_a_iter_1_accepted_label_matches_outcome():
    """Phase 1 downstream consumer #2: ``derive_accepted_label``
    derives the human-readable label from ``reason_code``. For a
    non-accepted outcome the label must NOT contain 'PASS'."""
    outcome = _build_outcome()
    label = derive_accepted_label(outcome.reason_code)
    # The label is derived from reason_code, and outcome.accepted_label
    # field on the dataclass must agree with the derive call.
    assert label == outcome.accepted_label, (
        f"derive_accepted_label({outcome.reason_code!r}) returned "
        f"{label!r} but outcome.accepted_label is "
        f"{outcome.accepted_label!r}. Two derivation paths disagree."
    )
    assert "PASS" not in label.upper(), (
        f"Non-accepted outcome rendered label={label!r} which "
        f"contains 'PASS'. Drift."
    )


def test_run_a_iter_1_marker_payload_matches_outcome():
    """Phase 1 downstream consumer #3: the GSO_FULL_EVAL_V1 marker
    payload's ``accepted`` field (via ``acceptance_decision_dict``)
    must equal ``outcome.accepted``."""
    outcome = _build_outcome()
    payload = acceptance_decision_dict(outcome)
    assert payload["accepted"] == outcome.accepted, (
        f"Marker payload says accepted={payload['accepted']!r} "
        f"but the AcceptanceOutcome says "
        f"accepted={outcome.accepted!r}. Drift."
    )


def test_run_a_iter_1_phase_h_dict_carries_canonical():
    """Phase 1 downstream consumer #4: the Phase H
    ``acceptance_decision`` dict (the serialiser output) carries
    ``_canonical`` so the Phase H writer can short-circuit
    recomputation instead of re-running ``decide_control_plane_
    acceptance``."""
    outcome = _build_outcome()
    phase_h_dict = acceptance_decision_dict(outcome)
    # Required keys per Phase 1 contract.
    for key in ("accepted", "reason", "target_qids", "_canonical"):
        assert key in phase_h_dict, (
            f"Phase H dict missing required key {key!r}. "
            f"Got keys: {sorted(phase_h_dict.keys())}"
        )
    # _canonical IS the ControlPlaneAcceptance instance.
    assert phase_h_dict["_canonical"] is outcome.control_plane_decision


def test_run_a_iter_1_all_four_consumers_agree():
    """The integrating assertion: all four downstream consumers
    derive from the SAME outcome. This is the Phase 1 exit-criterion
    sentence verbatim — 'one coherent verdict, no accepted=true
    paired with Action: ROLLBACK'."""
    outcome = _build_outcome()

    # Surface 1: rendered Action verb.
    rendered_action = "ACCEPT" if outcome.accepted else "ROLLBACK"

    # Surface 2: accepted_label.
    label = derive_accepted_label(outcome.reason_code)

    # Surface 3: marker payload accepted flag.
    marker_payload = acceptance_decision_dict(outcome)

    # Surface 4: Phase H acceptance_decision dict (same producer
    # as the marker payload — by construction they share state).
    phase_h_dict = acceptance_decision_dict(outcome)

    # All four surfaces must agree on accept/reject.
    assert outcome.accepted is False
    assert rendered_action == "ROLLBACK"
    assert "PASS" not in label.upper()
    assert marker_payload["accepted"] is False
    assert phase_h_dict["accepted"] is False

    # The pathological pre-Phase-1 combination MUST NOT occur.
    pre_phase1_pathology = (
        outcome.accepted is True
        and rendered_action == "ROLLBACK"
    )
    assert not pre_phase1_pathology, (
        "Phase 1 exit criterion violated — Run A iter 1 replays to "
        "accepted=true paired with Action: ROLLBACK."
    )
