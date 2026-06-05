"""P4 C6 OBSERVE-FIRST tests — GSO_HCRF_DIAGNOSTIC_V1 marker.

Contract:
  * The diagnostic is purely observational. No flag flips, no
    downgrade behavior change.
  * Marker emitted on every HCRF-eligible verdict reason
    (``high_collateral_risk_flagged``,
    ``blast_radius_exceeds_threshold``,
    ``passing_dependents_missing``, the two collateral_warning
    downgrade reasons).
  * Fields are deterministic given verdict + run context.
"""
from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stdout

import pytest

from genie_space_optimizer.optimization.hcrf_diagnostic import (
    HCRF_DIAGNOSTIC_MARKER_PREFIX,
    HcrfDiagnostic,
    compute_hcrf_diagnostic,
    compute_would_have_stamped,
    hcrf_diagnostic_marker,
    hcrf_diagnostic_marker_from_verdict,
    is_hcrf_eligible_reason,
)


# ---------- is_hcrf_eligible_reason --------------------------------


def test_is_hcrf_eligible_reason_pins_set():
    assert is_hcrf_eligible_reason("high_collateral_risk_flagged")
    assert is_hcrf_eligible_reason("blast_radius_exceeds_threshold")
    assert is_hcrf_eligible_reason("passing_dependents_missing")
    assert is_hcrf_eligible_reason("shared_cause_collateral_warning")
    assert is_hcrf_eligible_reason("non_semantic_collateral_warning")


def test_is_hcrf_eligible_reason_rejects_safe_reasons():
    assert not is_hcrf_eligible_reason("no_passing_dependents_outside_target")
    assert not is_hcrf_eligible_reason("within_threshold")
    assert not is_hcrf_eligible_reason("no_passing_dependents_field")
    assert not is_hcrf_eligible_reason("")
    assert not is_hcrf_eligible_reason(None)  # type: ignore[arg-type]


# ---------- compute_would_have_stamped -----------------------------


def test_would_have_stamped_true_when_all_outside_in_hard():
    assert compute_would_have_stamped(
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_009", "gs_010"],
        live_hard_qids=["gs_009", "gs_010", "gs_011"],
    ) is True


def test_would_have_stamped_false_when_some_outside_not_in_hard():
    assert compute_would_have_stamped(
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_009", "gs_010"],
        live_hard_qids=["gs_009"],
    ) is False


def test_would_have_stamped_false_when_outside_empty():
    assert compute_would_have_stamped(
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=[],
        live_hard_qids=["gs_009"],
    ) is False


def test_would_have_stamped_false_for_non_hcrf_reason():
    """Only ``high_collateral_risk_flagged`` is downgrade-eligible
    via the shared_cause path."""
    assert compute_would_have_stamped(
        why_fired="blast_radius_exceeds_threshold",
        outside_target_qids=["gs_009"],
        live_hard_qids=["gs_009"],
    ) is False


# ---------- compute_hcrf_diagnostic --------------------------------


def test_compute_hcrf_diagnostic_basic_shape():
    diag = compute_hcrf_diagnostic(
        patch_type="instruction_text",
        intent_id="intent_001",
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_009", "gs_010"],
        live_hard_qids=["gs_009"],
    )
    assert diag.patch_type == "instruction_text"
    assert diag.intent_id == "intent_001"
    assert diag.why_fired == "high_collateral_risk_flagged"
    assert diag.outside_target_qids == ("gs_009", "gs_010")
    assert diag.outside_target_qids_currently_hard_count == 1
    assert diag.adjacent_qid_regression_evidence_count == 0
    assert diag.hypothetical_branch_c_candidate_count == 0
    assert diag.would_have_stamped is False  # gs_010 not in hard


def test_compute_hcrf_diagnostic_would_have_stamped_via_shared_cause():
    diag = compute_hcrf_diagnostic(
        patch_type="instruction_text",
        intent_id="intent_002",
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_009", "gs_010"],
        live_hard_qids=["gs_009", "gs_010"],
    )
    assert diag.would_have_stamped is True


def test_compute_hcrf_diagnostic_would_have_stamped_via_non_semantic():
    """Callers that detect a non-semantic patch type pass
    ``non_semantic_downgrade_eligible=True`` and the field flips
    even without shared-cause overlap."""
    diag = compute_hcrf_diagnostic(
        patch_type="add_synonym",
        intent_id="intent_003",
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_100"],  # not in hard
        live_hard_qids=["gs_009"],
        non_semantic_downgrade_eligible=True,
    )
    assert diag.would_have_stamped is True


def test_compute_hcrf_diagnostic_dedupes_and_sorts_outside():
    """Field ordering is deterministic for byte-stable replay."""
    diag = compute_hcrf_diagnostic(
        patch_type="instruction_text",
        intent_id="intent_004",
        why_fired="blast_radius_exceeds_threshold",
        outside_target_qids=["gs_010", "gs_009", "gs_010", "", "gs_009"],
        live_hard_qids=[],
    )
    assert diag.outside_target_qids == ("gs_009", "gs_010")


def test_compute_hcrf_diagnostic_passes_through_counts():
    diag = compute_hcrf_diagnostic(
        patch_type="x",
        intent_id="y",
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=[],
        live_hard_qids=[],
        adjacent_qid_regression_evidence_count=3,
        hypothetical_branch_c_candidate_count=5,
    )
    assert diag.adjacent_qid_regression_evidence_count == 3
    assert diag.hypothetical_branch_c_candidate_count == 5


# ---------- hcrf_diagnostic_marker (line shape) --------------------


def test_marker_line_prefix_pinned():
    diag = compute_hcrf_diagnostic(
        patch_type="instruction_text",
        intent_id="intent_001",
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_009"],
        live_hard_qids=["gs_009"],
    )
    line = hcrf_diagnostic_marker(diag)
    assert line.startswith(HCRF_DIAGNOSTIC_MARKER_PREFIX + " ")
    payload = json.loads(line[len(HCRF_DIAGNOSTIC_MARKER_PREFIX) + 1:])
    # All 9 fields present, sorted.
    assert sorted(payload.keys()) == [
        "adjacent_qid_regression_evidence_count",
        "hypothetical_branch_c_candidate_count",
        "intent_id",
        "outside_target_qids",
        "outside_target_qids_count",
        "outside_target_qids_currently_hard_count",
        "patch_type",
        "why_fired",
        "would_have_stamped",
    ]


def test_marker_payload_byte_stable_across_invocations():
    diag = compute_hcrf_diagnostic(
        patch_type="instruction_text",
        intent_id="intent_001",
        why_fired="high_collateral_risk_flagged",
        outside_target_qids=["gs_010", "gs_009"],
        live_hard_qids=["gs_009"],
    )
    a = hcrf_diagnostic_marker(diag)
    b = hcrf_diagnostic_marker(diag)
    assert a == b


# ---------- hcrf_diagnostic_marker_from_verdict --------------------


def test_marker_from_verdict_high_collateral_risk_flagged():
    verdict = {
        "safe": False,
        "reason": "high_collateral_risk_flagged",
        "passing_dependents_outside_target": ["gs_009", "gs_010"],
    }
    line = hcrf_diagnostic_marker_from_verdict(
        verdict=verdict,
        patch_type="instruction_text",
        intent_id="intent_001",
        live_hard_qids=["gs_009", "gs_010"],
    )
    assert line is not None
    payload = json.loads(line[len(HCRF_DIAGNOSTIC_MARKER_PREFIX) + 1:])
    assert payload["why_fired"] == "high_collateral_risk_flagged"
    assert payload["outside_target_qids"] == ["gs_009", "gs_010"]
    assert payload["outside_target_qids_currently_hard_count"] == 2
    assert payload["would_have_stamped"] is True


def test_marker_from_verdict_safe_reason_returns_none():
    """Safe verdict reasons NOT in the HCRF set → no marker."""
    verdict = {"safe": True, "reason": "within_threshold"}
    line = hcrf_diagnostic_marker_from_verdict(
        verdict=verdict,
        patch_type="x",
        intent_id="y",
        live_hard_qids=[],
    )
    assert line is None


def test_marker_from_verdict_emits_for_passing_dependents_missing():
    verdict = {"safe": False, "reason": "passing_dependents_missing"}
    line = hcrf_diagnostic_marker_from_verdict(
        verdict=verdict,
        patch_type="x",
        intent_id="y",
        live_hard_qids=[],
    )
    assert line is not None
    payload = json.loads(line[len(HCRF_DIAGNOSTIC_MARKER_PREFIX) + 1:])
    assert payload["why_fired"] == "passing_dependents_missing"
    assert payload["outside_target_qids"] == []
    assert payload["would_have_stamped"] is False


# ---------- blast_radius_batch wiring -------------------------------


def test_blast_radius_batch_emits_hcrf_marker_on_reject():
    """Integration: when patch_blast_radius_is_safe returns
    ``high_collateral_risk_flagged``, the transformer emits
    ``GSO_HCRF_DIAGNOSTIC_V1`` to stdout AND still rejects the
    proposal (no behavior change). OBSERVE-FIRST contract."""
    from genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch import (
        _assess_blast_radius,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )

    # Minimal stubs that satisfy _assess_blast_radius's contract.
    class _StubProposalStore:
        def __init__(self, proposal):
            self._p = proposal

        def lookup(self, intent_id):
            return self._p

    class _StubCtx:
        def __init__(self, proposal):
            self.proposal_store = _StubProposalStore(proposal)
            self.passing_dependents_by_intent = {}
            self.high_collateral_risk_by_intent = {}
            self.benchmarks = ()
            self.live_hard_qids = ()

    class _StubProposalAttempt:
        intent_id = "intent_t"
        patch_type = "instruction_text"
        attempt_index = 0

    class _StubState:
        qid = "gs_001"
        proposals = (_StubProposalAttempt(),)

    # patch_body with passing_dependents + high_collateral_risk to
    # trigger HCRF reason.
    class _StubTypedProposal:
        patch_type = "instruction_text"
        target_qids = ("gs_001",)
        patch_body = {
            "passing_dependents": ["gs_009"],
            "high_collateral_risk": True,
        }

    typed = _StubTypedProposal()
    ctx = _StubCtx(typed)
    state = _StubState()

    buf = io.StringIO()
    with redirect_stdout(buf):
        verdict, drop = _assess_blast_radius(state, (state,), ctx)
    out = buf.getvalue()

    # Behavior unchanged: still rejects on HCRF.
    assert verdict == "reject"
    # OBSERVE-FIRST: diagnostic marker emitted to stdout.
    assert HCRF_DIAGNOSTIC_MARKER_PREFIX in out


def test_blast_radius_batch_does_not_emit_for_safe_paths():
    """Safe path (no dependents): no HCRF marker emission."""
    from genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch import (
        _assess_blast_radius,
    )

    class _StubProposalStore:
        def __init__(self, proposal):
            self._p = proposal

        def lookup(self, intent_id):
            return self._p

    class _StubCtx:
        def __init__(self, proposal):
            self.proposal_store = _StubProposalStore(proposal)
            self.passing_dependents_by_intent = {}
            self.high_collateral_risk_by_intent = {}
            self.benchmarks = ()
            self.live_hard_qids = ()

    class _StubProposalAttempt:
        intent_id = "intent_safe"
        patch_type = "instruction_text"
        attempt_index = 0

    class _StubState:
        qid = "gs_safe"
        proposals = (_StubProposalAttempt(),)

    class _StubTypedProposal:
        patch_type = "instruction_text"
        target_qids = ("gs_safe",)
        # No dependents → safe path.
        patch_body = {"passing_dependents": []}

    typed = _StubTypedProposal()
    ctx = _StubCtx(typed)
    state = _StubState()

    buf = io.StringIO()
    with redirect_stdout(buf):
        verdict, _ = _assess_blast_radius(state, (state,), ctx)
    out = buf.getvalue()

    assert verdict == "safe"
    assert HCRF_DIAGNOSTIC_MARKER_PREFIX not in out


def test_hcrf_diagnostic_is_dataclass():
    """Pin the dataclass contract."""
    diag = HcrfDiagnostic(
        patch_type="x",
        intent_id="y",
        why_fired="z",
        outside_target_qids=(),
        outside_target_qids_currently_hard_count=0,
        adjacent_qid_regression_evidence_count=0,
        hypothetical_branch_c_candidate_count=0,
        would_have_stamped=False,
    )
    assert diag.patch_type == "x"
    assert isinstance(diag.to_jsonable(), dict)


def test_d139_28x_hcrf_simulation():
    """Regression simulation: 28 HCRF rejections each emit exactly
    one marker line."""
    lines = []
    for i in range(28):
        verdict = {
            "safe": False,
            "reason": "high_collateral_risk_flagged",
            "passing_dependents_outside_target": [f"gs_{i:03d}"],
        }
        line = hcrf_diagnostic_marker_from_verdict(
            verdict=verdict,
            patch_type="instruction_text",
            intent_id=f"intent_{i}",
            live_hard_qids=[],
        )
        assert line is not None
        lines.append(line)
    assert len(lines) == 28
    # Every line is a single newline-free record (so a downstream
    # grep `^GSO_HCRF_DIAGNOSTIC_V1` would count exactly 28).
    for line in lines:
        assert "\n" not in line
