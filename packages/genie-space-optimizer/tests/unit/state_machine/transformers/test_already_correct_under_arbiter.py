"""Trial 19 C2 — acceptance_gate already_correct_under_arbiter unit test.

Verifies the new lane fires before ``kept_insufficient`` when:

* ``pre_apply_score >= 1.0`` (arbiter-correct before apply), AND
* ``post_apply_score >= 1.0`` (arbiter-correct after apply), AND
* ``behavioral_diff == "unchanged"`` (the apply was a no-op for an
  already-passing QID), AND
* No collateral regressions.

Falls back to the legacy ``kept_insufficient`` lane when the Trial 19
already-correct sub-flag is off (byte-stable replay).
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


@pytest.fixture(autouse=True)
def _trial_flags_on(monkeypatch):
    """Force both Trial 18 acceptance overhaul AND Trial 19 enforce +
    already-correct sub-flag ON for the C2 lane to be reachable."""
    monkeypatch.setenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", "1")
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL19_ALREADY_CORRECT_FILTER", "1")
    yield


def _state_at_evaluated(
    *,
    qid: str = "gs_test",
    pre_score: float = 1.0,
    post_score: float = 1.0,
    behavioral_diff: str = "unchanged",
    patch_type: str = "add_instruction",
    rca_kind: str = "missing_filter",
):
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", pre_score, "SELECT 1", "x", 1,
        ),
    )
    for from_s, to_s, kw in (
        (FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
         {"diagnosed": DiagnosisRecord(
             "plan11_stage1", rca_kind, "s", "f", "e", "high", "r")}),
        (FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
         {"clustered": ClusterMembershipRecord(
             "H1", "AG", (qid,), 6, "k")}),
        (FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
         {"proposals": (ProposalAttempt(
             0, "i", patch_type,
             FunnelStage.APPLIED, "applied", "ok"),)}),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (FunnelStage.APPLYABLE, FunnelStage.APPLIED,
         {"applied": AppliedRecord(1, "c", 0, ("i",))}),
        (FunnelStage.APPLIED, FunnelStage.EVALUATED,
         {"evaluated": EvaluatedRecord(
             pre_score, post_score, "SELECT 1", "SELECT 1", "rp",
             behavioral_diff=behavioral_diff)}),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


def test_pre_post_both_arbiter_correct_unchanged_fires_c2():
    """The canonical C2 case: pre = post = 1.0 + unchanged behavior."""
    s = _state_at_evaluated(
        pre_score=1.0, post_score=1.0, behavioral_diff="unchanged",
    )
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.ACCEPTED
    assert s2.accepted is not None
    assert s2.accepted.decision == "already_correct_under_arbiter", (
        f"Expected Trial 19 C2 decision; got {s2.accepted.decision!r}"
    )


def test_partial_behavior_does_not_fire_c2():
    """``behavioral_diff != "unchanged"`` ⇒ falls through (Genie's
    output changed, so the apply was not a no-op)."""
    s = _state_at_evaluated(
        pre_score=1.0, post_score=1.0, behavioral_diff="partial",
    )
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.accepted is None or (
        s2.accepted.decision != "already_correct_under_arbiter"
    ), (
        f"C2 must not fire on partial behavior; got "
        f"{getattr(s2.accepted, 'decision', None)!r}"
    )


def test_pre_failing_does_not_fire_c2():
    """``pre_apply_score < 1.0`` ⇒ this is a genuine repair attempt,
    not an already-correct QID; the lane must not fire."""
    s = _state_at_evaluated(
        pre_score=0.0, post_score=0.0, behavioral_diff="unchanged",
    )
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    # Should fall through to the Trial 18 KEPT_INSUFFICIENT lane.
    assert s2.accepted is not None
    assert s2.accepted.decision != "already_correct_under_arbiter"
    assert s2.accepted.decision == "kept_insufficient"


def test_trial19_already_correct_flag_off_falls_through(monkeypatch):
    """Flag-off byte-stable replay parity: the legacy
    ``kept_insufficient`` lane handles the pre==post equal-score case
    when the Trial 19 sub-flag is disabled."""
    monkeypatch.setenv("GSO_TRIAL19_ALREADY_CORRECT_FILTER", "0")
    s = _state_at_evaluated(
        pre_score=1.0, post_score=1.0, behavioral_diff="unchanged",
    )
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.accepted is not None
    assert s2.accepted.decision != "already_correct_under_arbiter", (
        f"C2 must NOT fire when sub-flag is off; got "
        f"{s2.accepted.decision!r}"
    )
    # The legacy kept_insufficient lane should pick up this case.
    assert s2.accepted.decision == "kept_insufficient"


def test_c2_run_outcome_is_optimizer_tried_insufficient_gain():
    """A C2 run with no genuine acceptance still classifies as
    ``OPTIMIZER_TRIED_INSUFFICIENT_GAIN`` — same outcome bucket as
    ``kept_insufficient`` (see ``outcome.py``)."""
    from genie_space_optimizer.optimization.state_machine.outcome import (
        classify_run_outcome,
    )
    from genie_space_optimizer.optimization.state_machine.trajectory import (
        build_trajectory,
    )

    s = _state_at_evaluated(
        pre_score=1.0, post_score=1.0, behavioral_diff="unchanged",
    )
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    traj = build_trajectory(qid=s.qid, iterations=(s2,))
    outcome = classify_run_outcome((traj,))
    assert outcome == "OPTIMIZER_TRIED_INSUFFICIENT_GAIN", (
        f"expected OPTIMIZER_TRIED_INSUFFICIENT_GAIN; got {outcome!r}"
    )
