"""Trial 18 Step 3 — KEPT_INSUFFICIENT outcome.

Pins the new acceptance funnel lane:

* The lane exists and is distinct from ``"accepted"`` / ``"rolled_back"``.
* Aggregators that produce ``accepted_count`` / ``OPTIMIZER_GAINED``
  exclude ``kept_insufficient`` rows.
* A typed ``insufficient_repair_signature`` is emitted alongside the
  decision so the strategist receives the cumulative-learning signal
  on the next iteration.
* ``behavioral_diff`` distinguishes "Genie ignored the patch"
  (``unchanged``) from "Genie consulted the patch but still got the
  wrong answer" (``partial``).
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.forbidden_signatures import (
    harvest_sm_insufficient_repair_signatures,
)
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.outcome import (
    classify_run_outcome,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
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
from genie_space_optimizer.optimization.state_machine.trajectory import (
    build_trajectory,
)
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


@pytest.fixture(autouse=True)
def _trial18_flag_on(monkeypatch):
    """Default the Trial 18 flag ON for this test module."""
    monkeypatch.delenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", raising=False)
    yield


def _state_at_evaluated(
    *,
    qid: str = "gs_009",
    pre_score: float = 0.0,
    post_score: float = 0.0,
    pre_sql: str = "SELECT 1",
    post_sql: str = "SELECT 1",
    behavioral_diff: str = "unchanged",
    patch_type: str = "add_example_sql",
    rca_kind: str = "rank_to_limit_top_n",
):
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", pre_score, pre_sql, "x", 1,
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
             pre_score, post_score, pre_sql, post_sql, "rp",
             behavioral_diff=behavioral_diff)}),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


# ── Test 1: post == pre, no collateral, patch applied -> kept_insufficient


def test_post_eq_pre_no_collateral_returns_kept_insufficient():
    """The new lane. Distinct from ``accepted`` and ``rolled_back``."""
    s = _state_at_evaluated(
        pre_score=0.0, post_score=0.0,
        pre_sql="SELECT 1", post_sql="SELECT 1",
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
    assert s2.accepted.decision == "kept_insufficient"
    # Must NOT be one of the existing lane values.
    assert s2.accepted.decision != "accepted"
    assert s2.accepted.decision != "rolled_back"


# ── Test 2: accepted_count aggregator excludes kept_insufficient ───────


def test_kept_insufficient_is_not_counted_in_accepted_count():
    """A run-summary aggregator that filters ``decision == "accepted"``
    must NOT include kept_insufficient rows.
    """
    records = (
        AcceptanceDecisionRecord(
            decision="accepted", arbiter_reason="target_fixed_no_regression",
            target_fixed=True, collateral_regressions=(),
        ),
        AcceptanceDecisionRecord(
            decision="kept_insufficient",
            arbiter_reason="kept_insufficient:behavior=unchanged",
            target_fixed=False, collateral_regressions=(),
            insufficient_repair_signature="lever-5:add_example_sql:insufficient:rca=x:behavior=unchanged",
            behavioral_diff="unchanged",
        ),
    )
    accepted_count = sum(
        1 for r in records if r.decision == "accepted"
    )
    assert accepted_count == 1, (
        "kept_insufficient must NOT be counted as accepted; "
        f"got {accepted_count} from {records!r}"
    )


# ── Test 3: kept_insufficient does NOT trigger OPTIMIZER_GAINED ────────


def test_kept_insufficient_does_not_trigger_optimizer_gained():
    """A run with zero ACCEPTED and one KEPT_INSUFFICIENT must classify
    as ``OPTIMIZER_TRIED_INSUFFICIENT_GAIN``, not ``OPTIMIZER_IMPROVED``
    and not ``OPTIMIZER_TRIED_NO_GAIN``.
    """
    s = _state_at_evaluated(
        pre_score=0.0, post_score=0.0,
        pre_sql="SELECT 1", post_sql="SELECT 1",
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


# ── Test 4: typed signature shape ──────────────────────────────────────


def test_kept_insufficient_emits_typed_signature():
    """Signature format pin:
    ``<lever>:<patch_type>:insufficient:rca=<rca_kind>:behavior=<diff>``.
    Mirrors ``forbidden_signature`` template style so the cluster_batch
    plumbing can render both channels with the same template.
    """
    s = _state_at_evaluated(
        pre_score=0.0, post_score=0.0,
        patch_type="add_example_sql",
        rca_kind="rank_to_limit_top_n",
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
    sig = s2.accepted.insufficient_repair_signature
    assert ":insufficient:" in sig
    assert "rca=rank_to_limit_top_n" in sig
    assert "behavior=unchanged" in sig
    assert "add_example_sql" in sig


# ── Test 5: harvest helper reads from state.accepted ───────────────────


def test_kept_insufficient_closes_qid_and_signature_harvests():
    """The QID is closed at ACCEPTED stage (not TERMINATED) but the
    signature still propagates via the new harvest helper, which
    reads from ``state.accepted.insufficient_repair_signature``
    (NOT ``state.terminal.forbidden_signature``).
    """
    s = _state_at_evaluated(pre_score=0.0, post_score=0.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    # QID is at ACCEPTED stage (closed for the iteration), NOT terminated.
    assert s2.current_stage == FunnelStage.ACCEPTED
    assert s2.terminal is None
    # The harvest helper still picks up the signature.
    harvested = harvest_sm_insufficient_repair_signatures((s2,))
    assert len(harvested) == 1
    assert ":insufficient:" in harvested[0]


# ── Test 6/7: behavioral_diff unchanged vs partial ────────────────────


def test_behavioral_diff_unchanged_when_sql_identical():
    """``pre_apply_sql == post_apply_sql`` -> ``behavior=unchanged``
    (Genie's planner ignored the patch)."""
    s = _state_at_evaluated(
        pre_sql="SELECT 1", post_sql="SELECT 1",
        behavioral_diff="unchanged",
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
    assert s2.accepted.behavioral_diff == "unchanged"
    assert "behavior=unchanged" in s2.accepted.insufficient_repair_signature


def test_behavioral_diff_partial_when_sql_changed():
    """``pre_apply_sql != post_apply_sql`` but score didn't move ->
    ``behavior=partial`` (Genie consulted the patch but still got the
    wrong answer)."""
    s = _state_at_evaluated(
        pre_sql="SELECT 1", post_sql="SELECT 2",
        behavioral_diff="partial",
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
    assert s2.accepted.behavioral_diff == "partial"
    assert "behavior=partial" in s2.accepted.insufficient_repair_signature


# ── Test 8: insufficient channel surfaces in TransformerContext ────────


def test_insufficient_channel_plumbed_in_transformer_context():
    """The new ``ctx.insufficient_repair_signatures`` channel must
    accept a tuple of strings and round-trip cleanly."""
    sigs = (
        "lever-5:add_example_sql:insufficient:rca=x:behavior=unchanged",
        "lever-3:add_instruction:insufficient:rca=y:behavior=partial",
    )
    ctx = TransformerContext(
        1, "r", ValidationContext(1, "r", {}),
        insufficient_repair_signatures=sigs,
    )
    assert ctx.insufficient_repair_signatures == sigs


# ── Test 9: rollback path — non-zero post still rejects terminally ─────


def test_post_lt_pre_still_rejects_terminally():
    """Score regression on the target still terminates ``rejected`` —
    KEPT_INSUFFICIENT only fires on post == pre. This pins the new
    branch's predicate strictly to the no-movement, no-collateral case.
    """
    s = _state_at_evaluated(pre_score=1.0, post_score=0.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.accepted is None
    assert s2.terminal is not None
    assert s2.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"


# ── Test 10: flag-off rollback parity ──────────────────────────────────


def test_kept_insufficient_disabled_when_flag_off(monkeypatch):
    """``GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0`` must revert to the
    pre-Trial-18 two-lane behaviour: post == pre is rejected
    terminally (no KEPT_INSUFFICIENT).
    """
    monkeypatch.setenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", "0")
    s = _state_at_evaluated(pre_score=0.0, post_score=0.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.accepted is None
