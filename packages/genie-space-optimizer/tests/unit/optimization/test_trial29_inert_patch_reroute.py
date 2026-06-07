"""Trial 29 W29.1 — kit-forced inert-patch re-route acceptance lane.

Phase 2 (first cohort): the AcceptanceDecisionRecord literal accepts
``"kit_forced_inert_reroute"`` and carries a ``rejected_mechanism``
field.

Phase 4 (second cohort): the acceptance gate routes a (kit-forced ∧
behavioral_diff="unchanged" ∧ post==pre) state to the new lane and
populates ``rejected_mechanism``. Kit-forced is derived from
``_kit_for_rca_companions(rca_kind) is not None`` — i.e. RCA kinds
in the KIT_FOR_RCA map (or the Trial 24/26 extensions when their
flags are on). Sub-flag OFF falls back to ``kept_insufficient``
(byte-stable rollback).
"""
from __future__ import annotations

from dataclasses import asdict
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
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
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def test_record_accepts_kit_forced_inert_reroute_literal():
    record = AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature="",
        behavioral_diff="unchanged",
        rejected_mechanism="add_sql_snippet_filter",
    )
    assert record.decision == "kit_forced_inert_reroute"
    assert record.rejected_mechanism == "add_sql_snippet_filter"


def test_record_rejected_mechanism_defaults_to_empty():
    record = AcceptanceDecisionRecord(
        decision="accepted",
        arbiter_reason="ok",
        target_fixed=True,
        collateral_regressions=(),
    )
    assert record.rejected_mechanism == ""


def test_record_serialises_rejected_mechanism():
    record = AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature=(
            "add_sql_snippet_filter:filter:insufficient:"
            "rca=wrong_aggregation:behavior=unchanged"
        ),
        behavioral_diff="unchanged",
        rejected_mechanism="add_sql_snippet_filter",
    )
    payload = asdict(record)
    assert payload["decision"] == "kit_forced_inert_reroute"
    assert payload["rejected_mechanism"] == "add_sql_snippet_filter"


# ── Phase 4: acceptance-gate behaviour ────────────────────────────────


@pytest.fixture(autouse=True)
def _trial_flags_on(monkeypatch):
    """Default ALL prereq flags ON for this test module.

    Trial 18 (kept_insufficient lane) is the fallback path; Trial 24
    + Trial 26 give us the kit-required RCAs the new lane targets;
    Trial 29 master + sub-flag enable the lane itself.
    """
    monkeypatch.delenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv("GSO_TRIAL29_INERT_REROUTE", raising=False)
    yield


def _state_at_evaluated(
    *,
    qid: str = "gs_026",
    pre_score: float = 0.0,
    post_score: float = 0.0,
    pre_sql: str = "SELECT 1",
    post_sql: str = "SELECT 1",
    behavioral_diff: str = "unchanged",
    patch_type: str = "add_sql_snippet_filter",
    rca_kind: str = "wrong_aggregation",  # Trial 26 kit-required
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


def _run_gate(state):
    """Run the acceptance gate with an empty collateral patch."""
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        return acceptance_gate.transform(
            state,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )


def test_kit_forced_rca_with_unchanged_behavior_routes_to_new_lane():
    """When the RCA carries a KIT_FOR_RCA contract AND the patch
    landed inert (post==pre, behavior==unchanged), the new lane
    fires instead of kept_insufficient. The rejected mechanism is
    recorded so the next iteration's harvest reads it.
    """
    s = _state_at_evaluated(
        rca_kind="wrong_aggregation",
        patch_type="add_sql_snippet_filter",
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="unchanged",
    )
    s2 = _run_gate(s)
    assert s2.current_stage == FunnelStage.ACCEPTED
    assert s2.accepted is not None
    assert s2.accepted.decision == "kit_forced_inert_reroute", (
        f"expected kit_forced_inert_reroute; got "
        f"{s2.accepted.decision!r}"
    )
    assert s2.accepted.rejected_mechanism != ""
    assert s2.accepted.behavioral_diff == "unchanged"


def test_non_kit_forced_rca_falls_through_to_kept_insufficient():
    """When the RCA has NO kit contract, the existing
    kept_insufficient lane handles the (post==pre, unchanged) case.
    Byte-stable for RCAs outside KIT_FOR_RCA.
    """
    s = _state_at_evaluated(
        rca_kind="rank_to_limit_top_n",  # not in KIT_FOR_RCA
        patch_type="add_example_sql",
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="unchanged",
    )
    s2 = _run_gate(s)
    assert s2.accepted is not None
    assert s2.accepted.decision == "kept_insufficient"
    assert s2.accepted.rejected_mechanism == ""


def test_kit_forced_rca_with_changed_behavior_does_not_route_to_new_lane():
    """When the patch actually moved behaviour (partial) — even on a
    kit-required RCA — the inert-reroute lane MUST NOT fire. The
    partial branch goes through kept_insufficient as before.
    """
    s = _state_at_evaluated(
        rca_kind="wrong_aggregation",
        pre_sql="SELECT 1",
        post_sql="SELECT 2",  # behaviour changed
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="partial",
    )
    s2 = _run_gate(s)
    assert s2.accepted is not None
    assert s2.accepted.decision == "kept_insufficient"


def test_sub_flag_off_restores_kept_insufficient(monkeypatch):
    """``GSO_TRIAL29_INERT_REROUTE=0`` reverts the kit-forced inert
    case to the pre-Trial-29 kept_insufficient lane (byte-stable
    rollback for the sub-flag)."""
    monkeypatch.setenv("GSO_TRIAL29_INERT_REROUTE", "0")
    s = _state_at_evaluated(
        rca_kind="wrong_aggregation",
        patch_type="add_sql_snippet_filter",
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="unchanged",
    )
    s2 = _run_gate(s)
    assert s2.accepted is not None
    assert s2.accepted.decision == "kept_insufficient"
    assert s2.accepted.rejected_mechanism == ""


def test_master_flag_off_forces_byte_stable_rollback(monkeypatch):
    """``GSO_TRIAL29_BEHAVIOR_DELTA=0`` overrides any sub-flag
    setting and forces every Trial 29 path off."""
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "0")
    monkeypatch.setenv("GSO_TRIAL29_INERT_REROUTE", "1")  # ignored
    s = _state_at_evaluated(
        rca_kind="wrong_aggregation",
        patch_type="add_sql_snippet_filter",
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="unchanged",
    )
    s2 = _run_gate(s)
    assert s2.accepted is not None
    assert s2.accepted.decision == "kept_insufficient"
    assert s2.accepted.rejected_mechanism == ""
