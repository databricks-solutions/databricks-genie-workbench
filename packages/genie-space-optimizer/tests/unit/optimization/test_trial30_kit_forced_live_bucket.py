"""Trial 30 W30.2(c) — kit_forced_inert_reroute same-iteration live bucket.

The ``kept_insufficient`` lane writes its rejected signature into a
shared mutable bucket on ``ctx.extras["_live_insufficient_repair_signatures"]``
the moment its verdict fires, so sibling clusters running later in the
SAME iteration see the signature without waiting for the end-of-iteration
harness harvest. The ``kit_forced_inert_reroute`` lane (its Trial 29
sibling) previously did NOT mirror that write, leaving a within-iteration
lag.

W30.2(c) mirrors the kept_insufficient ``ctx.extras`` write into the
kit_forced lane, gated by ``GSO_TRIAL30_INERT_HARVEST_WIRE`` so OFF is
byte-stable with Trial 29.

The ctx/state construction below is copied verbatim from
``test_trial29_inert_patch_reroute.py``'s positive case
(``test_kit_forced_rca_with_unchanged_behavior_routes_to_new_lane``),
except that the ``TransformerContext`` is constructed explicitly so the
test can inspect ``ctx.extras`` after the transform runs.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
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
    """Default the Trial 29 prereq flags ON. Individual tests set the
    Trial 30 flags explicitly so the byte-stable rollback case can flip
    just ``GSO_TRIAL30_INERT_HARVEST_WIRE`` off.
    """
    monkeypatch.delenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
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
    """Verbatim copy of the positive-case fixture from
    ``test_trial29_inert_patch_reroute.py``."""
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


def _run_gate(state, ctx):
    """Run the acceptance gate with an empty collateral patch against the
    supplied ``ctx`` so the caller can inspect ``ctx.extras`` afterwards.
    """
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        return acceptance_gate.transform(state, ctx)


def test_kit_forced_writes_live_bucket(monkeypatch):
    """With the harvest-wire flag ON, the kit_forced_inert_reroute lane
    writes its rejected signature into the same-iteration live bucket on
    ``ctx.extras["_live_insufficient_repair_signatures"]``.
    """
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "1")
    monkeypatch.setenv("GSO_TRIAL29_INERT_REROUTE", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")

    s = _state_at_evaluated(
        rca_kind="wrong_aggregation",
        patch_type="add_sql_snippet_filter",
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="unchanged",
    )
    ctx = TransformerContext(1, "r", ValidationContext(1, "r", {}))
    s2 = _run_gate(s, ctx)

    # The lane fired as expected.
    assert s2.accepted is not None
    assert s2.accepted.decision == "kit_forced_inert_reroute", (
        f"expected kit_forced_inert_reroute; got {s2.accepted.decision!r}"
    )

    # Same-iteration live bucket now carries the rejected signature.
    bucket = ctx.extras.get("_live_insufficient_repair_signatures", ())
    assert bucket, "live bucket must be populated by the kit_forced lane"
    assert any("kit_forced_inert" in s for s in bucket), (
        f"expected a kit_forced_inert signature in the live bucket; "
        f"got {bucket!r}"
    )
    # It matches exactly the signature the record carries.
    assert s2.accepted.insufficient_repair_signature in bucket


def test_flag_off_does_not_write_live_bucket(monkeypatch):
    """With ``GSO_TRIAL30_INERT_HARVEST_WIRE=0`` the kit_forced lane must
    NOT write the live bucket (byte-stable rollback to Trial 29), while
    still returning the ``kit_forced_inert_reroute`` verdict.
    """
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "1")
    monkeypatch.setenv("GSO_TRIAL29_INERT_REROUTE", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "0")

    s = _state_at_evaluated(
        rca_kind="wrong_aggregation",
        patch_type="add_sql_snippet_filter",
        pre_score=0.0,
        post_score=0.0,
        behavioral_diff="unchanged",
    )
    ctx = TransformerContext(1, "r", ValidationContext(1, "r", {}))
    s2 = _run_gate(s, ctx)

    # The lane still fires (the verdict is independent of the wire flag).
    assert s2.accepted is not None
    assert s2.accepted.decision == "kit_forced_inert_reroute"

    # But the live bucket is NOT written by this lane.
    assert "_live_insufficient_repair_signatures" not in ctx.extras, (
        "kit_forced lane must not write the live bucket when the "
        "harvest-wire flag is off"
    )
