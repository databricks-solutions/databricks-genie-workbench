"""Trial 17 Step 6 — gs_009 top-N pivot workbench test.

Pins the **forbidden_signature pivot signal** that Trial 17 introduces.

Production gap: in postmortem 289767602715184, gs_009 reached APPLIED
three iterations in a row, all with ``add_instruction`` patches, all
rejected by ``acceptance_gate`` with the same generic
``target_unchanged: post_score <= pre_score``. The next iteration's
LLM had no way to know which **lever** had failed for which **RCA**,
so it re-picked the same lever again.

Trial 17 contract (this test pins it):
- ``acceptance_gate`` emits a ``forbidden_signature`` of shape
  ``"<lever>:<patch_type>:target_unchanged:rca=<rca_kind>"``.
- The next iteration's Stage 3 prompt sees that string in
  ``ctx.forbidden_signatures`` (already plumbed by Trial 16.3).
- The LLM is therefore able to pivot to ``fallback_lever`` from its
  prior repair_plan, instead of re-proposing ``lever-5``.

This is RED before Trial 17 steps 1 + 3 land (current src emits the
generic reason string only) and GREEN afterwards.
"""
from __future__ import annotations

from unittest.mock import patch as mock_patch

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


def _gs009_state_at_evaluated(*, post_score: float):
    """Build an SM state that mimics gs_009's iteration-1 evaluated row
    from postmortem 289767602715184: a lever-5 ``add_instruction``
    patch landed at APPLIED with the same post score as pre score.
    """
    s = build_initial_state(
        qid="gs_009",
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1
        ),
    )
    for from_s, to_s, kw in (
        (
            FunnelStage.HARD_QID_SEEN,
            FunnelStage.DIAGNOSED,
            {
                "diagnosed": DiagnosisRecord(
                    "plan11_stage1",
                    "top_n_cardinality_collapse",  # rca_kind
                    "rca_gs_009_top_n",
                    "f",
                    "e",
                    "high",
                    "r",
                )
            },
        ),
        (
            FunnelStage.DIAGNOSED,
            FunnelStage.CLUSTERED,
            {
                "clustered": ClusterMembershipRecord(
                    "H2", "AG_H002", ("gs_009",), 6, "top_n_cardinality_collapse"
                )
            },
        ),
        (
            FunnelStage.CLUSTERED,
            FunnelStage.PROPOSED,
            {
                "proposals": (
                    ProposalAttempt(
                        0,
                        "H002_000",
                        "add_instruction",
                        FunnelStage.APPLIED,
                        "applied",
                        "applied_ok",
                    ),
                )
            },
        ),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (
            FunnelStage.APPLYABLE,
            FunnelStage.APPLIED,
            {"applied": AppliedRecord(1, "c", 0, ("H002_000",))},
        ),
        (
            FunnelStage.APPLIED,
            FunnelStage.EVALUATED,
            {
                "evaluated": EvaluatedRecord(
                    0.0, post_score, "SELECT 1", "SELECT 2", "rp"
                )
            },
        ),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


def test_trial17_gs009_acceptance_gate_forbidden_signature_carries_lever_and_rca():
    """RED before Trial 17 steps 1+3. GREEN after.

    Asserts that when acceptance_gate rejects gs_009 with target_unchanged,
    the TerminalRecord.forbidden_signature carries the full Trial 17
    contract: ``<lever>:<patch_type>:target_unchanged:rca=<rca_kind>``.

    The current pre-Trial-17 src emits only
    ``"target_unchanged: post_score <= pre_score"`` (no lever, no rca),
    so the assertion below fails.
    """
    s = _gs009_state_at_evaluated(post_score=0.0)
    with mock_patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )

    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal is not None
    assert s2.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"

    sig = s2.terminal.forbidden_signature or ""
    # Trial 17 contract: the signature must carry these three tokens.
    assert "add_instruction" in sig, (
        f"forbidden_signature missing patch_type: {sig!r}"
    )
    assert "target_unchanged" in sig, (
        f"forbidden_signature missing target_unchanged: {sig!r}"
    )
    assert "rca=top_n_cardinality_collapse" in sig or "top_n_cardinality_collapse" in sig, (
        f"forbidden_signature missing rca_kind: {sig!r}"
    )
    # And it must carry a recognisable lever token. lever-5 covers both
    # the prose (5a) and example_sql (5b) variants for add_instruction.
    assert "lever-5" in sig, (
        f"forbidden_signature missing lever token: {sig!r}"
    )
