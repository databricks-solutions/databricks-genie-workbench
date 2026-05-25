"""Trial 17 Step 6 — gs_026 superlative pivot workbench test.

Same shape as gs_009 but for the superlative / RANK RCA family.
Postmortem 230596834005670 shows gs_026 reached APPLIED with
``add_instruction`` and was rejected by ``acceptance_gate`` with the
generic ``target_unchanged`` message. Trial 17 must enrich the
``forbidden_signature`` so the next iteration's LLM has the lever +
patch_type + rca tokens to pivot on.

RED before Trial 17 steps 1 + 3. GREEN afterwards.
"""
from __future__ import annotations

from unittest.mock import patch as mock_patch

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


def _gs026_state_at_evaluated(*, post_score: float):
    s = build_initial_state(
        qid="gs_026",
        iteration=2,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "SELECT 1", "x", 2
        ),
    )
    for from_s, to_s, kw in (
        (
            FunnelStage.HARD_QID_SEEN,
            FunnelStage.DIAGNOSED,
            {
                "diagnosed": DiagnosisRecord(
                    "plan11_stage1",
                    "synonym_or_entity_match_missing",
                    "rca_gs_026_superlative",
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
                    "H_RANK",
                    "AG_RANK",
                    ("gs_026",),
                    6,
                    "synonym_or_entity_match_missing",
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
                        "H_RANK_000",
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
            {"applied": AppliedRecord(2, "c", 0, ("H_RANK_000",))},
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
            StageTransition(from_s, to_s, 2, "t", "validation_gate"),
            **kw,
        )
    return s


def test_trial17_gs026_acceptance_gate_forbidden_signature_carries_lever_and_rca():
    """RED before Trial 17. GREEN after.

    Asserts the superlative-RCA variant of the same enrichment contract.
    """
    s = _gs026_state_at_evaluated(post_score=0.0)
    with mock_patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(2, "r", ValidationContext(2, "r", {})),
        )

    assert s2.current_stage == FunnelStage.TERMINATED
    sig = s2.terminal.forbidden_signature or ""
    assert "add_instruction" in sig, f"sig missing patch_type: {sig!r}"
    assert "target_unchanged" in sig, f"sig missing target_unchanged: {sig!r}"
    assert "synonym_or_entity_match_missing" in sig, (
        f"sig missing rca_kind: {sig!r}"
    )
    assert "lever-5" in sig, f"sig missing lever token: {sig!r}"
