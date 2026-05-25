"""Trial 17 Step 6 — gs_013 example_sql pivot workbench test.

Postmortem 230596834005670 shows gs_013 reached APPLIED with
``add_example_sql`` (lever-5b) in iter 2 and was rejected by
``acceptance_gate`` with ``target_unchanged``. The next iteration's
LLM should see a forbidden_signature like
``"lever-5:add_example_sql:target_unchanged:rca=..."`` so it can pivot
to a different lever (e.g. lever-1 metadata or lever-6 snippet).

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


def _gs013_state_at_evaluated(*, post_score: float):
    s = build_initial_state(
        qid="gs_013",
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
                    "measure_swap",
                    "rca_gs_013_measure_swap",
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
                    "H_MS",
                    "AG_MS",
                    ("gs_013",),
                    6,
                    "measure_swap",
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
                        "H_MS_000",
                        "add_example_sql",
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
            {"applied": AppliedRecord(2, "c", 0, ("H_MS_000",))},
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


def test_trial17_gs013_acceptance_gate_forbidden_signature_carries_example_sql_lever():
    """RED before Trial 17. GREEN after.

    Asserts the example-SQL (lever-5b) variant of the enrichment.
    """
    s = _gs013_state_at_evaluated(post_score=0.0)
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
    assert "add_example_sql" in sig, f"sig missing patch_type: {sig!r}"
    assert "target_unchanged" in sig, f"sig missing target_unchanged: {sig!r}"
    assert "measure_swap" in sig, f"sig missing rca_kind: {sig!r}"
    # lever-5 covers both prose (5a) and example_sql (5b).
    assert "lever-5" in sig, f"sig missing lever token: {sig!r}"
