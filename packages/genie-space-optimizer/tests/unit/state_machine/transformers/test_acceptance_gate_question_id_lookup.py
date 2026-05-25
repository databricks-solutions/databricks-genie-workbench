"""Trial 16 RC2b — ``acceptance_gate._assess_collateral`` must use the
canonical :func:`extract_question_id` helper for baseline + post-apply
row matching.

Why this test exists:
    Symmetric with RC2a. ``acceptance_gate.py:65-70`` builds two
    by-qid dicts via ``str(r.get('question_id') or '')`` for both the
    ``ctx.baseline_eval_rows`` and ``ctx.post_apply_eval_rows`` slices.
    Whenever production rows carry their qid under
    ``inputs/question_id`` (the common MLflow-flattened shape),
    ``pre_by_qid`` becomes a singleton dict keyed by ``""`` and every
    collateral check abstains as "not re-evaluated post-apply" —
    silently masking real regressions or, in the other branch, missing
    the target acceptance row entirely.

    Like RC2a, the fix reuses ``_qid_extraction.extract_question_id``
    instead of reinventing the lookup.

The parametrization mirrors RC2a's four canonical qid carriers, but
the target row here is a *collateral* qid (``gs_007``) whose baseline
score is 1.0 and post-apply score is 0.0 — i.e. a clear regression.
The gate must detect the regression and roll back. With the strict
``r.get('question_id')`` lookup, three of the four carriers map the
row to key ``""`` and the regression is silently dropped, so the gate
incorrectly returns target_fixed-only acceptance.
"""
from __future__ import annotations

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


_STATE_QID = "gs_009"  # the patched qid (target)
_COLLATERAL_QID = "gs_007"  # the regressed qid the gate must detect


def _state_at_evaluated(*, target_post_score: float = 1.0):
    """Build a QuestionStateInIteration at EVALUATED with
    target_post_score > target_pre_score so target_fixed=True; the
    only question is whether collateral detection works."""
    s = build_initial_state(
        qid=_STATE_QID,
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "SELECT BASELINE", "x", 1,
        ),
    )
    for from_s, to_s, kw in (
        (
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
            {
                "diagnosed": DiagnosisRecord(
                    "plan11_stage1", "k", "s", "f", "e", "high", "r",
                )
            },
        ),
        (
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
            {
                "clustered": ClusterMembershipRecord(
                    "H1", "AG", (_STATE_QID,), 6, "k",
                )
            },
        ),
        (
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
            {
                "proposals": (
                    ProposalAttempt(
                        0, "i", "p", FunnelStage.APPLIED, "applied",
                        "ok",
                    ),
                )
            },
        ),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (
            FunnelStage.APPLYABLE, FunnelStage.APPLIED,
            {"applied": AppliedRecord(1, "c", 0, ("i",))},
        ),
        (
            FunnelStage.APPLIED, FunnelStage.EVALUATED,
            {
                "evaluated": EvaluatedRecord(
                    pre_apply_score=0.0,
                    post_apply_score=float(target_post_score),
                    pre_apply_sql="SELECT 1",
                    post_apply_sql="SELECT 2",
                    eval_row_id_post="row_post_target",
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


def _row_with_score(
    *, qid_key: str, qid_value: str, score: float,
) -> dict:
    """Synthetic collateral row using one of the canonical qid carriers."""
    row: dict = {"feedback/result_correctness/value": float(score)}
    if qid_key == "top_level_question_id":
        row["question_id"] = qid_value
    elif qid_key == "slash_inputs_question_id":
        row["inputs/question_id"] = qid_value
    elif qid_key == "dot_inputs_question_id":
        row["inputs.question_id"] = qid_value
    elif qid_key == "nested_inputs_dict":
        row["inputs"] = {"question_id": qid_value}
    else:
        raise AssertionError(f"unknown qid_key {qid_key!r}")
    return row


# Trial 16 RC2b — Chunk 2 swapped acceptance_gate's strict
# ``r.get("question_id")`` lookup for the canonical ``extract_question_id``
# helper, so all four canonical qid carriers (top-level / slash /
# dot / nested inputs) now route through the same code path and the
# gate consistently detects collateral regressions.
@pytest.mark.parametrize(
    "qid_key",
    [
        "top_level_question_id",
        "slash_inputs_question_id",
        "dot_inputs_question_id",
        "nested_inputs_dict",
    ],
)
def test_acceptance_gate_detects_collateral_regression_via_canonical_helper(
    qid_key: str,
) -> None:
    """Across the four canonical qid carriers on baseline + post rows,
    the gate must roll back when a collateral qid regresses.

    Both baseline and post rows use the SAME qid_key so the test
    exercises a single matcher contract end-to-end. The collateral row
    drops from 1.0 to 0.0 — this regression must be detected and the
    state must roll back to TERMINATED with OPTIMIZER_TRIED_NO_GAIN.
    """
    state = _state_at_evaluated(target_post_score=1.0)
    baseline_collateral = _row_with_score(
        qid_key=qid_key, qid_value=_COLLATERAL_QID, score=1.0,
    )
    post_collateral = _row_with_score(
        qid_key=qid_key, qid_value=_COLLATERAL_QID, score=0.0,
    )
    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc2b",
        validation_context=ValidationContext(1, "trial16-rc2b", {}),
        baseline_eval_rows=(baseline_collateral,),
        post_apply_eval_rows=(post_collateral,),
    )

    result = acceptance_gate.transform(state, ctx)

    assert result.current_stage == FunnelStage.TERMINATED, (
        f"qid_key={qid_key!r}: gate should have detected the collateral "
        f"regression on {_COLLATERAL_QID!r} and rolled back, but reached "
        f"{result.current_stage} (accepted={result.accepted!r}). The "
        f"acceptance gate matcher misses qids that live under non-"
        f"top-level canonical carriers."
    )
    assert result.terminal is not None
    assert result.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"
    assert _COLLATERAL_QID in result.terminal.reason, (
        f"qid_key={qid_key!r}: terminal reason must name the regressed "
        f"qid; got reason={result.terminal.reason!r}."
    )
