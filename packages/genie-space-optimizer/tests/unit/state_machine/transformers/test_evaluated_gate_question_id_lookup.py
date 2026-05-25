"""Trial 16 RC2a — ``evaluated_gate._run_post_apply_eval`` must use the
canonical :func:`extract_question_id` helper for post-apply row matching.

Why this test exists:
    Production postmortems 575892594490176 and 319530250904653 both
    timed out after the post-apply evaluation succeeded (rows came
    back) but every row was rejected with
    ``post_apply_eval_failed:no_post_apply_row_for_qid:<qid>``. The
    root cause: ``evaluated_gate.py:75-78`` looked up rows with
    ``str(r.get("question_id") or "") == state.qid``. MLflow-flattened
    eval rows commonly carry the canonical qid only under
    ``inputs/question_id`` (slash form), ``inputs.question_id`` (dot
    form), or nested ``inputs: {"question_id": ...}`` — none of which
    are matched by the direct ``r.get("question_id")`` lookup.

    The canonical helper ``_qid_extraction.extract_question_id`` is
    already used in 8+ production sites (``optimizer.py``, ``labeling.py``,
    ``ground_truth_corrections.py``, ``eval_row_access.py``,
    ``dispatch_input.py``, ``harness.py``). The fix is to bring this
    outlier gate in line — no new validator, no new logic.

This test exercises the four row-shape variants the canonical helper
handles. Until Chunk 2 lands the helper-reuse, the dot/slash/nested
variants will fail with ``no_post_apply_row_for_qid`` and the test is
``xfail``. After the fix, all four variants must produce the same
EVALUATED record with the row's score.
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
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate import (
    evaluated_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


# The qid the state machine is patching. Distilled from postmortem
# 575892594490176 (state.qid recorded under ``gs_026`` after Trial 13i
# canonicalization, eval row returns kept the inputs/question_id slash
# key).
_STATE_QID = "gs_026"


def _state_at_applied():
    """Build a QuestionStateInIteration at FunnelStage.APPLIED for
    ``_STATE_QID``, ready for ``evaluated_gate.transform``."""
    s = build_initial_state(
        qid=_STATE_QID,
        iteration=1,
        seen=HardQidSeenRecord(
            "row_baseline", "row_is_hard_failure", 0.0, "SELECT BASELINE",
            "x", 1,
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
            {"applied": AppliedRecord(1, "call_abc", 0, ("i",))},
        ),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


def _post_apply_row_with_score_one(*, qid_key: str, qid_value: str) -> dict:
    """Build a synthetic post-apply eval row that places the qid under
    one of the canonical key paths the helper handles.

    All other fields are stable so the parametrized cases only differ
    in *where* the qid lives, isolating the matcher behavior.
    """
    row: dict = {
        "feedback/result_correctness/value": 1.0,
        "generated_sql": "SELECT POSTAPPLY",
        "eval_row_id": "row_post_evaluated",
    }
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


# Trial 16 RC2a — all four canonical qid carriers must produce the
# same EVALUATED record. Up to Trial 15 the gate looked up rows with
# ``r.get("question_id")``, so dot/slash/nested variants tripped
# ``no_post_apply_row_for_qid``; Chunk 2 swapped in
# ``extract_question_id`` (canonical helper used by 8+ production
# sites), and now all four carriers route through the same code path.
@pytest.mark.parametrize(
    "qid_key",
    [
        "top_level_question_id",
        "slash_inputs_question_id",
        "dot_inputs_question_id",
        "nested_inputs_dict",
    ],
)
def test_evaluated_gate_matches_post_apply_row_via_canonical_helper(
    qid_key: str,
) -> None:
    """Across all four canonical qid carriers, the gate must reach
    EVALUATED with the row's score.

    The post-apply eval stub is a workbench escape-hatch
    (``ctx.extras["post_apply_eval"]``) that bypasses the row matching
    inside ``_run_post_apply_eval``, so we cannot use it — we must
    drive the production eval path. We monkeypatch
    ``stages.evaluation.evaluate_post_patch`` to return our synthetic
    row, so the gate exercises the exact ``r.get('question_id')`` line
    we want to replace with ``extract_question_id``.
    """
    state = _state_at_applied()
    row = _post_apply_row_with_score_one(
        qid_key=qid_key, qid_value=_STATE_QID,
    )

    class _FakeResult:
        eval_rows = (row,)

    ctx = TransformerContext(
        iteration=1,
        run_id="trial16-rc2a",
        validation_context=ValidationContext(1, "trial16-rc2a", {}),
        # ``extras`` deliberately omits ``post_apply_eval`` so the
        # production path through ``evaluate_post_patch`` runs.
        extras={},
        eval_qids=(_STATE_QID,),
        eval_kwargs={"benchmarks": []},
    )

    with patch(
        "genie_space_optimizer.optimization.stages.evaluation."
        "evaluate_post_patch",
        return_value=_FakeResult(),
    ):
        result = evaluated_gate.transform(state, ctx)

    assert result.current_stage == FunnelStage.EVALUATED, (
        f"qid_key={qid_key!r}: expected EVALUATED but got "
        f"{result.current_stage}; terminal={result.terminal!r}. The gate "
        f"failed to locate the post-apply row for state.qid "
        f"{_STATE_QID!r} because the matcher only inspects "
        f"r.get('question_id')."
    )
    assert result.evaluated is not None
    assert result.evaluated.post_apply_score == 1.0
    assert result.evaluated.post_apply_sql == "SELECT POSTAPPLY"
