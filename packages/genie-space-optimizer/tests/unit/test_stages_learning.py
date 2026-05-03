from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.learning import (
    LearningInput,
    LearningUpdate,
)


def _stub_ctx() -> StageContext:
    return StageContext(
        run_id="r1",
        iteration=4,
        space_id="s1",
        domain="airline",
        catalog="main",
        schema="gso",
        apply_mode="real",
        journey_emit=MagicMock(),
        decision_emit=MagicMock(),
        mlflow_anchor_run_id=None,
        feature_flags={},
    )


def test_learning_input_required_fields() -> None:
    inp = LearningInput(
        prior_reflection_buffer=(),
        prior_do_not_retry=set(),
        prior_rolled_back_content_fingerprints=set(),
        ag_outcomes_by_id={"AG_001": {
            "outcome": "rolled_back",
            "content_fingerprint": "abc",
            "target_qids": ("q1",),
        }},
        applied_signature="sig_x",
        accuracy_delta=0.0,
        current_hard_failure_qids=("q3",),
    )
    assert inp.ag_outcomes_by_id["AG_001"]["content_fingerprint"] == "abc"


def test_learning_update_required_fields() -> None:
    upd = LearningUpdate(
        new_reflection_buffer=(),
        new_do_not_retry={"sig_doomed"},
        new_rolled_back_content_fingerprints={"abc"},
        terminal_decision={"status": "patchable_in_progress",
                           "should_continue": True, "reason": "still_patchable"},
        retired_ags=(),
        ag_retired_records=(),
    )
    assert upd.terminal_decision["should_continue"] is True


def test_update_appends_to_reflection_buffer() -> None:
    """Each iteration appends one entry to reflection_buffer."""
    from genie_space_optimizer.optimization.stages import learning as lrn
    ctx = _stub_ctx()

    inp = lrn.LearningInput(
        prior_reflection_buffer=({"iter": 1, "accepted": False},),
        prior_do_not_retry=set(),
        prior_rolled_back_content_fingerprints=set(),
        ag_outcomes_by_id={"AG_001": {
            "outcome": "accepted", "content_fingerprint": "abc",
            "target_qids": ("q1",),
        }},
        applied_signature="sig_x",
        accuracy_delta=0.05,
        current_hard_failure_qids=("q3",),
    )
    out = lrn.update(ctx, inp)
    assert len(out.new_reflection_buffer) == 2
    # Newest entry has expected fields.
    last = out.new_reflection_buffer[-1]
    assert last["iter"] == 4
    assert last["accepted"] is True
    assert last["accuracy_delta"] == 0.05


def test_update_accumulates_rolled_back_content_fingerprints() -> None:
    """PR-E groundwork: rolled-back fingerprints accumulate across iterations."""
    from genie_space_optimizer.optimization.stages import learning as lrn
    ctx = _stub_ctx()

    inp = lrn.LearningInput(
        prior_reflection_buffer=(),
        prior_do_not_retry=set(),
        prior_rolled_back_content_fingerprints={"old_fp"},
        ag_outcomes_by_id={"AG_001": {
            "outcome": "rolled_back", "content_fingerprint": "new_fp",
            "target_qids": ("q1",),
        }},
        applied_signature="sig_x",
        accuracy_delta=0.0,
        current_hard_failure_qids=("q1",),
    )
    out = lrn.update(ctx, inp)
    assert {"old_fp", "new_fp"}.issubset(out.new_rolled_back_content_fingerprints)


def test_update_emits_ag_retired_record_when_target_no_longer_hard() -> None:
    """PR-B2 regression: when the plateau resolver retires AGs, one
    AG_RETIRED DecisionRecord is emitted per retired AG."""
    from genie_space_optimizer.optimization.stages import learning as lrn
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType, DecisionOutcome, ReasonCode,
    )
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = lrn.LearningInput(
        prior_reflection_buffer=(),
        prior_do_not_retry=set(),
        prior_rolled_back_content_fingerprints=set(),
        ag_outcomes_by_id={"AG_decomposed_h003": {
            "outcome": "rolled_back",
            "content_fingerprint": "abc",
            "target_qids": ("q016",),
        }},
        applied_signature="sig_x",
        accuracy_delta=0.0,
        # q016 is NOT in current_hard_failure_qids — its target is no
        # longer hard, so the plateau resolver retires the AG.
        current_hard_failure_qids=(),
        pending_buffered_ags=(
            {"id": "AG_decomposed_h003", "_stable_signature":
             ("AG_decomposed_h003", ("q016",)),
             "affected_questions": ["q016"]},
        ),
    )

    out = lrn.update(ctx, inp)

    assert len(out.retired_ags) == 1
    assert out.retired_ags[0][0] == "AG_decomposed_h003"
    ag_retired = [
        r for r in captured if r.decision_type == DecisionType.AG_RETIRED
    ]
    assert len(ag_retired) == 1
    rec = ag_retired[0]
    assert rec.outcome == DecisionOutcome.RETIRED
    assert rec.reason_code == ReasonCode.AG_TARGET_NO_LONGER_HARD
    assert rec.ag_id == "AG_decomposed_h003"


def test_update_resolves_terminal_decision_dict_shape() -> None:
    """terminal_decision exposes status/should_continue/reason."""
    from genie_space_optimizer.optimization.stages import learning as lrn
    ctx = _stub_ctx()

    inp = lrn.LearningInput(
        prior_reflection_buffer=(),
        prior_do_not_retry=set(),
        prior_rolled_back_content_fingerprints=set(),
        ag_outcomes_by_id={},
        applied_signature="sig_x",
        accuracy_delta=0.0,
        # Active hard qid that is not yet retired.
        current_hard_failure_qids=("q1",),
    )
    out = lrn.update(ctx, inp)
    assert "status" in out.terminal_decision
    assert "should_continue" in out.terminal_decision
    assert "reason" in out.terminal_decision


def test_update_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.stages import learning as lrn
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = lrn.LearningInput(
        prior_reflection_buffer=(),
        prior_do_not_retry=set(),
        prior_rolled_back_content_fingerprints=set(),
        ag_outcomes_by_id={},
        applied_signature="",
        accuracy_delta=0.0,
        current_hard_failure_qids=(),
    )
    out = lrn.update(ctx, inp)
    # Even empty AG outcomes appends one reflection-buffer entry.
    assert len(out.new_reflection_buffer) == 1
    # No retired AGs, no records.
    assert out.retired_ags == ()
    assert captured == []
