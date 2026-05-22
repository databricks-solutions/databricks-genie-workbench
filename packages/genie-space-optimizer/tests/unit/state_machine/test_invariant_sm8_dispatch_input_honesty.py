"""SM8: every hard eval row produces a QuestionStateInIteration."""
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm8_dispatch_input_honesty,
)


def test_sm8_clean_when_every_hard_qid_has_state():
    eval_hard_qids = ("gs_009", "gs_024")
    state_qids = ("gs_009", "gs_024")
    assert check_sm8_dispatch_input_honesty(
        eval_hard_qids=eval_hard_qids, state_qids=state_qids,
    ) == []


def test_sm8_violation_when_hard_qid_has_no_state():
    eval_hard_qids = ("gs_009", "gs_024")
    state_qids = ("gs_009",)
    violations = check_sm8_dispatch_input_honesty(
        eval_hard_qids=eval_hard_qids, state_qids=state_qids,
    )
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM8"
    assert "gs_024" in violations[0]["message"]
