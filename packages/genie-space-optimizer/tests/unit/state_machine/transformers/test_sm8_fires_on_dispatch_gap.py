"""SM8 fires when eval has hard rows but the dispatch input adapter produced no states."""
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm8_dispatch_input_honesty,
)


def test_sm8_fires_when_known_hard_row_missing_from_states():
    eval_hard_qids = ("gs_009", "gs_024")
    state_qids = ("gs_009",)  # gs_024 was dropped — SM8 violation
    violations = check_sm8_dispatch_input_honesty(
        eval_hard_qids=eval_hard_qids, state_qids=state_qids,
    )
    assert len(violations) == 1
    assert "gs_024" in violations[0]["message"]


def test_sm8_silent_when_dispatch_complete():
    assert check_sm8_dispatch_input_honesty(
        eval_hard_qids=("gs_009",), state_qids=("gs_009",),
    ) == []
