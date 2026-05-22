"""target not fixed (post_score == pre_score) → roll back."""
from unittest.mock import patch

from tests.unit.state_machine.transformers.test_acceptance_gate_target_fixed_no_regression import (
    _state_at_evaluated,
)
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def test_rolls_back_on_target_unchanged():
    s = _state_at_evaluated(post_score=0.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"
    assert "target_unchanged" in s2.terminal.reason
