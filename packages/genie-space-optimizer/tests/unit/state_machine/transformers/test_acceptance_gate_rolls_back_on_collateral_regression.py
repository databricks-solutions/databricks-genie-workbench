"""collateral regressions detected → roll back to TERMINATED with OPTIMIZER_TRIED_NO_GAIN."""
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


def test_rolls_back_on_collateral_regression():
    s = _state_at_evaluated(post_score=1.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=("gs_003",),
    ):
        s2 = acceptance_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"
    assert "gs_003" in s2.terminal.reason
