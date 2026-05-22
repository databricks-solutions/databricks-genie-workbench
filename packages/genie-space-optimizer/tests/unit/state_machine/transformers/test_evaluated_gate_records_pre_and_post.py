"""Evaluated gate carries pre-apply SQL from baseline_sql for trajectory comparison."""
from unittest.mock import patch

from tests.unit.state_machine.transformers.test_evaluated_gate_runs_post_apply_eval import (
    _state_at_applied,
)
from genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate import (
    evaluated_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def test_pre_apply_sql_from_baseline():
    s = _state_at_applied()
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate._run_post_apply_eval",
        return_value=(1.0, "SELECT POST", "row_post_1"),
    ):
        s2 = evaluated_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.evaluated.pre_apply_sql == s.seen.baseline_sql
    assert s2.evaluated.post_apply_sql == "SELECT POST"
