"""Applier gate transition emits GSO_PATCH_OUTCOME_V1 witness for the applied attempt."""
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.transformers.applier_gate import (
    applier_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)
# Reuse fixture builder from sibling test
from tests.unit.state_machine.transformers.test_applier_gate_wraps_side_effect import (
    _state_at_applyable,
)


def test_witness_marker_emitted_on_apply(capsys):
    s = _state_at_applyable()
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.applier_gate._apply_via_genie_api",
        return_value=("apply_call_abc", True, ""),
    ):
        applier_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    out = capsys.readouterr().out
    assert "GSO_PATCH_OUTCOME_V1" in out
