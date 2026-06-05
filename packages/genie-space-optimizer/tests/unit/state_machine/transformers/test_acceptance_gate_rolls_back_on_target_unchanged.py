"""target_unchanged behaviour — flag-off pins legacy rollback, Trial 18
on pins ``KEPT_INSUFFICIENT`` lane. The dual coverage protects the
rollback escape hatch (``GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0``) while
also pinning the new Trial 18 cumulative-learning lane.
"""
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


def test_rolls_back_on_target_unchanged_when_flag_off(monkeypatch):
    """Pre-Trial-18 contract preserved as rollback escape hatch."""
    monkeypatch.setenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", "0")
    s = _state_at_evaluated(post_score=0.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"
    assert "target_unchanged" in s2.terminal.reason


def test_target_unchanged_enters_kept_insufficient_when_flag_on(monkeypatch):
    """Trial 18 contract: same input now lands in the KEPT_INSUFFICIENT
    lane (config kept live, typed signature emitted, NOT counted as
    accepted, NOT terminal)."""
    monkeypatch.delenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", raising=False)
    s = _state_at_evaluated(post_score=0.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.ACCEPTED
    assert s2.terminal is None
    assert s2.accepted is not None
    assert s2.accepted.decision == "kept_insufficient"
    assert ":insufficient:" in s2.accepted.insufficient_repair_signature
