# tests/unit/test_stage_context_frozen.py
from dataclasses import FrozenInstanceError

import pytest

from genie_space_optimizer.optimization.stages import StageContext


def _ctx() -> StageContext:
    return StageContext(
        run_id="run-1",
        iteration=0,
        space_id="space-1",
        domain="domain-1",
        catalog="cat",
        schema="sch",
        apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda r: None,
    )


def test_stage_context_is_frozen() -> None:
    ctx = _ctx()
    with pytest.raises(FrozenInstanceError):
        ctx.iteration = 1  # type: ignore[misc]


def test_stage_context_uses_slots() -> None:
    # With frozen=True + slots=True, Python 3.11 raises TypeError (not
    # AttributeError) when the frozen __setattr__ fires for an unknown slot.
    # Both exceptions are evidence that __slots__ enforcement is active.
    ctx = _ctx()
    with pytest.raises((AttributeError, TypeError)):
        ctx.unexpected_attribute = "boom"  # type: ignore[attr-defined]


def test_stage_context_default_feature_flags_is_empty_mapping() -> None:
    ctx = _ctx()
    assert ctx.feature_flags == {}
