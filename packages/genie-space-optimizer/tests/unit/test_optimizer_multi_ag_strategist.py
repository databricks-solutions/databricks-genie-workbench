"""Pin multi-AG strategist output."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import optimizer


def test_strategist_does_not_slice_to_first_ag() -> None:
    src = inspect.getsource(optimizer._call_llm_for_adaptive_strategy)
    assert "action_groups[:1]" not in src, "remove [:1] slice"
    assert "MAX_ACTION_GROUPS_PER_STRATEGY" in src, (
        "strategist must bound output by config knob, not literal 1"
    )
