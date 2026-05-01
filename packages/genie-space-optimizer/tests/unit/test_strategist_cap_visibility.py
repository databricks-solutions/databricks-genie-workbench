"""Pin that the strategist prompt assembly references the cap budget."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import optimizer


def test_call_llm_for_adaptive_strategy_passes_max_ag_patches() -> None:
    sig = inspect.signature(optimizer._call_llm_for_adaptive_strategy)
    assert "max_ag_patches" in sig.parameters, (
        "_call_llm_for_adaptive_strategy must accept max_ag_patches so the "
        "strategist sees the cap budget when bundling clusters"
    )


def test_strategist_prompt_mentions_cap_budget() -> None:
    src = inspect.getsource(optimizer._call_llm_for_adaptive_strategy)
    assert "MAX_AG_PATCHES" in src or "patch budget" in src.lower(), (
        "strategist prompt must surface the cap budget"
    )
