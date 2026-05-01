"""Pin that the strategist prompt assembly references the cap budget."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.optimizer import (
    _format_strategist_budget_preamble,
)


def test_call_llm_for_adaptive_strategy_passes_max_ag_patches() -> None:
    sig = inspect.signature(optimizer._call_llm_for_adaptive_strategy)
    assert "max_ag_patches" in sig.parameters, (
        "_call_llm_for_adaptive_strategy must accept max_ag_patches so the "
        "strategist sees the cap budget when bundling clusters"
    )


def test_budget_preamble_renders_budget_and_cluster_count() -> None:
    out = _format_strategist_budget_preamble(budget=3, n_clusters=5)
    assert "PATCH BUDGET" in out
    assert "3 applied patches" in out
    assert "Active hard clusters: 5" in out


def test_strategist_function_prepends_budget_preamble() -> None:
    """The function source must call the helper, not duplicate the string."""
    src = inspect.getsource(optimizer._call_llm_for_adaptive_strategy)
    assert "_format_strategist_budget_preamble" in src, (
        "_call_llm_for_adaptive_strategy must invoke the budget-preamble "
        "formatter so the cap budget reaches the prompt"
    )
    assert "budget_text" in src and "+ prompt" in src, (
        "the budget preamble must be prepended to the formatted prompt"
    )
