"""Workbench V1.5 — registry shape contract for the three llm_modes.

stage1-only must remain stopping at DIAGNOSED (we never want the
workbench to silently start firing evaluated_gate in the cheapest
"is Stage 1 wired" smoke mode).

live-databricks and sm-tape must include evaluated_gate at APPLIED
and acceptance_gate at EVALUATED so a single workbench iteration can
verify the Trial 15 plumbing claim.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from local_lever_workbench.local_runner import (
    LLM_MODE_LIVE,
    LLM_MODE_STAGE1_ONLY,
    LLM_MODE_TAPE,
    _build_registry,
)


@pytest.mark.workbench
def test_stage1_only_registry_stops_at_diagnosed() -> None:
    registry = _build_registry(LLM_MODE_STAGE1_ONLY)
    assert set(registry.keys()) == {FunnelStage.HARD_QID_SEEN}
    assert FunnelStage.APPLIED not in registry


@pytest.mark.workbench
@pytest.mark.parametrize(
    "llm_mode",
    [LLM_MODE_TAPE, LLM_MODE_LIVE],
    ids=["sm-tape", "live-databricks"],
)
def test_full_pipeline_registry_includes_evaluated_and_acceptance_gates(
    llm_mode: str,
) -> None:
    registry = _build_registry(llm_mode)
    assert FunnelStage.APPLIED in registry
    assert FunnelStage.EVALUATED in registry
    # transformers are tuples of callables; name attribute distinguishes
    applied_names = [getattr(t, "name", "") for t in registry[FunnelStage.APPLIED]]
    evaluated_names = [getattr(t, "name", "") for t in registry[FunnelStage.EVALUATED]]
    assert "evaluated_gate" in applied_names
    assert "acceptance_gate" in evaluated_names
