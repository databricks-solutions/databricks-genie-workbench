"""Phase 6.4 — ``dispatch_forced_structural_synthesis`` must be
callable even when ``_l5_ag_drops`` is empty, so the safety-net
branch of the dispatcher can run on 'no-drops + SQL-shape cluster'
cases.

This test exercises the helper that wraps the dispatch with the
flag-gated unconditional-call semantics
(``_maybe_dispatch_forced_structural_synthesis``).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_dispatch_is_called_when_drops_empty(monkeypatch):
    """Phase 6.4 — when drops are empty and the flag is on, the
    underlying dispatcher must be invoked so its own
    ``_should_invoke_safety_net`` predicate can run."""
    from genie_space_optimizer.optimization.harness import (
        _maybe_dispatch_forced_structural_synthesis,
    )
    monkeypatch.setenv("GSO_FORCED_SYNTHESIS_UNCONDITIONAL_ENABLED", "1")
    fake_dispatcher = MagicMock(
        return_value=MagicMock(
            appended_proposals=[], emitted_decision_records=[],
        ),
    )
    with patch(
        "genie_space_optimizer.optimization."
        "forced_synthesis_dispatch.dispatch_forced_structural_synthesis",
        fake_dispatcher,
    ):
        _maybe_dispatch_forced_structural_synthesis(
            ag={"ag_id": "AG_001", "source_cluster_ids": ["gs_007"]},
            ag_id="AG_001",
            iteration_counter=2,
            l5_ag_drops=[],  # empty — the regression case
            reflection_buffer=[],
            clusters_by_id={
                "gs_007": {
                    "cluster_id": "gs_007",
                    "root_cause": "plural_top_n_collapse",
                    "asi_failure_type": "other",
                },
            },
            workspace_client=MagicMock(),
            benchmarks=[],
            run_id="r-001",
        )
        assert fake_dispatcher.called


def test_dispatch_is_called_when_drops_non_empty(monkeypatch):
    """Behavior on non-empty drops is unchanged from pre-Phase-6."""
    from genie_space_optimizer.optimization.harness import (
        _maybe_dispatch_forced_structural_synthesis,
    )
    monkeypatch.setenv("GSO_FORCED_SYNTHESIS_UNCONDITIONAL_ENABLED", "1")
    fake_dispatcher = MagicMock(
        return_value=MagicMock(
            appended_proposals=[], emitted_decision_records=[],
        ),
    )
    with patch(
        "genie_space_optimizer.optimization."
        "forced_synthesis_dispatch.dispatch_forced_structural_synthesis",
        fake_dispatcher,
    ):
        _maybe_dispatch_forced_structural_synthesis(
            ag={"ag_id": "AG_001", "source_cluster_ids": ["gs_007"]},
            ag_id="AG_001",
            iteration_counter=2,
            l5_ag_drops=[{
                "ag_id": "AG_001",
                "root_causes": ("plural_top_n_collapse",),
            }],
            reflection_buffer=[],
            clusters_by_id={
                "gs_007": {
                    "cluster_id": "gs_007",
                    "root_cause": "plural_top_n_collapse",
                },
            },
            workspace_client=MagicMock(),
            benchmarks=[],
            run_id="r-001",
        )
        assert fake_dispatcher.called


def test_dispatch_is_not_called_when_drops_empty_and_flag_off(monkeypatch):
    """Flag-off preserves pre-Phase-6 behavior (dispatch skipped when
    no drops)."""
    from genie_space_optimizer.optimization.harness import (
        _maybe_dispatch_forced_structural_synthesis,
    )
    monkeypatch.setenv("GSO_FORCED_SYNTHESIS_UNCONDITIONAL_ENABLED", "0")
    fake_dispatcher = MagicMock(
        return_value=MagicMock(
            appended_proposals=[], emitted_decision_records=[],
        ),
    )
    with patch(
        "genie_space_optimizer.optimization."
        "forced_synthesis_dispatch.dispatch_forced_structural_synthesis",
        fake_dispatcher,
    ):
        _maybe_dispatch_forced_structural_synthesis(
            ag={"ag_id": "AG_001", "source_cluster_ids": ["gs_007"]},
            ag_id="AG_001",
            iteration_counter=2,
            l5_ag_drops=[],
            reflection_buffer=[],
            clusters_by_id={
                "gs_007": {
                    "cluster_id": "gs_007",
                    "root_cause": "plural_top_n_collapse",
                },
            },
            workspace_client=MagicMock(),
            benchmarks=[],
            run_id="r-001",
        )
        assert not fake_dispatcher.called
