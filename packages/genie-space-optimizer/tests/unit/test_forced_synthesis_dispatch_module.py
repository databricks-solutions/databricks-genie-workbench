"""Unit tests for the extracted L5 forced-synthesis dispatch callable.

This module pins the contract the replay driver depends on:

1. ``ForcedSynthesisDispatchResult`` is a frozen dataclass with the three
   fields the replay driver reads.
2. ``dispatch_forced_structural_synthesis`` is importable from the new
   module and accepts the parameter list pinned in Task 0.

Behavior parity with ``harness.py:22720-22929`` is verified by the
parity-pin test in Task 4, not here.
"""
from __future__ import annotations


def test_result_dataclass_shape() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        ForcedSynthesisDispatchResult,
    )

    r = ForcedSynthesisDispatchResult(
        attempted_dispatches=(),
        appended_proposals=(),
        emitted_decision_records=(),
    )
    assert r.attempted_dispatches == ()
    assert r.appended_proposals == ()
    assert r.emitted_decision_records == ()


def test_dispatch_function_callable_with_empty_inputs() -> None:
    """When no L5 drops are present, dispatch returns an empty result."""
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        ForcedSynthesisDispatchResult,
        dispatch_forced_structural_synthesis,
    )

    def _synthesize_stub(*args, **kwargs):  # noqa: ARG001
        raise AssertionError(
            "synthesize must not be called when no L5 drops are present"
        )

    result = dispatch_forced_structural_synthesis(
        run_id="test_run",
        iteration=1,
        ag={"id": "AG_TEST", "affected_questions": ["gs_001"]},
        l5_ag_drops=[],
        iter_source_clusters_by_id={},
        iter_rca_id_by_cluster={},
        metadata_snapshot={},
        benchmarks=[],
        catalog="",
        schema="",
        w=None,
        spark=None,
        lever_keys=(),
        reflection_buffer=(),
        current_iter_inputs={},
        synthesize=_synthesize_stub,
    )
    assert isinstance(result, ForcedSynthesisDispatchResult)
    assert result.attempted_dispatches == ()
    assert result.appended_proposals == ()
    assert result.emitted_decision_records == ()
