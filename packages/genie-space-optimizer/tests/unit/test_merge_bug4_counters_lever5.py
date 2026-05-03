"""Cycle 8 Bug 1 Phase 3b Task A — fold lever5_text_only_blocked into eval_result.

The Lever 5 structural gate at ``optimizer.py:13961-13971`` increments
``_BUG4_COUNTERS["lever5_text_only_blocked"]`` every time it fires.
``_merge_bug4_counters`` (``harness.py:770``) folds module-level Bug-4
counters into ``eval_result`` so ``write_iteration`` and the iteration
banner can see them. Today only ``secondary_mining_blocked`` and the
firewall counter are surfaced; ``lever5_text_only_blocked`` is invisible.

These tests pin that the counter is folded, reset between calls, and
summed when the key already exists in eval_result.

Plan: ``docs/2026-05-04-cycle8-bug1-phase3b-lever5-structural-gate-rerouting-plan.md``
Task A.
"""
from __future__ import annotations


def test_merge_bug4_counters_folds_lever5_text_only_blocked() -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.harness import _merge_bug4_counters

    optimizer.reset_bug4_counters()
    optimizer._BUG4_COUNTERS["lever5_text_only_blocked"] = 3

    out = _merge_bug4_counters({})

    assert out["lever5_text_only_blocked"] == 3


def test_merge_bug4_counters_resets_lever5_counter_between_calls() -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.harness import _merge_bug4_counters

    optimizer.reset_bug4_counters()
    optimizer._BUG4_COUNTERS["lever5_text_only_blocked"] = 5

    first = _merge_bug4_counters({})
    second = _merge_bug4_counters({})

    assert first["lever5_text_only_blocked"] == 5
    # The first call resets the module-level counter, so a second
    # merge starting from a clean eval_result sees zero.
    assert second["lever5_text_only_blocked"] == 0


def test_merge_bug4_counters_sums_lever5_counter_when_key_already_present() -> None:
    """When the eval_result already carries a ``lever5_text_only_blocked``
    value (e.g. from a prior slice/p0 scope that surfaced it), the merge
    sums rather than overwrites — same shape as
    ``secondary_mining_blocked`` at ``harness.py:797-800``."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.harness import _merge_bug4_counters

    optimizer.reset_bug4_counters()
    optimizer._BUG4_COUNTERS["lever5_text_only_blocked"] = 2

    out = _merge_bug4_counters({"lever5_text_only_blocked": 4})

    assert out["lever5_text_only_blocked"] == 6
