"""Phase 2 (2026-05-16) — pin that the forbidden-set router is
out of observe-only mode.

Audit anchors (current observe-only literals):
* ``harness.py``: ``prior_forbidden_set=frozenset()``
* ``harness.py``: ``forbidden_set_size_after=0,``

Phase 2 Task 8 replaces both with a loop-scoped ``_forbidden_set``
variable so:
1. The pre-iteration pivot decision (``decide_pre_iteration_pivot_action``)
   sees real prior-iteration signatures.
2. The marker emits the actual after-admission size, not 0.

Source-inspection style.
"""
from __future__ import annotations

import inspect
import re

from genie_space_optimizer.optimization import harness


def test_no_hardcoded_empty_prior_forbidden_set():
    """The router call site must NOT pass
    ``prior_forbidden_set=frozenset()`` as a hard-coded empty literal."""
    source = inspect.getsource(harness)
    matches = re.findall(
        r"prior_forbidden_set\s*=\s*frozenset\(\s*\)",
        source,
    )
    assert not matches, (
        "Found ``prior_forbidden_set=frozenset()`` literal in "
        "harness.py — the router is still in observe-only mode. "
        "Replace with the loop-scoped ``_forbidden_set`` variable."
    )


def test_no_hardcoded_zero_forbidden_set_size_after():
    """The marker call must NOT pass
    ``forbidden_set_size_after=0`` as a hard-coded literal."""
    source = inspect.getsource(harness)
    matches = re.findall(
        r"forbidden_set_size_after\s*=\s*0\s*,",
        source,
    )
    assert not matches, (
        "Found ``forbidden_set_size_after=0,`` literal in harness.py "
        "— the marker still reports observe-only size. Replace with "
        "``len(_forbidden_set)``."
    )


def test_forbidden_set_initialised_in_lever_loop_scope():
    """The loop-scoped ``_forbidden_set`` variable must be initialised
    inside ``_run_lever_loop`` BEFORE the iteration loop begins."""
    source = inspect.getsource(harness)
    matches = re.findall(
        r"_forbidden_set\s*(?::\s*[^=]+)?=\s*set\(\s*\)",
        source,
    )
    assert matches, (
        "Expected ``_forbidden_set = set()`` initialisation in "
        "harness.py — the loop-scoped forbidden set is missing."
    )


def test_forbidden_set_threaded_into_router_call():
    """The router call must reference ``_forbidden_set`` (or a frozen
    view of it) for the ``prior_forbidden_set=`` kwarg."""
    source = inspect.getsource(harness)
    matches = re.findall(
        r"prior_forbidden_set\s*=\s*(?:frozenset\(_forbidden_set\)|_forbidden_set)",
        source,
    )
    assert matches, (
        "Expected ``prior_forbidden_set=frozenset(_forbidden_set)`` "
        "or ``prior_forbidden_set=_forbidden_set`` in the router "
        "call site."
    )


def test_forbidden_set_size_uses_len_expression():
    """The marker call must report
    ``forbidden_set_size_after=len(_forbidden_set)``."""
    source = inspect.getsource(harness)
    matches = re.findall(
        r"forbidden_set_size_after\s*=\s*len\(_forbidden_set\)",
        source,
    )
    assert matches, (
        "Expected ``forbidden_set_size_after=len(_forbidden_set)`` in "
        "the marker call site — the size still reports a hard-coded "
        "value."
    )


def test_router_grows_forbidden_set_when_action_admits():
    """When ``_router_action.add_to_forbidden_set`` is truthy, the
    harness must add the iteration's signature to the loop-scoped
    set BEFORE the next iteration."""
    source = inspect.getsource(harness)
    matches = re.search(
        r"if\s+_router_action\.add_to_forbidden_set[^:]*:\s*\n\s*"
        r"_forbidden_set\.add\(_tsig_for_router\)",
        source,
    )
    assert matches, (
        "Expected admission idiom ``if _router_action.add_to_"
        "forbidden_set: _forbidden_set.add(_tsig_for_router)`` "
        "in the router post-decision block."
    )
