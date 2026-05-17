"""Phase 1 — Acceptance Unification Task 5.

After Task 5, both ``acceptance_decision`` dicts in ``harness.py``
are constructed via ``_acceptance_decision_dict(_acceptance_outcome)``.
This source-inspection guard asserts there are no remaining inline
literals (``"acceptance_decision": {``).
"""

from __future__ import annotations

from pathlib import Path


HARNESS_PATH = (
    Path(__file__).parent.parent.parent
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)


def test_no_inline_acceptance_decision_dict_literal():
    """Both pass-path and rollback-path `acceptance_decision` dicts
    must be produced by the serialiser, not inline literals. The
    serialiser unconditionally carries `_canonical`, closing the
    asymmetry between the two fork paths."""
    src = HARNESS_PATH.read_text()
    matches = [
        i for i, line in enumerate(src.splitlines(), start=1)
        if '"acceptance_decision": {' in line
    ]
    assert not matches, (
        f"Bug 5 Surface 3 regression — found inline "
        f"`\"acceptance_decision\": {{` literal(s) at harness.py:{matches}. "
        f"Both fork paths must use `_acceptance_decision_dict(outcome)` "
        f"so the rollback path carries `_canonical` for Phase H "
        f"short-circuit."
    )


def test_acceptance_decision_dict_serialiser_called_twice():
    """Both fork paths use the serialiser — assert the call appears
    at least twice in harness.py (once per path)."""
    src = HARNESS_PATH.read_text()
    matches = [
        i for i, line in enumerate(src.splitlines(), start=1)
        if "_acceptance_decision_dict(" in line
        and "import" not in line
    ]
    # 2 invocations (one per fork) — additional callers are fine.
    assert len(matches) >= 2, (
        f"Expected _acceptance_decision_dict() invocations on both "
        f"fork paths; found {len(matches)} at harness.py:{matches}."
    )
