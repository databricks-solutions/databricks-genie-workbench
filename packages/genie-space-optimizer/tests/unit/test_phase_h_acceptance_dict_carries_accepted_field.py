"""F5 contract bug from the 2314bb2c trial postmortem.

Originally pinned that every ``"acceptance_decision": {`` dict literal
in ``harness.py`` declared an ``"accepted"`` key (the rejection-path
literal carried it, the acceptance-path literal omitted it — that
asymmetry caused false-positive ``GSO_PHASE_H_ACCEPTANCE_DRIFT_V1``
markers on every accepted iteration).

Phase 1 Task 5 (2026-05-16) replaces BOTH inline literals with calls
to ``_acceptance_decision_dict(_acceptance_outcome)``. The serialiser
unconditionally emits ``accepted`` and ``_canonical`` so the F5 bug
is now structurally impossible. This test is rewritten to assert the
post-Phase-1 structure: every reachable ``acceptance_decision``
construction goes through the serialiser.
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


def test_acceptance_decision_construction_routes_through_serialiser():
    """Both fork paths in harness.py must build ``acceptance_decision``
    via ``_acceptance_decision_dict(outcome)`` instead of inline dict
    literals. The serialiser unconditionally carries ``accepted`` and
    ``_canonical``, so the F5 asymmetry is structurally closed."""
    src = HARNESS_PATH.read_text()
    lines = src.splitlines()

    # No inline `"acceptance_decision": {` literals after Phase 1.
    inline_literals = [
        i for i, line in enumerate(lines, start=1)
        if '"acceptance_decision": {' in line
    ]
    assert not inline_literals, (
        f"F5 regression — inline `\"acceptance_decision\": {{` "
        f"literal(s) detected at harness.py:{inline_literals}. The "
        f"serialiser is the canonical path; inline literals can omit "
        f"either the `accepted` field or the `_canonical` field, "
        f"reopening the F5 asymmetry."
    )

    # Both fork paths use the serialiser. We expect >=2 invocations
    # (one per path) — additional callers are fine.
    serialiser_calls = [
        i for i, line in enumerate(lines, start=1)
        if "_acceptance_decision_dict(" in line
        and "import" not in line
        and "def " not in line
    ]
    assert len(serialiser_calls) >= 2, (
        f"Phase 1 wiring incomplete — expected >=2 "
        f"`_acceptance_decision_dict(` invocations (one per fork path); "
        f"found {len(serialiser_calls)} at harness.py:{serialiser_calls}."
    )
