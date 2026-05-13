"""F5 contract bug from the 2314bb2c trial postmortem.

The acceptance-path ``acceptance_decision`` dict builder in
``harness.py`` (around line 16382, immediately after the
``# PASSED`` branch's control_plane_decision is constructed) omits
the ``"accepted": True`` field. The rejection-path builder at
line 16189 carries ``"accepted": False`` explicitly. The asymmetry
causes the Phase H acceptance-drift caller at line 26733 to read
``False`` from the default and emit a false-positive
``GSO_PHASE_H_ACCEPTANCE_DRIFT_V1`` marker whenever a candidate is
accepted (witnessed on iter 1 of the 2314bb2c trial:
``canonical_outcome=rolled_back, phase_h_outcome=accepted``).

This test fails on the current main and passes after Task 6.
"""

from __future__ import annotations

import re
from pathlib import Path


HARNESS_PATH = (
    Path(__file__).parent.parent.parent
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)


def test_acceptance_path_dict_carries_accepted_true_field():
    """Every ``acceptance_decision`` dict literal in harness.py must
    declare an ``"accepted"`` key. The acceptance-path builder must
    set it to ``True``; the rejection-path builder must set it to
    ``False`` (current state). Catches the F5 asymmetry."""
    src = HARNESS_PATH.read_text()
    # Find every line that opens an acceptance_decision dict literal.
    # Each literal opens with ``"acceptance_decision": {``.
    literal_starts = [
        i
        for i, line in enumerate(src.splitlines(), start=1)
        if '"acceptance_decision": {' in line
    ]
    assert len(literal_starts) >= 2, (
        f"expected >= 2 acceptance_decision dict literals in "
        f"harness.py; found {len(literal_starts)}"
    )
    lines = src.splitlines()
    for start_line in literal_starts:
        body_window = "\n".join(lines[start_line - 1 : start_line + 30])
        assert re.search(
            r'"accepted"\s*:\s*(True|False)\b', body_window
        ), (
            f'acceptance_decision dict literal at harness.py:{start_line} '
            f'does not declare an "accepted": True|False field within '
            f'30 lines. The Phase H drift-detector caller depends on it.'
        )
