"""Phase 1 — Surface-level structural guards for AcceptanceOutcome
threading.

These tests are source-inspection style (no harness invocation) — the
harness's full-eval site is deep inside a 17-k-line ``_run_lever_loop``
that cannot be re-entered in isolation.
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


def test_build_acceptance_outcome_called_before_regression_block():
    src = HARNESS_PATH.read_text()
    lines = src.splitlines()

    build_line = next(
        (i for i, line in enumerate(lines, start=1)
         if "build_acceptance_outcome(" in line),
        None,
    )
    regressions_block = next(
        (i for i, line in enumerate(lines, start=1)
         if re.match(r"\s+if regressions:\s*$", line)),
        None,
    )

    assert build_line is not None, (
        "Phase 1 Task 3 wiring missing — build_acceptance_outcome() is "
        "not called anywhere in harness.py."
    )
    assert regressions_block is not None
    assert build_line < regressions_block, (
        f"build_acceptance_outcome() at harness.py:{build_line} must "
        f"appear BEFORE `if regressions:` at harness.py:{regressions_block}."
    )


def test_no_strict_decision_accepted_branch_appends_regression_inline():
    """Bug 5 Surface 1 regression guard. After Task 3, the harness must
    NOT contain a literal ``if not _strict_decision.accepted:`` block
    that pushes into a local ``regressions`` list."""
    src = HARNESS_PATH.read_text()

    pattern = re.compile(
        r"if not _strict_decision\.accepted:.*?regressions\.append\(",
        re.DOTALL,
    )
    matches = list(pattern.finditer(src))

    assert not matches, (
        f"Bug 5 Surface 1 regression — the harness still has "
        f"{len(matches)} inline `if not _strict_decision.accepted: "
        f"regressions.append(...)` block(s)."
    )
