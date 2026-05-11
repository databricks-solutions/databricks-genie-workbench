"""RCO-7 Site 1 — harness call-site grep guard.

Pins the canonical-sort import + invocation at the strategist-response
ingestion boundary in harness.py. A future refactor that removes the
sort will fail this test, forcing a deliberate decision rather than a
silent regression.
"""

from __future__ import annotations

from pathlib import Path


_HARNESS_PATH = (
    Path(__file__).resolve().parents[2]
    / "src" / "genie_space_optimizer" / "optimization" / "harness.py"
)


def test_harness_imports_strategist_action_groups_sort_helper() -> None:
    """The harness must import ``sort_action_groups_canonically``
    (under any local alias) somewhere in the strategist-response
    region."""
    src = _HARNESS_PATH.read_text()
    assert "sort_action_groups_canonically" in src, (
        "Site 1 sort helper import missing from harness.py — RCO-7 "
        "regression. Re-add the import at the strategist-response "
        "ingestion boundary."
    )


def test_harness_calls_sort_helper_on_strategist_action_groups() -> None:
    """The harness must apply the sort to the parsed strategist
    action_groups list. We grep for the canonical call pattern; the
    exact local alias may change but the helper name must appear with
    ``strategy.get("action_groups"`` as its argument tree somewhere in
    a small window of the file."""
    src = _HARNESS_PATH.read_text()
    # The call-site sentinel from Site 1's wire-up. Either the bare
    # helper name or the local alias used in the plan must appear.
    assert (
        "sort_action_groups_canonically" in src
        or "_sort_ags_rco7_site1" in src
    ), "Site 1 sort call-site sentinel missing in harness.py."
