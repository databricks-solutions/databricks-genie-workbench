"""Trial 20 — postmortem-tape replay integration test.

Replays the two postmortem fixtures (airline 519131527536322 and 7now
766686021706995) through the Trial 20 primitives and asserts the
success criteria from the trial plan:

* Airline (Workstream A): a candidate that gains post-arbiter accuracy
  with no out-of-target hard regressions accepts via the new
  ``accepted_post_arbiter_gain_absorbs_pre_arbiter_regression`` reason
  code, even when pre-arbiter byte-match accuracy drops.
* 7now (Workstreams B + C): when prior_patch_family ==
  ``add_example_sql``, the Trial 20 ``_PIVOT_GRAPH`` cycles to a
  different family (``add_sql_snippet_filter``), NOT the legacy
  ``_PIVOT_FROM_FAMILY_AFTER_FAILURE`` constant which would have
  returned ``add_example_sql`` again.

These tests run against the Trial 20 primitives directly (no full
lever loop) so they stay fast and deterministic.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = (
    Path(__file__).parent.parent
    / "fixtures"
    / "trial20_postmortem"
)
AIRLINE_FIXTURE = FIXTURE_DIR / "airline_519131527536322.json"
SEVEN_NOW_FIXTURE = FIXTURE_DIR / "7now_766686021706995.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(autouse=True)
def _trial20_flags_on(monkeypatch):
    """Force Trial 20 master + all sub-flags ON for the replay."""
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL20_PRE_ARBITER_VETO_FIX", "1")
    monkeypatch.setenv("GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL", "1")
    monkeypatch.setenv("GSO_TRIAL20_FAMILY_PIVOT_GRAPH", "1")
    monkeypatch.setenv("GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT", "1")
    monkeypatch.setenv("GSO_TRIAL20_BLAST_RADIUS_MANDATORY", "1")
    yield


def test_airline_pre_arbiter_veto_absorbs_post_arbiter_gain() -> None:
    """Workstream A2: the airline candidate with post-arbiter gain
    and zero out-of-target hard regressions accepts under Trial 20."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_pre_arbiter_regression_guardrail,
    )

    fixture = _load(AIRLINE_FIXTURE)
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=float(
            fixture["baseline_pre_arbiter_accuracy"]
        ),
        candidate_pre_arbiter_accuracy=float(
            fixture["candidate_pre_arbiter_accuracy"]
        ),
        # Empty target_fixed_qids forces the new absorption logic
        # (the airline postmortem evidence showed target_fixed_qids=()
        # for the iter-2 candidate because target attribution drifted).
        target_fixed_qids=(),
        max_pre_arbiter_regression_pp=5.0,
        post_arbiter_delta_pp=float(fixture["post_arbiter_delta_pp"]),
        out_of_target_hard_regressions=int(
            fixture["out_of_target_hard_regressions"]
        ),
    )
    assert decision.accepted, (
        "Trial 20 must accept airline iter-2 candidate: "
        f"reason={decision.reason_code!r}"
    )
    assert decision.reason_code == (
        "accepted_post_arbiter_gain_absorbs_pre_arbiter_regression"
    ), (
        f"Expected absorption reason; got {decision.reason_code!r}"
    )


def test_airline_with_out_of_target_hard_regression_still_blocks() -> None:
    """Workstream A2: the absorption path is NOT a blanket-accept —
    out-of-target hard regressions still veto, otherwise we'd be
    masking genuine collateral damage. Symmetric to the airline
    success case."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_pre_arbiter_regression_guardrail,
    )

    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
        target_fixed_qids=(),
        max_pre_arbiter_regression_pp=5.0,
        post_arbiter_delta_pp=4.2,
        out_of_target_hard_regressions=2,
    )
    assert not decision.accepted, (
        "Out-of-target hard regressions must veto the absorption path"
    )


def test_7now_pivot_graph_cycles_off_add_example_sql() -> None:
    """Workstream C1: when prior_patch_family is add_example_sql and
    we've just kept-insufficient on it, the Trial 20 _PIVOT_GRAPH
    cycles to a different family. The legacy constant returned
    add_example_sql, which is exactly the bug."""
    from dataclasses import dataclass
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )

    @dataclass
    class _Sig:
        terminal_reason: str = ""

    fixture = _load(SEVEN_NOW_FIXTURE)
    nxt = next_patch_family_for_cluster(
        cluster_id="cluster_7now_demand_forecast",
        prior_patch_family=str(fixture["prior_patch_family"]),
        prior_terminal_signatures=[_Sig(terminal_reason="kept_insufficient")],
    )
    # Whatever the next family is, it must NOT be the same family
    # that just kept-insufficient.
    assert nxt != fixture["prior_patch_family"], (
        "Plan 12 pivot must move off the kept-insufficient family; "
        f"got {nxt!r}"
    )
    # And the Trial 20 graph cycles to a specific next family.
    assert nxt == fixture["expected_pivot_next_family"], (
        f"Expected pivot to {fixture['expected_pivot_next_family']!r}, "
        f"got {nxt!r}"
    )


def test_7now_kept_insufficient_in_pivot_set() -> None:
    """Workstream B3: ``kept_insufficient`` is a pivot-triggering
    termination, so a prior iteration that kept-insufficient drives
    Plan 12 to recommend a pivot rather than continuing on the same
    family."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        _TERMINATIONS_REQUIRING_PIVOT,
    )

    assert "kept_insufficient" in _TERMINATIONS_REQUIRING_PIVOT, (
        "Trial 20 B3 must include kept_insufficient in the pivot set"
    )


def test_terminal_reason_kept_insufficient_enum() -> None:
    """Workstream B1: the typed TerminalReason enum carries the
    KEPT_INSUFFICIENT member so the iteration-terminal selector can
    project SM acceptance lane outcomes into the marker stream
    without using the catch-all NO_APPLIED_PATCHES."""
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    assert TerminalReason.KEPT_INSUFFICIENT.value == "kept_insufficient"
