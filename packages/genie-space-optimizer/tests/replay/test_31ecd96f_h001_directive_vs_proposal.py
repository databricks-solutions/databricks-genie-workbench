"""Phase 5 fixture: directive truthfulness between L5 outcome and
proposal-renderer behavior.

Maps to user-text `test_h001_directive_vs_proposal_truthful`.
Anchor: 31ecd96f iter-1 H001 with L5 outcome=no_structural_candidate.
"""
from __future__ import annotations

import pytest

from tests.replay.fixtures.phase5._helpers import load


def test_l6_proposal_not_emitted_under_h001_when_l5_says_no_structural() -> None:
    iter1 = load("31ecd96f_iter1_h001.json")
    l5 = iter1["directives"]["L5"]
    rendered = iter1["rendered_proposal_kinds"]

    assert l5["outcome"] == "no_structural_candidate"

    forbidden_kinds = {"L6_metric_view", "L6_add_sql_snippet_measure"}
    leaked = forbidden_kinds & set(rendered)
    assert not leaked, (
        f"L6 proposal kinds rendered under H001 despite L5 outcome "
        f"no_structural_candidate: {leaked}. Per directive-truthfulness "
        f"invariant, L6 must be its own AG (e.g. H002), not folded "
        f"under H001."
    )


@pytest.mark.xfail(
    strict=False,
    reason=(
        "This iteration's fixture records the bug; the assertion exists "
        "to fail when the renderer is fixed AND the fixture is refreshed. "
        "Phase 5 wires the fixture-as-witness; the renderer fix lands in "
        "phase 1+2 Task 11 or a follow-up. When fixed, drop xfail."
    ),
)
def test_when_renderer_fixed_h001_emits_no_l6() -> None:
    """Forward-looking gate: once Phase 1+2 Task 11's directive_outcome
    invariant raises on this case, the fixture should be refreshed (or
    a synthetic iter1_h001_fixed.json added) and this xfail flipped to
    a hard assertion.
    """
    iter1 = load("31ecd96f_iter1_h001.json")
    assert iter1["rendered_proposal_kinds"] == [], (
        "renderer fix landed but fixture still encodes the bug — "
        "refresh fixture or drop xfail"
    )
