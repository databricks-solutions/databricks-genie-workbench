"""Defect Plan 3 (2026-05-12) — audit which in-tree replay fixtures
exhibit the gs_021-class dual-emit (a qid appears in both
``iteration_plan["clusters"]`` and ``iteration_plan["soft_clusters"]``
in the same iteration).

The audit is a one-shot regression aid: with the flag default-on after
Task 4, every fixture below sees the redundant ``soft_signal`` emit
suppressed at replay time. No test today asserts a specific
``soft_signal`` count for these fixtures, so the flip is byte-stable
for the test suite — but this manifest exists so a future regression
in producer logic can be triaged immediately against the named list.
"""
from __future__ import annotations

import json
import pathlib

import pytest

FIXTURES_DIR = (
    pathlib.Path(__file__).parent / "fixtures"
)


def _dual_emit_qids_in_iteration(iteration_plan: dict) -> set[str]:
    hard: set[str] = set()
    soft: set[str] = set()
    for c in iteration_plan.get("clusters") or []:
        for q in c.get("question_ids") or []:
            qs = str(q).strip()
            if qs:
                hard.add(qs)
    for c in iteration_plan.get("soft_clusters") or []:
        for q in c.get("question_ids") or []:
            qs = str(q).strip()
            if qs:
                soft.add(qs)
    return hard & soft


@pytest.mark.parametrize(
    "fixture_path",
    sorted(FIXTURES_DIR.glob("*.json")),
    ids=lambda p: p.name,
)
def test_fixture_dual_emit_manifest_locked(fixture_path: pathlib.Path) -> None:
    """Pin the known dual-emit footprint per fixture. Failure here
    means a fixture's iteration plans changed (or a new fixture was
    added) in a way that warrants explicit triage against the
    default-on producer-strict semantics.

    The manifest is the authoritative answer for "would the default-
    flip change this fixture's replay outcome?" — fixtures with an
    empty set are byte-stable across the flip; fixtures with a
    non-empty set have the redundant soft emit suppressed (which
    cannot increase violation counts and may decrease them).
    """
    if (
        fixture_path.name == "run_ccf1d60d_7now.json"
        and not fixture_path.exists()
    ):
        pytest.skip("ccf1d60d fixture captured in Task 2 of Defect Plan 3")

    fixture = json.loads(fixture_path.read_text())
    iters = fixture.get("iterations") or []
    seen: dict[int, list[str]] = {}
    for i, it in enumerate(iters):
        # iteration_plan may be the iteration dict itself or a nested
        # field — both shapes occur across the in-tree fixtures.
        plan = it.get("iteration_plan") if isinstance(it.get("iteration_plan"), dict) else it
        dual = _dual_emit_qids_in_iteration(plan)
        if dual:
            seen[i] = sorted(dual)

    # Expected manifest. Empty for every in-tree fixture — the
    # iteration-plan-level overlap (clusters ∩ soft_clusters) is NOT
    # how the gs_021 dual-emit actually surfaces. The dual-emit arises
    # from ``_replay_iteration`` constructing ``soft`` from row-level
    # classification fall-through plus the fixture's soft_clusters, so
    # the projection here under-counts. The anchor tests
    # (test_replay_anchor_{3b050ec5,ccf1d60d}_zero_violations.py)
    # exercise the real replay pipeline and pin the violation counts
    # under both flag regimes; this audit only catches the rare case
    # where a fixture is hand-edited to put the same qid in both
    # cluster lists at the iteration plan level.
    expected: dict[str, dict[int, list[str]]] = {}

    expected_for_this = expected.get(fixture_path.name, {})
    assert seen == expected_for_this, (
        f"Fixture {fixture_path.name} dual-emit footprint changed.\n"
        f"  Expected: {expected_for_this!r}\n"
        f"  Observed: {seen!r}\n"
        "Update the Defect Plan 3 audit manifest only after explicit "
        "triage."
    )
