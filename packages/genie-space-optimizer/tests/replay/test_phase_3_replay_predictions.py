"""Phase 3 replay predictions on the ccf1d60d run.

These tests are the cross-cutting sentinels: they assert each Phase 3
fixture lands as predicted given the merged Phase 0 / 1 / 2 substrate.

If you change the four-tier acceptance bounds (Phase 1 Action 1.2),
the matcher rules (Phase 1 Addendum), the kit-safety thresholds (Phase
2 Section B), or the strategist prompt — and one of these tests starts
failing — that is the signal that the change has invalidated a
prediction. Re-derive the prediction by hand from the new substrate
before updating the fixture.

The tests skip cleanly when either the fixture files or the
``run_replay_for_run_id`` test helper are missing, so they coexist
with replay infrastructure that may still be under construction.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "phase_3"


def _load_run_replay_helper():
    """Resolve the ``run_replay_for_run_id`` helper or return ``None``.

    The plan documents this helper as living in
    ``tests/replay/conftest.py``; we look there first, then fall
    back to ``tests/replay/_helpers.py`` for forward-compatibility
    with helper-extraction refactors. Returns ``None`` (skip the
    test) when neither module exposes the symbol — that lets these
    sentinels coexist with replay infrastructure under construction.
    """
    try:
        from tests.replay import conftest as _replay_conftest  # type: ignore
        helper = getattr(_replay_conftest, "run_replay_for_run_id", None)
        if callable(helper):
            return helper
    except Exception:
        pass
    try:
        from tests.replay import _helpers as _replay_helpers  # type: ignore
        helper = getattr(_replay_helpers, "run_replay_for_run_id", None)
        if callable(helper):
            return helper
    except Exception:
        pass
    return None


def test_iteration_feedback_diagnostic_hold_predicted() -> None:
    """ccf1d60d iter-1 must produce IterationFeedback with
    acceptance_class=diagnostic_hold per Phase 1 Action 1.2's worked
    example AND the typed regression_debt_classification carries
    gs_012 under unknown_to_hard."""
    fixture_path = FIXTURES / "ccf1d60d_iter1_iteration_feedback.json"
    if not fixture_path.exists():
        pytest.skip("Phase 3 Section A fixture not present")
    run_replay_for_run_id = _load_run_replay_helper()
    if run_replay_for_run_id is None:
        pytest.skip(
            "tests/replay does not yet expose run_replay_for_run_id; "
            "Phase 3 sentinel will activate when the helper lands."
        )
    fixture = json.loads(fixture_path.read_text())
    env = {
        "GSO_ITERATION_FEEDBACK": "1",
        "GSO_ACCEPTANCE_FOUR_TIER_GATE": "1",
    }
    summaries = run_replay_for_run_id(
        "ccf1d60d-d686-467b-bafa-1640131b4393", env=env,
    )
    iter1_fb = summaries[1]["iteration_feedback"]
    assert iter1_fb.acceptance_class.value == fixture["acceptance_class"]
    assert iter1_fb.accept == fixture["accept"]
    assert list(iter1_fb.target_qids) == fixture["target_qids"]
    assert iter1_fb.regression_debt_classification == (
        fixture["regression_debt_classification"]
    )


def test_near_miss_reflection_diagnostic_hold_predicted() -> None:
    """ccf1d60d iter-1's diagnostic_hold outcome must produce at least
    one NearMissReflection with ``required_next_iter_change="either"``
    targeting gs_026 (per Phase 1 Action 1.2's worked example)."""
    fixture_path = FIXTURES / "ccf1d60d_iter1_near_miss_reflection.json"
    if not fixture_path.exists():
        pytest.skip("Phase 3 Section B fixture not present")
    run_replay_for_run_id = _load_run_replay_helper()
    if run_replay_for_run_id is None:
        pytest.skip(
            "tests/replay does not yet expose run_replay_for_run_id; "
            "Phase 3 sentinel will activate when the helper lands."
        )
    fixture = json.loads(fixture_path.read_text())
    env = {
        "GSO_ITERATION_FEEDBACK": "1",
        "GSO_NEAR_MISS_REFLECTION": "1",
        "GSO_ACCEPTANCE_FOUR_TIER_GATE": "1",
    }
    summaries = run_replay_for_run_id(
        "ccf1d60d-d686-467b-bafa-1640131b4393", env=env,
    )
    nmrs = summaries[1]["iteration_feedback"].near_miss_reflections
    assert len(nmrs) >= 1
    nmr = nmrs[0]
    assert nmr.kind == fixture["kind"]
    assert nmr.required_next_iter_change == fixture["required_next_iter_change"]
    assert list(nmr.target_qids) == fixture["target_qids"]


def test_soft_signal_trend_report_constraints_satisfied() -> None:
    """ccf1d60d end-of-run SoftSignalTrendReport must satisfy the
    constraint-style fixture: S001 (the soft cluster mining
    time_window evidence) MUST be matched against H002 once
    Sections C/D are wired — i.e. S001 must NOT appear in
    ``unmatched_clusters``."""
    fixture_path = FIXTURES / "ccf1d60d_run_soft_trend_report.json"
    if not fixture_path.exists():
        pytest.skip("Phase 3 Section E fixture not present")
    run_replay_for_run_id = _load_run_replay_helper()
    if run_replay_for_run_id is None:
        pytest.skip(
            "tests/replay does not yet expose run_replay_for_run_id; "
            "Phase 3 sentinel will activate when the helper lands."
        )
    constraints = json.loads(fixture_path.read_text())
    env = {
        "GSO_RCA_CARD_BUILDER": "1",
        "GSO_RCA_CARD_SOFT_EVIDENCE": "1",
        "GSO_KIT_AWARE_PATCH_CAP": "1",
        "GSO_SOFT_SIGNAL_TREND_REPORT": "1",
    }
    summaries = run_replay_for_run_id(
        "ccf1d60d-d686-467b-bafa-1640131b4393", env=env,
    )
    metadata = summaries.get("_run_metadata", {})
    report = metadata.get("_soft_signal_trend_report")
    assert report is not None, (
        "Soft-signal trend report not stashed on run metadata. Check "
        "the harness end-of-run wiring (Phase 3 Task 3.3.3.4)."
    )
    assert report.total_soft_clusters >= constraints["total_soft_clusters_min"]
    unmatched_ids = {u.cluster_id for u in report.unmatched_clusters}
    for must_be_matched in constraints["matched_clusters_must_include"]:
        assert must_be_matched not in unmatched_ids, (
            f"Soft cluster {must_be_matched} should be MATCHED but was "
            f"found in unmatched_clusters; the matcher / lift wiring "
            f"is broken."
        )
    for must_not_be_unmatched in constraints[
        "unmatched_clusters_must_not_include"
    ]:
        assert must_not_be_unmatched not in unmatched_ids, (
            f"Soft cluster {must_not_be_unmatched} should NOT be in "
            f"unmatched_clusters."
        )
