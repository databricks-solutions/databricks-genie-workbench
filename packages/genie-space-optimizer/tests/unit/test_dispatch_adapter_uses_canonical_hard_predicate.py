"""Plan 11 dispatch adapter must use the canonical hard-failure predicate.

The 2026-05-22 postmortems for 98ec8950 / dc89d1a9 both report:

  GSO_PLAN11_DISPATCH_DECISION_V1 outcome=skipped
                                  skip_reason=no_failing_qids
                                  failing_qids_count=0

while the surrounding transcript and replay clearly classify multiple
hard failures (gs_009, gs_024 / gs_026, gs_004, gs_021). Root cause:
the dispatch adapter (``optimizer._stamp_failing_qids_from_eval_results``
→ ``optimizer._row_is_failing``) uses ``row['score'] < 0.5`` as the
failing predicate, but production eval rows do not carry a ``score``
field at all — they carry ``result_correctness`` + ``arbiter``. The
canonical predicate is :func:`evaluation.row_is_hard_failure`, which
both the accuracy gate and legacy clustering use.

These tests pin the contract: the Plan 11 dispatch must use the same
canonical predicate so it sees the same hard QIDs the rest of the
optimizer sees.

Production-shaped eval row example (from replay fixture
``98ec8950-d7d4-40b3-b5c0-36dcfb3fb610/.../replay_fixture_from_latest_export_529413209270226.json``):

    {"arbiter": "ground_truth_correct",
     "question_id": "airline_ticketing_and_fare_analysis_gs_009",
     "result_correctness": "no"}

No ``score`` field. The current ``_row_is_failing`` defaults missing
``score`` to ``0.0`` → ``< 0.5`` → ``True`` for every row, but in
practice production runs still emit ``failing_qids_count=0``, which
means the stamping path either isn't reached or the upstream caller
clears the metadata. Regardless, the predicate must be canonical so
the new code is correct on the row shape it actually receives.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _production_shaped_row(qid: str, *, rc: str = "no", arbiter: str = "ground_truth_correct") -> dict:
    """Build an eval row matching the production replay-fixture shape:
    ``arbiter`` + ``question_id`` + ``result_correctness``. No ``score``.
    """
    return {"arbiter": arbiter, "question_id": qid, "result_correctness": rc}


def _eval_results_with_rows(rows: list[dict]) -> dict:
    return {"rows": rows}


def test_stamp_classifies_production_shaped_hard_failures(monkeypatch):
    """Given production-shaped rows (rc=no, arbiter=ground_truth_correct)
    with no ``score`` field, the stamper must stamp them as failing.

    This is the bug from 98ec8950 / dc89d1a9 postmortems: the score-only
    predicate would either over-include (defaults 0.0 → all rows) or
    under-include (depending on upstream metadata mutation), neither of
    which matches the canonical hard predicate.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        _production_shaped_row("gs_009"),
        _production_shaped_row("gs_024"),
        # Soft override — rc=no but arbiter overrides to correct. NOT hard.
        _production_shaped_row("gs_003", rc="no", arbiter="both_correct"),
        # Passing.
        _production_shaped_row("gs_007", rc="yes", arbiter="both_correct"),
    ])

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=eval_results,
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    assert metadata_snapshot["_failing_qids"] == ["gs_009", "gs_024"], (
        "Stamped _failing_qids must use the canonical row_is_hard_failure "
        "predicate (rc=no AND arbiter not in correct-verdicts) and not "
        f"the score-based fallback; got {metadata_snapshot.get('_failing_qids')!r}"
    )


def test_stamp_does_not_overcount_when_no_score_field_present(monkeypatch):
    """Production rows have no ``score`` field. The legacy predicate
    ``score < 0.5`` defaults missing ``score`` to ``0.0`` → ``True``,
    which would mark every row as failing. The canonical predicate
    correctly distinguishes pass / fail without ``score``.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        _production_shaped_row("gs_001", rc="yes", arbiter="both_correct"),
        _production_shaped_row("gs_002", rc="yes", arbiter="both_correct"),
        _production_shaped_row("gs_003", rc="yes", arbiter="both_correct"),
    ])

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=eval_results,
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    assert metadata_snapshot["_failing_qids"] == [], (
        "Three rc=yes rows must produce zero failing QIDs; legacy "
        "predicate would have stamped all three because missing 'score' "
        f"defaults to 0.0. Got {metadata_snapshot.get('_failing_qids')!r}"
    )


def test_arbiter_override_keeps_row_soft(monkeypatch):
    """Pinned contract from row_is_hard_failure: rc=no + arbiter override
    to ``genie_correct`` / ``both_correct`` makes the row SOFT (not hard).

    This is the Tier 1.4 ghost-ceiling fix from evaluation.py:3649 —
    Plan 11 dispatch must respect it.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    metadata_snapshot: dict = {"iteration": 1, "optimization_run_id": "run_x"}

    eval_results = _eval_results_with_rows([
        _production_shaped_row("gs_hard_a", rc="no", arbiter="ground_truth_correct"),
        _production_shaped_row("gs_soft_b", rc="no", arbiter="both_correct"),
        _production_shaped_row("gs_soft_c", rc="no", arbiter="genie_correct"),
        _production_shaped_row("gs_hard_d", rc="no", arbiter=""),  # missing
    ])

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=eval_results,
            metadata_snapshot=metadata_snapshot,
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    assert metadata_snapshot["_failing_qids"] == ["gs_hard_a", "gs_hard_d"], (
        "Only rc=no rows whose arbiter is NOT in (both_correct, "
        "genie_correct) should be classified as hard. Got "
        f"{metadata_snapshot.get('_failing_qids')!r}"
    )


def test_dispatch_emits_entered_with_production_row_shape(
    capsys, monkeypatch,
):
    """End-to-end contract: with production-shaped rows that contain
    canonical hard failures, the Plan 11 dispatch must emit
    ``outcome=entered`` (not ``skipped no_failing_qids``).
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    with patch.object(
        _stage1_mod, "diagnose_failing_qids", return_value=[],
    ), patch.object(
        _stage2_mod, "cluster_diagnoses", return_value=[],
    ):
        optimizer.cluster_failures(
            eval_results=_eval_results_with_rows([
                _production_shaped_row("gs_009"),
                _production_shaped_row("gs_024"),
            ]),
            metadata_snapshot={"iteration": 1, "optimization_run_id": "run_x"},
            rca_evidence_typed=None,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    out = capsys.readouterr().out
    assert "GSO_PLAN11_DISPATCH_DECISION_V1" in out, (
        "Dispatch marker should be emitted regardless of outcome"
    )
    assert '"outcome":"entered"' in out, (
        "With production-shaped rows containing canonical hard failures, "
        "the dispatch must enter Plan 11 — not skip with no_failing_qids. "
        f"Captured output:\n{out}"
    )
    assert "no_failing_qids" not in out, (
        "This is the postmortem regression: dispatch reports zero failing "
        "QIDs even though canonical predicate finds them."
    )
