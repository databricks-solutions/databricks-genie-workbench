"""Trial 16.1 — ``eval_qids_from_benchmarks`` is the shared helper the
harness uses to build ``ctx.eval_qids`` (the SM lane's set of qids that
post-apply evaluation cares about) from the benchmark list.

The original harness code at ``harness.py:19972-19976`` and
``harness.py:16309-16313`` used ``b.get("question_id")`` — a strict
top-level accessor. Production benchmarks loaded from MLflow
``genai.datasets`` carry the canonical qid under nested ``inputs.*``
or flat-slash ``inputs/question_id``, so the strict accessor silently
produced ``()`` and the gates fell back to ``(state.qid,)`` — the
single-element tuple visible in postmortems 127751814861356 and
813949510175466 as ``requested_qids=['<namespaced qid>']
benchmarks_count=0``. The fix is to use the same canonical extractor
as ``stages/evaluation._run_full_evaluation`` and
``state_machine/transformers/evaluated_gate``.
"""
from __future__ import annotations


def test_eval_qids_from_benchmarks_with_top_level_question_id() -> None:
    """Backward-compat: top-level ``question_id`` rows still resolve."""
    from genie_space_optimizer.optimization._qid_extraction import (
        eval_qids_from_benchmarks,
    )

    benchmarks = [
        {"question_id": "gs_001"},
        {"question_id": "gs_009"},
    ]
    assert eval_qids_from_benchmarks(benchmarks) == ("gs_001", "gs_009")


def test_eval_qids_from_benchmarks_with_nested_inputs() -> None:
    """MLflow ``genai.datasets`` nested shape resolves via the canonical
    extractor; the previous strict ``b.get('question_id')`` produced
    ``()`` for these rows."""
    from genie_space_optimizer.optimization._qid_extraction import (
        eval_qids_from_benchmarks,
    )

    benchmarks = [
        {"inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_001"}},
        {"inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_009"}},
    ]
    assert eval_qids_from_benchmarks(benchmarks) == (
        "airline_ticketing_and_fare_analysis_gs_001",
        "airline_ticketing_and_fare_analysis_gs_009",
    )


def test_eval_qids_from_benchmarks_with_id_only() -> None:
    """Rows whose canonical qid lives in ``id`` (not ``question_id``)
    resolve via the canonical extractor's top-level alias chain."""
    from genie_space_optimizer.optimization._qid_extraction import (
        eval_qids_from_benchmarks,
    )

    benchmarks = [{"id": "gs_026"}]
    assert eval_qids_from_benchmarks(benchmarks) == ("gs_026",)


def test_eval_qids_from_benchmarks_with_flat_slash_inputs() -> None:
    """MLflow flat-slash carrier ``inputs/question_id`` resolves."""
    from genie_space_optimizer.optimization._qid_extraction import (
        eval_qids_from_benchmarks,
    )

    benchmarks = [{"inputs/question_id": "7now_delivery_analytics_space_gs_013"}]
    assert eval_qids_from_benchmarks(benchmarks) == (
        "7now_delivery_analytics_space_gs_013",
    )


def test_eval_qids_from_benchmarks_skips_rows_without_qid() -> None:
    """Rows with no extractable qid are skipped silently — the same
    contract as ``b.get('question_id')`` had for missing keys."""
    from genie_space_optimizer.optimization._qid_extraction import (
        eval_qids_from_benchmarks,
    )

    benchmarks = [
        {"question_id": "gs_001"},
        {"some_unrelated_field": "noise"},
        {"id": "gs_009"},
    ]
    assert eval_qids_from_benchmarks(benchmarks) == ("gs_001", "gs_009")


def test_eval_qids_from_benchmarks_empty_input() -> None:
    """Empty / ``None`` input → empty tuple, no exception."""
    from genie_space_optimizer.optimization._qid_extraction import (
        eval_qids_from_benchmarks,
    )

    assert eval_qids_from_benchmarks([]) == ()
    assert eval_qids_from_benchmarks(None) == ()  # type: ignore[arg-type]
