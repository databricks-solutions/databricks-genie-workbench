"""Contract tests for the iteration API's denominator and exclusion shape.

Bug #2 root cause: different code paths in the API were computing
``evaluated`` vs ``total_questions`` differently, producing the "12/14 here
but something else over there" mismatch. The contract we freeze in this file:

    overall_accuracy * evaluated / 100 ≈ correct   (within 0.5pp)
    evaluated = total - excluded                    (new rows)
    evaluated = total                               (legacy rows, missing col)
    quarantined = len(quarantined_benchmarks_json)

All API shapes — IterationSummary, IterationDetail, step summary, levers
— MUST agree on these numbers. This test locks that invariant by calling
``_resolve_eval_counts`` — the single source of truth that all those
endpoints now delegate to.
"""

from __future__ import annotations

import json

from genie_space_optimizer.backend.routes.runs import _resolve_eval_counts


def _iter_row(
    *,
    total: int | None = None,
    correct: int | None = None,
    evaluated: int | None = None,
    excluded: int | None = None,
    quarantined: list[dict] | str | None = None,
) -> dict:
    row: dict = {}
    if total is not None:
        row["total_questions"] = total
    if correct is not None:
        row["correct_count"] = correct
    if evaluated is not None:
        row["evaluated_count"] = evaluated
    if excluded is not None:
        row["excluded_count"] = excluded
    if quarantined is not None:
        if isinstance(quarantined, list):
            row["quarantined_benchmarks_json"] = json.dumps(quarantined)
        else:
            row["quarantined_benchmarks_json"] = quarantined
    return row


def test_none_row_returns_all_zeros() -> None:
    counts = _resolve_eval_counts(None)
    assert counts == {
        "total": 0, "evaluated": 0, "correct": 0,
        "excluded": 0, "quarantined": 0,
    }


def test_empty_dict_row_returns_all_zeros() -> None:
    assert _resolve_eval_counts({}) == {
        "total": 0, "evaluated": 0, "correct": 0,
        "excluded": 0, "quarantined": 0,
    }


def test_happy_path_passes_values_through() -> None:
    """Fresh row with all new columns populated: counts pass through 1:1."""
    row = _iter_row(total=14, correct=12, evaluated=14, excluded=0)
    counts = _resolve_eval_counts(row)
    assert counts["total"] == 14
    assert counts["evaluated"] == 14
    assert counts["correct"] == 12
    assert counts["excluded"] == 0
    assert counts["quarantined"] == 0


def test_bug2_denominator_contract_12_correct_of_14_with_2_excluded() -> None:
    """The scenario from the bug report: 14 questions, 2 excluded at runtime,
    12 correct of the 12 remaining → 100% on an evaluated_count of 12, not
    "12/14 = 85.7% somewhere else"."""
    row = _iter_row(total=14, correct=12, evaluated=12, excluded=2)
    counts = _resolve_eval_counts(row)
    assert counts["evaluated"] == 12
    assert counts["correct"] == 12
    assert counts["excluded"] == 2
    # overall_accuracy = correct / evaluated * 100 = 100.0 — the UI must use
    # evaluated (12), never total (14), as the denominator.
    assert counts["correct"] / counts["evaluated"] == 1.0


def test_back_compat_missing_evaluated_derives_from_total_minus_excluded() -> None:
    """Old rows written before Bug #2 didn't have evaluated_count. The helper
    must derive it so stored overall_accuracy keeps tying out."""
    row = _iter_row(total=14, correct=12, excluded=2)
    counts = _resolve_eval_counts(row)
    assert counts["evaluated"] == 12, "derived: 14 - 2 = 12"


def test_back_compat_missing_both_evaluated_and_excluded_falls_back_to_total() -> None:
    """Very old rows lacking both new columns must at least not divide by
    zero — fall back to total_questions."""
    row = _iter_row(total=14, correct=12)
    counts = _resolve_eval_counts(row)
    assert counts["evaluated"] == 14
    assert counts["excluded"] == 0


def test_quarantined_json_length_is_reported() -> None:
    quarantined = [
        {"question_id": "q1", "reason_code": "quarantined"},
        {"question_id": "q2", "reason_code": "quarantined"},
        {"question_id": "q3", "reason_code": "quarantined"},
    ]
    row = _iter_row(total=10, correct=7, evaluated=7, excluded=0, quarantined=quarantined)
    counts = _resolve_eval_counts(row)
    assert counts["quarantined"] == 3


def test_quarantined_accepts_raw_list_column() -> None:
    """Lakebase may hydrate the column as a list rather than a JSON string.
    Helper must handle both shapes identically."""
    row = _iter_row(total=5, correct=5, evaluated=5)
    row["quarantined_benchmarks_json"] = [{"question_id": "q_bad"}]
    counts = _resolve_eval_counts(row)
    assert counts["quarantined"] == 1


def test_quarantined_malformed_json_is_safe() -> None:
    """Never crash the API on malformed quarantine payloads — default to 0."""
    row = _iter_row(total=5, correct=5, evaluated=5, quarantined="{not: valid json")
    counts = _resolve_eval_counts(row)
    assert counts["quarantined"] == 0


def test_quarantined_non_list_json_is_safe() -> None:
    """A JSON object (instead of array) should not crash; just yield 0."""
    row = _iter_row(total=5, correct=5, evaluated=5, quarantined='{"not": "an array"}')
    counts = _resolve_eval_counts(row)
    assert counts["quarantined"] == 0


def test_negative_derived_evaluated_clamps_to_total() -> None:
    """Corrupt data with excluded > total must not yield negative evaluated —
    fall back to total_questions."""
    row = _iter_row(total=10, correct=5, excluded=999)
    counts = _resolve_eval_counts(row)
    assert counts["evaluated"] == 10, "negative derived value should clamp up to total"


def test_string_coerced_numeric_fields_still_parse() -> None:
    """Delta fallback returns strings. _safe_int inside _resolve_eval_counts
    must coerce them transparently."""
    row = {
        "total_questions": "14",
        "correct_count": "12",
        "evaluated_count": "14",
        "excluded_count": "0",
    }
    counts = _resolve_eval_counts(row)
    assert counts["total"] == 14
    assert counts["evaluated"] == 14
    assert counts["correct"] == 12


def test_derived_accuracy_prefers_correct_over_evaluated() -> None:
    """Bug #2 round-2: even if stored overall_accuracy is stale, the API must
    serve the UI what correct/evaluated says. Otherwise the KPI card and the
    tab label in RunDetailView can disagree (the ticket the user reopened)."""
    from genie_space_optimizer.backend.routes.runs import _derived_accuracy

    row = _iter_row(total=22, correct=16, evaluated=19, excluded=3)
    row["overall_accuracy"] = 72.7  # stale stored value (e.g. 16/22)
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 84.21


def test_derived_accuracy_falls_back_to_stored_for_legacy_rows() -> None:
    """Pre-migration rows lack evaluated_count. We must not blindly divide
    by total_questions (that's the original Bug #2) — instead, honour the
    stored overall_accuracy so old dashboards don't go blank or wrong."""
    from genie_space_optimizer.backend.routes.runs import _derived_accuracy

    row = {"total_questions": 22, "correct_count": 16, "overall_accuracy": 84.21}
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 84.21


def test_derived_accuracy_logs_drift_above_half_pp(caplog) -> None:
    """When stored and derived disagree by >0.5pp, emit an INFO-level log so
    oncall can spot rows that need backfilling. Log volume is bounded to one
    line per iteration per API call — safe for a polling endpoint."""
    import logging

    from genie_space_optimizer.backend.routes.runs import _derived_accuracy

    row = _iter_row(total=22, correct=16, evaluated=19, excluded=3)
    row["overall_accuracy"] = 72.7  # drift: derived will be 84.21

    with caplog.at_level(logging.INFO, logger="genie_space_optimizer.backend.routes.runs"):
        _derived_accuracy(row, run_id="run-xyz", iteration=0)

    drift_logs = [r for r in caplog.records if "accuracy_drift" in r.getMessage()]
    assert drift_logs, "Expected gso.runs.accuracy_drift INFO log on >0.5pp mismatch"
    assert "run-xyz" in drift_logs[0].getMessage()
    assert "stored_overall_accuracy=72.70" in drift_logs[0].getMessage()
    assert "derived=84.21" in drift_logs[0].getMessage()


def test_derived_accuracy_no_drift_log_when_in_tolerance(caplog) -> None:
    import logging

    from genie_space_optimizer.backend.routes.runs import _derived_accuracy

    row = _iter_row(total=19, correct=16, evaluated=19, excluded=0)
    row["overall_accuracy"] = 84.2  # within 0.5pp of derived 84.21

    with caplog.at_level(logging.INFO, logger="genie_space_optimizer.backend.routes.runs"):
        _derived_accuracy(row, run_id="r1", iteration=0)

    drift_logs = [r for r in caplog.records if "accuracy_drift" in r.getMessage()]
    assert not drift_logs


def test_derived_accuracy_ignores_non_numeric_evaluated_count() -> None:
    """PR #79 review #5 — if ``evaluated_count`` arrives as a non-numeric
    value (e.g. a stringly-typed legacy row written outside the normal
    pipeline), the previous guard `evaluated_raw is not None` would still
    fall through to the derived denominator and divide by `total - excluded`.
    That IS the Bug #2 regression. Post-fix: parse first, gate on the
    parsed result → unparseable = honour stored."""
    from genie_space_optimizer.backend.routes.runs import _derived_accuracy

    row = {
        "total_questions": 22,
        "correct_count": 16,
        "excluded_count": 3,
        "evaluated_count": "unknown",  # non-numeric, not None
        "overall_accuracy": 84.21,
    }
    # Must return stored, NOT round(100 * 16 / (22 - 3), 2) == 84.21 by
    # coincidence. Bump correct + stored so the two answers diverge, so
    # the test actually distinguishes the two branches.
    row["correct_count"] = 10
    row["overall_accuracy"] = 55.55
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 55.55


def test_derived_accuracy_trusts_zero_evaluated_returns_stored() -> None:
    """All benchmarks excluded/quarantined — evaluated_count is legitimately 0.
    Must not divide by zero; must fall back to stored overall_accuracy."""
    from genie_space_optimizer.backend.routes.runs import _derived_accuracy

    row = _iter_row(total=14, correct=0, evaluated=0, excluded=14)
    row["overall_accuracy"] = 0.0
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 0.0


def test_get_baseline_and_best_accuracy_uses_derived_values() -> None:
    """PipelineRun.baselineScore / optimizedScore must match what the tab
    labels compute, not the stored overall_accuracy."""
    from genie_space_optimizer.backend.routes.runs import _get_baseline_and_best_accuracy

    iters = [
        {
            "iteration": 0,
            "eval_scope": "full",
            "overall_accuracy": 72.7,  # stale; derived should win
            "total_questions": 22,
            "correct_count": 16,
            "evaluated_count": 19,
            "excluded_count": 3,
        },
        {
            "iteration": 1,
            "eval_scope": "full",
            "overall_accuracy": 80.0,  # stale; derived should win
            "total_questions": 22,
            "correct_count": 18,
            "evaluated_count": 19,
            "excluded_count": 3,
        },
    ]
    baseline, best = _get_baseline_and_best_accuracy(iters, run_id="r1")
    assert baseline == 84.21
    assert best == 94.74  # round(100 * 18 / 19, 2)


def test_all_endpoints_use_same_helper() -> None:
    """Regression guard: grep the runs.py source file and confirm every
    call-site that builds a per-iteration counts payload goes through
    _resolve_eval_counts. A future contributor who inlines the math will
    re-open Bug #2 — this test catches that in CI.
    """
    import inspect

    from genie_space_optimizer.backend.routes import runs as runs_module

    source = inspect.getsource(runs_module)

    # Expected call sites (grew as endpoints were standardized for Bug #2):
    #   _build_step_summary (Baseline Evaluation)
    #   _build_step_io (Baseline Evaluation) — via _build_step_summary
    #   _build_levers (full_row counts)
    #   get_iterations (IterationSummary)
    #   get_iteration_detail (IterationDetail)
    # 5 call sites minimum + the definition itself = 6 occurrences.
    occurrences = source.count("_resolve_eval_counts")
    assert occurrences >= 6, (
        f"Expected at least 6 references to _resolve_eval_counts in runs.py "
        f"(definition + 5 call sites); found {occurrences}. "
        "A caller may have regressed to inline math — re-opens Bug #2."
    )
