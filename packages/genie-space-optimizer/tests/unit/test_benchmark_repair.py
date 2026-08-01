"""Unit tests for the bounded inline benchmark repair loop (Phase 7, §01).

Covers the progress §5 RESOLVED try-counting semantics (K=3):
  * discovery validation is triage, not a try;
  * a try is consumed only when ≥1 question is still invalid after EXPLAIN
    re-validation (a sweep that clears everything is free);
  * after ``max_tries`` consumed tries, still-invalid rows are excluded from
    this run while the valid subset is returned to the caller.

The control loop is exercised with stub ``validate_fn`` / ``repair_fn``
callables so no live workspace / Spark / LLM is needed.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.common.config import (
    MAX_BENCHMARK_COUNT,
    MIN_VALID_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
)
from genie_space_optimizer.optimization.benchmark_repair import (
    INSUFFICIENT_VALID_BENCHMARKS,
    DEFAULT_BENCHMARK_REPAIR_MAX_TRIES,
    BenchmarkCorpusTooSmallError,
    BenchmarkRepairOutcome,
    require_minimum_valid_benchmarks,
    run_bounded_benchmark_repair,
)
from genie_space_optimizer.optimization.benchmark_quality import (
    build_actionable_warning_repair,
)


def _q(qid: str, valid: bool) -> dict:
    """A benchmark question stub carrying an EXPLAIN validity flag."""
    return {"id": qid, "_valid": valid, "question": f"q-{qid}"}


def _validate_by_flag(benchmarks: list[dict]):
    """Partition on the stub ``_valid`` flag — stands in for EXPLAIN."""
    valid = [b for b in benchmarks if b.get("_valid")]
    invalid = [b for b in benchmarks if not b.get("_valid")]
    return valid, invalid


def test_default_max_tries_is_three():
    assert DEFAULT_BENCHMARK_REPAIR_MAX_TRIES == 3


def test_default_corpus_uses_15_floor_30_target_and_40_ceiling():
    assert MIN_VALID_BENCHMARK_COUNT == 15
    assert TARGET_BENCHMARK_COUNT == 30
    assert MAX_BENCHMARK_COUNT == 40


def test_fourteen_valid_benchmarks_fail_the_corpus_floor():
    with pytest.raises(BenchmarkCorpusTooSmallError) as exc_info:
        require_minimum_valid_benchmarks([_q(str(i), True) for i in range(14)])

    err = exc_info.value
    assert err.terminal_reason == INSUFFICIENT_VALID_BENCHMARKS
    assert err.valid_count == 14
    assert err.minimum_count == 15
    assert err.target_count == 30


@pytest.mark.parametrize("count", [15, 17, 30])
def test_minimum_or_larger_valid_corpus_passes(count):
    require_minimum_valid_benchmarks([_q(str(i), True) for i in range(count)])


def test_all_valid_at_discovery_no_repair_called():
    """When discovery finds everything valid, repair_fn is never called and
    no try is consumed."""
    bms = [_q("a", True), _q("b", True)]
    repair_calls = []

    def repair_fn(invalid, valid):
        repair_calls.append(invalid)
        return invalid

    out = run_bounded_benchmark_repair(
        bms, validate_fn=_validate_by_flag, repair_fn=repair_fn,
    )
    assert isinstance(out, BenchmarkRepairOutcome)
    assert out.tries_used == 0
    assert out.terminal_reason is None
    assert repair_calls == []
    assert {b["id"] for b in out.benchmarks} == {"a", "b"}


def test_single_sweep_fixes_all_is_a_free_try():
    """A repair sweep that clears every failure does NOT consume a try
    (productive sweep is free, progress §5)."""
    bms = [_q("a", True), _q("bad", False)]

    def repair_fn(invalid, valid):
        # The repair makes the broken question valid.
        return [_q(b["id"], True) for b in invalid]

    out = run_bounded_benchmark_repair(
        bms, validate_fn=_validate_by_flag, repair_fn=repair_fn,
    )
    assert out.tries_used == 0  # the productive sweep is free
    assert out.terminal_reason is None
    assert "bad" in out.repaired_ids
    # Working set = original valid + repaired.
    assert {b["id"] for b in out.benchmarks} == {"a", "bad"}
    # Exactly one sweep ran, and it did not consume a try.
    assert len(out.sweeps) == 1
    assert out.sweeps[0]["consumed_try"] is False


def test_two_sweeps_first_partial_then_clears_counts_one_try():
    """First sweep fixes one of two failures (still invalid → consumes a
    try); second sweep clears the rest (free) → tries_used == 1."""
    bms = [_q("ok", True), _q("x", False), _q("y", False)]
    sweep = {"n": 0}

    def repair_fn(invalid, valid):
        sweep["n"] += 1
        if sweep["n"] == 1:
            # Fix only x; y stays broken.
            return [_q("x", True), _q("y", False)]
        # Second sweep fixes the remaining y.
        return [_q(b["id"], True) for b in invalid]

    out = run_bounded_benchmark_repair(
        bms, validate_fn=_validate_by_flag, repair_fn=repair_fn,
    )
    assert out.tries_used == 1  # only the first (partial) sweep consumed a try
    assert out.terminal_reason is None
    assert set(out.repaired_ids) == {"x", "y"}
    assert {b["id"] for b in out.benchmarks} == {"ok", "x", "y"}
    assert len(out.sweeps) == 2
    assert [s["consumed_try"] for s in out.sweeps] == [True, False]


def test_question_rewrite_can_trigger_follow_up_expected_sql_repair():
    """A second actionable proposal discovered during re-review must run.

    This reproduces the production cascade where QC first clarified the
    question, then discovered that the expected SQL no longer represented the
    clarified question.  The stable benchmark ID must not make the second
    proposal look "already attempted".
    """
    benchmark = {
        "id": "q1",
        "question": "Show revenue",
        "expected_sql": "SELECT gross_revenue FROM sales",
    }
    latest_results: dict[str, dict] = {}
    repair_rounds: dict[str, int] = {}

    def validate_fn(benchmarks):
        valid: list[dict] = []
        invalid: list[dict] = []
        for candidate in benchmarks:
            if candidate["question"] == "Show revenue":
                result = {
                    "question_id": candidate["id"],
                    "disposition": "warning",
                    "findings": [{
                        "category": "question_quality",
                        "code": "AMBIGUOUS_METRIC",
                        "severity": "warning",
                        "proposed_question": "Show net revenue",
                    }],
                }
            elif candidate["expected_sql"] == "SELECT gross_revenue FROM sales":
                result = {
                    "question_id": candidate["id"],
                    "disposition": "warning",
                    "findings": [{
                        "category": "question_sql_alignment",
                        "code": "WRONG_METRIC",
                        "severity": "warning",
                        "proposed_sql": "SELECT net_revenue FROM sales",
                    }],
                }
            else:
                result = {
                    "question_id": candidate["id"],
                    "disposition": "passed",
                    "findings": [],
                }
            latest_results[candidate["id"]] = result
            repair, _change = build_actionable_warning_repair(candidate, result)
            (invalid if repair is not None else valid).append(candidate)
        return valid, invalid

    def repair_fn(invalid, _valid):
        repaired: list[dict] = []
        for candidate in invalid:
            repair, _change = build_actionable_warning_repair(
                candidate,
                latest_results[candidate["id"]],
            )
            assert repair is not None
            repaired.append(repair)
            repair_rounds[candidate["id"]] = (
                repair_rounds.get(candidate["id"], 0) + 1
            )
        return repaired

    out = run_bounded_benchmark_repair(
        [benchmark],
        validate_fn=validate_fn,
        repair_fn=repair_fn,
        max_tries=3,
    )

    assert out.tries_used == 1
    assert len(out.sweeps) == 2
    assert repair_rounds == {"q1": 2}
    assert out.repaired_ids == ["q1"]
    assert out.benchmarks == [{
        "id": "q1",
        "question": "Show net revenue",
        "expected_sql": "SELECT net_revenue FROM sales",
    }]
    assert latest_results["q1"]["disposition"] == "passed"


def test_repeated_actionable_warning_repairs_stop_at_retry_limit():
    """Fresh proposals remain repairable but cannot exceed the loop budget."""
    latest_results: dict[str, dict] = {}
    repair_calls = 0

    def validate_fn(benchmarks):
        invalid: list[dict] = []
        for candidate in benchmarks:
            sql_version = int(candidate["expected_sql"].rsplit(" ", 1)[-1])
            latest_results[candidate["id"]] = {
                "question_id": candidate["id"],
                "disposition": "warning",
                "findings": [{
                    "category": "question_sql_alignment",
                    "code": "STILL_MISALIGNED",
                    "severity": "warning",
                    "proposed_sql": f"SELECT {sql_version + 1}",
                }],
            }
            invalid.append(candidate)
        return [], invalid

    def repair_fn(invalid, _valid):
        nonlocal repair_calls
        repair_calls += 1
        repaired: list[dict] = []
        for candidate in invalid:
            repair, _change = build_actionable_warning_repair(
                candidate,
                latest_results[candidate["id"]],
            )
            assert repair is not None
            repaired.append(repair)
        return repaired

    out = run_bounded_benchmark_repair(
        [{"id": "q1", "question": "Show metric", "expected_sql": "SELECT 0"}],
        validate_fn=validate_fn,
        repair_fn=repair_fn,
        max_tries=3,
    )

    assert repair_calls == 3
    assert out.tries_used == 3
    assert out.repair_exhausted is True
    assert out.still_invalid_ids == ["q1"]
    assert out.excluded_benchmarks[0]["expected_sql"] == "SELECT 3"
    assert out.benchmarks == []
    assert len(out.sweeps) == 3


def test_prune_as_repair_drops_unfixable_and_succeeds():
    """Returning fewer candidates (pruning the invalid rows) is a legitimate
    repair: the pruned rows leave the working set and the loop succeeds."""
    bms = [_q("keep", True), _q("drop", False)]

    def repair_fn(invalid, valid):
        return []  # prune all invalid

    out = run_bounded_benchmark_repair(
        bms, validate_fn=_validate_by_flag, repair_fn=repair_fn,
    )
    assert out.tries_used == 0
    assert out.terminal_reason is None
    assert {b["id"] for b in out.benchmarks} == {"keep"}
    assert [b["id"] for b in out.excluded_benchmarks] == ["drop"]


def test_repair_exhaustion_excludes_after_exactly_max_tries():
    """A persistent failure is excluded after exactly ``max_tries`` sweeps."""
    bms = [_q("ok", True), _q("never", False)]
    repair_calls = {"n": 0}

    def repair_fn(invalid, valid):
        repair_calls["n"] += 1
        return [_q(b["id"], False) for b in invalid]  # still broken

    out = run_bounded_benchmark_repair(
        bms, validate_fn=_validate_by_flag, repair_fn=repair_fn, max_tries=3,
    )

    assert out.terminal_reason is None
    assert out.tries_used == 3
    assert out.repair_exhausted is True
    assert out.still_invalid_ids == ["never"]
    assert [b["id"] for b in out.excluded_benchmarks] == ["never"]
    assert len(out.sweeps) == 3
    # K=3 sweeps attempted; the 4th is gated by the budget check.
    assert repair_calls["n"] == 3
    # The non-failing question survives in the preserved valid set.
    assert {b["id"] for b in out.benchmarks} == {"ok"}


def test_two_exhausted_rows_leave_36_valid_and_pass_corpus_floor():
    """Regression for run 0e2a7962: exhausted rows must not stop QC."""
    bms = [
        *[_q(f"valid-{index}", True) for index in range(36)],
        _q("exhausted-1", False),
        _q("exhausted-2", False),
    ]

    out = run_bounded_benchmark_repair(
        bms,
        validate_fn=_validate_by_flag,
        repair_fn=lambda invalid, _valid: [
            _q(benchmark["id"], False) for benchmark in invalid
        ],
        max_tries=3,
    )

    assert len(out.benchmarks) == 36
    assert out.tries_used == 3
    assert out.still_invalid_ids == ["exhausted-1", "exhausted-2"]
    assert [row["id"] for row in out.excluded_benchmarks] == [
        "exhausted-1",
        "exhausted-2",
    ]
    require_minimum_valid_benchmarks(out.benchmarks)


def test_custom_max_tries_one():
    """max_tries=1 excludes after a single unproductive sweep."""
    bms = [_q("bad", False)]
    calls = {"n": 0}

    def repair_fn(invalid, valid):
        calls["n"] += 1
        return [_q("bad", False)]

    out = run_bounded_benchmark_repair(
        bms, validate_fn=_validate_by_flag, repair_fn=repair_fn, max_tries=1,
    )
    assert out.tries_used == 1
    assert out.still_invalid_ids == ["bad"]
    assert out.repair_exhausted is True
    assert calls["n"] == 1


def test_discovery_validation_is_not_a_try():
    """validate_fn is called once for discovery before any repair; that call
    is triage, never charged as a try (a still-broken set that is then fixed
    on the first sweep ends with tries_used == 0)."""
    bms = [_q("bad", False)]
    validate_calls = {"n": 0}

    def counting_validate(benchmarks):
        validate_calls["n"] += 1
        return _validate_by_flag(benchmarks)

    def repair_fn(invalid, valid):
        return [_q("bad", True)]  # fixed on the first sweep

    out = run_bounded_benchmark_repair(
        bms, validate_fn=counting_validate, repair_fn=repair_fn,
    )
    # discovery (1) + one re-validation (1) = 2 validate calls, 0 tries used.
    assert validate_calls["n"] == 2
    assert out.tries_used == 0
