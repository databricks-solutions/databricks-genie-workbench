"""Unit tests for the bounded inline benchmark repair loop (Phase 7, §01).

Covers the progress §5 RESOLVED try-counting semantics (K=3):
  * discovery validation is triage, not a try;
  * a try is consumed only when ≥1 question is still invalid after EXPLAIN
    re-validation (a sweep that clears everything is free);
  * hard-fail ``BENCHMARK_UNREPAIRABLE`` only after ``max_tries`` consumed
    tries still leave ≥1 invalid question.

The control loop is exercised with stub ``validate_fn`` / ``repair_fn``
callables so no live workspace / Spark / LLM is needed.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.common.config import (
    MIN_VALID_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
)
from genie_space_optimizer.optimization.benchmark_repair import (
    BENCHMARK_UNREPAIRABLE,
    INSUFFICIENT_VALID_BENCHMARKS,
    DEFAULT_BENCHMARK_REPAIR_MAX_TRIES,
    BenchmarkCorpusTooSmallError,
    BenchmarkRepairOutcome,
    BenchmarkUnrepairableError,
    require_minimum_valid_benchmarks,
    run_bounded_benchmark_repair,
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


def test_default_corpus_floor_is_15_and_generation_target_remains_30():
    assert MIN_VALID_BENCHMARK_COUNT == 15
    assert TARGET_BENCHMARK_COUNT == 30


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


def test_unrepairable_raises_after_exactly_max_tries():
    """When repair never fixes the failure, the loop hard-fails with
    BENCHMARK_UNREPAIRABLE after exactly ``max_tries`` consumed tries, and
    repair_fn is invoked exactly ``max_tries`` times (the K+1-th is gated)."""
    bms = [_q("ok", True), _q("never", False)]
    repair_calls = {"n": 0}

    def repair_fn(invalid, valid):
        repair_calls["n"] += 1
        return [_q(b["id"], False) for b in invalid]  # still broken

    with pytest.raises(BenchmarkUnrepairableError) as exc_info:
        run_bounded_benchmark_repair(
            bms, validate_fn=_validate_by_flag, repair_fn=repair_fn, max_tries=3,
        )

    err = exc_info.value
    assert err.terminal_reason == BENCHMARK_UNREPAIRABLE
    assert err.tries_used == 3
    assert [b["id"] for b in err.still_invalid] == ["never"]
    # K=3 sweeps attempted; the 4th is gated by the budget check.
    assert repair_calls["n"] == 3
    # The non-failing question survives in the preserved valid set.
    assert {b["id"] for b in err.valid} == {"ok"}


def test_custom_max_tries_one():
    """max_tries=1 hard-fails after a single unproductive sweep."""
    bms = [_q("bad", False)]
    calls = {"n": 0}

    def repair_fn(invalid, valid):
        calls["n"] += 1
        return [_q("bad", False)]

    with pytest.raises(BenchmarkUnrepairableError) as exc_info:
        run_bounded_benchmark_repair(
            bms, validate_fn=_validate_by_flag, repair_fn=repair_fn, max_tries=1,
        )
    assert exc_info.value.tries_used == 1
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
