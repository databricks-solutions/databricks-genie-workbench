"""Bounded inline benchmark repair loop for ``benchmark_qc_and_repair``.

GSO v2 orchestration (Phase 7, arch §5 / progress §5 RESOLVED K=3). The old
6-notebook shape dead-ended a failed benchmark QC into a separate
``benchmark_repair_prune`` job that forced a manual re-trigger. The new ``01``
task validates → **repairs/excludes inline** → re-validates, bounded by
``benchmark_repair_max_tries`` (default 3), and flows into ``02`` when the
valid corpus still meets the configured floor. A benchmark that is still
invalid after K tries is excluded from the current evaluation corpus without
being deleted from the live Genie Agent. Callers treat a resulting corpus
below the floor as the business-skip reason
``INSUFFICIENT_VALID_BENCHMARKS``.

This module owns the bounded try-counting control loop and the final corpus
floor guard. It reuses the existing Phase-2 validation/repair primitives
(passed in as callables) — it does NOT reinvent EXPLAIN validation, benchmark
synthesis, the §3.6 leakage guard, or the §3.5 mutation ledger.

Try-counting semantics (progress §5 RESOLVED, arch §13.1):

* One **try** = one complete repair sweep: review every still-invalid question,
  produce a repaired candidate (fixed SQL / corrected expected answer / dedup /
  replacement / run-local exclusion), then EXPLAIN-re-validate.
* The **initial discovery validation** that *finds* the failures is triage, NOT
  a repair try.
* A try is **consumed only when ≥1 question is still invalid after the EXPLAIN
  re-validation**. A sweep that clears every failure is "free" (productive) and
  does not consume a slot.
* After ``max_tries`` consumed tries, return the remaining invalid questions
  as run-local exclusions. The caller owns the final corpus-floor decision.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from genie_space_optimizer.common.config import (
    MIN_VALID_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
)

logger = logging.getLogger(__name__)

# Legacy terminal reason retained for compatibility with historical artifacts
# and callers. New repair runs return exhausted questions as exclusions.
BENCHMARK_UNREPAIRABLE = "BENCHMARK_UNREPAIRABLE"

# Terminal reason used when QC succeeds for each remaining row but too few
# valid rows survive to provide a meaningful optimization corpus.
INSUFFICIENT_VALID_BENCHMARKS = "INSUFFICIENT_VALID_BENCHMARKS"

# Bound on the inline repair loop (arch §12 ``benchmark_repair_max_tries``).
DEFAULT_BENCHMARK_REPAIR_MAX_TRIES = 3

DEFAULT_BENCHMARK_TOP_UP_MAX_ATTEMPTS = 3
DEFAULT_BENCHMARK_TOP_UP_BATCH_SIZE = 10
DEFAULT_BENCHMARK_TOP_UP_MAX_NO_PROGRESS = 2


def default_id_of(benchmark: dict) -> str:
    """Stable benchmark identity for repair bookkeeping.

    Mirrors the ``id`` / ``question_id`` fallback used across preflight
    (`preflight_validate_benchmarks`).
    """
    return str(benchmark.get("id", benchmark.get("question_id", "")) or "")


# ``validate_fn(benchmarks) -> (valid, invalid)`` — EXPLAIN-style validation.
ValidateFn = Callable[[list[dict]], "tuple[list[dict], list[dict]]"]
# ``repair_fn(invalid, valid) -> repaired_candidates`` — one repair sweep over
# the still-invalid set. May fix in place, replace, or exclude (return fewer).
RepairFn = Callable[[list[dict], list[dict]], list[dict]]
TopUpGenerateFn = Callable[[list[dict], int], list[dict]]


class BenchmarkUnrepairableError(RuntimeError):
    """Legacy error for historical callers that hard-failed exhausted repair.

    ``run_bounded_benchmark_repair`` no longer raises this error. Keep the
    type importable while older artifacts and integrations still recognize
    the ``BENCHMARK_UNREPAIRABLE`` terminal reason.
    """

    terminal_reason = BENCHMARK_UNREPAIRABLE

    def __init__(
        self,
        *,
        still_invalid: list[dict],
        tries_used: int,
        valid: list[dict] | None = None,
    ) -> None:
        self.still_invalid = still_invalid
        self.tries_used = tries_used
        self.valid = valid or []
        ids = [default_id_of(b) for b in still_invalid]
        super().__init__(
            f"{BENCHMARK_UNREPAIRABLE}: {len(still_invalid)} benchmark "
            f"question(s) still invalid after {tries_used} repair "
            f"tr{'y' if tries_used == 1 else 'ies'}: {ids[:10]}"
        )


class BenchmarkCorpusTooSmallError(RuntimeError):
    """Raised when fewer than the required valid benchmarks survive QC."""

    terminal_reason = INSUFFICIENT_VALID_BENCHMARKS

    def __init__(
        self,
        *,
        valid_count: int,
        minimum_count: int,
        target_count: int,
        context: str,
    ) -> None:
        self.valid_count = valid_count
        self.minimum_count = minimum_count
        self.target_count = target_count
        self.context = context
        super().__init__(
            f"{INSUFFICIENT_VALID_BENCHMARKS}: only {valid_count} valid "
            f"benchmark question(s) remain after {context}; at least "
            f"{minimum_count} are required (generation target: {target_count})"
        )


def require_minimum_valid_benchmarks(
    benchmarks: list[dict],
    *,
    minimum_count: int = MIN_VALID_BENCHMARK_COUNT,
    target_count: int = TARGET_BENCHMARK_COUNT,
    context: str = "quality review",
) -> None:
    """Fail closed when the valid corpus is below the optimization floor."""
    if len(benchmarks) < minimum_count:
        raise BenchmarkCorpusTooSmallError(
            valid_count=len(benchmarks),
            minimum_count=minimum_count,
            target_count=target_count,
            context=context,
        )


@dataclass
class BenchmarkRepairOutcome:
    """Result of a bounded inline repair run.

    Attributes:
        benchmarks: the accumulated VALID working set after repair.
        tries_used: number of repair sweeps that consumed a try (each left
            ≥1 question still invalid after re-validation). The discovery
            validation and any sweep that fully cleared the failures are NOT
            counted.
        terminal_reason: ``None``. Exhausted rows are exclusions, not a run
            terminal condition.
        repaired_ids: ids of questions that moved invalid → valid.
        still_invalid_ids: ids that remained invalid after the repair budget
            and were excluded from this run's evaluation corpus.
        excluded_benchmarks: invalid input rows omitted by a replacement sweep
            plus rows corresponding to ``still_invalid_ids``. These are not
            deleted from the live Agent.
        repair_exhausted: whether the loop reached ``max_tries`` with at least
            one invalid row remaining.
        sweeps: per-sweep telemetry for the ``benchmark_qc`` artifact.
    """

    benchmarks: list[dict]
    tries_used: int
    terminal_reason: str | None = None
    repaired_ids: list[str] = field(default_factory=list)
    still_invalid_ids: list[str] = field(default_factory=list)
    excluded_benchmarks: list[dict] = field(default_factory=list)
    repair_exhausted: bool = False
    sweeps: list[dict] = field(default_factory=list)


@dataclass
class BenchmarkTopUpOutcome:
    """Result of bounded, count-driven benchmark generation."""

    benchmarks: list[dict]
    attempts_used: int
    stop_reason: str
    attempts: list[dict] = field(default_factory=list)
    requested_count: int = 0
    generated_count: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    duplicate_count: int = 0


def _normalized_question(benchmark: dict) -> str:
    question = benchmark.get("question")
    if isinstance(question, list):
        question = question[0] if question else ""
    return " ".join(str(question or "").strip().lower().split())


def run_bounded_benchmark_top_up(
    benchmarks: list[dict],
    *,
    generate_fn: TopUpGenerateFn,
    validate_fn: ValidateFn,
    target_count: int = TARGET_BENCHMARK_COUNT,
    max_attempts: int = DEFAULT_BENCHMARK_TOP_UP_MAX_ATTEMPTS,
    batch_size: int = DEFAULT_BENCHMARK_TOP_UP_BATCH_SIZE,
    max_no_progress: int = DEFAULT_BENCHMARK_TOP_UP_MAX_NO_PROGRESS,
) -> BenchmarkTopUpOutcome:
    """Generate and validate toward ``target_count`` with bounded retries.

    The input corpus is assumed to have already passed comprehensive QC.
    ``generate_fn`` returns only net-new candidates; this controller still
    defensively removes duplicate IDs and normalized question text before
    calling ``validate_fn``. Generation and validation errors are recorded as
    zero-progress attempts so an otherwise usable 15–29 question corpus can
    continue after the bounded budget is exhausted.
    """
    valid = list(benchmarks)
    attempts: list[dict] = []
    requested_total = 0
    generated_total = 0
    accepted_total = 0
    rejected_total = 0
    duplicate_total = 0
    no_progress = 0

    seen_ids = {default_id_of(row) for row in valid if default_id_of(row)}
    seen_questions = {
        question for row in valid if (question := _normalized_question(row))
    }

    if len(valid) >= target_count:
        return BenchmarkTopUpOutcome(
            benchmarks=valid,
            attempts_used=0,
            stop_reason="target_already_met",
        )

    stop_reason = "attempt_limit"
    for attempt_index in range(1, max_attempts + 1):
        requested = min(batch_size, target_count - len(valid))
        requested_total += requested
        generated_count = 0
        unique_count = 0
        accepted_count = 0
        rejected_count = 0
        duplicate_count = 0
        error_type: str | None = None
        error_message: str | None = None

        try:
            candidates = list(generate_fn(list(valid), requested))
            generated_count = len(candidates)
            unique_candidates: list[dict] = []
            pending_ids: set[str] = set()
            pending_questions: set[str] = set()
            for candidate in candidates:
                candidate_id = default_id_of(candidate)
                candidate_question = _normalized_question(candidate)
                if (
                    (candidate_id and (candidate_id in seen_ids or candidate_id in pending_ids))
                    or (
                        candidate_question
                        and (
                            candidate_question in seen_questions
                            or candidate_question in pending_questions
                        )
                    )
                ):
                    duplicate_count += 1
                    continue
                unique_candidates.append(candidate)
                if candidate_id:
                    pending_ids.add(candidate_id)
                if candidate_question:
                    pending_questions.add(candidate_question)

            unique_count = len(unique_candidates)
            accepted, rejected = (
                validate_fn(unique_candidates) if unique_candidates else ([], [])
            )
            accepted_rows: list[dict] = []
            for candidate in accepted:
                candidate_id = default_id_of(candidate)
                candidate_question = _normalized_question(candidate)
                if (
                    (candidate_id and candidate_id in seen_ids)
                    or (candidate_question and candidate_question in seen_questions)
                ):
                    duplicate_count += 1
                    continue
                accepted_rows.append(candidate)
                if candidate_id:
                    seen_ids.add(candidate_id)
                if candidate_question:
                    seen_questions.add(candidate_question)

            accepted_count = len(accepted_rows)
            rejected_count = len(rejected)
            valid.extend(accepted_rows)
        except Exception as exc:
            error_type = type(exc).__name__
            error_message = str(exc)[:300]
            logger.warning(
                "Benchmark top-up attempt %d/%d failed: %s: %s",
                attempt_index,
                max_attempts,
                error_type,
                error_message,
            )

        generated_total += generated_count
        accepted_total += accepted_count
        rejected_total += rejected_count
        duplicate_total += duplicate_count
        no_progress = no_progress + 1 if accepted_count == 0 else 0
        attempts.append(
            {
                "attempt_index": attempt_index,
                "requested_count": requested,
                "generated_count": generated_count,
                "unique_count": unique_count,
                "accepted_count": accepted_count,
                "rejected_count": rejected_count,
                "duplicate_count": duplicate_count,
                "valid_count_after": len(valid),
                "error_type": error_type,
                "error_message": error_message,
            }
        )

        if len(valid) >= target_count:
            stop_reason = "target_reached"
            break
        if no_progress >= max_no_progress:
            stop_reason = "no_progress"
            break

    return BenchmarkTopUpOutcome(
        benchmarks=valid,
        attempts_used=len(attempts),
        stop_reason=stop_reason,
        attempts=attempts,
        requested_count=requested_total,
        generated_count=generated_total,
        accepted_count=accepted_total,
        rejected_count=rejected_total,
        duplicate_count=duplicate_total,
    )


def run_bounded_benchmark_repair(
    benchmarks: list[dict],
    *,
    validate_fn: ValidateFn,
    repair_fn: RepairFn,
    max_tries: int = DEFAULT_BENCHMARK_REPAIR_MAX_TRIES,
    id_of: Callable[[dict], str] = default_id_of,
) -> BenchmarkRepairOutcome:
    """Validate → bounded inline repair/exclusion → re-validate (progress §5 K=3).

    Args:
        benchmarks: the benchmark working set to validate and repair.
        validate_fn: partitions a list into ``(valid, invalid)``. Called once
            up front for discovery (triage, not a try) and once per repair
            sweep to re-validate the repaired candidates.
        repair_fn: one repair sweep — given the still-invalid questions (and
            the current valid set for context) returns repaired candidates to
            re-validate. May fix in place, replace, or return fewer candidates.
            Returning fewer candidates excludes omitted rows from the working
            corpus; it never authorizes deleting live Genie Agent benchmarks.
        max_tries: bound on consumed repair tries (``benchmark_repair_max_tries``).
        id_of: benchmark identity function for bookkeeping.

    Returns:
        ``BenchmarkRepairOutcome`` with the accumulated valid set on success.

    Questions still invalid after ``max_tries`` are returned in
    ``excluded_benchmarks``. The caller must enforce the minimum valid-corpus
    size before optimization proceeds.
    """
    valid, invalid = validate_fn(list(benchmarks))
    sweeps: list[dict] = []
    repaired_ids: list[str] = []
    excluded_benchmarks: list[dict] = []
    tries = 0

    logger.info(
        "Benchmark QC discovery: %d valid, %d invalid (max_tries=%d)",
        len(valid), len(invalid), max_tries,
    )

    while invalid:
        if tries >= max_tries:
            # K consumed tries already and still ≥1 invalid. Exclude those
            # rows from this evaluation run without deleting them from the
            # live Genie Agent; the caller applies the corpus-floor gate.
            return BenchmarkRepairOutcome(
                benchmarks=valid,
                tries_used=tries,
                repaired_ids=repaired_ids,
                still_invalid_ids=[id_of(b) for b in invalid],
                excluded_benchmarks=excluded_benchmarks + list(invalid),
                repair_exhausted=True,
                sweeps=sweeps,
            )

        before_invalid_ids = [id_of(b) for b in invalid]
        candidates = repair_fn(list(invalid), list(valid))
        candidate_ids = {id_of(candidate) for candidate in candidates}
        excluded_benchmarks.extend(
            benchmark
            for benchmark in invalid
            if id_of(benchmark) not in candidate_ids
        )
        rv_valid, rv_invalid = validate_fn(list(candidates))

        # Accumulate questions that moved invalid -> valid this sweep.
        for b in rv_valid:
            repaired_ids.append(id_of(b))
        valid = valid + rv_valid

        # A try is consumed ONLY when ≥1 question is still invalid after the
        # EXPLAIN re-validation (progress §5). A sweep that clears everything
        # is free (productive) and the loop exits below without counting it.
        consumed = bool(rv_invalid)
        if consumed:
            tries += 1

        sweeps.append(
            {
                "try_index": tries,
                "consumed_try": consumed,
                "before_invalid": before_invalid_ids,
                "candidates_returned": len(candidates),
                "now_valid": [id_of(b) for b in rv_valid],
                "still_invalid": [id_of(b) for b in rv_invalid],
            }
        )
        logger.info(
            "Benchmark repair sweep: %d candidates -> %d valid, %d still "
            "invalid (try consumed=%s, tries_used=%d)",
            len(candidates), len(rv_valid), len(rv_invalid), consumed, tries,
        )

        invalid = rv_invalid

    return BenchmarkRepairOutcome(
        benchmarks=valid,
        tries_used=tries,
        terminal_reason=None,
        repaired_ids=repaired_ids,
        still_invalid_ids=[],
        excluded_benchmarks=excluded_benchmarks,
        sweeps=sweeps,
    )
