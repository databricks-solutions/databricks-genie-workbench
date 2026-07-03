"""Bounded inline benchmark repair loop for ``benchmark_qc_and_repair``.

GSO v2 orchestration (Phase 7, arch §5 / progress §5 RESOLVED K=3). The old
6-notebook shape dead-ended a failed benchmark QC into a separate
``benchmark_repair_prune`` job that forced a manual re-trigger. The new ``01``
task validates → **repairs/prunes inline** → re-validates, bounded by
``benchmark_repair_max_tries`` (default 3), and flows **unconditionally** into
``02``. Only a benchmark that is still invalid after K tries hard-fails with
terminal reason ``BENCHMARK_UNREPAIRABLE``.

This module owns ONLY the bounded try-counting control loop. It reuses the
existing Phase-2 validation/repair primitives (passed in as callables) — it
does NOT reinvent EXPLAIN validation, benchmark synthesis, the §3.6 leakage
guard, or the §3.5 mutation ledger.

Try-counting semantics (progress §5 RESOLVED, arch §13.1):

* One **try** = one complete repair sweep: review every still-invalid question,
  produce a repaired candidate (fixed SQL / corrected expected answer / dedup /
  replacement / prune), then EXPLAIN-re-validate.
* The **initial discovery validation** that *finds* the failures is triage, NOT
  a repair try.
* A try is **consumed only when ≥1 question is still invalid after the EXPLAIN
  re-validation**. A sweep that clears every failure is "free" (productive) and
  does not consume a slot.
* Hard-fail ``BENCHMARK_UNREPAIRABLE`` only after ``max_tries`` consumed tries
  still leave ≥1 invalid question.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)

# Terminal reason raised when the benchmark set cannot be repaired within the
# try budget. Mirrors the arch-doc hard stop (§5.1 / §6 notebook contract).
BENCHMARK_UNREPAIRABLE = "BENCHMARK_UNREPAIRABLE"

# Bound on the inline repair loop (arch §12 ``benchmark_repair_max_tries``).
DEFAULT_BENCHMARK_REPAIR_MAX_TRIES = 3


def default_id_of(benchmark: dict) -> str:
    """Stable benchmark identity for repair bookkeeping.

    Mirrors the ``id`` / ``question_id`` fallback used across preflight
    (`preflight_validate_benchmarks`).
    """
    return str(benchmark.get("id", benchmark.get("question_id", "")) or "")


# ``validate_fn(benchmarks) -> (valid, invalid)`` — EXPLAIN-style validation.
ValidateFn = Callable[[list[dict]], "tuple[list[dict], list[dict]]"]
# ``repair_fn(invalid, valid) -> repaired_candidates`` — one repair sweep over
# the still-invalid set. May fix in place, replace, or prune (return fewer).
RepairFn = Callable[[list[dict], list[dict]], list[dict]]


class BenchmarkUnrepairableError(RuntimeError):
    """Raised when the benchmark set is still invalid after ``max_tries``.

    Carries the terminal reason (``BENCHMARK_UNREPAIRABLE``) and the still-
    invalid questions so ``01`` can surface them in the ``benchmark_qc``
    artifact and hard-fail the job with an actionable message.
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


@dataclass
class BenchmarkRepairOutcome:
    """Result of a bounded inline repair run.

    Attributes:
        benchmarks: the accumulated VALID working set after repair.
        tries_used: number of repair sweeps that consumed a try (each left
            ≥1 question still invalid after re-validation). The discovery
            validation and any sweep that fully cleared the failures are NOT
            counted.
        terminal_reason: ``None`` on success; never ``BENCHMARK_UNREPAIRABLE``
            (that is raised, not returned).
        repaired_ids: ids of questions that moved invalid → valid.
        still_invalid_ids: ids still invalid on a successful exit (always
            empty on success; populated only inside the raised error).
        sweeps: per-sweep telemetry for the ``benchmark_qc`` artifact.
    """

    benchmarks: list[dict]
    tries_used: int
    terminal_reason: str | None = None
    repaired_ids: list[str] = field(default_factory=list)
    still_invalid_ids: list[str] = field(default_factory=list)
    sweeps: list[dict] = field(default_factory=list)


def run_bounded_benchmark_repair(
    benchmarks: list[dict],
    *,
    validate_fn: ValidateFn,
    repair_fn: RepairFn,
    max_tries: int = DEFAULT_BENCHMARK_REPAIR_MAX_TRIES,
    id_of: Callable[[dict], str] = default_id_of,
) -> BenchmarkRepairOutcome:
    """Validate → bounded inline repair/prune → re-validate (progress §5 K=3).

    Args:
        benchmarks: the benchmark working set to validate and repair.
        validate_fn: partitions a list into ``(valid, invalid)``. Called once
            up front for discovery (triage, not a try) and once per repair
            sweep to re-validate the repaired candidates.
        repair_fn: one repair sweep — given the still-invalid questions (and
            the current valid set for context) returns repaired candidates to
            re-validate. May fix in place, replace, or **prune** (return
            fewer). Pruning is a legitimate repair (D8: never silent-delete
            user rows at scale, but EXPLAIN-invalid rows may be dropped).
        max_tries: bound on consumed repair tries (``benchmark_repair_max_tries``).
        id_of: benchmark identity function for bookkeeping.

    Returns:
        ``BenchmarkRepairOutcome`` with the accumulated valid set on success.

    Raises:
        BenchmarkUnrepairableError: if ≥1 question is still invalid after
            ``max_tries`` consumed tries.
    """
    valid, invalid = validate_fn(list(benchmarks))
    sweeps: list[dict] = []
    repaired_ids: list[str] = []
    tries = 0

    logger.info(
        "Benchmark QC discovery: %d valid, %d invalid (max_tries=%d)",
        len(valid), len(invalid), max_tries,
    )

    while invalid:
        if tries >= max_tries:
            # K consumed tries already and still ≥1 invalid — unrepairable.
            raise BenchmarkUnrepairableError(
                still_invalid=invalid, tries_used=tries, valid=valid,
            )

        before_invalid_ids = [id_of(b) for b in invalid]
        candidates = repair_fn(list(invalid), list(valid))
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
        sweeps=sweeps,
    )
