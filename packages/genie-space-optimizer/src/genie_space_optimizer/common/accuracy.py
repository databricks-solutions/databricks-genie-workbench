"""Canonical per-iteration accuracy derivation for GSO.

Bug #2 contract: the UI must see accuracy that matches correct/evaluated to
the decimal point. Before this module existed, both the standalone GSO
backend (``genie_space_optimizer.backend.routes.runs``) and the Workbench
router (``backend.routers.auto_optimize``) each carried near-identical copies
of ``_derived_accuracy`` — easy for the two to drift (and they did; see PR #79
review finding #4). Extracted here as the single source of truth, mirroring
the ``common/prompt_registry.py`` pattern.

The guard that gates derived vs. stored accuracy also fixes PR #79 review
finding #5: parse ``evaluated_count`` first, then gate on the parsed result,
so a non-numeric string in the column can never silently drop us into the
``total - excluded`` derived-denominator branch (which is exactly the
pre-Bug-#2 regression this whole pipeline exists to prevent).

This module ALSO owns ``compute_run_scores`` — the canonical "Baseline +
Optimized" pair every UI surface must consume. The contract is documented on
the function. Adding new endpoints? Call ``compute_run_scores``. Don't write
another loop.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from genie_space_optimizer.backend.utils import safe_float, safe_int

__all__ = [
    "RunScores",
    "compute_run_scores",
    "compute_run_scores_by_run_id",
    "derived_accuracy",
]


def derived_accuracy(
    iter_row: dict[str, Any] | None,
    *,
    run_id: str | None = None,
    iteration: int | None = None,
    logger: logging.Logger | None = None,
) -> float | None:
    """Return the canonical per-iteration accuracy percentage.

    Prefers ``correct_count / evaluated_count * 100`` (the same math the
    frontend uses for tab labels via ``ui/lib/eval-counts.ts``) so KPI cards
    and tab labels agree to the decimal. Falls back to stored
    ``overall_accuracy`` when ``evaluated_count`` is absent or unparseable
    (legacy rows written before the Bug #2 column migration).

    When both derived and stored exist and disagree by more than 0.5pp, emits
    an INFO-level drift log via *logger* (when provided) so oncall can spot
    stale ``overall_accuracy`` rows without page noise. Derived wins — stored
    is effectively a read-only back-pointer.

    Args:
        iter_row: A Delta iteration row shaped like the ``genie_opt_iterations``
            SELECT result. ``None`` or empty → returns ``None``.
        run_id / iteration: Only used for drift-log identification. Safe to
            omit; the log message simply carries ``None`` for the missing field.
        logger: Python logger to emit drift lines on. When ``None`` we skip
            logging so pure numeric callers don't pollute an arbitrary logger.

    Returns:
        ``float`` percentage (0–100), or ``None`` when the row is empty and
        no stored value exists. Returns the stored value (including ``0.0``)
        whenever derivation isn't safe.
    """
    if not iter_row:
        return None

    stored = safe_float(iter_row.get("overall_accuracy"))

    # Parse first, then gate on the parsed result. A non-numeric value in
    # ``evaluated_count`` (e.g. a stringly-typed row from a partial write)
    # must NOT trick us into the ``total - excluded`` derived-denominator
    # branch — that IS the Bug #2 regression we're guarding against.
    evaluated = safe_int(iter_row.get("evaluated_count"))
    if evaluated is None:
        return stored
    if evaluated <= 0:
        # Every benchmark was excluded or quarantined. The stored accuracy
        # is the only meaningful signal here; deriving would divide by zero.
        return stored

    correct = safe_int(iter_row.get("correct_count")) or 0
    derived = round(100.0 * correct / evaluated, 2)

    if (
        logger is not None
        and stored is not None
        and abs(derived - stored) > 0.5
    ):
        logger.info(
            "gso.runs.accuracy_drift run_id=%s iteration=%s "
            "stored_overall_accuracy=%.2f derived=%.2f correct=%d evaluated=%d "
            "(Bug #2 drift — reading derived; row may need backfill)",
            run_id,
            iteration,
            stored,
            derived,
            correct,
            evaluated,
        )

    return derived


# ---------------------------------------------------------------------------
# Canonical "Baseline + Optimized" pair
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunScores:
    """Canonical baseline + optimized pair for a run.

    Wire/UI contract (locks down the bug where a 100% slice probe was being
    rendered as the "Optimized" headline mid-run, see PR description):

    * ``baseline`` is iteration 0's full-scope arbiter-adjusted accuracy.
      ``None`` only before iteration 0 has been written (i.e. before the
      Baseline Evaluation step completes).
    * ``optimized`` is ``max(baseline, best_non_rolled_back_candidate)``
      where candidates are:
      - any iter > 0 row with ``eval_scope == "full"``, AND
      - the iter 0 row with ``eval_scope == "enrichment"`` (post-enrichment
        eval), if present. This lets the headline reflect the
        "baseline 91.7 → optimized 96.2 driven by enrichment" delta even
        when the lever loop short-circuits because enrichment alone met
        thresholds.

      Two important consequences:
      - Optimized is NEVER less than baseline. Regressions don't get
        deployed, so they don't count as the Optimized headline either.
      - When no candidate has been written yet (mid-run after Baseline
        Evaluation), Optimized equals Baseline. The frontend interprets the
        ``best_iteration == 0 AND best_eval_scope == "full"`` signal to
        render "—" with an "Optimization in progress" tooltip and to wire
        the existing convergence-reason copy with "Baseline retained" once
        the run is terminal.
    * ``baseline_iteration`` is ``0`` when baseline exists, else ``None``.
    * ``best_iteration`` is the iteration whose accuracy ``optimized`` came
      from. Tie goes to baseline (returns ``0``) — we only credit a later
      candidate when it strictly exceeds baseline. ``None`` only when
      baseline itself is ``None``.
    * ``best_eval_scope`` disambiguates the ``best_iteration == 0`` case:
      - ``"full"`` and ``best_iteration == 0`` → baseline retained / mid-run.
      - ``"enrichment"`` and ``best_iteration == 0`` → enrichment drove the
        improvement (lever loop may have skipped).
      - ``"full"`` and ``best_iteration > 0`` → lever-loop iteration N drove
        the improvement.
      Defaults to ``"full"`` so existing callers stay compatible.

    Wire format: callers MUST send floats on the 0–100 scale. Pydantic
    validators in ``backend/models.py`` enforce this (PR 2).
    """

    baseline: float | None
    optimized: float | None
    baseline_iteration: int | None
    best_iteration: int | None
    best_eval_scope: str = "full"


def _is_rolled_back(row: dict[str, Any]) -> bool:
    """True iff the iteration was rejected by detect_regressions (or similar).

    Mirrors ``backend.routes.runs._is_rolled_back``: legacy rows written
    before the Tier 1.1 column migration return ``False`` so historical
    dashboards don't suddenly drop iterations from the max() pool.
    """
    val = row.get("rolled_back")
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.strip().lower() in {"true", "t", "1", "yes", "y"}
    return False


def compute_run_scores(
    iter_rows: list[dict[str, Any]] | None,
    *,
    run_id: str | None = None,
    logger: logging.Logger | None = None,
) -> RunScores:
    """Canonical baseline + optimized scores for a run.

    Args:
        iter_rows: All iteration rows for the run, in any order. Each row is
            a Delta-shaped dict (the ``genie_opt_iterations`` SELECT
            result). ``None`` / empty → ``RunScores(None, None, None, None)``.
        run_id: Only used for drift-log identification.
        logger: Optional logger for drift lines emitted by
            :func:`derived_accuracy`.

    Returns:
        :class:`RunScores`. See class docstring for the full contract.

    The selection algorithm:

    1. Filter to ``eval_scope == "full"`` rows for baseline derivation.
       Slice/p0/held-out probes evaluate on a tiny subset and routinely
       show 100% — they MUST NOT contribute to the headline.
    2. Iteration 0 (full scope) is always retained, even if some bug stamped
       ``rolled_back=true`` on it. Baseline is the floor.
    3. Candidates for ``optimized`` are:
       - rows with ``eval_scope == "full"`` AND ``iteration > 0``, plus
       - the iter 0 row with ``eval_scope == "enrichment"`` (post-enrichment
         eval). It can win because enrichment may have already mutated the
         space enough to clear thresholds before the lever loop runs.
       Drop ``rolled_back == true`` rows from this candidate pool — they
       were rejected by the regression detector and never deployed.
    4. Baseline = ``derived_accuracy(iter_0_full_row)``.
    5. Best candidate = max over the candidate pool. If its accuracy
       strictly exceeds baseline, that row drives ``optimized``.
       Tie-break: lowest iteration number, then full scope before
       enrichment scope (matches the existing ``promote_best_model``
       earliest-plateau preference).
    6. ``optimized = max(baseline, best_candidate)`` so the headline is
       never below baseline. (PR description: "regressions don't get posted —
       they should either stay as baseline or an improvement.")
    7. ``best_eval_scope`` reports the scope of the winning candidate
       (``"full"`` or ``"enrichment"``). When baseline is retained the
       value is ``"full"``.
    """
    if not iter_rows:
        return RunScores(None, None, None, None)

    full_rows = [
        r for r in iter_rows
        if str(r.get("eval_scope") or "full").lower() == "full"
    ]
    if not full_rows:
        return RunScores(None, None, None, None)

    iter_zero_rows = [r for r in full_rows if safe_int(r.get("iteration")) == 0]
    iter_zero = iter_zero_rows[0] if iter_zero_rows else None
    baseline = derived_accuracy(
        iter_zero, run_id=run_id, iteration=0, logger=logger,
    )

    if baseline is None:
        # No baseline yet — there is nothing meaningful to call "Optimized"
        # either. The frontend renders "—" with a tooltip in this state.
        return RunScores(None, None, None, None)

    # Candidate pool: scope -> list of (iteration, accuracy).
    # ``"full"`` covers iter > 0; ``"enrichment"`` covers the iter-0
    # post-enrichment row (if persisted). Both pools share the
    # rolled-back filter.
    candidates: list[tuple[int, float, str]] = []
    for row in full_rows:
        it = safe_int(row.get("iteration"))
        if it is None or it <= 0:
            continue
        if _is_rolled_back(row):
            continue
        acc = derived_accuracy(
            row, run_id=run_id, iteration=it, logger=logger,
        )
        if acc is None:
            continue
        candidates.append((it, acc, "full"))

    for row in iter_rows:
        if str(row.get("eval_scope") or "").lower() != "enrichment":
            continue
        it = safe_int(row.get("iteration"))
        if it != 0:
            continue
        if _is_rolled_back(row):
            continue
        acc = derived_accuracy(
            row, run_id=run_id, iteration=0, logger=logger,
        )
        if acc is None:
            continue
        candidates.append((0, acc, "enrichment"))

    if not candidates:
        # Mid-run: Baseline Evaluation finished but no candidate has been
        # accepted yet. Optimized == baseline, best_iteration == 0. The
        # frontend uses ``(best_iteration == 0, best_eval_scope == "full")``
        # to render "—" / "Optimization in progress" while the run is
        # active and "Baseline retained" once the run is terminal.
        return RunScores(
            baseline=baseline,
            optimized=baseline,
            baseline_iteration=0,
            best_iteration=0,
            best_eval_scope="full",
        )

    # Pick the highest accuracy; tie-break on lowest iteration number, then
    # prefer ``"full"`` before ``"enrichment"`` so an iter > 0 lever win
    # always wins over a tied iter-0 enrichment candidate. Matches
    # ``promote_best_model``'s earliest-plateau preference.
    _scope_rank = {"full": 0, "enrichment": 1}
    candidates.sort(
        key=lambda triple: (-triple[1], triple[0], _scope_rank.get(triple[2], 99)),
    )
    best_it, best_acc, best_scope = candidates[0]

    if best_acc > baseline:
        return RunScores(
            baseline=baseline,
            optimized=best_acc,
            baseline_iteration=0,
            best_iteration=best_it,
            best_eval_scope=best_scope,
        )

    # Best candidate didn't exceed baseline — baseline retained.
    return RunScores(
        baseline=baseline,
        optimized=baseline,
        baseline_iteration=0,
        best_iteration=0,
        best_eval_scope="full",
    )


def compute_run_scores_by_run_id(
    iter_rows: list[dict[str, Any]] | None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, RunScores]:
    """Group ``iter_rows`` by ``run_id`` and compute :class:`RunScores` per group.

    Built for the list endpoints (``/activity``, ``/spaces/{id}`` history,
    ``/runs/recent``) so they make ONE Delta query for N runs and still get
    the canonical floor-at-baseline semantics. Pre-fix those endpoints read
    a stored ``best_accuracy`` column that drifted from the per-iteration
    derivation, especially for runs with rolled-back iterations.

    Missing ``run_id`` rows are silently skipped (defensive — should never
    happen but no reason to blow up a list endpoint over one bad row).
    """
    if not iter_rows:
        return {}
    by_run: dict[str, list[dict[str, Any]]] = {}
    for row in iter_rows:
        run_id = row.get("run_id")
        if not run_id:
            continue
        by_run.setdefault(str(run_id), []).append(row)
    return {
        rid: compute_run_scores(rows, run_id=rid, logger=logger)
        for rid, rows in by_run.items()
    }
