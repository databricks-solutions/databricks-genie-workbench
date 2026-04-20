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
"""

from __future__ import annotations

import logging
from typing import Any

from genie_space_optimizer.backend.utils import safe_float, safe_int

__all__ = ["derived_accuracy"]


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
