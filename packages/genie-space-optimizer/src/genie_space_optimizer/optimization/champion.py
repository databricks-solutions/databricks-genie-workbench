"""Shared champion selection for optimization iterations."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, NamedTuple

PROMOTION_EVAL_SCOPES: frozenset[str] = frozenset({"full"})


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    try:
        if value != value:
            return False
    except Exception:
        pass
    try:
        return bool(value)
    except TypeError:
        return False


def _eval_scope(row: Mapping[str, Any]) -> str:
    return str(row.get("eval_scope") or "")


def _is_baseline_row(row: Mapping[str, Any]) -> bool:
    return _as_int(row.get("iteration")) == 0 and _eval_scope(row) == "full"


def _is_rolled_back(row: Mapping[str, Any]) -> bool:
    return _truthy(row.get("rolled_back"))


def _is_champion_flag(row: Mapping[str, Any]) -> bool:
    return _truthy(row.get("is_champion"))


def _accuracy_key(row: Mapping[str, Any]) -> float:
    return _as_float(row.get("overall_accuracy")) or 0.0


def _promotion_universe(rows: list[dict]) -> list[dict]:
    scoped = [row for row in rows if _eval_scope(row) in PROMOTION_EVAL_SCOPES]
    return scoped or rows


class BaselineReset(NamedTuple):
    """A kept metric-view attach's re-baseline (MV-D114 d7).

    ``eval_run_id`` is the baseline eval the attach was measured against; it
    names the iteration-0 row the reset applies to.
    """

    eval_run_id: str
    accuracy: float


def _is_reset_row(row: Mapping[str, Any], reset: BaselineReset) -> bool:
    if not _is_baseline_row(row):
        return False
    return not reset.eval_run_id or str(row.get("eval_run_id") or "") == reset.eval_run_id


def select_champion_row(
    rows: Iterable[Mapping[str, Any]],
    *,
    baseline_reset: BaselineReset | None = None,
) -> dict | None:
    """Return the champion row using the promotion/audit candidate rules.

    Candidate universe:
    - Prefer ``eval_scope='full'``; fall back to all rows only
      when no such rows exist.
    - Existing ``is_champion`` flags are authoritative inside that universe.
    - Otherwise choose the highest-accuracy non-rolled-back row, keeping the
      iteration-0 full baseline as the floor even if it is mislabeled rolled back.

    ``baseline_reset`` is the loop's re-baseline after a kept metric-view attach
    (MV-D114 d7). The stored iteration-0 row keeps its pre-attach score (MV-D18),
    so the selection scores the iteration-0 row whose eval the attach was measured
    against at the post-attach accuracy instead; the returned row is a copy
    carrying it. A restart's second iteration-0 row keeps its own score.
    """
    materialized = [dict(row) for row in rows]
    if not materialized:
        return None
    if baseline_reset is not None:
        for row in materialized:
            if _is_reset_row(row, baseline_reset):
                row["overall_accuracy"] = baseline_reset.accuracy

    universe = _promotion_universe(materialized)
    flagged = [row for row in universe if _is_champion_flag(row)]
    if flagged:
        return max(flagged, key=_accuracy_key)

    candidates = [
        row for row in universe if (not _is_rolled_back(row)) or _is_baseline_row(row)
    ]
    if not candidates:
        candidates = universe
    return max(candidates, key=_accuracy_key)
