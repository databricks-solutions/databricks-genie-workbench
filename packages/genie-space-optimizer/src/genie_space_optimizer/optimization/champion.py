"""Shared champion selection for optimization iterations."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

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


def select_champion_row(rows: Iterable[Mapping[str, Any]]) -> dict | None:
    """Return the champion row using the promotion/audit candidate rules.

    Candidate universe:
    - Prefer ``eval_scope='full'``; fall back to all rows only
      when no such rows exist.
    - Existing ``is_champion`` flags are authoritative inside that universe.
    - Otherwise choose the highest-accuracy non-rolled-back row, keeping the
      iteration-0 full baseline as the floor even if it is mislabeled rolled back.
    """
    materialized = [dict(row) for row in rows]
    if not materialized:
        return None

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
