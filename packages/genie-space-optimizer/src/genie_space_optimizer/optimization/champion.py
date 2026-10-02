"""Shared champion selection for optimization iterations."""

from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, NamedTuple

from genie_space_optimizer.common.config import MV_ATTACH_PHASE_NAME

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
    ``remaining_failures`` is the post-attach eval's failing-question count,
    when the stage row recorded one.
    """

    eval_run_id: str
    accuracy: float
    remaining_failures: int | None = None


def is_reset_row(row: Mapping[str, Any], reset: BaselineReset) -> bool:
    if not _is_baseline_row(row):
        return False
    return not reset.eval_run_id or str(row.get("eval_run_id") or "") == reset.eval_run_id


# The attach phase's kept verdict (``mv_attach.VERDICT_ATTACHED``); this module
# stays importable without the phase, and a test pins the two equal.
_KEPT_VERDICT = "ATTACHED"


def apply_baseline_reset(
    rows: Iterable[Mapping[str, Any]], reset: BaselineReset | None,
) -> list[dict]:
    """Copies of ``rows`` with the reset's iteration-0 row scored at the reset accuracy."""
    materialized = [dict(row) for row in rows]
    if reset is not None:
        for row in materialized:
            if is_reset_row(row, reset):
                row["overall_accuracy"] = reset.accuracy
    return materialized


def baseline_reset_from_stage_rows(
    records: Iterable[Mapping[str, Any]] | None,
) -> BaselineReset | None:
    """The reset a kept attach gave the loop, from ``genie_opt_stages`` rows oldest first.

    The latest ``MV_ATTACH`` row decides: its verdict must be the kept one and it
    must carry ``post_attach_accuracy`` (MV-D114 d7). Pure: the job and the app
    both call it, the app on rows it read from Lakebase or Delta.
    """
    stage_name = MV_ATTACH_PHASE_NAME.upper()
    latest: Mapping[str, Any] | None = None
    for row in records or ():
        if not isinstance(row, Mapping) or str(row.get("stage") or "").upper() != stage_name:
            continue
        detail = row.get("detail_json")
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except (TypeError, ValueError):
                continue
        if isinstance(detail, Mapping):
            latest = detail
    if latest is None or str(latest.get("verdict") or "").upper() != _KEPT_VERDICT:
        return None
    accuracy = _as_float(latest.get("post_attach_accuracy"))
    if accuracy is None:
        return None
    remaining = latest.get("post_attach_remaining_failures")
    count = (
        remaining
        if isinstance(remaining, int) and not isinstance(remaining, bool) and remaining >= 0
        else None
    )
    return BaselineReset(str(latest.get("baseline_eval_run_id") or ""), accuracy, count)


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
    materialized = apply_baseline_reset(rows, baseline_reset)
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
