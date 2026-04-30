"""Leak-safe SQL-shape deltas for rejected lever-loop candidates."""

from __future__ import annotations

import re
from typing import Any


_EQUALITY_FILTER_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'([^']*)'",
    re.IGNORECASE,
)
_DATE_SUB_RE = re.compile(
    r"DATE_SUB\s*\(\s*CURRENT_DATE\s*\(\s*\)\s*,\s*(\d+)\s*\)",
    re.IGNORECASE,
)


def _normalize_sql(sql: str) -> str:
    return " ".join(str(sql or "").replace("`", "").split())


def _equality_filters(sql: str) -> set[str]:
    norm = _normalize_sql(sql)
    return {f"{m.group(1)} = '{m.group(2)}'" for m in _EQUALITY_FILTER_RE.finditer(norm)}


def _date_sub_days(sql: str) -> set[int]:
    return {int(m.group(1)) for m in _DATE_SUB_RE.finditer(_normalize_sql(sql))}


def compute_sql_shape_delta(
    *,
    target_qid: str,
    accepted_sql: str,
    candidate_sql: str,
    ground_truth_sql: str,
    accepted_row_count: int | None = None,
    candidate_row_count: int | None = None,
) -> dict[str, Any]:
    """Return a compact summary of candidate movement toward ground truth.

    Surfaces ``improved`` (deltas the candidate already accomplished) and
    ``remaining`` (still-untried shape changes). Designed to feed strategist
    memory across rejected ActionGroups without leaking benchmark text.
    """
    accepted_norm = _normalize_sql(accepted_sql)
    candidate_norm = _normalize_sql(candidate_sql)
    ground_truth_norm = _normalize_sql(ground_truth_sql)

    improved: list[str] = []
    remaining: list[str] = []

    removed_filters = sorted(
        f for f in _equality_filters(accepted_norm)
        if f not in _equality_filters(candidate_norm)
        and f not in _equality_filters(ground_truth_norm)
    )
    improved.extend(f"removed_filter: {f}" for f in removed_filters)

    if accepted_row_count is not None and candidate_row_count is not None:
        if int(candidate_row_count) != int(accepted_row_count):
            improved.append(
                f"row_count: {int(accepted_row_count)} -> {int(candidate_row_count)}"
            )

    candidate_days = _date_sub_days(candidate_norm)
    gt_days = _date_sub_days(ground_truth_norm)
    if candidate_days and gt_days and candidate_days != gt_days:
        c = sorted(candidate_days)[0]
        g = sorted(gt_days)[0]
        remaining.append(f"date_window: {c}_vs_{g}")

    has_between = " BETWEEN " in candidate_norm.upper()
    gt_uses_gte = " >= " in ground_truth_norm.upper()
    if has_between and gt_uses_gte:
        remaining.append("predicate_form: between_vs_gte")

    next_hint = ""
    if any(r.startswith("date_window:") for r in remaining):
        day = sorted(gt_days)[0] if gt_days else 30
        next_hint = (
            f"teach recent_window_days archetype with DATE_SUB(CURRENT_DATE(), {day})"
        )

    return {
        "target_qid": str(target_qid),
        "improved": improved,
        "remaining": remaining,
        "next_hint": next_hint,
    }
