"""Normalize RCA-generated column proposal shapes before patch expansion."""

from __future__ import annotations

import copy
from typing import Any


_COLUMN_PATCH_TYPES = frozenset({"update_column_description", "add_column_synonym"})


class ProposalShapeError(ValueError):
    """Raised when a producer emits a malformed proposal shape."""


def _is_list_shaped_string(value: str) -> bool:
    s = str(value or "").strip()
    return (s.startswith("[") and s.endswith("]")) or "," in s


def validate_column_proposal_shape(proposal: dict[str, Any]) -> None:
    """Validate producer-side shape for column proposals.

    This is intentionally stricter than ``normalize_column_proposals``.
    Producers must emit one proposal per concrete table/column target; the
    normalizer remains tolerant only as a compatibility backstop.
    """
    patch_type = _patch_type(proposal)
    if patch_type not in _COLUMN_PATCH_TYPES:
        return
    table = proposal.get("table") or proposal.get("target_table")
    column = proposal.get("column") or proposal.get("column_name")
    pid = _proposal_id(proposal) or "<unknown>"
    if not isinstance(table, str) or not table.strip():
        raise ProposalShapeError(
            f"{pid}: column proposal missing scalar table"
        )
    if not isinstance(column, str) or not column.strip():
        raise ProposalShapeError(
            f"{pid}: column proposal missing scalar column"
        )
    if _is_list_shaped_string(column):
        raise ProposalShapeError(
            f"{pid}: column proposal has list-shaped column target {column!r}"
        )
    if _is_list_shaped_string(table):
        raise ProposalShapeError(
            f"{pid}: column proposal has list-shaped table target {table!r}"
        )


def _proposal_id(proposal: dict[str, Any]) -> str:
    return str(proposal.get("proposal_id") or proposal.get("id") or "")


def _patch_type(proposal: dict[str, Any]) -> str:
    return str(proposal.get("patch_type") or proposal.get("type") or "")


def _column_value(proposal: dict[str, Any]) -> Any:
    return (
        proposal.get("column")
        or proposal.get("column_name")
        or proposal.get("target_column")
        or proposal.get("target")
    )


def _table_value(proposal: dict[str, Any]) -> str:
    raw = proposal.get("table") or proposal.get("target_table") or ""
    return str(raw).strip() if raw is not None else ""


def _decision(
    proposal: dict[str, Any],
    *,
    decision: str,
    reason: str,
    output_count: int = 0,
) -> dict[str, Any]:
    return {
        "proposal_id": _proposal_id(proposal),
        "patch_type": _patch_type(proposal),
        "decision": decision,
        "reason": reason,
        "output_count": int(output_count),
    }


def _uc_matches_for_column(
    column: str,
    uc_columns: list[dict[str, Any]],
) -> list[str]:
    matches: list[str] = []
    for row in uc_columns or []:
        if str(row.get("column_name") or "").strip() != column:
            continue
        table = str(
            row.get("table_full_name")
            or row.get("table")
            or row.get("table_name")
            or ""
        ).strip()
        if table and table not in matches:
            matches.append(table)
    return matches


def _resolve_qualified_column(
    value: str,
    uc_columns: list[dict[str, Any]],
) -> tuple[str, str] | None:
    if "." not in value:
        return None
    table_part, column = value.rsplit(".", 1)
    table_part = table_part.strip()
    column = column.strip()
    if not table_part or not column:
        return None
    for row in uc_columns or []:
        row_column = str(row.get("column_name") or "").strip()
        if row_column != column:
            continue
        full_name = str(row.get("table_full_name") or row.get("table") or "").strip()
        table_name = str(row.get("table_name") or "").strip()
        if table_part in {full_name, table_name} or full_name.endswith("." + table_part):
            return full_name or table_part, column
    return table_part, column


def _normalise_one(
    proposal: dict[str, Any],
    *,
    column: str,
    table: str,
    uc_columns: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    qualified = _resolve_qualified_column(column, uc_columns)
    if qualified is not None:
        table, column = qualified
        out = copy.deepcopy(proposal)
        out["table"] = table
        out["column"] = column
        out["target"] = table
        return out, "qualified_column_split"

    # Metric-view-aware fallback: when the column is not present in
    # uc_columns (e.g. it lives behind a metric view definition), check
    # the proposal's own ``metric_view_columns`` enrichment payload.
    if not table:
        mv_columns = proposal.get("metric_view_columns") or []
        for mv_entry in mv_columns:
            if not isinstance(mv_entry, dict):
                continue
            mv_col = str(mv_entry.get("column_name") or "").strip()
            mv_target = str(
                mv_entry.get("metric_view_full_name")
                or mv_entry.get("metric_view")
                or ""
            ).strip()
            if mv_col == column and mv_target:
                out = copy.deepcopy(proposal)
                out["table"] = mv_target
                out["column"] = column
                out["target"] = mv_target
                return out, "resolved_via_metric_view_fallback"

    if not table:
        matches = _uc_matches_for_column(column, uc_columns)
        if len(matches) == 1:
            table = matches[0]
            out = copy.deepcopy(proposal)
            out["table"] = table
            out["column"] = column
            out["target"] = table
            return out, "inferred_table_from_uc_columns"
        if len(matches) > 1:
            return None, "ambiguous_table_for_column"
        return None, "missing_table_for_column"

    out = copy.deepcopy(proposal)
    out["table"] = table
    out["column"] = column
    out["target"] = table
    return out, "already_concrete"


def normalize_column_proposals(
    proposals: list[dict[str, Any]],
    *,
    uc_columns: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize RCA column proposals into renderable table/column shapes."""
    output: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    for proposal in proposals or []:
        if _patch_type(proposal) not in _COLUMN_PATCH_TYPES:
            output.append(proposal)
            continue

        raw_column = _column_value(proposal)
        table = _table_value(proposal)
        if raw_column in (None, "", []):
            decisions.append(_decision(proposal, decision="dropped", reason="missing_column"))
            continue

        if isinstance(raw_column, list):
            columns = [str(c).strip() for c in raw_column if str(c).strip()]
            if not columns:
                decisions.append(_decision(proposal, decision="dropped", reason="missing_column"))
                continue
            if len(columns) > 1:
                expanded: list[dict[str, Any]] = []
                for idx, column in enumerate(columns, start=1):
                    child, reason = _normalise_one(
                        proposal,
                        column=column,
                        table=table,
                        uc_columns=uc_columns,
                    )
                    if child is None:
                        decisions.append(
                            _decision(proposal, decision="dropped", reason=reason)
                        )
                        continue
                    pid = _proposal_id(proposal)
                    child["proposal_id"] = f"{pid}#col{idx}" if pid else f"col{idx}"
                    child["source_proposal_id"] = pid
                    expanded.append(child)
                output.extend(expanded)
                decisions.append(
                    _decision(
                        proposal,
                        decision="expanded",
                        reason="multi_column_fanout",
                        output_count=len(expanded),
                    )
                )
                continue
            raw_column = columns[0]

        if not isinstance(raw_column, str):
            decisions.append(_decision(proposal, decision="dropped", reason="invalid_column_target"))
            continue

        child, reason = _normalise_one(
            proposal,
            column=raw_column.strip(),
            table=table,
            uc_columns=uc_columns,
        )
        if child is None:
            decisions.append(_decision(proposal, decision="dropped", reason=reason))
            continue
        output.append(child)
        if reason != "already_concrete":
            decisions.append(_decision(proposal, decision="normalized", reason=reason, output_count=1))

    return output, decisions
