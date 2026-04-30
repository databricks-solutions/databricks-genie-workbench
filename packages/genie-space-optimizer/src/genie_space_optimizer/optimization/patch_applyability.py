"""Dry-run applyability contract for lever-loop patch selection.

This module is intentionally pure from the caller's perspective: it deep-copies
the provided metadata snapshot, renders a patch, and attempts to apply the
rendered action to the copy. It never calls the Genie API and never mutates the
caller's snapshot.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any


_COLUMN_PATCH_TYPES = frozenset(
    {
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "hide_column",
        "unhide_column",
        "rename_column_alias",
    }
)


@dataclass(frozen=True)
class PatchApplyabilityDecision:
    proposal_id: str
    expanded_patch_id: str
    patch_type: str
    target: str
    table: str
    column: str
    applyable: bool
    reason: str
    error_excerpt: str = ""


def _patch_id(patch: dict[str, Any]) -> str:
    return str(
        patch.get("expanded_patch_id")
        or patch.get("id")
        or patch.get("proposal_id")
        or ""
    )


def _patch_type(patch: dict[str, Any]) -> str:
    return str(patch.get("type") or patch.get("patch_type") or "")


def _target(patch: dict[str, Any]) -> str:
    return str(
        patch.get("target")
        or patch.get("target_object")
        or patch.get("target_table")
        or patch.get("table")
        or ""
    )


def _scalar(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _decision(
    *,
    patch: dict[str, Any],
    applyable: bool,
    reason: str,
    table: str = "",
    column: str = "",
    error: str = "",
) -> PatchApplyabilityDecision:
    pid = _patch_id(patch)
    return PatchApplyabilityDecision(
        proposal_id=str(patch.get("proposal_id") or pid),
        expanded_patch_id=pid,
        patch_type=_patch_type(patch),
        target=_target(patch),
        table=table,
        column=column,
        applyable=applyable,
        reason=reason,
        error_excerpt=str(error)[:500] if error else "",
    )


def _action_command(action: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = json.loads(str(action.get("command") or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def check_patch_applyability(
    *,
    patch: dict[str, Any],
    metadata_snapshot: dict[str, Any],
    space_id: str,
) -> PatchApplyabilityDecision:
    """Return whether ``patch`` can mutate ``metadata_snapshot`` in dry-run."""
    patch_type = _patch_type(patch)
    table = _scalar(patch.get("table") or patch.get("target"))
    raw_column = patch.get("column", "")
    column = _scalar(raw_column)

    if patch_type in _COLUMN_PATCH_TYPES:
        if not table:
            return _decision(
                patch=patch,
                applyable=False,
                reason="missing_table",
                table=table,
                column=column,
            )
        if not column or not isinstance(raw_column, str):
            return _decision(
                patch=patch,
                applyable=False,
                reason="invalid_column_target",
                table=table,
                column=column,
            )

    from genie_space_optimizer.optimization.applier import (
        _apply_action_to_config,
        _find_table_in_config,
        render_patch,
    )

    config_copy = copy.deepcopy(metadata_snapshot or {})
    try:
        rendered = render_patch(patch, space_id, config_copy)
    except RuntimeError as exc:
        return _decision(
            patch=patch,
            applyable=False,
            reason="render_validation_error",
            table=table,
            column=column,
            error=str(exc),
        )
    except Exception as exc:
        return _decision(
            patch=patch,
            applyable=False,
            reason="render_exception",
            table=table,
            column=column,
            error=str(exc),
        )

    command = _action_command(rendered)
    if command.get("section") == "column_configs":
        cmd_table = _scalar(command.get("table"))
        cmd_column = _scalar(command.get("column"))
        if not cmd_table:
            return _decision(
                patch=patch,
                applyable=False,
                reason="missing_table",
                table=cmd_table,
                column=cmd_column,
            )
        if not cmd_column:
            return _decision(
                patch=patch,
                applyable=False,
                reason="invalid_column_target",
                table=cmd_table,
                column=cmd_column,
            )
        if _find_table_in_config(config_copy, cmd_table) is None:
            return _decision(
                patch=patch,
                applyable=False,
                reason="missing_table",
                table=cmd_table,
                column=cmd_column,
            )
        table = cmd_table
        column = cmd_column

    try:
        applied = _apply_action_to_config(config_copy, rendered)
    except Exception as exc:
        return _decision(
            patch=patch,
            applyable=False,
            reason="apply_exception",
            table=table,
            column=column,
            error=str(exc),
        )

    if not applied:
        return _decision(
            patch=patch,
            applyable=False,
            reason="apply_action_returned_false",
            table=table,
            column=column,
        )
    return _decision(
        patch=patch,
        applyable=True,
        reason="applyable",
        table=table,
        column=column,
    )


def filter_applyable_patches(
    *,
    patches: list[dict[str, Any]],
    metadata_snapshot: dict[str, Any],
    space_id: str,
) -> tuple[list[dict[str, Any]], list[PatchApplyabilityDecision]]:
    """Return patches that pass dry-run applyability plus all decisions."""
    kept: list[dict[str, Any]] = []
    decisions: list[PatchApplyabilityDecision] = []
    for patch in patches or []:
        decision = check_patch_applyability(
            patch=patch,
            metadata_snapshot=metadata_snapshot,
            space_id=space_id,
        )
        decisions.append(decision)
        if decision.applyable:
            kept.append(patch)
        else:
            patch["_drop_reason"] = decision.reason
            patch["_applyability_error_excerpt"] = decision.error_excerpt
    return kept, decisions
