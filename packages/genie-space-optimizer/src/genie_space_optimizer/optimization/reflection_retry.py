"""Precise retry signatures for reflection-as-validator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RetryDecision:
    allowed: bool
    reason: str


def _patch_type(patch: dict[str, Any]) -> str:
    return str(patch.get("type") or patch.get("patch_type") or "").strip()


def _target_table(patch: dict[str, Any]) -> str:
    return str(
        patch.get("target_table")
        or patch.get("table")
        or patch.get("target_object")
        or patch.get("target")
        or ""
    ).strip()


def _target_column(patch: dict[str, Any]) -> str:
    value = patch.get("column") or patch.get("target_column") or ""
    if isinstance(value, str):
        return value.strip()
    return ""


def _section_set(patch: dict[str, Any]) -> frozenset[str]:
    raw = (
        patch.get("structured_section_set")
        or patch.get("instruction_sections")
        or patch.get("instruction_section")
        or patch.get("section_name")
        or []
    )
    if isinstance(raw, str):
        raw = [raw]
    return frozenset(str(v).strip() for v in raw if str(v).strip())


def patch_retry_signature(patch: dict[str, Any]) -> tuple[str, str, str, frozenset[str]]:
    """Return a precise retry key for one patch shape.

    Tuple of (patch_type, target_table, target_column, section_set). Two
    patches with the same key target the exact same column or instruction
    section, so reflection's "this rolled back" memory should compare on
    this key rather than table-only.
    """
    return (_patch_type(patch), _target_table(patch), _target_column(patch), _section_set(patch))


_DIRECT_BEHAVIOR_TYPES = frozenset({
    "add_instruction",
    "update_instruction_section",
    "add_sql_snippet_filter",
    "add_sql_snippet_measure",
    "add_sql_snippet_expression",
    "add_example_sql",
})


def _is_direct_behavior_patch(patch: dict[str, Any]) -> bool:
    ptype = _patch_type(patch)
    try:
        lever = int(patch.get("lever", 0) or 0)
    except (TypeError, ValueError):
        lever = 0
    root = str(patch.get("root_cause") or patch.get("rca_kind") or "").strip()
    return ptype in _DIRECT_BEHAVIOR_TYPES and lever in {5, 6} and bool(root)


def retry_allowed_after_rollback(
    *,
    current_patch: dict[str, Any],
    rolled_back_patches: list[dict[str, Any]],
    rollback_cause: str,
) -> RetryDecision:
    """Decide whether reflection should allow a patch after a rollback.

    A patch with a new precise signature is always allowed (column-level
    or instruction-section-level changes were not the harmful patch).
    Patches that match a previously rolled-back signature are blocked
    unless the rollback cause was infra/insufficient-gain/target-still-hard.
    """
    current_sig = patch_retry_signature(current_patch)
    previous_sigs = {patch_retry_signature(p) for p in rolled_back_patches}
    if current_sig not in previous_sigs:
        if _is_direct_behavior_patch(current_patch):
            return RetryDecision(True, "adds_direct_behavior_shape")
        return RetryDecision(True, "new_precise_patch_signature")
    if rollback_cause in {"infra_schema_failure", "insufficient_gain", "target_still_hard"}:
        return RetryDecision(True, f"retry_allowed_for_{rollback_cause}")
    return RetryDecision(False, "same_harmful_patch_signature")
