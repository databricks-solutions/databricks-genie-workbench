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


def _parent_proposal_id(patch: dict[str, Any]) -> str:
    """Return the parent proposal id for a split-child, else empty.

    Track E: only set on patches stamped with ``_split_from`` by
    ``_split_rewrite_instruction_patch``. Non-split patches return ""
    so their signature is unaffected by this addition.
    """
    if patch.get("_split_from"):
        return str(patch.get("parent_proposal_id") or "").strip()
    return ""


def _target_content_fingerprint(patch: dict[str, Any]) -> str:
    """Return a short stable hash of the patch's content-bearing fields.

    Track E: two split-children for the same section but different
    proposed content (instruction text, snippet body, column
    description) must produce different signatures so reflection-as-
    validator does not over-block fresh content. The hash covers the
    union of fields applier reads as the "value" of a patch:

      * ``new_text`` (rewrite/instruction prose)
      * ``value`` (heterogeneous payload — used by ``add_sql_snippet_*``,
        ``add_join_spec``, ``add_measure``, etc.)
      * ``description`` / ``new_description`` (column description patches)
      * ``snippet`` (legacy alias)

    Returns first 16 hex chars of SHA-256 over a JSON-stable
    serialization. Collisions at 64 bits are negligible for the
    in-memory rolled-back-patch set.
    """
    import hashlib
    import json

    payload = {
        "new_text": patch.get("new_text", ""),
        "value": patch.get("value", ""),
        "description": patch.get("description") or patch.get("new_description") or "",
        "snippet": patch.get("snippet", ""),
    }
    try:
        encoded = json.dumps(payload, sort_keys=True, default=str)
    except (TypeError, ValueError):
        # Fall back to repr for non-JSON-serializable payloads.
        encoded = repr(sorted(payload.items()))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def patch_body_fingerprint(patch: dict[str, Any]) -> str:
    """Body-only content fingerprint for intra-AG dedup.

    Cycle 2 Task 1: ``patch_retry_signature`` keys on
    ``(patch_type, target_table, target_column, section_set,
    parent_proposal_id, content_fingerprint)``. That is correct for
    cross-iteration rollback dedup but it cannot collapse two
    proposals that carry identical body text under different
    ``patch_type`` values (the iter-1 ``AG_DECOMPOSED_H001`` pattern
    in run ``2afb0be2-88b6-4832-99aa-c7e78fbc90f7``). This helper
    returns a stable hash keyed on body text alone, normalised by
    stripping leading/trailing whitespace and collapsing internal
    whitespace runs to single spaces. Patches with empty bodies
    yield the empty string.
    """
    body = str(
        patch.get("body")
        or patch.get("content")
        or patch.get("proposal_text")
        or ""
    ).strip()
    if not body:
        return ""
    normalised = " ".join(body.split())
    import hashlib

    return hashlib.sha1(normalised.encode("utf-8")).hexdigest()[:12]


def patch_retry_signature(
    patch: dict[str, Any],
) -> tuple[str, str, str, frozenset[str], str, str]:
    """Return a precise retry key for one patch shape.

    Track E (Phase A burn-down): tuple of ``(patch_type, target_table,
    target_column, section_set, parent_proposal_id, content_fingerprint)``.

    The two new components, both empty for non-split-child patches,
    distinguish split-children of two different parent rewrites that
    happen to touch the same section, AND distinguish two attempts at
    the same parent+section with different proposed content. Reflection-
    as-validator can therefore block exact re-proposals while allowing
    fresh content for the same section.

    Backwards compatibility: callers receive a 6-tuple instead of a
    4-tuple. ``retry_allowed_after_rollback`` and the
    ``_patch_forbidden_signatures`` set in ``harness.py`` use the tuple
    opaquely (set membership only), so the change is transparent to
    them. Callers that index into the tuple positionally must update.
    """
    return (
        _patch_type(patch),
        _target_table(patch),
        _target_column(patch),
        _section_set(patch),
        _parent_proposal_id(patch),
        _target_content_fingerprint(patch),
    )


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
