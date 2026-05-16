"""Canonicalize Stage-2 proposals so every adapter emits the same shape.

Contract (verbatim from the contract-first hardening plan):

  * ``patch_type`` is a non-empty string — empty values trigger
    ``narrow_skipped_no_original_patch_type`` downstream.
  * ``target`` is the per-proposal target object (table-column, MV-column,
    TVF, etc.). Empty string is allowed for AG-scoped proposals
    (Lever 5a's merged instruction document).
  * ``provenance.skill_id`` is set so the projection step can route the
    proposal back to its legacy lever key.

The helper strips the underscore-prefixed aliases (``_patch_type``,
``_target``) that some Stage-2 adapters historically emitted.
"""

from __future__ import annotations

from typing import Any


_UNDERSCORE_ALIASES = {
    "_patch_type": "patch_type",
    "_target": "target",
}


def canonicalize_stage_2_proposal(
    proposal: Any,
    *,
    skill_id: str,
    target: str,
    patch_type: str,
) -> dict:
    """Return a dict copy of ``proposal`` with canonical keys.

    See module docstring for the contract.
    """
    if not isinstance(proposal, dict):
        raise TypeError(
            f"canonicalize_stage_2_proposal expects a dict, got "
            f"{type(proposal).__name__}"
        )
    if not patch_type:
        raise ValueError(
            "canonicalize_stage_2_proposal: patch_type must be a "
            "non-empty string (empty patch_type triggers "
            "narrow_skipped_no_original_patch_type downstream)"
        )

    out = dict(proposal)
    for underscore_key, canonical_key in _UNDERSCORE_ALIASES.items():
        if underscore_key in out:
            out.setdefault(canonical_key, out[underscore_key])
            del out[underscore_key]

    out["patch_type"] = patch_type
    out["target"] = target

    provenance = dict(out.get("provenance") or {})
    provenance.setdefault("skill_id", skill_id)
    out["provenance"] = provenance

    return out
