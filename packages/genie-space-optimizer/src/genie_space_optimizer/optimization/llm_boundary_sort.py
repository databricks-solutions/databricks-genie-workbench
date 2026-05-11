"""RCO-7 — canonical sort at LLM-to-deterministic boundaries.

Four helpers, one per documented LLM ingestion boundary:

  * ``sort_action_groups_canonically`` — strategist response and
    AG stage ingestion (canonical key: ``id`` / ``ag_id``).
  * ``sort_proposals_canonically`` — proposal stage ingestion
    (canonical key: ``expanded_patch_id`` / ``proposal_id``).
  * ``sort_patches_canonically`` — patch-selection input
    (canonical key: ``_stable_identity`` from patch_selection).
  * ``canonicalize_arbiter_verdict`` — arbiter judge verdict
    ingestion (sorts list fields lexicographically/numerically).

Every helper:
  - returns a NEW list or dict (never mutates input),
  - uses a stable Python sort, so equal keys preserve incoming
    relative order,
  - tolerates missing canonical-key fields by falling back to an
    empty string sort key.

Scope-control: this module exists solely to neutralize LLM-output
ordering at four named boundaries. Do not add new helpers here for
shapes that are not LLM-derived — add them next to their producer.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping


# ── Action groups ─────────────────────────────────────────────────────


def _ag_key(ag: Mapping[str, Any]) -> str:
    """Canonical sort key for an action group dict."""
    return str(ag.get("id") or ag.get("ag_id") or "")


def sort_action_groups_canonically(
    action_groups: Iterable[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Return a new list of AG dicts sorted by canonical key.

    Canonical key: ``ag.id`` if present, else ``ag.ag_id``, else
    empty string (id-less entries sort to the front).
    """
    if not action_groups:
        return []
    return sorted((dict(a) for a in action_groups), key=_ag_key)


# ── Proposals ─────────────────────────────────────────────────────────


def _proposal_key(proposal: Mapping[str, Any]) -> str:
    """Canonical sort key for a proposal dict."""
    return str(
        proposal.get("expanded_patch_id")
        or proposal.get("proposal_id")
        or ""
    )


def sort_proposals_canonically(
    proposals: Iterable[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Return a new list of proposal dicts sorted by canonical key.

    Canonical key: ``expanded_patch_id`` if present, else
    ``proposal_id``, else empty string.
    """
    if not proposals:
        return []
    return sorted((dict(p) for p in proposals), key=_proposal_key)


# ── Patches ───────────────────────────────────────────────────────────


def _patch_key(patch: Mapping[str, Any]) -> tuple[str, str]:
    """Canonical sort key for a patch dict.

    Primary: ``expanded_patch_id`` / ``proposal_id``.
    Tiebreaker: ``_stable_identity`` from ``patch_selection`` so two
    patches sharing an id but differing in lever / type / target
    fingerprint sort deterministically.
    """
    # Imported lazily to avoid a circular-import risk between
    # llm_boundary_sort and patch_selection in future refactors.
    from genie_space_optimizer.optimization.patch_selection import (
        _stable_identity,
    )
    primary = str(
        patch.get("expanded_patch_id")
        or patch.get("proposal_id")
        or ""
    )
    return (primary, _stable_identity(dict(patch)))


def sort_patches_canonically(
    patches: Iterable[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Return a new list of patch dicts sorted by canonical key."""
    if not patches:
        return []
    return sorted((dict(p) for p in patches), key=_patch_key)


# ── Arbiter verdict ───────────────────────────────────────────────────


_ARBITER_STRING_LIST_FIELDS: tuple[str, ...] = (
    "blame_set",
    "expected_objects",
    "actual_objects",
)
_ARBITER_INT_LIST_FIELDS: tuple[str, ...] = (
    "recommended_levers",
)


def _coerce_int(value: Any) -> int:
    """Coerce an arbiter recommended_levers entry to int. Falls back
    to ``-1`` on unparseable input so non-numeric entries sort to the
    front rather than raising."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def canonicalize_arbiter_verdict(verdict: Mapping[str, Any]) -> dict[str, Any]:
    """Return a new dict with list-valued arbiter fields sorted.

    String-list fields (``blame_set``, ``expected_objects``,
    ``actual_objects``) sort lexicographically.
    Int-list fields (``recommended_levers``) sort numerically.
    Missing fields stay absent. Scalar fields pass through unchanged.
    """
    out: dict[str, Any] = dict(verdict)
    for field in _ARBITER_STRING_LIST_FIELDS:
        if field in out and isinstance(out[field], list):
            out[field] = sorted(str(x) for x in out[field])
    for field in _ARBITER_INT_LIST_FIELDS:
        if field in out and isinstance(out[field], list):
            out[field] = sorted(_coerce_int(x) for x in out[field])
    return out
