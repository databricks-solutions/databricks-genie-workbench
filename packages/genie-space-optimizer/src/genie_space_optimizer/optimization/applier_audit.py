"""Per-patch applier decision audit + cap-vs-applied reconciliation.

Pure helpers. No side effects, no Genie API calls. The harness and the
applier consume these rows for structured logging and downstream gate
diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal


ApplierDecisionStatus = Literal[
    "applied",
    "dropped_validation",
    "dropped_dedupe",
    "dropped_no_op",
    "dropped_exception",
]


@dataclass(frozen=True)
class ApplierDecision:
    proposal_id: str
    parent_proposal_id: str
    expanded_patch_id: str
    lever: int
    patch_type: str
    target_asset: str
    rca_id: str
    target_qids: tuple[str, ...]
    causal_attribution_tier: str
    decision: ApplierDecisionStatus
    reason: str
    error_excerpt: str


def _patch_target_asset(patch: dict) -> str:
    for key in (
        "target_object",
        "target_table",
        "target",
        "patch_target",
        "object_full_name",
    ):
        value = patch.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _patch_target_qids(patch: dict) -> tuple[str, ...]:
    raw = patch.get("_grounding_target_qids") or patch.get("target_qids") or []
    if isinstance(raw, (list, tuple)):
        return tuple(str(q) for q in raw if str(q))
    return ()


def build_applier_decision(
    *,
    patch: dict,
    decision: ApplierDecisionStatus,
    reason: str,
    error: str = "",
) -> ApplierDecision:
    """Construct an immutable audit row for a single applier decision."""
    excerpt = str(error)[:500] if error else ""
    try:
        lever = int(patch.get("lever") or 0)
    except (TypeError, ValueError):
        lever = 0
    return ApplierDecision(
        proposal_id=str(patch.get("proposal_id") or ""),
        parent_proposal_id=str(patch.get("parent_proposal_id") or ""),
        expanded_patch_id=str(patch.get("expanded_patch_id") or patch.get("id") or ""),
        lever=lever,
        patch_type=str(patch.get("type") or patch.get("patch_type") or ""),
        target_asset=_patch_target_asset(patch),
        rca_id=str(patch.get("rca_id") or ""),
        target_qids=_patch_target_qids(patch),
        causal_attribution_tier=str(patch.get("causal_attribution_tier") or ""),
        decision=decision,
        reason=str(reason or ""),
        error_excerpt=excerpt,
    )


@dataclass(frozen=True)
class CapVsAppliedDiff:
    selected_but_not_applied: tuple[str, ...]
    applied_but_not_selected: tuple[str, ...]
    in_agreement: bool


def diff_selected_vs_applied(
    *,
    selected_ids: Iterable[str],
    applied_ids: Iterable[str],
) -> CapVsAppliedDiff:
    """Diff the cap's selected ID set against the applier's applied set."""
    selected = tuple(str(x) for x in (selected_ids or []) if str(x))
    applied = tuple(str(x) for x in (applied_ids or []) if str(x))
    selected_set = set(selected)
    applied_set = set(applied)
    missing = tuple(x for x in selected if x not in applied_set)
    extra = tuple(x for x in applied if x not in selected_set)
    return CapVsAppliedDiff(
        selected_but_not_applied=missing,
        applied_but_not_selected=extra,
        in_agreement=not missing and not extra,
    )
