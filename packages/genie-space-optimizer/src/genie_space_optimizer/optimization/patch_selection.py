"""Patch selection helpers for RCA-driven action-group bundles."""

from __future__ import annotations

from typing import Any

_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


def _proposal_id(patch: dict[str, Any], index: int) -> str:
    return str(
        patch.get("proposal_id")
        or patch.get("expanded_patch_id")
        or patch.get("source_proposal_id")
        or patch.get("parent_proposal_id")
        or patch.get("id")
        or f"idx_{index}"
    )


def _lever(patch: dict[str, Any]) -> int:
    try:
        return int(patch.get("lever", 5))
    except (TypeError, ValueError):
        return 5


def _score(patch: dict[str, Any], name: str, default: float = 0.0) -> float:
    try:
        return float(patch.get(name, default) or default)
    except (TypeError, ValueError):
        return default


def _risk_rank(patch: dict[str, Any]) -> int:
    return _RISK_ORDER.get(str(patch.get("risk_level", "medium")).lower(), 1)


def select_causal_patch_cap(
    patches: list[dict[str, Any]],
    *,
    max_patches: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Select a capped patch bundle with causal relevance as the primary key.

    Diversity is intentionally a tiebreaker. A lower-relevance patch should
    never displace a higher-relevance RCA patch merely because it belongs to
    a different lever or instruction section.
    """
    if max_patches <= 0:
        return [], [
            {
                "proposal_id": _proposal_id(p, idx),
                "decision": "dropped",
                "selection_reason": "max_patches_zero",
                "rank": None,
            }
            for idx, p in enumerate(patches)
        ]
    if len(patches) <= max_patches:
        return list(patches), [
            {
                "proposal_id": _proposal_id(p, idx),
                "decision": "selected",
                "selection_reason": "under_cap",
                "rank": idx + 1,
                "relevance_score": _score(p, "relevance_score"),
                "lever": _lever(p),
            }
            for idx, p in enumerate(patches)
        ]

    remaining: list[tuple[int, dict[str, Any]]] = list(enumerate(patches))
    selected: list[tuple[int, dict[str, Any], str]] = []
    seen_levers: set[int] = set()

    while remaining and len(selected) < max_patches:
        def sort_key(item: tuple[int, dict[str, Any]]) -> tuple:
            idx, patch = item
            lever = _lever(patch)
            relevance = _score(patch, "relevance_score")
            diversity_bonus = 1 if lever not in seen_levers else 0
            return (
                -relevance,
                -diversity_bonus,
                _risk_rank(patch),
                -_score(patch, "confidence"),
                -_score(patch, "net_impact"),
                idx,
            )

        best = min(remaining, key=sort_key)
        remaining.remove(best)
        _, patch = best
        reason = (
            "highest_causal_relevance"
            if not selected or _score(patch, "relevance_score") > 0
            else "stable_fallback"
        )
        selected.append((best[0], patch, reason))
        seen_levers.add(_lever(patch))

    selected_ids = {_proposal_id(p, idx) for idx, p, _reason in selected}
    rank_by_id = {
        _proposal_id(p, idx): rank
        for rank, (idx, p, _reason) in enumerate(selected, start=1)
    }
    reason_by_id = {
        _proposal_id(p, idx): reason
        for idx, p, reason in selected
    }
    decisions: list[dict[str, Any]] = []
    for idx, patch in enumerate(patches):
        pid = _proposal_id(patch, idx)
        selected_flag = pid in selected_ids
        decisions.append({
            "proposal_id": pid,
            "decision": "selected" if selected_flag else "dropped",
            "selection_reason": (
                reason_by_id[pid] if selected_flag else "lower_causal_rank"
            ),
            "rank": rank_by_id.get(pid),
            "relevance_score": _score(patch, "relevance_score"),
            "lever": _lever(patch),
            "patch_type": patch.get("patch_type") or patch.get("type"),
            "rca_id": patch.get("rca_id"),
            "target_qids": list(patch.get("target_qids") or []),
        })

    return [p for _idx, p, _reason in selected], decisions


def _target_qids(patch: dict[str, Any]) -> tuple[str, ...]:
    raw: list = []
    raw.extend(patch.get("_grounding_target_qids") or [])
    raw.extend(patch.get("target_qids") or [])
    return tuple(dict.fromkeys(str(q) for q in raw if str(q)))


def select_target_aware_causal_patch_cap(
    patches: list[dict[str, Any]],
    *,
    target_qids: tuple[str, ...],
    max_patches: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Cap patches while preserving at least one patch per target QID.

    For each target QID in order, picks the highest-relevance patch
    targeting that QID (by relevance, risk, confidence, then declaration
    order). Remaining capacity is filled by the global causal-relevance
    ranking from ``select_causal_patch_cap``. This prevents the cap from
    dropping the only patch covering a secondary target QID just because
    the primary target dominates the relevance leaderboard.
    """
    if max_patches <= 0:
        return select_causal_patch_cap(patches, max_patches=max_patches)
    if len(patches) <= max_patches:
        return select_causal_patch_cap(patches, max_patches=max_patches)

    target_set = tuple(dict.fromkeys(str(q) for q in target_qids if str(q)))
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    for target in target_set:
        if len(selected) >= max_patches:
            break
        # Skip targets already covered by an already-selected patch.
        if any(target in _target_qids(p) for p in selected):
            continue
        candidates = [
            (idx, patch)
            for idx, patch in enumerate(patches)
            if target in _target_qids(patch)
            and _proposal_id(patch, idx) not in selected_ids
        ]
        if not candidates:
            continue
        idx, patch = min(
            candidates,
            key=lambda item: (
                -_score(item[1], "relevance_score"),
                _risk_rank(item[1]),
                -_score(item[1], "confidence"),
                item[0],
            ),
        )
        selected.append(patch)
        selected_ids.add(_proposal_id(patch, idx))

    remaining = [
        patch
        for idx, patch in enumerate(patches)
        if _proposal_id(patch, idx) not in selected_ids
    ]
    if len(selected) < max_patches and remaining:
        filler, _ = select_causal_patch_cap(
            remaining,
            max_patches=max_patches - len(selected),
        )
        selected.extend(filler)
        for fp in filler:
            try:
                fp_idx = patches.index(fp)
            except ValueError:
                continue
            selected_ids.add(_proposal_id(fp, fp_idx))

    rank_by_pid: dict[str, int] = {}
    for rank, patch in enumerate(selected, start=1):
        try:
            idx = patches.index(patch)
        except ValueError:
            idx = rank - 1
        rank_by_pid[_proposal_id(patch, idx)] = rank

    selected_pid_set = set(rank_by_pid)
    decisions: list[dict[str, Any]] = []
    for idx, patch in enumerate(patches):
        pid = _proposal_id(patch, idx)
        selected_flag = pid in selected_pid_set
        decisions.append({
            "proposal_id": pid,
            "decision": "selected" if selected_flag else "dropped",
            "selection_reason": (
                "target_coverage" if selected_flag else "lower_causal_rank"
            ),
            "rank": rank_by_pid.get(pid),
            "relevance_score": _score(patch, "relevance_score"),
            "lever": _lever(patch),
            "patch_type": patch.get("patch_type") or patch.get("type"),
            "rca_id": patch.get("rca_id"),
            "target_qids": list(_target_qids(patch)),
        })

    return selected, decisions
