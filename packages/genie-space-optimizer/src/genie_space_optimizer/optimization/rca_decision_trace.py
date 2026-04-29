"""Structured trace helpers for RCA lever-loop decisions."""

from __future__ import annotations

from typing import Any


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def summarize_patch_for_trace(patch: dict[str, Any]) -> dict[str, Any]:
    target = (
        patch.get("section_name")
        or patch.get("section")
        or patch.get("column")
        or patch.get("function")
        or patch.get("target")
        or patch.get("target_object")
        or patch.get("display_name")
        or ""
    )
    return {
        "proposal_id": str(patch.get("proposal_id") or patch.get("id") or ""),
        "lever": _as_int(patch.get("lever"), 5),
        "patch_type": patch.get("patch_type") or patch.get("type"),
        "target": str(target),
        "rca_id": patch.get("rca_id"),
        "patch_family": patch.get("patch_family"),
        "target_qids": list(patch.get("target_qids") or []),
        "relevance_score": _as_float(patch.get("relevance_score")),
    }


def patch_cap_decision_rows(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, decision in enumerate(decisions, start=1):
        proposal_id = str(decision.get("proposal_id") or "")
        selected = decision.get("decision") == "selected"
        rows.append({
            "run_id": run_id,
            "iteration": iteration,
            "ag_id": ag_id,
            "decision_order": idx,
            "stage_letter": "I",
            "gate_name": "patch_cap",
            "decision": "accepted" if selected else "dropped",
            "reason_code": None if selected else decision.get("selection_reason"),
            "reason_detail": decision.get("selection_reason"),
            "affected_qids": list(decision.get("target_qids") or []),
            "source_cluster_ids": [],
            "proposal_ids": [proposal_id] if proposal_id else [],
            "proposal_to_patch_map": None,
            "metrics": {
                "selection_reason": decision.get("selection_reason"),
                "rank": decision.get("rank"),
                "relevance_score": _as_float(decision.get("relevance_score")),
                "lever": _as_int(decision.get("lever"), 5),
                "patch_type": decision.get("patch_type"),
                "rca_id": decision.get("rca_id"),
                "target_qids": list(decision.get("target_qids") or []),
            },
        })
    return rows


def format_patch_inventory(
    patches: list[dict[str, Any]],
    *,
    max_rows: int = 8,
) -> str:
    summaries = [summarize_patch_for_trace(p) for p in patches[:max_rows]]
    parts = [
        (
            f"{s['proposal_id']} L{s['lever']} {s['patch_type']} "
            f"target={s['target']} rel={s['relevance_score']:.2f} "
            f"rca={s['rca_id']} qids={s['target_qids']}"
        )
        for s in summaries
    ]
    if len(patches) > max_rows:
        parts.append(f"+{len(patches) - max_rows} more")
    return "; ".join(parts) if parts else "(none)"
