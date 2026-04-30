"""Deterministic replay of static judge verdicts through optimizer policy.

This module intentionally avoids Genie, Spark, MLflow, SQL execution, and LLM
calls. It composes the pure policy helpers used by the live harness so unit
tests can prove judge verdicts are translated into RCA control behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    decide_control_plane_acceptance,
    decide_quarantine_continuation,
    target_qids_from_action_group,
)
from genie_space_optimizer.optimization.patch_selection import (
    select_target_aware_causal_patch_cap,
)
from genie_space_optimizer.optimization.proposal_grounding import (
    instruction_patch_scope_is_safe,
    patch_blast_radius_is_safe,
    proposal_is_defect_compatible,
)


@dataclass(frozen=True)
class StaticJudgeReplayResult:
    target_qids: tuple[str, ...]
    kept_proposals: list[dict[str, Any]]
    dropped_proposals: list[dict[str, Any]]
    kept_patches: list[dict[str, Any]]
    dropped_patches: list[dict[str, Any]]
    patch_cap_decisions: list[dict[str, Any]]
    acceptance: ControlPlaneAcceptance
    quarantine_decision: dict[str, Any] | None


def _proposal_id(item: dict[str, Any]) -> str:
    return str(
        item.get("proposal_id")
        or item.get("expanded_patch_id")
        or item.get("source_proposal_id")
        or item.get("id")
        or "?"
    )


def _proposal_to_patch(proposal: dict[str, Any]) -> dict[str, Any]:
    patch = dict(proposal)
    if "type" not in patch:
        patch["type"] = patch.get("patch_type")
    return patch


def run_static_judge_replay(
    *,
    baseline_accuracy: float,
    candidate_accuracy: float,
    baseline_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    action_group: dict[str, Any],
    source_clusters: list[dict[str, Any]],
    proposals: list[dict[str, Any]],
    patches: list[dict[str, Any]] | None = None,
    max_patches: int = 3,
    min_gain_pp: float = 1.0,
    max_new_hard_regressions: int = 1,
    max_new_passing_to_hard_regressions: int = 0,
    protected_qids: tuple[str, ...] = (),
    quarantined_qids: set[str] | None = None,
    unresolved_patchable_qids: set[str] | None = None,
    hard_cluster_count_after_prune: int = 0,
    soft_cluster_count_after_prune: int = 0,
) -> StaticJudgeReplayResult:
    target_qids = target_qids_from_action_group(action_group, source_clusters)

    kept_proposals: list[dict[str, Any]] = []
    dropped_proposals: list[dict[str, Any]] = []
    for proposal in proposals:
        decision = proposal_is_defect_compatible(proposal)
        if decision["compatible"]:
            kept_proposals.append(dict(proposal))
        else:
            dropped = dict(proposal)
            dropped["_drop_reason"] = decision["reason"]
            dropped_proposals.append(dropped)

    candidate_patches = (
        [dict(p) for p in patches]
        if patches is not None
        else [_proposal_to_patch(p) for p in kept_proposals]
    )

    gate_kept: list[dict[str, Any]] = []
    dropped_patches: list[dict[str, Any]] = []
    for patch in candidate_patches:
        blast = patch_blast_radius_is_safe(
            patch,
            ag_target_qids=target_qids,
            max_outside_target=0,
        )
        if not blast["safe"]:
            dropped = dict(patch)
            dropped["_drop_reason"] = blast["reason"]
            dropped_patches.append(dropped)
            continue

        scope = instruction_patch_scope_is_safe(
            patch,
            ag_target_qids=target_qids,
        )
        if not scope["safe"]:
            dropped = dict(patch)
            dropped["_drop_reason"] = scope["reason"]
            dropped_patches.append(dropped)
            continue

        gate_kept.append(dict(patch))

    kept_patches, patch_cap_decisions = select_target_aware_causal_patch_cap(
        gate_kept,
        target_qids=target_qids,
        max_patches=max_patches,
    )

    acceptance = decide_control_plane_acceptance(
        baseline_accuracy=baseline_accuracy,
        candidate_accuracy=candidate_accuracy,
        target_qids=target_qids,
        pre_rows=baseline_rows,
        post_rows=candidate_rows,
        min_gain_pp=min_gain_pp,
        max_new_hard_regressions=max_new_hard_regressions,
        max_new_passing_to_hard_regressions=max_new_passing_to_hard_regressions,
        protected_qids=protected_qids,
    )

    quarantine_decision = None
    if quarantined_qids is not None or unresolved_patchable_qids is not None:
        quarantine_decision = decide_quarantine_continuation(
            quarantined_qids=quarantined_qids or set(),
            unresolved_patchable_qids=unresolved_patchable_qids or set(),
            hard_cluster_count_after_prune=hard_cluster_count_after_prune,
            soft_cluster_count_after_prune=soft_cluster_count_after_prune,
        )

    return StaticJudgeReplayResult(
        target_qids=target_qids,
        kept_proposals=kept_proposals,
        dropped_proposals=dropped_proposals,
        kept_patches=kept_patches,
        dropped_patches=dropped_patches,
        patch_cap_decisions=patch_cap_decisions,
        acceptance=acceptance,
        quarantine_decision=quarantine_decision,
    )
