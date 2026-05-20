"""Plan 9 Task 8 — PLAN5_ANCHOR_ACTIVATION_V1 marker.

One marker per anchor in every iteration of every lever-loop run.
Status enum:

  * PLAN5_INTENT_INVOKED — Plan 5 synthesizer dispatched; LLM
    call made.
  * PLAN5_INTENT_DECLINED — LLM returned abstain or empty
    RepairProposal (no synthesis).
  * PLAN5_INTENT_VALIDATOR_REJECTED — synthesizer returned a
    RepairProposal but a deterministic validator (patch_body
    shape, blame_set allowlist, leakage firewall) rejected it.
  * PLAN5_INTENT_ROUTED — cross-lever router redirected the
    intent (e.g. L5b intent routed to L6 generator); the routed
    proposal still produces a candidate.
  * PLAN5_INTENT_MATERIALIZED — proposal_dict produced and added
    to all_proposals.

Postmortem invariant: every anchor in every iteration MUST
produce exactly ONE marker. Anchors with no marker are bugs
(test_plan9_activation_markers_all_anchors_covered pins this).
"""
from __future__ import annotations

from enum import StrEnum

from genie_space_optimizer.optimization.run_analysis_contract import (
    marker_line,
)


class ActivationStatus(StrEnum):
    PLAN5_INTENT_INVOKED = "plan5_intent_invoked"
    PLAN5_INTENT_DECLINED = "plan5_intent_declined"
    PLAN5_INTENT_VALIDATOR_REJECTED = "plan5_intent_validator_rejected"
    PLAN5_INTENT_ROUTED = "plan5_intent_routed"
    PLAN5_INTENT_MATERIALIZED = "plan5_intent_materialized"


def emit_plan5_activation(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    status: ActivationStatus,
    reason: str = "",
    patch_type: str = "",
    intent_id: str = "",
) -> None:
    """Emit one PLAN5_ANCHOR_ACTIVATION_V1 marker line to stdout."""
    payload = {
        "optimization_run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "status": str(status.value),
        "reason": str(reason),
        "patch_type": str(patch_type),
        "intent_id": str(intent_id),
    }
    print(marker_line("GSO_PLAN5_ANCHOR_ACTIVATION_V1", payload), flush=True)
