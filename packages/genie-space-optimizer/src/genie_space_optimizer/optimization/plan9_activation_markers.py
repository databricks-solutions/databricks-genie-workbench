"""Plan 9 Task 8 + Task 9.1 — GSO_PLAN5_ANCHOR_ACTIVATION_V1 marker.

Marker types:

  * Five IN-DISPATCHER statuses (T8) emitted from
    _dispatch_lever_5b_for_cluster /
    dispatch_lever_6_with_intent. One per AG-iteration pair that
    enters dispatch; carries the typed Plan-5 outcome.

      - PLAN5_INTENT_INVOKED
      - PLAN5_INTENT_DECLINED
      - PLAN5_INTENT_VALIDATOR_REJECTED
      - PLAN5_INTENT_ROUTED
      - PLAN5_INTENT_MATERIALIZED

  * Three HARNESS-LEVEL statuses (T9.1) emitted from the per-AG
    loop in harness.py. Cover AG-iteration pairs that never reach
    a dispatcher AND record the hand-off when they do.

      - ANCHOR_FORBIDDEN_SET_DROPPED  — T9 pre-generation filter
        dropped the AG.
      - ANCHOR_COLLISION_GUARD_DROPPED — legacy collision-pair
        guard dropped the AG.
      - ANCHOR_ENTERED_PLAN5_DISPATCH — harness handed the AG to
        the Plan-5 pipeline; followed by exactly one in-dispatcher
        marker for this AG.

Postmortem invariant: every AG-iteration pair MUST produce
EXACTLY ONE harness-level marker. AG-iteration pairs that enter
dispatch produce EXACTLY ONE additional in-dispatcher marker.
"""
from __future__ import annotations

from enum import StrEnum

from genie_space_optimizer.optimization.run_analysis_contract import (
    marker_line,
)


class ActivationStatus(StrEnum):
    # ─── In-dispatcher statuses (T8) ───────────────────────────
    # Emitted from _dispatch_lever_5b_for_cluster /
    # dispatch_lever_6_with_intent for AGs that enter Plan-5
    # dispatch. Carry the typed Plan-5 outcome.
    PLAN5_INTENT_INVOKED = "plan5_intent_invoked"
    PLAN5_INTENT_DECLINED = "plan5_intent_declined"
    PLAN5_INTENT_VALIDATOR_REJECTED = "plan5_intent_validator_rejected"
    PLAN5_INTENT_ROUTED = "plan5_intent_routed"
    PLAN5_INTENT_MATERIALIZED = "plan5_intent_materialized"
    # ─── Harness-level statuses (T9.1) ─────────────────────────
    # Emitted from the per-AG harness loop. Cover the AG-iteration
    # pairs that never reach a dispatcher (forbidden-set filter
    # drop, collision-guard drop) AND record when the harness
    # successfully hands an AG to the Plan-5 pipeline. The latter
    # is followed by exactly one in-dispatcher marker for that AG.
    ANCHOR_FORBIDDEN_SET_DROPPED = "anchor_forbidden_set_dropped"
    ANCHOR_COLLISION_GUARD_DROPPED = "anchor_collision_guard_dropped"
    ANCHOR_ENTERED_PLAN5_DISPATCH = "anchor_entered_plan5_dispatch"


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
