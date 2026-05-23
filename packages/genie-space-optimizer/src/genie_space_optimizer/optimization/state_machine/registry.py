"""Production registry: maps FunnelStage → tuple of registered transformers.

Single source of truth for what the StateMachine does in production.

2026-05-23 SM Cutover Phase 3 — ``routing_gate`` and
``escalation_ladder`` have been quarantined to
``optimization/_legacy/state_machine/transformers/``. The production
state machine is now a single linear sequence: a declined stage
terminates the QID for the iteration; there is no in-SM escalation
ladder mimicking the legacy ``lever 1 → lever 5 → lever 6`` cascade.
See ``docs/llmdrivenarchitecture/v3/2026-05-21-optimizer-state-machine-design.md``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.orchestrator import StateMachine
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.transformers.applier_gate import (
    applier_gate,
)
from genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch import (
    blast_radius_batch,
)
from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (
    plan11_stage2_clustering,
)
from genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate import (
    evaluated_gate,
)
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    plan11_stage1_diagnosis,
)
from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
    narrow_replacement_gate,
)
from genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate import (
    structural_repair_gate,
)
from genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm import (
    plan11_stage3_synthesis,
)


# Phase 2 production wiring.
PHASE2_REGISTRY = {
    FunnelStage.HARD_QID_SEEN: (plan11_stage1_diagnosis,),
    FunnelStage.DIAGNOSED:     (plan11_stage2_clustering,),
    FunnelStage.CLUSTERED:     (plan11_stage3_synthesis,),  # routing_gate quarantined Phase 3
    FunnelStage.PROPOSED:      (structural_repair_gate,),
    FunnelStage.NORMALIZED:    (blast_radius_batch,),
    FunnelStage.APPLYABLE:     (applier_gate,),
}


# Phase 3 production wiring (post-2026-05-23 SM Cutover):
#   * structural_repair_gate decline → terminate (no escalation_ladder).
#   * narrow_replacement_gate runs immediately after blast_radius_batch
#     at NORMALIZED so a collateral-risk drop can hand off to the
#     LLM-driven narrow-replacement flow in the same orchestrator step.
#   * evaluated_gate runs at APPLIED to record post-apply eval scores.
#   * acceptance_gate runs at EVALUATED to decide accept-or-rollback.
PHASE3_REGISTRY = {
    **PHASE2_REGISTRY,
    FunnelStage.NORMALIZED: (blast_radius_batch, narrow_replacement_gate),
    FunnelStage.APPLIED:    (evaluated_gate,),
    FunnelStage.EVALUATED:  (acceptance_gate,),
}


def build_production_state_machine() -> StateMachine:
    """Return the production StateMachine wired with the current phase's transformers."""
    return StateMachine(transformers=PHASE3_REGISTRY)
