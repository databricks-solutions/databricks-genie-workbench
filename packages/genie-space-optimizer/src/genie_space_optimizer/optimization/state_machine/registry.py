"""Production registry: maps FunnelStage → tuple of registered transformers.

Single source of truth for what the StateMachine does in production.
Phase 3 appends escalation_ladder at PROPOSED and adds evaluated_gate
+ acceptance_gate at APPLIED and EVALUATED respectively. Phase 5
deletes the legacy lane that runs alongside; the registry alone
defines behaviour.
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
from genie_space_optimizer.optimization.state_machine.transformers.escalation_ladder import (
    escalation_ladder,
)
from genie_space_optimizer.optimization.state_machine.transformers.narrow_replacement_gate import (
    narrow_replacement_gate,
)
from genie_space_optimizer.optimization.state_machine.transformers.routing_gate import (
    routing_gate,
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
    FunnelStage.CLUSTERED:     (routing_gate, plan11_stage3_synthesis),
    FunnelStage.PROPOSED:      (structural_repair_gate,),  # P3 appends escalation_ladder
    FunnelStage.NORMALIZED:    (blast_radius_batch,),
    FunnelStage.APPLYABLE:     (applier_gate,),
    # APPLIED → EVALUATED → ACCEPTED transformers land in Phase 3 PR 3.3.
}


# Phase 3 update:
#   * escalation_ladder runs immediately after structural_repair_gate
#     at PROPOSED so a failed structural check triggers the softer
#     artifact in the same orchestrator step.
#   * narrow_replacement_gate runs immediately after blast_radius_batch
#     at NORMALIZED so a collateral-risk drop can hand off to the
#     LLM-driven narrow-replacement flow in the same orchestrator step.
#   * evaluated_gate runs at APPLIED to record post-apply eval scores.
#   * acceptance_gate runs at EVALUATED to decide accept-or-rollback.
PHASE3_REGISTRY = {
    **PHASE2_REGISTRY,
    FunnelStage.PROPOSED:   (structural_repair_gate, escalation_ladder),
    FunnelStage.NORMALIZED: (blast_radius_batch, narrow_replacement_gate),
    FunnelStage.APPLIED:    (evaluated_gate,),
    FunnelStage.EVALUATED:  (acceptance_gate,),
}


def build_production_state_machine() -> StateMachine:
    """Return the production StateMachine wired with the current phase's transformers."""
    return StateMachine(transformers=PHASE3_REGISTRY)
