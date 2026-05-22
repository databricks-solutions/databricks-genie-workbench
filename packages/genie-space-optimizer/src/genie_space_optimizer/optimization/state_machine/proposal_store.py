"""ProposalStore — in-iteration bridge from intent_id → typed RepairProposal.

Stage 3 (synthesize) writes the proposal here keyed by ``intent_id``;
downstream gates (structural repair, blast radius, applier, escalation
ladder) read the full body when they need it. The state machine's
``ProposalAttempt`` record only carries ``intent_id``, so this bridge
exists to avoid bloating ``QuestionStateInIteration`` (which is
serialized to Lakebase at trajectory boundaries) with the fat patch body.

Scope: per-iteration. ``TransformerContext`` carries one store via
``default_factory=ProposalStore``; each iteration's context gets a
fresh instance.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )


@dataclass
class ProposalStore:
    """Mutable in-iteration map: ``intent_id`` → typed ``RepairProposal``."""

    by_intent_id: "dict[str, RepairProposal]" = field(default_factory=dict)

    def remember(self, proposal: "RepairProposal") -> None:
        """Store ``proposal`` under its ``intent_id``. Last write wins —
        narrow-replacement rebuilds may legitimately overwrite."""
        self.by_intent_id[proposal.intent_id] = proposal

    def lookup(self, intent_id: str) -> "RepairProposal | None":
        """Return the stored proposal for ``intent_id``, or ``None``."""
        return self.by_intent_id.get(intent_id)
