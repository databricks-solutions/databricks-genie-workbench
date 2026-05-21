"""Plan 12 — PatchOutcome typed envelope.

Every RepairProposal that exits Stage 3 must terminate in exactly one
PatchOutcome. The outcome is emitted to stdout via
``patch_outcome_marker`` (single canonical point in
``patch_survival_emitter``) and consumed by invariant I22 (outcome
coverage) and the postmortem renderer.

The four terminal kinds are exhaustive:

  * ``APPLIED``                — patch landed; eval ran.
  * ``VALIDATOR_REJECTED``     — applier / SQL / schema validation failed.
  * ``BLAST_RADIUS_REJECTED``  — collateral_qids regressed; narrow tried.
  * ``CONTRACT_FAILED``        — proposal violated the survival contract
                                 (e.g. missing required field at Stage 3
                                 exit; see ``validate_survival_contract``
                                 in ``repair_proposal_typed.py``).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


class PatchOutcomeKind(StrEnum):
    APPLIED = "applied"
    VALIDATOR_REJECTED = "validator_rejected"
    BLAST_RADIUS_REJECTED = "blast_radius_rejected"
    CONTRACT_FAILED = "contract_failed"


@dataclass(frozen=True, slots=True)
class PatchOutcome(JsonRoundTrip):
    intent_id: str
    outcome_kind: PatchOutcomeKind
    terminal_reason: str
    validator_errors: tuple[str, ...]
    collateral_qids: tuple[str, ...]
    narrow_replacement_attempted: bool
    narrow_outcome: str
    applied_patch_id: str

    def to_json(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "outcome_kind": self.outcome_kind.value,
            "terminal_reason": self.terminal_reason,
            "validator_errors": list(self.validator_errors),
            "collateral_qids": list(self.collateral_qids),
            "narrow_replacement_attempted": bool(
                self.narrow_replacement_attempted
            ),
            "narrow_outcome": self.narrow_outcome,
            "applied_patch_id": self.applied_patch_id,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "PatchOutcome":
        return cls(
            intent_id=str(payload["intent_id"]),
            outcome_kind=PatchOutcomeKind(payload["outcome_kind"]),
            terminal_reason=str(payload.get("terminal_reason", "")),
            validator_errors=tuple(
                str(e) for e in payload.get("validator_errors", [])
            ),
            collateral_qids=tuple(
                str(q) for q in payload.get("collateral_qids", [])
            ),
            narrow_replacement_attempted=bool(
                payload.get("narrow_replacement_attempted", False)
            ),
            narrow_outcome=str(payload.get("narrow_outcome", "")),
            applied_patch_id=str(payload.get("applied_patch_id", "")),
        )
