"""Plan 11 Stage 2 clustering as a BatchTransformer.

Operates on the tuple of DIAGNOSED states in one iteration; returns
each member's state advanced to CLUSTERED with a ClusterMembershipRecord.
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)


@dataclass(frozen=True, slots=True)
class Stage2BatchMember:
    qid: str
    rca_kind_label: str
    evidence_summary: str
    rca_card_id: str


@dataclass(frozen=True, slots=True)
class Stage2BatchInput:
    members: tuple[Stage2BatchMember, ...]
    forbidden_signatures: tuple[str, ...]


def build_stage2_batch_input(
    states: tuple[QuestionStateInIteration, ...],
    *,
    forbidden_signatures: tuple[str, ...],
) -> Stage2BatchInput:
    """Project DIAGNOSED states into Stage 2 LLM batch input."""
    members = tuple(
        Stage2BatchMember(
            qid=s.qid,
            rca_kind_label=s.diagnosed.rca_kind_label if s.diagnosed else "",
            evidence_summary=s.diagnosed.evidence_summary if s.diagnosed else "",
            rca_card_id=s.diagnosed.rca_card_id if s.diagnosed else "",
        )
        for s in states
        if s.diagnosed is not None
    )
    return Stage2BatchInput(members=members, forbidden_signatures=forbidden_signatures)
