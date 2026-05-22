"""Plan 11 Stage 2 clustering as a BatchTransformer.

Operates on the tuple of DIAGNOSED states in one iteration; returns
each member's state advanced to CLUSTERED with a ClusterMembershipRecord.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
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


# ─── BatchTransformer assembly ─────────────────────────────────────────


def _invoke_stage2_llm(batch_input: Stage2BatchInput, ctx: TransformerContext):
    """Dispatch the actual Stage 2 LLM call. Patchable for tests.

    Production wire-in lands in PR 2.5 (route through
    ``stages.cluster_plan11`` Stage 2 alongside legacy code). Phase 5
    deletes the legacy callsite.
    """
    raise NotImplementedError(
        "Stage 2 LLM dispatch not yet wired to production lever loop. "
        "Tests must monkeypatch _invoke_stage2_llm with a fake response."
    )


@dataclass(frozen=True, slots=True)
class _Plan11Stage2BatchTransformer:
    name: str = "plan11_stage2_clustering"
    from_stage: FunnelStage = FunnelStage.DIAGNOSED
    to_stage_on_success: FunnelStage = FunnelStage.CLUSTERED
    to_stage_on_reject: FunnelStage = FunnelStage.TERMINATED

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        """Single-state adapter so the StateMachine orchestrator's
        per-state ``step()`` can call BatchTransformer implementations.
        Wraps the input in a 1-tuple, runs ``transform_batch``, returns
        the single result."""
        out = self.transform_batch((state,), ctx)
        return out[0]

    def transform_batch(
        self,
        states: tuple[QuestionStateInIteration, ...],
        ctx: TransformerContext,
    ) -> tuple[QuestionStateInIteration, ...]:
        batch_input = build_stage2_batch_input(
            states, forbidden_signatures=ctx.forbidden_signatures,
        )
        response = _invoke_stage2_llm(batch_input, ctx)
        now_ms = int(time.time() * 1000)

        if not getattr(response, "succeeded", False):
            reason = f"abstain: {getattr(response, 'declined', 'unknown')}"
            return tuple(
                s.terminate(
                    transition=StageTransition(
                        from_stage=self.from_stage,
                        to_stage=FunnelStage.TERMINATED,
                        at_ms=now_ms,
                        transformer_name=self.name,
                        transition_kind="batch",
                        reason=reason,
                    ),
                    terminal=TerminalRecord(
                        kind="OPTIMIZER_NO_CANDIDATES",
                        reason=reason,
                        deepest_stage_reached=s.deepest_stage_reached,
                        forbidden_signature="",
                    ),
                )
                for s in states
            )

        # Build qid → ClusterMember lookup from the LLM result.
        parsed = response.parsed_output
        members_by_qid = {m.qid: m for m in getattr(parsed, "members", ())}
        out: list[QuestionStateInIteration] = []
        for s in states:
            m = members_by_qid.get(s.qid)
            if m is None:
                # LLM dropped this QID from clustering — terminate it cleanly.
                out.append(s.terminate(
                    transition=StageTransition(
                        from_stage=self.from_stage,
                        to_stage=FunnelStage.TERMINATED,
                        at_ms=now_ms,
                        transformer_name=self.name,
                        transition_kind="batch",
                        reason="dropped_by_stage2_clustering",
                    ),
                    terminal=TerminalRecord(
                        kind="OPTIMIZER_NO_CANDIDATES",
                        reason="dropped_by_stage2_clustering",
                        deepest_stage_reached=s.deepest_stage_reached,
                        forbidden_signature="",
                    ),
                ))
                continue
            cluster = ClusterMembershipRecord(
                cluster_id=str(m.cluster_id),
                ag_id=str(m.ag_id),
                co_member_qids=tuple(str(q) for q in m.co_member_qids),
                effective_target_lever=0,                 # routing gate writes this
                routing_evidence_kind=str(m.routing_evidence_kind),
            )
            out.append(s.advance(
                to_stage=self.to_stage_on_success,
                transition=StageTransition(
                    from_stage=self.from_stage,
                    to_stage=self.to_stage_on_success,
                    at_ms=now_ms,
                    transformer_name=self.name,
                    transition_kind="batch",
                ),
                clustered=cluster,
            ))
        return tuple(out)


plan11_stage2_clustering = _Plan11Stage2BatchTransformer()
