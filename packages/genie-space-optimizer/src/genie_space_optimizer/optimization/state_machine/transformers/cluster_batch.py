"""Plan 11 Stage 2 clustering as a BatchTransformer.

Operates on the tuple of DIAGNOSED states in one iteration; returns
each member's state advanced to CLUSTERED with a ClusterMembershipRecord.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

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
    # Trial 18 Step 3 — sibling channel to ``forbidden_signatures``.
    # Stage 2 / Stage 3 LLM prompts MUST treat these as "do not
    # re-propose this (lever, patch_type, rca_kind, behavior) shape
    # alone — reinforce or pivot" — distinct from
    # ``forbidden_signatures`` which carries hard-rejection shapes.
    # Defaults to empty tuple so existing callers / test fixtures
    # don't need updating.
    insufficient_repair_signatures: tuple[str, ...] = ()
    # Trial 29 W29.1 — kit-forced inert-patch history. Stage 3
    # synthesis renders the per-(qid, rca_kind) rejected mechanisms
    # so the LLM picks from
    # ``_structural_fix_mechanisms(rca) - rejected``. Typed as
    # ``tuple[Any, ...]`` to avoid a circular import; callers pass
    # ``tuple[InertMechanismHistory, ...]``.
    inert_mechanism_history: tuple[Any, ...] = ()


def build_stage2_batch_input(
    states: tuple[QuestionStateInIteration, ...],
    *,
    forbidden_signatures: tuple[str, ...],
    insufficient_repair_signatures: tuple[str, ...] = (),
    inert_mechanism_history: tuple[Any, ...] = (),
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
    return Stage2BatchInput(
        members=members,
        forbidden_signatures=forbidden_signatures,
        insufficient_repair_signatures=insufficient_repair_signatures,
        inert_mechanism_history=inert_mechanism_history,
    )


# ─── BatchTransformer assembly ─────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _ClusterMember:
    """Adapter shape matching what ``transform_batch`` reads from
    ``response.parsed_output.members``."""
    qid: str
    cluster_id: str
    ag_id: str
    co_member_qids: tuple[str, ...]
    routing_evidence_kind: str


@dataclass(frozen=True, slots=True)
class _ClusterParsed:
    members: tuple[_ClusterMember, ...]


@dataclass(frozen=True, slots=True)
class _ClusterResponse:
    succeeded: bool
    parsed_output: _ClusterParsed | None = None
    declined: str | None = None


def _state_to_per_qid_diagnosis(state: QuestionStateInIteration):
    """Reverse-project a DIAGNOSED state into a ``PerQidDiagnosis``.

    ``DiagnosisRecord`` does not carry ``generated_sql_issue`` or
    ``blame_set`` — those are synthesized empty here. The clustering
    LLM mostly uses ``rca_kind_label`` + ``evidence_summary``.
    """
    # Lazy import to avoid loading the stages package at module import time.
    from genie_space_optimizer.optimization.stages.plan11_types import (
        PerQidDiagnosis,
    )
    d = state.diagnosed
    return PerQidDiagnosis(
        qid=state.qid,
        rca_kind_label=d.rca_kind_label,
        observed_failure=d.observed_failure,
        generated_sql_issue="",
        expected_sql_shape=d.expected_sql_shape,
        blame_set=(),
        evidence_summary=d.evidence_summary,
        confidence=d.confidence,
        # Trial 19 B5 — propagate the LLM-emitted repair intent into
        # the Stage 2 input so the cluster builder + Stage 3 prompt see
        # it verbatim. ``getattr`` keeps replays of pre-Trial-19
        # ``DiagnosisRecord`` rows byte-stable (the default empty
        # string is preserved).
        intended_patch_shape=str(
            getattr(d, "intended_patch_shape", "") or ""
        ),
    )


def _failure_cluster_to_members(cluster, all_member_qids: tuple[str, ...]):
    """Fan one FailureCluster out into per-QID adapter members.

    ``ag_id`` is synthesized as ``AG_{cluster_id}``; production lever
    loop will reconcile this with the legacy AG numbering at the
    harness callsite.

    ``routing_evidence_kind`` is mandatory and must be non-empty
    (``ClusterMembershipRecord`` validator). Fall-back chain:
    ``repair_hypothesis → semantic_theme → cluster_id``.
    """
    routing_evidence = (
        cluster.repair_hypothesis
        or cluster.semantic_theme
        or f"cluster_{cluster.cluster_id}"
    )
    ag_id = f"AG_{cluster.cluster_id}"
    return tuple(
        _ClusterMember(
            qid=str(qid),
            cluster_id=str(cluster.cluster_id),
            ag_id=ag_id,
            co_member_qids=all_member_qids,
            routing_evidence_kind=str(routing_evidence),
        )
        for qid in cluster.member_qids
    )


def _stub_cluster_response(
    states: tuple[QuestionStateInIteration, ...],
) -> _ClusterResponse:
    """Build a deterministic single-cluster-per-QID response from
    diagnosed states.

    Test-stub override: when the state machine is driven with synthetic
    diagnose_llm/synthesize_llm stubs (no real workspace client), the
    real Stage 2 clustering LLM cannot run. We synthesize one self-
    cluster per diagnosed state so the funnel can continue. The
    ``cluster_id`` is derived from the QID's ``rca_card_id`` so the
    Stage 3 synthesizer (which keys off ``state.clustered.cluster_id``)
    sees a stable identifier.
    """
    members: list[_ClusterMember] = []
    for s in states:
        if s.diagnosed is None:
            continue
        cluster_id = f"cluster_{s.qid}"
        ag_id = f"AG_{cluster_id}"
        routing_evidence = s.diagnosed.rca_kind_label or f"cluster_{s.qid}"
        members.append(_ClusterMember(
            qid=s.qid,
            cluster_id=cluster_id,
            ag_id=ag_id,
            co_member_qids=(s.qid,),
            routing_evidence_kind=str(routing_evidence),
        ))
    if not members:
        return _ClusterResponse(
            succeeded=False, declined="no_diagnosed_states_for_stub",
        )
    return _ClusterResponse(
        succeeded=True, parsed_output=_ClusterParsed(members=tuple(members)),
    )


def _invoke_stage2_llm(
    batch_input: Stage2BatchInput, ctx: TransformerContext,
    states: tuple[QuestionStateInIteration, ...] = (),
):
    """Dispatch Stage 2 clustering. Adapter over
    ``stages.cluster_plan11.cluster_diagnoses``.

    The ``batch_input`` carries the projected fields used by the legacy
    Stage 2 prompt path; the full ``states`` are passed so the adapter
    can rebuild ``PerQidDiagnosis`` (which needs more fields than the
    projection carries).

    Test-stub override:
      ``ctx.extras["cluster_llm"]`` may be a callable returning a list
      of ``_ClusterMember``-shaped dicts. When absent but the wider
      diagnose/synthesize stubs are wired (signalling test-replay mode),
      ``_stub_cluster_response`` synthesizes a deterministic per-QID
      self-cluster so the funnel can progress without a live LLM.

    Returns a ``_ClusterResponse`` exposing ``.succeeded`` and
    ``.parsed_output.members`` — the shape the existing transformer
    happy-path consumes.
    """
    if not states:
        return _ClusterResponse(
            succeeded=False, declined="no_states_in_batch",
        )

    # Stub seam — preferred over the live LLM when extras supply one.
    extras = getattr(ctx, "extras", {}) or {}
    if "cluster_llm" in extras or "diagnose_llm" in extras:
        return _stub_cluster_response(states)

    diagnoses = [_state_to_per_qid_diagnosis(s) for s in states if s.diagnosed]
    if not diagnoses:
        return _ClusterResponse(
            succeeded=False, declined="no_diagnosed_states_in_batch",
        )

    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )

    # Trial 16.3 — forward ``ctx.forbidden_signatures`` (typed strings
    # harvested from prior-iteration SM ``TerminalRecord.forbidden_signature``)
    # into the Stage 2 LLM prompt so the strategist sees which lever /
    # patch_type shapes have already been tried and rejected. Producer
    # side is wired (applier_gate / evaluated_gate / acceptance_gate /
    # synthesize_llm all set typed signatures on terminal records);
    # this is the consumer-side seam that was dead-ended pre-Trial-16.3.
    clusters = cluster_diagnoses(
        diagnoses=diagnoses,
        schema_columns=list(ctx.schema_columns),
        optimization_run_id=ctx.run_id,
        iteration=ctx.iteration,
        namespace="hard",
        w=ctx.w,
        forbidden_signatures=tuple(ctx.forbidden_signatures),
        # Trial 18 Step 3 — propagate the sibling channel down to the
        # Stage 2 LLM prompt so it can route the typed signal to
        # Stage 3 alongside the existing forbidden_signatures channel.
        insufficient_repair_signatures=tuple(
            getattr(ctx, "insufficient_repair_signatures", ()) or (),
        ),
    )
    if not clusters:
        return _ClusterResponse(
            succeeded=False, declined="cluster_returned_empty",
        )

    members: list[_ClusterMember] = []
    for cluster in clusters:
        all_qids = tuple(str(q) for q in cluster.member_qids)
        members.extend(_failure_cluster_to_members(cluster, all_qids))
    return _ClusterResponse(
        succeeded=True, parsed_output=_ClusterParsed(members=tuple(members)),
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
            states,
            forbidden_signatures=ctx.forbidden_signatures,
            insufficient_repair_signatures=getattr(
                ctx, "insufficient_repair_signatures", (),
            ) or (),
        )
        response = _invoke_stage2_llm(batch_input, ctx, states)
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
