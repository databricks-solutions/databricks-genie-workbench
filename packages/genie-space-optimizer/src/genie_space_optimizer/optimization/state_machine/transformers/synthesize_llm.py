"""Stage 3 synthesis as LlmStateTransformer.

Returns a typed RepairProposal; writes a ProposalAttempt onto state.proposals.
Validates the Phase 1 contract (validate_synthesis_output_for_state_machine)
at exit so contract failures are visible as typed terminals — never silent.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
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
class _Stage3ProposalAdapter:
    """Duck-typed proxy over a typed ``RepairProposal`` exposing the v3
    attribute names the transformer's ``_repair_proposal_to_dict`` reads.

    Real ``RepairProposal`` carries ``patch_body``, not
    ``original_patch_body``; carries ``blame_set`` and
    ``target_objects`` of typed ``TargetObject`` instead of plain
    strings; and has no ``rca_card_id`` / ``causal_target`` at all.
    The adapter projects all of these so the v3 contract validator
    (``validate_synthesis_output_for_state_machine``) sees the field
    names it requires.
    """
    intent_id: str
    patch_type: str
    target_objects: tuple
    target_qids: tuple
    rca_card_id: str
    causal_target: str
    original_patch_body: dict


def _build_failure_cluster_from_state(
    state: QuestionStateInIteration,
    ctx: TransformerContext | None = None,
):
    """Reverse-project ``state.clustered`` + ``state.diagnosed`` into a
    ``FailureCluster`` the Stage 3 entry point consumes.

    ``ClusterMembershipRecord`` is information-lossy vs ``FailureCluster``;
    the missing fields are reconstructed from ``state.diagnosed``:

      * ``semantic_theme`` / ``repair_hypothesis`` → routing_evidence_kind
      * ``unifying_evidence`` → diagnosed.evidence_summary
      * ``primary_blame_set`` → derived from ``ctx.rca_evidence_typed``
        (Trial 13g). The per-QID SM path only carries a single QID,
        so the primary blame seed is just that QID's typed evidence
        blame_set. Defaults to ``()`` when no typed evidence was
        threaded onto the context (legacy harness paths, unit tests).
      * ``confidence`` → diagnosed.confidence
    """
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    primary_blame_set: tuple[str, ...] = ()
    if ctx is not None:
        typed_ev_map = getattr(ctx, "rca_evidence_typed", None) or {}
        typed_ev = typed_ev_map.get(state.qid)
        if typed_ev is not None:
            primary_blame_set = tuple(
                str(b) for b in (getattr(typed_ev, "blame_set", ()) or ())
            )

    return FailureCluster(
        cluster_id=state.clustered.cluster_id,
        semantic_theme=state.clustered.routing_evidence_kind,
        member_qids=tuple(state.clustered.co_member_qids),
        unifying_evidence=state.diagnosed.evidence_summary,
        repair_hypothesis=state.clustered.routing_evidence_kind,
        primary_blame_set=primary_blame_set,
        confidence=state.diagnosed.confidence,
    )


def _build_member_qid_evidence_from_ctx(
    state: QuestionStateInIteration,
    ctx: TransformerContext | None,
) -> list[dict]:
    """Trial 13g — build the ``member_qid_evidence`` list the Stage 3
    LLM consumes from the typed RCA evidence on ``ctx``.

    The per-QID SM path drives Stage 3 on a single QID at a time, so
    this returns a one-element list (or empty when typed evidence is
    not present). Each entry mirrors the keys the
    ``plan11_synthesize`` prompt's ``<context_inputs>`` block
    documents: ``qid``, ``blame_set``, ``observed_failure``,
    ``expected_sql_shape``, ``confidence``. ``diagnosis`` carries the
    same fields under a nested key so downstream readers that follow
    the prompt's ``diagnosis (PerQidDiagnosis)`` convention still
    work.
    """
    if ctx is None:
        return []
    typed_ev_map = getattr(ctx, "rca_evidence_typed", None) or {}
    typed_ev = typed_ev_map.get(state.qid)
    if typed_ev is None:
        return []
    blame_set = tuple(
        str(b) for b in (getattr(typed_ev, "blame_set", ()) or ())
    )
    entry: dict = {
        "qid": str(state.qid),
        "blame_set": list(blame_set),
        "observed_failure": str(getattr(typed_ev, "observed_failure", "")),
        "expected_sql_shape": str(
            getattr(typed_ev, "expected_sql_shape", "")
        ),
        "confidence": str(getattr(typed_ev, "confidence", "low")),
        "diagnosis": {
            "qid": str(state.qid),
            "blame_set": list(blame_set),
            "observed_failure": str(
                getattr(typed_ev, "observed_failure", "")
            ),
            "expected_sql_shape": str(
                getattr(typed_ev, "expected_sql_shape", "")
            ),
            "confidence": str(getattr(typed_ev, "confidence", "low")),
        },
    }
    return [entry]


def _derive_causal_target(rp) -> str:
    """Pick a non-empty causal_target so the v3 contract validator
    (which forbids ``""``) passes.

    Priority: first blame_set member → first target_object identifier
    → intent_id (last-resort non-empty fallback)."""
    if rp.blame_set:
        return str(rp.blame_set[0])
    if rp.target_objects:
        return str(rp.target_objects[0].identifier)
    return str(rp.intent_id)


def _stub_proposal_adapter(
    state: QuestionStateInIteration,
    ctx: TransformerContext,
    proposal_payload: dict,
):
    """Translate a stub-emitted proposal dict (anchor fixture
    ``expected_proposal``) into a typed ``RepairProposal`` registered
    in ``ctx.proposal_store`` plus a ``_Stage3ProposalAdapter`` the
    transformer consumes.

    Fixture-key conventions:
      * ``patch_type`` — closed enum string (``add_sql_snippet_*`` /
        ``add_example_sql`` / ``add_instruction``).
      * ``target_object`` — single qualified identifier
        (``table.column``); mapped to a one-element
        ``TargetObject(asset_kind=COLUMN)``.
      * ``snippet`` — sql expression string. Mapped to ``sql_expression``
        for ``add_sql_snippet_*`` patch types and the ``name`` is
        synthesized from the QID.
      * ``example_question`` / ``example_sql`` — passed through to the
        ``ADD_EXAMPLE_SQL`` patch body verbatim.
    """
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    from genie_space_optimizer.optimization.target_object_typed import (
        AssetKind, TargetObject,
    )

    patch_type_str = str(proposal_payload.get("patch_type") or "")
    try:
        patch_type = PatchType(patch_type_str)
    except ValueError:
        return None

    target_object_id = str(proposal_payload.get("target_object") or "")
    if not target_object_id:
        return None
    target_object = TargetObject(
        asset_kind=AssetKind.COLUMN,
        identifier=target_object_id,
        columns=(),
    )

    target_qids = tuple(
        str(q) for q in (proposal_payload.get("target_qids") or (state.qid,))
    )
    rca_card_id = str(
        proposal_payload.get("rca_card_id")
        or (state.diagnosed.rca_card_id if state.diagnosed else "")
    )
    causal_target = str(
        proposal_payload.get("causal_target") or target_object_id
    )

    # Build the per-patch-type body the typed RepairProposal validator
    # accepts.
    body: dict = {}
    if patch_type in (
        PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        PatchType.ADD_SQL_SNIPPET_FILTER,
        PatchType.ADD_SQL_SNIPPET_MEASURE,
    ):
        body = {
            "name": f"{patch_type.value}_{state.qid}",
            "sql_expression": str(proposal_payload.get("snippet") or ""),
        }
    elif patch_type == PatchType.ADD_EXAMPLE_SQL:
        body = {
            "example_question": str(
                proposal_payload.get("example_question") or ""
            ),
            "example_sql": str(proposal_payload.get("example_sql") or ""),
        }
    elif patch_type == PatchType.ADD_INSTRUCTION:
        body = {
            "instruction_text": str(
                proposal_payload.get("instruction_text") or ""
            ),
        }
    else:
        body = dict(proposal_payload)

    intent_id = f"stub_{state.qid}_{patch_type.value}"
    typed = RepairProposal(
        intent_id=intent_id,
        intent_name=intent_id,
        intent_description=str(
            proposal_payload.get("evidence_summary") or "stub proposal"
        ),
        repair_shape=RepairShape.OTHER,
        patch_type=patch_type,
        rationale=str(
            proposal_payload.get("rationale")
            or "synthesized via stub_synthesize_llm"
        ),
        confidence="high",
        patch_body=body,
        blame_set=(causal_target,),
        target_objects=(target_object,),
        repair_hypothesis=str(
            (state.clustered.routing_evidence_kind
             if state.clustered else "")
            or "stub"
        ),
        target_qids=target_qids,
    )
    ctx.proposal_store.remember(typed)

    return _Stage3ProposalAdapter(
        intent_id=intent_id,
        patch_type=patch_type.value,
        target_objects=tuple(t.identifier for t in typed.target_objects),
        target_qids=target_qids,
        rca_card_id=rca_card_id,
        causal_target=causal_target,
        original_patch_body=dict(body),
    )


def _invoke_stage3_llm(state: QuestionStateInIteration, ctx: TransformerContext):
    """Dispatch Stage 3 synthesis. Adapter over
    ``stages.synthesize.run_plan11_synthesis_for_single_cluster``.

    1. Reconstruct ``FailureCluster`` from state records.
    2. Call the legacy entry point.
    3. If ``result.proposal is None`` → return ``None`` (transformer
       terminates the state cleanly).
    4. Otherwise hydrate ``RepairProposal.from_json(result.proposal)``,
       store the typed proposal in ``ctx.proposal_store``, and return
       a duck-typed adapter with the v3 attribute names.

    Test-stub override:
      When ``ctx.extras["synthesize_llm"]`` is callable, it is invoked
      with ``(state, ctx)`` and expected to return a proposal-dict
      shaped like the anchor fixture's ``expected_proposal`` field.
      The dict is translated to a typed ``RepairProposal`` via
      ``_stub_proposal_adapter`` and bypasses the live LLM call.
    """
    if state.clustered is None or state.diagnosed is None:
        return None

    extras = getattr(ctx, "extras", {}) or {}
    stub = extras.get("synthesize_llm") if extras else None
    if callable(stub):
        try:
            payload = stub(state=state, ctx=ctx)
        except TypeError:
            payload = stub()
        if isinstance(payload, dict) and payload:
            return _stub_proposal_adapter(state, ctx, payload)
        return None

    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    cluster = _build_failure_cluster_from_state(state, ctx)
    member_qid_evidence = _build_member_qid_evidence_from_ctx(state, ctx)
    # Trial 16.3 — forward ``ctx.forbidden_signatures`` (typed strings
    # harvested from prior-iteration SM ``TerminalRecord.forbidden_signature``)
    # into the Stage 3 LLM prompt so the lever LLM avoids re-proposing
    # patch_type / shape combinations whose typed rejection already
    # appears there. Postmortem 813949510175466 evidence: gs_013
    # re-proposed ``update_column_description`` for the same
    # ``missing_table`` root cause that just rejected
    # ``add_column_description`` — exactly the cross-iteration recycle
    # this seam closes.
    result = run_plan11_synthesis_for_single_cluster(
        cluster,
        dict(ctx.schema_slice),
        [dict(h) for h in ctx.history],
        member_qid_evidence=member_qid_evidence or None,
        optimization_run_id=ctx.run_id,
        iteration=ctx.iteration,
        ag_id=state.clustered.ag_id,
        w=ctx.w,
        forbidden_signatures=tuple(ctx.forbidden_signatures),
    )

    proposal_dict = getattr(result, "proposal", None)
    if proposal_dict is None:
        return None

    typed = RepairProposal.from_json(proposal_dict)
    ctx.proposal_store.remember(typed)

    return _Stage3ProposalAdapter(
        intent_id=typed.intent_id,
        patch_type=typed.patch_type.value,
        target_objects=tuple(t.identifier for t in typed.target_objects),
        target_qids=tuple(typed.target_qids),
        rca_card_id=state.diagnosed.rca_card_id,
        causal_target=_derive_causal_target(typed),
        original_patch_body=dict(typed.patch_body),
    )


def _repair_proposal_to_dict(rp) -> dict:
    return {
        "intent_id": getattr(rp, "intent_id", ""),
        "patch_type": getattr(rp, "patch_type", ""),
        "target_objects": tuple(getattr(rp, "target_objects", ())),
        "target_qids": tuple(getattr(rp, "target_qids", ())),
        "rca_card_id": getattr(rp, "rca_card_id", ""),
        "causal_target": getattr(rp, "causal_target", ""),
        "original_patch_body": getattr(rp, "original_patch_body", ""),
    }


def _terminate_no_candidates(state: QuestionStateInIteration, name: str, reason: str):
    return state.terminate(
        transition=StageTransition(
            from_stage=state.current_stage,
            to_stage=FunnelStage.TERMINATED,
            at_ms=int(time.time() * 1000),
            transformer_name=name,
            transition_kind="llm",
            reason=reason,
        ),
        terminal=TerminalRecord(
            kind="OPTIMIZER_NO_CANDIDATES",
            reason=reason,
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ),
    )


def _terminate_invariant(
    state: QuestionStateInIteration,
    name: str,
    failed_attempt: ProposalAttempt,
):
    return state.terminate(
        transition=StageTransition(
            from_stage=state.current_stage,
            to_stage=FunnelStage.TERMINATED,
            at_ms=int(time.time() * 1000),
            transformer_name=name,
            transition_kind="llm",
            reason=failed_attempt.outcome_reason,
            proposal_attempt_index=failed_attempt.attempt_index,
        ),
        # Trial 16 Chunk 3 — combine the patch_type with the typed
        # rejection reason so the next iteration's strategist (via
        # cluster_batch's ``ctx.forbidden_signatures``) avoids
        # re-proposing the same shape. Up to Trial 15 this signature
        # was empty, so synthesis invariant violations leaked no
        # actionable feedback into the strategist's prompt.
        terminal=TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason=failed_attempt.outcome_reason,
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature=(
                f"{failed_attempt.patch_type}:{failed_attempt.outcome_reason}"
            ),
        ),
    )


@dataclass(frozen=True, slots=True)
class _Plan11Stage3Transformer:
    name: str = "plan11_stage3_synthesis"
    from_stage: FunnelStage = FunnelStage.CLUSTERED
    to_stage_on_success: FunnelStage = FunnelStage.PROPOSED
    to_stage_on_reject: FunnelStage = FunnelStage.TERMINATED

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        proposal = _invoke_stage3_llm(state, ctx)
        if proposal is None:
            return _terminate_no_candidates(state, self.name, "stage3_returned_none")

        # Validate the Phase 1 state-machine contract — non-empty intent_id,
        # target_qids, rca_card_id, causal_target, original_patch_body, etc.
        # Failure surfaces as a typed terminal, never silent.
        from genie_space_optimizer.optimization.stages.synthesize import (
            StageThreeContractError,
            validate_synthesis_output_for_state_machine,
        )
        try:
            validate_synthesis_output_for_state_machine(
                _repair_proposal_to_dict(proposal),
            )
        except StageThreeContractError as e:
            attempt = ProposalAttempt(
                attempt_index=len(state.proposals),
                intent_id=getattr(proposal, "intent_id", "") or "unknown",
                patch_type=getattr(proposal, "patch_type", "") or "unknown",
                deepest_stage_in_attempt=FunnelStage.PROPOSED,
                outcome="contract_failed",
                outcome_reason=str(e),
            )
            return _terminate_invariant(state, self.name, attempt)

        # In-flight sentinel: outcome="applied" is the pre-terminal placeholder
        # downstream gates overwrite to the real outcome. SM7 (escalated_to
        # _attempt_index iff outcome=='escalated') tolerates this because
        # outcome is "applied" not "escalated" here.
        attempt = ProposalAttempt(
            attempt_index=len(state.proposals),
            intent_id=str(proposal.intent_id),
            patch_type=str(proposal.patch_type),
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied",
            outcome_reason="pending_gates",
        )

        return state.advance(
            to_stage=self.to_stage_on_success,
            transition=StageTransition(
                from_stage=self.from_stage,
                to_stage=self.to_stage_on_success,
                at_ms=int(time.time() * 1000),
                transformer_name=self.name,
                transition_kind="llm",
                proposal_attempt_index=attempt.attempt_index,
            ),
            proposals=state.proposals + (attempt,),
        )


plan11_stage3_synthesis = _Plan11Stage3Transformer()
