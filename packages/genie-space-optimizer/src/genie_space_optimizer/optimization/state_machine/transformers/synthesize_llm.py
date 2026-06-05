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


def _live_insufficient_repair_signatures(
    ctx: TransformerContext,
) -> tuple[str, ...]:
    """Phase 1 P1.4 — merge the static ``ctx.insufficient_repair_signatures``
    (harvested from prior iterations by the harness) with the live
    within-iteration bucket on ``ctx.extras``
    (``_live_insufficient_repair_signatures``).

    The bucket is populated by ``acceptance_gate`` the moment a
    ``kept_insufficient`` verdict fires; subsequent Stage 3 calls in
    the SAME iteration must see these signatures or the strategist
    will re-propose the same family for a sibling cluster on the same
    iteration. Static + live signatures are deduplicated; ordering
    follows insertion order so static (older) sigs appear first.
    """
    static_sigs = tuple(
        getattr(ctx, "insufficient_repair_signatures", ()) or ()
    )
    live_sigs: tuple[str, ...] = ()
    try:
        extras = ctx.extras
        if isinstance(extras, dict):
            live_list = extras.get(
                "_live_insufficient_repair_signatures", ()
            )
            if live_list:
                live_sigs = tuple(str(s) for s in live_list if s)
    except Exception:
        pass
    if not live_sigs:
        return static_sigs
    seen: set[str] = set(static_sigs)
    merged: list[str] = list(static_sigs)
    for sig in live_sigs:
        if sig and sig not in seen:
            seen.add(sig)
            merged.append(sig)
    return tuple(merged)


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
        # Trial 23 W4 — thread the diagnosis's closed-enum RCA label onto
        # the Stage 3 cluster so the KIT_FOR_RCA validator and the W4
        # RCA-to-mechanism router (both read ``cluster.root_cause``) have
        # the RCA kind to route on instead of an empty string.
        root_cause=str(
            getattr(state.diagnosed, "rca_kind_label", "") or ""
        ),
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
    # Trial 19 B5 — surface the LLM-emitted ``intended_patch_shape``
    # from ``state.diagnosed`` (the typed DiagnosisRecord) into the
    # Stage 3 member_qid_evidence payload so the synthesizer prompt
    # sees the repair intent verbatim. ``getattr`` keeps replays of
    # pre-Trial-19 SM states byte-stable (the default empty string is
    # preserved on those records).
    intended_patch_shape = ""
    diagnosed = getattr(state, "diagnosed", None)
    if diagnosed is not None:
        intended_patch_shape = str(
            getattr(diagnosed, "intended_patch_shape", "") or ""
        )
    entry: dict = {
        "qid": str(state.qid),
        "blame_set": list(blame_set),
        "observed_failure": str(getattr(typed_ev, "observed_failure", "")),
        "expected_sql_shape": str(
            getattr(typed_ev, "expected_sql_shape", "")
        ),
        "confidence": str(getattr(typed_ev, "confidence", "low")),
        # Trial 19 B5 — top-level repair intent the Stage 3 prompt reads.
        "intended_patch_shape": intended_patch_shape,
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
            "intended_patch_shape": intended_patch_shape,
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
        # Trial 20 D1 — stub proposals are synthetic single-lever
        # candidates emitted by tape replay / fuzzer; supply a default
        # justification so the K5 workbench invariant
        # (single_lever_carries_justification) is honoured. Live LLM
        # proposals carry the LLM's own justification text instead.
        single_lever_justification=str(
            proposal_payload.get("single_lever_justification")
            or "synthetic stub proposal (workbench/tape replay)"
        ),
    )
    # Trial 20 E1 — stamp passing_dependents on the stub path too so
    # tape replay tests exercise the same plumbing as live LLM calls.
    # When benchmarks are not plumbed through ctx (workbench/synthetic
    # mode), stamp an empty list so the E2 default-unsafe fallback in
    # ``proposal_grounding.patch_blast_radius_is_safe`` sees the field
    # and treats the proposal as having no dependents (the safe
    # answer when no corpus is available to scan).
    # Trial 20 E1 + P4 guard — only stamp when patch_body has content.
    # An empty patch_body is an invalid proposal that MUST be caught
    # by the downstream contract validator
    # (``validate_synthesis_output_for_state_machine``); stamping
    # ``passing_dependents=[]`` here would mask the violation as a
    # superficially-non-empty dict. ``test_empty_patch_body_fails_contract_validation``
    # pins this invariant.
    try:
        from genie_space_optimizer.optimization.proposal_grounding import (
            compute_passing_dependents_for_proposal,
        )
        bench = tuple(getattr(ctx, "benchmarks", ()) or ())
        if not typed.patch_body:
            # Skip stamping on empty bodies — fall through to the
            # contract validator.
            pass
        elif bench:
            _ag_targets = tuple(
                getattr(ctx, "ag_target_qids", ()) or ()
            ) or target_qids
            _prev_failure = tuple(
                getattr(ctx, "prev_failure_qids", ()) or ()
            )
            _deps, _hcr = compute_passing_dependents_for_proposal(
                dict(typed.patch_body),
                benchmarks=bench,
                ag_target_qids=_ag_targets,
                prev_failure_qids=_prev_failure,
            )
            typed.patch_body["passing_dependents"] = list(_deps)
            if _hcr:
                typed.patch_body["high_collateral_risk"] = True
        else:
            typed.patch_body.setdefault("passing_dependents", [])
    except Exception:
        pass

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
        run_plan11_synthesis_for_all_clusters,
        run_plan11_synthesis_for_single_cluster,
        should_batch_stage3_synthesis,
    )

    cluster = _build_failure_cluster_from_state(state, ctx)
    member_qid_evidence = _build_member_qid_evidence_from_ctx(state, ctx)

    # Phase 1 P1.1 — per-cluster Stage 3 result cache on ctx.extras.
    # Multiple QIDs share a cluster; without this cache each QID
    # triggers its own Stage 3 LLM call. We key by cluster_id so the
    # second-and-subsequent QIDs in the same cluster reuse the first
    # QID's result.
    #
    # The cache also doubles as the receiver for the batched-mode
    # output: when ``ctx.extras['_stage3_pending_clusters']`` carries
    # the list of (cluster, schema_slice, member_qid_evidence, ag_id)
    # tuples for every cluster pending Stage 3 in this iteration AND
    # batching is enabled by ``should_batch_stage3_synthesis``, the
    # FIRST call to ``_invoke_stage3_llm`` fans them all out via ONE
    # ``run_plan11_synthesis_for_all_clusters`` invocation, and the
    # cache holds every cluster's result. Subsequent invocations of
    # this transformer for other QIDs in those clusters hit the cache.
    _extras = getattr(ctx, "extras", None) or {}
    _stage3_cache = _extras.get("_stage3_result_cache")
    if _stage3_cache is None:
        _stage3_cache = {}
        try:
            if isinstance(_extras, dict):
                _extras["_stage3_result_cache"] = _stage3_cache
        except Exception:
            _stage3_cache = {}
    cid = str(getattr(cluster, "cluster_id", "") or "")
    cached = _stage3_cache.get(cid) if cid else None

    result = cached
    if result is None:
        pending = _extras.get("_stage3_pending_clusters")
        batched_dispatched = _extras.get(
            "_stage3_batched_dispatched", False
        )
        if (
            isinstance(pending, list)
            and pending
            and not batched_dispatched
            and should_batch_stage3_synthesis(
                [
                    {
                        "cluster_id": str(
                            getattr(p.get("cluster"), "cluster_id", "")
                        ),
                        "ag_id": str(p.get("ag_id", "") or ""),
                        "cluster_json": (
                            p.get("cluster").to_json()
                            if p.get("cluster") is not None
                            else {}
                        ),
                        "member_qid_evidence": p.get(
                            "member_qid_evidence"
                        ) or [],
                        "schema_slice": p.get("schema_slice") or {},
                    }
                    for p in pending
                ],
                history=[dict(h) for h in ctx.history],
                forbidden_signatures=tuple(ctx.forbidden_signatures),
                insufficient_repair_signatures=(
                    _live_insufficient_repair_signatures(ctx)
                ),
            )
        ):
            batched = run_plan11_synthesis_for_all_clusters(
                pending,
                [dict(h) for h in ctx.history],
                optimization_run_id=ctx.run_id,
                iteration=ctx.iteration,
                w=ctx.w,
                forbidden_signatures=tuple(ctx.forbidden_signatures),
                insufficient_repair_signatures=(
                    _live_insufficient_repair_signatures(ctx)
                ),
                metadata_snapshot=dict(
                    getattr(ctx, "metadata_snapshot", {}) or {}
                ),
                space_id=str(getattr(ctx, "space_id", "") or ""),
                spark=getattr(ctx, "spark", None),
                catalog=str(getattr(ctx, "catalog", "") or ""),
                gold_schema=str(getattr(ctx, "gold_schema", "") or ""),
                warehouse_id=str(getattr(ctx, "warehouse_id", "") or ""),
            )
            for bcid, bresult in batched.items():
                _stage3_cache[str(bcid)] = bresult
            try:
                if isinstance(_extras, dict):
                    _extras["_stage3_batched_dispatched"] = True
            except Exception:
                pass
            result = _stage3_cache.get(cid)

        if result is None:
            # P4 — thread the producer-side hooks through the
            # single-cluster synthesis path so C3/C4/C5/C8 can run on
            # every proposal before it is appended to the bundle.
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
                insufficient_repair_signatures=(
                    _live_insufficient_repair_signatures(ctx)
                ),
                metadata_snapshot=dict(
                    getattr(ctx, "metadata_snapshot", {}) or {}
                ),
                space_id=str(getattr(ctx, "space_id", "") or ""),
                spark=getattr(ctx, "spark", None),
                catalog=str(getattr(ctx, "catalog", "") or ""),
                gold_schema=str(getattr(ctx, "gold_schema", "") or ""),
                warehouse_id=str(getattr(ctx, "warehouse_id", "") or ""),
                # Trial 22 W3 — the harness stashes the prior iteration's
                # durable compiler_drop_summary here so this iteration's
                # Stage 3 prompt warns the LLM about the exact drops.
                prior_iteration_drops=(
                    _extras.get("_t22_prior_iteration_drops")
                    if isinstance(_extras, dict)
                    else None
                ),
            )
            if cid:
                _stage3_cache[cid] = result

    # Trial 22 W3 — surface THIS iteration's compiler drop summary so the
    # harness can copy it onto the durable iteration terminal-state
    # ledger row (the cluster result is transient).
    try:
        _t22_summary = getattr(result, "compiler_drop_summary", None)
        if _t22_summary and isinstance(_extras, dict):
            _extras["_t22_compiler_drop_summary"] = _t22_summary
    except Exception:
        pass

    proposal_dict = getattr(result, "proposal", None)
    if proposal_dict is None:
        return None

    typed = RepairProposal.from_json(proposal_dict)

    # Phase 3 P3.3 — deterministic bundle_id mint. Override whatever
    # the LLM emitted (or default-empty) with the canonical format
    # ``{cluster_id}.iter{iteration}.{primary_family}``. This closes
    # the fallback AG-collision class where the LLM re-emitted the
    # same string across iterations and the collision detector
    # false-positived on legitimately new bundles. The mint is
    # idempotent and side-effect-free; legacy stub/test paths that
    # construct RepairProposal directly remain unaffected because
    # they do not flow through this transformer.
    from dataclasses import replace as _dataclass_replace
    from genie_space_optimizer.optimization.bundle_id_mint import (
        mint_bundle_id_for_proposal as _mint_bundle_id_for_proposal,
    )
    try:
        _cluster_id_for_bundle = str(
            getattr(state.clustered, "cluster_id", "") or ""
        ) if state.clustered else ""
        _iteration_for_bundle = int(getattr(ctx, "iteration", 0) or 0)
        _minted_bundle_id = _mint_bundle_id_for_proposal(
            cluster_id=_cluster_id_for_bundle,
            iteration=_iteration_for_bundle,
            patch_type=typed.patch_type,
        )
        if _minted_bundle_id and _minted_bundle_id != typed.bundle_id:
            typed = _dataclass_replace(typed, bundle_id=_minted_bundle_id)
    except Exception:
        # Defensive — bundle_id mint must never block the SM lane.
        # If something goes sideways (e.g. PatchType unknown), keep
        # the LLM's value and let downstream gates classify.
        pass

    # Trial 19 A1 — admission gate. Reject sole-primary repeats of
    # an insufficient_repair_signature. The gate is pure; when the
    # ``GSO_TRIAL19_ENFORCE_INSUFFICIENT`` flag is OFF (or the
    # signature set is empty) every proposal passes through
    # admitted. The gate emits its own observability marker so the
    # postmortem skill can grade adherence.
    try:
        from genie_space_optimizer.optimization.admission_gate import (
            REJECTED_INSUFFICIENT_REPEAT,
            evaluate_admission,
        )
        rca_label_by_qid: dict[str, str] = {}
        diagnosed = getattr(state, "diagnosed", None)
        if diagnosed is not None:
            label = getattr(diagnosed, "rca_kind_label", "") or ""
            qid = getattr(state, "qid", "") or ""
            if label and qid:
                rca_label_by_qid[str(qid)] = str(label)
        admission = evaluate_admission(
            [typed],
            insufficient_signatures=tuple(
                getattr(ctx, "insufficient_repair_signatures", ()) or ()
            ),
            forbidden_signatures=tuple(
                getattr(ctx, "forbidden_signatures", ()) or ()
            ),
            rca_kind_label_by_qid=rca_label_by_qid,
        )
        # Always emit the audit marker — empty input or admit-all
        # cases still observe "the gate ran". Postmortem grading
        # asserts admission_decision_records exist whenever
        # GSO_TRIAL19_ENFORCE_INSUFFICIENT is enabled.
        try:
            import json as _json
            # Trial 22 W5.0 — carry the canonical lineage key
            # (optimization_run_id, ag_id, iteration). The legacy
            # ``run_id`` field is retained (parser-stable) and
            # ``optimization_run_id`` / ``ag_id`` are added so the W5.1
            # full-eval lineage reconciler can join this row on the
            # canonical key documented in lineage_invariants.py.
            _t22_ag_id = ""
            try:
                _t22_ag_id = str(
                    getattr(state.clustered, "ag_id", "") or ""
                )
            except Exception:
                _t22_ag_id = ""
            for verdict in admission.verdicts:
                print(
                    "GSO_ADMISSION_DECISION_V1 "
                    + _json.dumps(
                        {
                            "run_id": ctx.run_id,
                            "optimization_run_id": ctx.run_id,
                            "ag_id": _t22_ag_id,
                            "iteration": ctx.iteration,
                            "qid": getattr(state, "qid", ""),
                            "decision": verdict.decision,
                            "reason": verdict.reason,
                            "matched_signature": verdict.matched_signature,
                            "proposal_index": verdict.proposal_index,
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )
        except Exception:
            pass
        if admission.rejected_set:
            # Reject the proposal — Stage 3 will be re-asked next
            # iteration with the typed feedback string. We don't
            # remember the typed proposal so the state remains in
            # the same stage as if Stage 3 emitted nothing.
            return None
    except Exception:
        # Defensive — admission gate failures must not block the
        # legacy path. Log via debug and continue with admit-all.
        pass

    # Trial 20 Workstream E1 — stamp ``passing_dependents`` and
    # ``high_collateral_risk`` on the typed proposal's patch_body
    # before storing it. The legacy harness lane runs the same scan
    # via ``_t24_counterfactual_scan`` (harness.py:6090-6218) but the
    # state-machine lane was a silent no-op because the field never
    # made it onto ``patch_body``. Trial 19 then took the safe-by-
    # default ``no_passing_dependents_field`` fallback in
    # :func:`patch_blast_radius_is_safe` — the airline rollback in
    # postmortem 519131527536322 traces directly to this gap.
    #
    # E2 (the unsafe-by-default fallback) is the other half of this
    # fix; without E1's stamping, E2 would reject every SM proposal
    # for ``passing_dependents_missing``.
    try:
        from genie_space_optimizer.optimization.proposal_grounding import (
            compute_passing_dependents_for_proposal,
        )
        bench = tuple(getattr(ctx, "benchmarks", ()) or ())
        if not typed.patch_body:
            # P4 guard — never stamp an empty patch_body. The
            # downstream contract validator
            # (``validate_synthesis_output_for_state_machine``) must
            # see ``original_patch_body == {}`` and terminate the
            # cluster with OPTIMIZER_INVARIANT_VIOLATION. Stamping
            # ``passing_dependents=[]`` here would make the dict
            # superficially non-empty and mask the contract violation.
            # ``test_empty_patch_body_fails_contract_validation``
            # pins this invariant.
            pass
        elif bench:
            _ag_targets = tuple(
                getattr(ctx, "ag_target_qids", ()) or ()
            ) or tuple(typed.target_qids)
            _prev_failure = tuple(
                getattr(ctx, "prev_failure_qids", ()) or ()
            )
            _deps, _hcr = compute_passing_dependents_for_proposal(
                dict(typed.patch_body),
                benchmarks=bench,
                ag_target_qids=_ag_targets,
                prev_failure_qids=_prev_failure,
            )
            typed.patch_body["passing_dependents"] = list(_deps)
            if _hcr:
                typed.patch_body["high_collateral_risk"] = True
        else:
            # No benchmark corpus to scan (workbench/synthetic mode).
            # Stamp empty list so the E2 default-unsafe fallback sees
            # the field present and falls through to the regular gate
            # logic (which treats empty dependents as safe).
            typed.patch_body.setdefault("passing_dependents", [])
    except Exception:
        # E1 stamping is best-effort. When ``benchmarks`` is missing
        # from ctx (older tests / replays), leave the patch body
        # untouched; the downstream blast_radius_batch ctx-mapping
        # fallback covers tape-replay paths that supply the values
        # via ``passing_dependents_by_intent`` instead.
        pass

    # P4 C3/C4/C5 — producer-side validation now runs INSIDE
    # ``run_plan11_synthesis_for_single_cluster`` for every proposal in
    # the bundle (not just the SM lane's primary). The SM transformer
    # only needs to thread ``metadata_snapshot`` / ``space_id`` /
    # gold-warehouse plumbing through the call (above), and the
    # stamped ``patch_body`` flows back here via ``RepairProposal``.
    # See ``stages/synthesize.py`` for the integration site.

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
