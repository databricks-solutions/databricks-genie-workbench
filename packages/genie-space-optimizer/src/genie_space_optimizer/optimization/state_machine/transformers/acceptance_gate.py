"""Acceptance gate: EVALUATED → ACCEPTED, or roll back to TERMINATED.

Reads ``state.evaluated.{pre,post}_apply_score`` plus the collateral
assessment to decide. Acceptance requires both:
  * target_fixed (post > pre), AND
  * no collateral regressions.

Failure of either → ``OPTIMIZER_TRIED_NO_GAIN`` terminal, which the
postmortem renderers + run-outcome classifier treat as
"tried, applied, eval did not improve — clean rollback".
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    gate_reasoning_marker,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord, TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import ValidationGate
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict, TransformerContext,
)


def _assess_collateral(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[str, ...]:
    """Compute per-QID collateral regression by comparing
    ``ctx.baseline_eval_rows`` (pre-apply) with
    ``ctx.post_apply_eval_rows`` (set by §H's eval step).

    A QID is collateral-regressed when:
      * It is *not* in the rejected proposal's ``target_qids`` (those
        are intentional fixes), AND
      * Its baseline ``feedback/result_correctness/value`` was > 0
        (was passing), AND
      * Its post-apply value is <= 0 (now failing).

    When either side has no rows (v3 iteration 1 without the harness
    plumbing), returns ``()`` — the acceptance gate falls back to
    target_fixed-only acceptance, which is the safer-side default.
    """
    baseline = ctx.baseline_eval_rows or ()
    post = ctx.post_apply_eval_rows or ()
    if not baseline or not post:
        return ()

    # Target QIDs are the intentional fixes — never count them as collateral.
    target_qids: set[str] = set()
    if state.proposals:
        latest = state.proposals[-1]
        proposal = ctx.proposal_store.lookup(latest.intent_id)
        if proposal is not None:
            target_qids = {str(q) for q in proposal.target_qids}
    target_qids.add(state.qid)  # always exclude the state's own target

    # Trial 16 RC2b — use the canonical extractor instead of the
    # strict ``r.get("question_id")`` lookup. Symmetric with RC2a in
    # ``evaluated_gate``: MLflow-flattened rows carry the qid under
    # ``inputs/question_id`` / ``inputs.question_id`` / nested inputs,
    # and the strict lookup mapped every such row to key ``""`` —
    # silently masking real collateral regressions.
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    # Trial 18 Step 2 — symmetric with ``evaluated_gate``. The
    # collateral predicate compares baseline vs post-apply semantic
    # correctness per QID; before Trial 18 it used the raw byte-match
    # which is 0.0 in both directions for arbiter-rescued rows, hiding
    # genuine regressions on QIDs that were passing under the arbiter.
    # The flag keeps the legacy path available for rollback.
    from genie_space_optimizer.optimization.trial18_flags import (
        trial18_acceptance_overhaul_enabled,
    )
    from genie_space_optimizer.optimization.evaluation import (
        row_semantic_score,
    )

    _t18_on = trial18_acceptance_overhaul_enabled()

    def _score(row: dict) -> float:
        if _t18_on:
            return float(row_semantic_score(row))
        return float(row.get("feedback/result_correctness/value", 0.0) or 0.0)

    pre_by_qid = {
        extract_question_id(dict(r))[0]: _score(r) for r in baseline
    }
    post_by_qid = {
        extract_question_id(dict(r))[0]: _score(r) for r in post
    }
    regressed: list[str] = []
    for qid, pre_score in pre_by_qid.items():
        if qid in target_qids or not qid:
            continue
        if pre_score <= 0:
            continue  # was already failing; no regression
        post_score = post_by_qid.get(qid)
        if post_score is None:
            continue  # not re-evaluated post-apply — silent on regression
        if post_score <= 0:
            regressed.append(qid)
    return tuple(regressed)


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    assert state.evaluated is not None, "acceptance_gate requires evaluated record"
    target_fixed = state.evaluated.post_apply_score > state.evaluated.pre_apply_score
    collateral = _assess_collateral(state, ctx)

    if target_fixed and not collateral:
        return GateVerdict.success(record=AcceptanceDecisionRecord(
            decision="accepted",
            arbiter_reason="target_fixed_no_regression",
            target_fixed=True,
            collateral_regressions=(),
            insufficient_repair_signature="",
            behavioral_diff=getattr(
                state.evaluated, "behavioral_diff", "unchanged",
            ),
        ))

    # Trial 19 C2 — ALREADY_CORRECT_UNDER_ARBITER lane.
    #
    # When pre AND post are both arbiter-correct (semantic score
    # 1.0 == 1.0) AND ``behavioral_diff == "unchanged"``, the QID
    # was already correct under arbitration BEFORE we applied any
    # patch — the apply was a no-op for an already-passing QID.
    # Sibling of ``kept_insufficient`` but more specific: the
    # pre-score was already passing, so we should not count this as
    # an attempted repair at all.
    #
    # Fires BEFORE ``kept_insufficient`` because the equal-score
    # branch below also catches this case, but with the weaker
    # ``kept_insufficient`` label. The C2 label is more diagnostic:
    # it tells the postmortem reader "C1 should have caught this
    # QID at dispatcher entry; the eval rows now make that visible
    # at acceptance time too".
    #
    # Flag-gated. Off ⇒ falls through to legacy ``kept_insufficient``
    # lane below (byte-stable replay).
    try:
        from genie_space_optimizer.optimization.trial19_flags import (
            trial19_already_correct_filter_enabled,
        )
        _trial19_ac_on = trial19_already_correct_filter_enabled()
    except Exception:
        _trial19_ac_on = False
    _pre_post_arbiter_correct = (
        state.evaluated.pre_apply_score >= 1.0
        and state.evaluated.post_apply_score >= 1.0
    )
    _behavior_unchanged = (
        getattr(state.evaluated, "behavioral_diff", "unchanged")
        == "unchanged"
    )
    if (
        _trial19_ac_on
        and _pre_post_arbiter_correct
        and _behavior_unchanged
        and not collateral
        and state.applied is not None
    ):
        try:
            import json as _json
            print(
                "GSO_ACCEPTANCE_ALREADY_CORRECT_UNDER_ARBITER_V1 "
                + _json.dumps(
                    {
                        "qid": state.qid,
                        "iteration": ctx.iteration,
                        "pre_apply_score": state.evaluated.pre_apply_score,
                        "post_apply_score": state.evaluated.post_apply_score,
                        "behavioral_diff": getattr(
                            state.evaluated, "behavioral_diff", "unchanged"
                        ),
                    },
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
        except Exception:
            pass
        return GateVerdict.success(record=AcceptanceDecisionRecord(
            decision="already_correct_under_arbiter",
            arbiter_reason=(
                "already_correct_under_arbiter:"
                "pre_post_arbiter_correct_unchanged_behavior"
            ),
            target_fixed=True,
            collateral_regressions=(),
            insufficient_repair_signature="",
            behavioral_diff=getattr(
                state.evaluated, "behavioral_diff", "unchanged"
            ),
        ))

    # Trial 29 W29.1 — KIT_FORCED_INERT_REROUTE lane.
    #
    # Sibling of ``kept_insufficient`` but more specific: the kit
    # gate fired (``_kit_for_rca_companions(rca_kind) is not None``)
    # AND the patch landed cleanly (post_score == pre_score AND no
    # collateral) AND ``behavioral_diff == "unchanged"`` — i.e. the
    # kit-mandated mechanism produced an inert patch that Genie's
    # planner ignored. The next iteration MUST pick a DIFFERENT
    # structural mechanism from
    # ``_structural_fix_mechanisms(rca) - {rejected}``; recording
    # the rejected mechanism here is the lever-loop's feedback
    # channel.
    #
    # Fires BEFORE the Trial 18 ``kept_insufficient`` block below so
    # the kit-forced inert subset routes through the more diagnostic
    # lane. Sub-flag OFF (``GSO_TRIAL29_INERT_REROUTE=0``) falls
    # through to ``kept_insufficient`` for byte-stable rollback.
    try:
        from genie_space_optimizer.optimization.trial29_flags import (
            trial29_inert_reroute_enabled,
        )
        _trial29_w29_1_on = trial29_inert_reroute_enabled()
    except Exception:
        _trial29_w29_1_on = False
    if (
        _trial29_w29_1_on
        and not target_fixed
        and state.evaluated.post_apply_score == state.evaluated.pre_apply_score
        and not collateral
        and state.applied is not None
    ):
        _t29_rca_kind = ""
        if state.diagnosed is not None:
            _t29_rca_kind = str(
                getattr(state.diagnosed, "rca_kind_label", "") or ""
            )
        _t29_behavior = getattr(
            state.evaluated, "behavioral_diff", "unchanged",
        )
        if _t29_rca_kind and _t29_behavior == "unchanged":
            from genie_space_optimizer.optimization.stages.action_groups import (  # noqa: E501
                _kit_for_rca_companions as _t29_kit_companions,
                _normalize_rca_kind as _t29_normalize_rca,
            )
            _t29_canonical_rca = _t29_normalize_rca(_t29_rca_kind)
            _t29_kit_forced = (
                _t29_kit_companions(_t29_canonical_rca) is not None
            )
            if _t29_kit_forced:
                from genie_space_optimizer.optimization.levers_contract import (  # noqa: E501
                    infer_lever_from_patch_type as _t29_infer_lever,
                )
                _t29_rejected_lever = ""
                _t29_patch_type = ""
                if state.proposals:
                    _t29_latest = state.proposals[-1]
                    _t29_patch_type = str(_t29_latest.patch_type or "")
                    _t29_prop = ctx.proposal_store.lookup(
                        _t29_latest.intent_id
                    )
                    if _t29_prop is not None:
                        _t29_rejected_lever = str(
                            getattr(_t29_prop, "selected_lever", "") or ""
                        )
                    if not _t29_rejected_lever and _t29_patch_type:
                        _t29_rejected_lever = _t29_infer_lever(
                            _t29_patch_type
                        )
                _t29_signature = (
                    f"{_t29_rejected_lever or '?'}:{_t29_patch_type or '?'}"
                    f":kit_forced_inert:rca={_t29_canonical_rca}"
                    f":behavior=unchanged"
                )
                try:
                    import json as _json
                    print(
                        "GSO_TRIAL29_INERT_PATCH_REROUTE_V1 "
                        + _json.dumps(
                            {
                                "qid": state.qid,
                                "iteration": ctx.iteration,
                                "rca_kind": _t29_canonical_rca,
                                "rejected_mechanism": _t29_rejected_lever,
                                "patch_type": _t29_patch_type,
                                "behavioral_diff": "unchanged",
                                "signature": _t29_signature,
                            },
                            sort_keys=True,
                            default=str,
                        ),
                        flush=True,
                    )
                except Exception:
                    pass
                return GateVerdict.success(record=AcceptanceDecisionRecord(
                    decision="kit_forced_inert_reroute",
                    arbiter_reason=(
                        f"kit_forced_inert_reroute:"
                        f"rca={_t29_canonical_rca}:behavior=unchanged"
                    ),
                    target_fixed=False,
                    collateral_regressions=(),
                    insufficient_repair_signature=_t29_signature,
                    behavioral_diff="unchanged",
                    rejected_mechanism=_t29_rejected_lever,
                ))

    # Trial 18 Step 3 — KEPT_INSUFFICIENT lane.
    #
    # When the patch landed cleanly (post_score == pre_score AND no
    # collateral regression), the applier already wrote the config to
    # Genie. Rather than rolling it back terminally and burning the
    # next iteration on the same family, we keep the config live and
    # emit a typed ``insufficient_repair_signature`` so the strategist
    # must reinforce or pivot on the next iteration.
    #
    # IMPORTANT: this is **not** an ACCEPTED success — the run-summary
    # aggregator (``classify_run_outcome``) treats this lane as
    # ``OPTIMIZER_TRIED_INSUFFICIENT_GAIN`` and dashboards / metric
    # tiles must NOT include kept_insufficient rows in ``accepted_count``.
    # The postmortem skill ``KEPT_INSUFFICIENT_COUNTED_AS_ACCEPTED``
    # guardrail flags any aggregator that violates this contract.
    #
    # Flag-gated so ``GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0`` reverts to
    # the pre-Trial-18 two-lane verdict line-for-line.
    from genie_space_optimizer.optimization.trial18_flags import (
        trial18_acceptance_overhaul_enabled,
    )
    if (
        trial18_acceptance_overhaul_enabled()
        and not target_fixed
        and state.evaluated.post_apply_score == state.evaluated.pre_apply_score
        and not collateral
        and state.applied is not None
    ):
        from genie_space_optimizer.optimization.levers_contract import (
            infer_lever_from_patch_type,
        )
        selected_lever = ""
        patch_type_str = ""
        if state.proposals:
            latest = state.proposals[-1]
            patch_type_str = str(latest.patch_type or "")
            proposal = ctx.proposal_store.lookup(latest.intent_id)
            if proposal is not None:
                selected_lever = str(
                    getattr(proposal, "selected_lever", "") or ""
                )
            if not selected_lever and patch_type_str:
                selected_lever = infer_lever_from_patch_type(patch_type_str)
        rca_kind = ""
        if state.diagnosed is not None:
            rca_kind = str(
                getattr(state.diagnosed, "rca_kind_label", "") or ""
            )
        behavioral_diff = getattr(
            state.evaluated, "behavioral_diff", "unchanged",
        )
        # Format mirrors ``forbidden_signature`` so the cluster_batch
        # plumbing into Stage 3 prompts can render both channels with
        # the same template. ``insufficient`` is the verb token
        # distinguishing this from the terminal-rejection signatures.
        signature = (
            f"{selected_lever or '?'}:{patch_type_str or '?'}"
            f":insufficient:rca={rca_kind or '?'}"
            f":behavior={behavioral_diff}"
        )
        # Trial 18 Step 3 — emit a marker distinct from
        # ``GSO_GATE_REASONING_V1`` so dashboards remain unambiguous
        # about which lane fired. Plain ``print`` matches the existing
        # marker conventions in this module.
        try:
            import json as _json
            print(
                "GSO_ACCEPTANCE_KEPT_INSUFFICIENT_V1 "
                + _json.dumps(
                    {
                        "qid": state.qid,
                        "iteration": ctx.iteration,
                        "selected_lever": selected_lever,
                        "patch_type": patch_type_str,
                        "rca_kind": rca_kind,
                        "behavioral_diff": behavioral_diff,
                        "signature": signature,
                    },
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
        except Exception:
            pass
        # Phase 1 P1.4 — within-iteration write-through. The harness
        # harvest (line 20407 of harness.py) only fires at the END of
        # the iteration, AFTER all cluster transformers have run. That
        # leaves a within-iteration lag: cluster B's synthesize call
        # later in the SAME iteration cannot see cluster A's
        # ``kept_insufficient`` signature, so the strategist may
        # re-propose the same family. Closing the lag means writing
        # the signature into a SHARED mutable bucket on ``ctx.extras``
        # at the moment the verdict fires; subsequent
        # ``run_plan11_synthesis_for_single_cluster`` calls read this
        # bucket alongside ``ctx.insufficient_repair_signatures`` and
        # therefore see the live signature without waiting for the
        # next iteration's harness harvest. Best-effort: failure to
        # write must not block the acceptance verdict itself.
        try:
            extras = ctx.extras
            if isinstance(extras, dict):
                live_bucket = extras.setdefault(
                    "_live_insufficient_repair_signatures", []
                )
                if signature and signature not in live_bucket:
                    live_bucket.append(signature)
                # Phase 2 P2.5 — stamp the kit-aware extensions onto
                # ctx.extras so the harness end-of-iteration harvest
                # can fold them into the cluster's TerminalSignature
                # via :func:`terminal_signature_for_iteration`.
                # ``prior_patch_family`` is the patch_family that
                # triggered this terminal; ``prior_lever_set`` is the
                # full kit composition the LLM emitted (lever_id
                # strings). Both are EXACT replays of what the LLM
                # actually committed at this iteration so the next
                # iteration's pivot helper has typed evidence.
                kit_bucket = extras.setdefault(
                    "_p2_5_terminal_signature_kit_inputs", {}
                )
                qid_kit = kit_bucket.setdefault(state.qid, [])
                proposal_for_kit = None
                if state.proposals:
                    latest_for_kit = state.proposals[-1]
                    proposal_for_kit = ctx.proposal_store.lookup(
                        latest_for_kit.intent_id
                    )
                kit_levers: list[str] = []
                if proposal_for_kit is not None:
                    selected_levers_attr = getattr(
                        proposal_for_kit, "selected_levers", ()
                    )
                    if selected_levers_attr:
                        kit_levers = [
                            str(s) for s in selected_levers_attr if s
                        ]
                if not kit_levers and selected_lever:
                    kit_levers = [selected_lever]
                qid_kit.append(
                    {
                        "prior_lever_set": tuple(kit_levers),
                        "prior_patch_family": patch_type_str,
                        "signature": signature,
                    }
                )
        except Exception:
            # The verdict must still fire even if the bookkeeping
            # failed; the harness end-of-iteration harvest is the
            # backstop.
            pass
        return GateVerdict.success(record=AcceptanceDecisionRecord(
            decision="kept_insufficient",
            arbiter_reason=f"kept_insufficient:behavior={behavioral_diff}",
            target_fixed=False,
            collateral_regressions=(),
            insufficient_repair_signature=signature,
            behavioral_diff=behavioral_diff,
        ))

    if not target_fixed:
        reason = "target_unchanged: post_score <= pre_score"
    else:
        reason = f"collateral_regressions={list(collateral)}"
    # Trial 18 Step 2 — extend ``GSO_GATE_REASONING_V1`` ``predicate_inputs``
    # with both the canonical semantic score (now driving the decision)
    # AND the raw byte-match for diagnostic continuity. Operators
    # debugging an unexpected reject can see both scalars side-by-side
    # without having to grep across markers.
    _byte_match_inputs = {}
    try:
        from genie_space_optimizer.optimization.trial18_flags import (
            trial18_acceptance_overhaul_enabled,
        )
        if trial18_acceptance_overhaul_enabled():
            # Surface the raw byte-match scores when canonical is the
            # active driver, so reviewers can see the divergence.
            _baseline_row = None
            _post_row = None
            if ctx.baseline_eval_rows:
                from genie_space_optimizer.optimization._qid_extraction import (
                    extract_question_id,
                )
                for r in ctx.baseline_eval_rows:
                    if extract_question_id(dict(r))[0] == state.qid:
                        _baseline_row = r
                        break
            if ctx.post_apply_eval_rows:
                from genie_space_optimizer.optimization._qid_extraction import (
                    extract_question_id,
                )
                for r in ctx.post_apply_eval_rows:
                    if extract_question_id(dict(r))[0] == state.qid:
                        _post_row = r
                        break
            if _baseline_row is not None:
                _byte_match_inputs["pre_apply_byte_match"] = float(
                    _baseline_row.get(
                        "feedback/result_correctness/value", 0.0,
                    ) or 0.0
                )
            if _post_row is not None:
                _byte_match_inputs["post_apply_byte_match"] = float(
                    _post_row.get(
                        "feedback/result_correctness/value", 0.0,
                    ) or 0.0
                )
    except Exception:
        # Diagnostic continuity is best-effort; never fail the gate.
        pass
    print(
        gate_reasoning_marker(
            gate="acceptance_gate",
            qid=state.qid,
            verdict="rejected",
            predicate_inputs={
                "pre_apply_score": state.evaluated.pre_apply_score,
                "post_apply_score": state.evaluated.post_apply_score,
                "target_fixed": target_fixed,
                "collateral_regressions": list(collateral),
                **_byte_match_inputs,
            },
            reason=reason,
        ),
        flush=True,
    )
    # Trial 16 Chunk 3 + Trial 17 — surface the typed reason as the
    # ``forbidden_signature`` so cluster_batch's
    # ``ctx.forbidden_signatures`` channel can teach the next
    # iteration's strategist that the applied patch shape didn't
    # close the gap. Trial 17 enriches the format to carry the lever +
    # patch_type + rca_kind tokens so the LLM can pivot to a different
    # lever instead of re-proposing the same one.
    #
    # Format:
    #   "<selected_lever>:<patch_type>:target_unchanged:rca=<rca_kind>"
    # or for the collateral case:
    #   "<selected_lever>:<patch_type>:collateral_regressions=[...]"
    #
    # ``selected_lever`` comes from the proposal_store; when the
    # proposal landed before Trial 17 wired selected_lever (legacy),
    # we infer the lever from patch_type via
    # ``levers_contract.infer_lever_from_patch_type``. ``rca_kind``
    # comes from ``state.diagnosed.rca_kind_label``.
    from genie_space_optimizer.optimization.levers_contract import (
        infer_lever_from_patch_type,
    )

    selected_lever = ""
    patch_type_str = ""
    if state.proposals:
        latest = state.proposals[-1]
        patch_type_str = str(latest.patch_type or "")
        proposal = ctx.proposal_store.lookup(latest.intent_id)
        if proposal is not None:
            selected_lever = str(
                getattr(proposal, "selected_lever", "") or ""
            )
        if not selected_lever and patch_type_str:
            selected_lever = infer_lever_from_patch_type(patch_type_str)

    rca_kind = ""
    if state.diagnosed is not None:
        rca_kind = str(
            getattr(state.diagnosed, "rca_kind_label", "") or ""
        )

    if not target_fixed:
        forbidden_signature = (
            f"{selected_lever or '?'}:{patch_type_str or '?'}"
            f":target_unchanged:rca={rca_kind or '?'}"
        )
    else:
        forbidden_signature = (
            f"{selected_lever or '?'}:{patch_type_str or '?'}"
            f":collateral_regressions={list(collateral)}"
            f":rca={rca_kind or '?'}"
        )

    return GateVerdict.reject_terminal(TerminalRecord(
        kind="OPTIMIZER_TRIED_NO_GAIN",
        reason=reason,
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature=forbidden_signature,
    ))


acceptance_gate = ValidationGate(
    name="acceptance_gate",
    from_stage=FunnelStage.EVALUATED,
    to_stage_on_success=FunnelStage.ACCEPTED,
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
