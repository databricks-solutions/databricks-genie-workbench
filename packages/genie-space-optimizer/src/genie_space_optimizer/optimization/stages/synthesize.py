"""Plan 11 — Stage 3: LLM-driven per-cluster patch synthesis.

Replaces the archetype catalog
(``cluster_driven_synthesis.py:pick_archetype``). Returns the same
:class:`ClusterSynthesisResult` envelope the legacy synthesizer does so
``optimizer.py`` callsites in PR 2 are drop-in replacements.

Entry point: :func:`run_plan11_synthesis_for_single_cluster`. The handler
is dormant during PR 1 (flag-off); PR 2 wires it in.
"""
from __future__ import annotations

import json
import time
from typing import Any

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ClusterSynthesisResult,
)
from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningRequest
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_stage3_synthesis_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster
from genie_space_optimizer.skills._loader import _SKILL_LOADER
from genie_space_optimizer.skills.plan11_synthesize.output_schema import (
    Plan11SynthesizeOutput,
)


_SKILL_ID = "plan11_synthesize"
_PROMPT_CONST = "PLAN11_SYNTHESIZE_PROMPT"


def _classify_synthesis_empty_reason(
    *,
    raw_proposals: list,
    parsed_output: Any,
) -> str:
    """Map the parsed Stage 3 response shape to a typed empty-synthesis
    reason.

    Trial 13 Track 5 — the dc89d1a9 run emitted 6 ``empty_synthesis``
    markers with no diagnostic signal. The classifier walks the
    response in fail-loud order:

    * ``"all_candidates_unsafe"`` — every raw proposal carried a
      patch_type the survival contract rejected. (raw_proposals
      non-empty, all dropped during ``_safe_patch_type`` filtering.)
    * ``"no_applicable_archetype"`` — the LLM emitted a
      ``decline_reason`` of ``no_applicable_archetype``.
    * ``"prompt_constraint_collision"`` — the LLM emitted a
      ``decline_reason`` of ``prompt_constraint_collision``.
    * ``"parse_returned_zero"`` — none of the above; the LLM returned
      a parseable response whose ``proposals`` list was already empty.
    """
    decline_reason = ""
    if parsed_output is not None:
        try:
            decline_reason = str(
                parsed_output.get("decline_reason") or ""
            ).strip().lower()
        except (AttributeError, TypeError):
            decline_reason = ""
    if decline_reason == "no_applicable_archetype":
        return "no_applicable_archetype"
    if decline_reason == "prompt_constraint_collision":
        return "prompt_constraint_collision"
    if raw_proposals:
        return "all_candidates_unsafe"
    return "parse_returned_zero"


def _build_request(
    *,
    cluster: FailureCluster,
    schema_slice: dict[str, Any],
    member_qid_evidence: list[dict[str, Any]],
    history: list[dict[str, Any]],
    iteration: int,
) -> LlmReasoningRequest:
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md "
            "frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    user_prompt = json.dumps(
        {
            "iteration": iteration,
            "cluster": cluster.to_json(),
            "member_qid_evidence": member_qid_evidence,
            "schema_slice": schema_slice,
            "history": history,
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=(
            f"plan11_stage3_synthesize.{cluster.cluster_id}"
            f".iter_{int(iteration)}"
        ),
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def _target_objects_from_blame_set(
    blame_set: list[str] | tuple[str, ...],
) -> tuple:
    """Plan 12 — derive RepairProposal.target_objects from blame_set
    entries when the Stage 3 LLM did not emit a target_objects field
    (the current Plan 11 ``Plan11SynthesizeOutput`` schema doesn't).

    Identifier shapes accepted (Trial 13g — defense-in-depth alongside
    the Stage 3 prompt's FQN guidance):

      * ``catalog.schema.table.column`` (4+ parts) — fully qualified;
        grouped by the 3-part table prefix with the column appended.
      * ``catalog.schema.table`` (3 parts) — TABLE TargetObject with
        empty ``columns``.
      * ``table.column`` (2 parts) — TABLE TargetObject using the
        ``table`` portion as the identifier with the column appended.
        Tolerated because Stage 1 RCA evidence and Genie Space
        configs frequently surface 2-part identifiers when the
        catalog/schema is implicit; ``TargetObject.identifier`` only
        enforces non-empty.
      * ``table`` (1 part, no dot) — TABLE TargetObject with empty
        ``columns``.

    Entries are deduplicated by identifier in arrival order;
    whitespace-only entries are skipped.
    """
    from genie_space_optimizer.optimization.target_object_typed import (
        AssetKind,
        TargetObject,
    )

    by_table: dict[str, list[str]] = {}
    table_order: list[str] = []
    for raw in blame_set or ():
        s = str(raw).strip()
        if not s:
            continue
        parts = s.split(".")
        if len(parts) >= 4:
            table_id = ".".join(parts[:3])
            column = parts[3]
        elif len(parts) == 3:
            table_id = s
            column = ""
        elif len(parts) == 2:
            # Trial 13g — accept 2-part ``table.column``. The LLM (and
            # the Stage 1 evidence it grounds in) often elides the
            # catalog/schema prefix; dropping these entries left
            # ``target_objects`` empty and tripped the Plan 12 survival
            # contract on otherwise-valid proposals.
            table_id = parts[0].strip()
            column = parts[1].strip()
        else:
            # Single bare identifier — treat as a TABLE with no
            # explicit column. Non-empty was guaranteed above.
            table_id = s
            column = ""
        if not table_id:
            continue
        if table_id not in by_table:
            by_table[table_id] = []
            table_order.append(table_id)
        if column and column not in by_table[table_id]:
            by_table[table_id].append(column)

    out: list[TargetObject] = []
    for table_id in table_order:
        out.append(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier=table_id,
                columns=tuple(by_table[table_id]),
            )
        )
    return tuple(out)


def _union_member_blame_sets(
    member_qid_evidence: list[dict[str, Any]] | None,
) -> list[str]:
    """Trial 13g — final fallback in the blame derivation chain.

    Iterates ``member_qid_evidence`` (the per-QID Stage 1 diagnoses
    threaded into Stage 3 by the SM transformer or batch caller) and
    returns the deduplicated union of every entry's ``blame_set`` in
    arrival order. An entry may surface ``blame_set`` at the top
    level (the shape the SM transformer emits) or nested under a
    ``diagnosis`` key (matching the prompt's
    ``<context_inputs>`` ``diagnosis (PerQidDiagnosis)`` convention).

    Returns ``[]`` when ``member_qid_evidence`` is empty or no entry
    carries a blame_set — the caller treats that as the final
    ``"empty"`` source bucket in :data:`PROPOSALS_BLAME_SET_SOURCES`.
    """
    if not member_qid_evidence:
        return []
    seen: set[str] = set()
    unioned: list[str] = []
    for entry in member_qid_evidence:
        if not isinstance(entry, dict):
            continue
        blame_raw = entry.get("blame_set")
        if not blame_raw:
            diagnosis = entry.get("diagnosis") or {}
            if isinstance(diagnosis, dict):
                blame_raw = diagnosis.get("blame_set")
        for blame in (blame_raw or ()):
            s = str(blame).strip()
            if s and s not in seen:
                seen.add(s)
                unioned.append(s)
    return unioned


def _safe_patch_type(raw: str) -> PatchType | None:
    """Resolve a free-form patch_type string to the closed enum.

    Returns ``None`` if the LLM emitted a value that does not match
    any ``PatchType`` member after case-folding (Step 0 of
    ``validate_patch.py`` would reject the proposal anyway; we drop it
    here so the repair loop in Task 9 has a typed surface).

    Trial 13e — delegates to :func:`coerce_patch_type` so an LLM that
    emits UPPER_SNAKE (``"ADD_INSTRUCTION"``) is tolerated instead of
    silently dropping every proposal.
    """
    from genie_space_optimizer.optimization.repair_intent import (
        coerce_patch_type,
    )
    return coerce_patch_type(raw)


def run_plan11_synthesis_for_single_cluster(
    cluster: FailureCluster,
    schema_slice: dict[str, Any],
    history: list[dict[str, Any]],
    *,
    member_qid_evidence: list[dict[str, Any]] | None = None,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    w: Any,
) -> ClusterSynthesisResult:
    """Plan 11 Stage 3 — synthesize patches for one cluster via LLM.

    Returns :class:`ClusterSynthesisResult` (same type as the legacy
    ``run_cluster_driven_synthesis_for_single_cluster`` so PR 2 wiring is
    a drop-in replacement).

    ``proposal`` carries the first RepairProposal's ``to_json()`` dict
    (legacy contract). ``skipped_reason`` uses an ``exception:…`` colon
    prefix when the LLM declines, so ``ClusterSynthesisResult`` accepts
    it under the closed :class:`SkippedReason` invariant.
    """
    request = _build_request(
        cluster=cluster,
        schema_slice=schema_slice,
        member_qid_evidence=member_qid_evidence or [],
        history=history,
        iteration=iteration,
    )

    t0 = time.monotonic()
    resp = LlmReasoningCall().invoke(w=w, request=request)
    duration_ms = int((time.monotonic() - t0) * 1000)
    tokens_in = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out = int(getattr(resp, "tokens_output", 0) or 0)

    if not resp.succeeded or resp.parsed_output is None:
        abstain_reason = ""
        abstain_explanation = ""
        if resp.declined is not None:
            abstain_reason = str(getattr(resp.declined, "reason", ""))
            abstain_explanation = str(getattr(resp.declined, "explanation", ""))
        outcome = "declined" if resp.declined is not None else "llm_error"
        print(
            plan11_stage3_synthesis_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                outcome=outcome,
                abstain_reason=abstain_reason,
                abstain_explanation=abstain_explanation,
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason=f"exception:plan11_stage3_{outcome}",
        )

    raw_proposals = resp.parsed_output.get("proposals", []) or []
    proposals: list[RepairProposal] = []
    # Trial 13e — track the *raw* ``patch_type`` string for every
    # proposal that fails coercion to PatchType so the
    # ``empty_synthesis`` marker can surface them. Empty / whitespace
    # raws are tracked under a ``"<empty>"`` sentinel so the
    # ``all_candidates_unsafe`` guardrail in the marker always sees a
    # non-empty map (an LLM emitting proposals with missing patch_type
    # is its own vocabulary defect — visible, not silent).
    rejected_patch_types_raw: dict[str, int] = {}
    # Trial 13g — track which source supplied each surviving
    # proposal's ``blame_set`` so the Stage 3 marker exposes whether
    # the upstream evidence chain bottomed out. Closed vocabulary:
    # ``llm`` / ``cluster`` / ``member_union`` / ``empty``.
    proposals_blame_set_source: dict[str, int] = {}
    member_union_blame_set = _union_member_blame_sets(member_qid_evidence)
    for idx, item in enumerate(raw_proposals):
        raw_pt = str(item.get("patch_type", ""))
        pt = _safe_patch_type(raw_pt)
        if pt is None:
            # Unknown patch_type — skip (the repair loop will pick it up
            # when validate_patch returns patch_type_unknown). Capture
            # the raw string for the Stage 3 marker.
            key = raw_pt.strip() or "<empty>"
            rejected_patch_types_raw[key] = (
                rejected_patch_types_raw.get(key, 0) + 1
            )
            continue
        item_blame_set = [str(b) for b in (item.get("blame_set") or [])]
        # Plan 12 — derive target_objects from blame_set so the Plan 11
        # synthesize output passes the survival contract until the
        # ``Plan11SynthesizeOutput`` schema is extended to emit
        # target_objects directly. Trial 13g — 3-step fallback chain:
        # LLM blame_set -> cluster.primary_blame_set -> union of
        # member_qid_evidence blame_sets. The latter is the Stage 1
        # per-QID seed which is always populated by the diagnose
        # contract; reaching it means the Stage 3 LLM AND the Stage 2
        # clustering both dropped their blame seed. Each branch
        # records the source for the Stage 3 marker so postmortems
        # can pinpoint where the chain bottomed out.
        if item_blame_set:
            effective_blame_set = item_blame_set
            source = "llm"
        elif cluster.primary_blame_set:
            effective_blame_set = [str(b) for b in cluster.primary_blame_set]
            source = "cluster"
        elif member_union_blame_set:
            effective_blame_set = list(member_union_blame_set)
            source = "member_union"
        else:
            effective_blame_set = []
            source = "empty"
        proposals_blame_set_source[source] = (
            proposals_blame_set_source.get(source, 0) + 1
        )
        proposals.append(
            RepairProposal(
                intent_id=f"{cluster.cluster_id}_{idx:03d}",
                intent_name=str(item.get("intent_name", ""))[:80],
                intent_description=str(item.get("intent_description", "")),
                repair_shape=RepairShape.OTHER,  # legacy field; new code reads repair_hypothesis
                patch_type=pt,
                rationale=str(item.get("rationale", "")),
                confidence=item.get("confidence", "low"),  # type: ignore[arg-type]
                patch_body=dict(item.get("patch_body") or {}),
                blame_set=tuple(effective_blame_set),
                target_objects=_target_objects_from_blame_set(
                    effective_blame_set,
                ),
                repair_hypothesis=str(item.get("repair_hypothesis", "")),
                target_qids=tuple(
                    str(q) for q in (item.get("target_qids") or [])
                ),
            )
        )

    # Trial 13 Track 5 — typed ``synthesis_empty_reason`` + populated
    # ``target_qids_union`` on ``empty_synthesis``. The dc89d1a9 run
    # emitted 6 markers with no reason and no target QIDs, leaving the
    # postmortem with no signal. We compute the union from the input
    # cluster's member QIDs (the bail-out attributes back to the
    # QIDs whose evidence we tried to repair) regardless of whether
    # any proposal survived parse.
    if proposals:
        outcome_label = "synthesized"
        target_qids_union = sorted(
            {q for p in proposals for q in p.target_qids}
        )
        synthesis_empty_reason = ""
    else:
        outcome_label = "empty_synthesis"
        target_qids_union = sorted(
            {str(q) for q in (cluster.member_qids or ())}
        )
        synthesis_empty_reason = _classify_synthesis_empty_reason(
            raw_proposals=raw_proposals,
            parsed_output=resp.parsed_output,
        )
    print(
        plan11_stage3_synthesis_marker(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            outcome=outcome_label,
            proposals_count=len(proposals),
            proposal_ids=[p.intent_id for p in proposals],
            patch_types=[p.patch_type.value for p in proposals],
            target_qids_union=target_qids_union,
            duration_ms=duration_ms,
            tokens_input=tokens_in,
            tokens_output=tokens_out,
            synthesis_empty_reason=synthesis_empty_reason,
            synthesis_rejected_patch_types=(
                dict(rejected_patch_types_raw)
                if outcome_label == "empty_synthesis"
                else None
            ),
            proposals_blame_set_source=(
                dict(proposals_blame_set_source)
                if proposals_blame_set_source
                else None
            ),
        )
    )

    # Plan 12 — survival-contract validation at Stage 3 exit. Proposals
    # that fail the contract get a CONTRACT_FAILED GSO_PATCH_OUTCOME_V1
    # marker (idempotent per intent_id) and are dropped from the
    # returned list. The Stage 3 marker above carries the pre-filter
    # proposal_ids so I22's coverage check sees a matching outcome for
    # every declared intent_id (CONTRACT_FAILED for the dropped ones,
    # APPLIED / VALIDATOR_REJECTED / BLAST_RADIUS_REJECTED for the
    # survivors once downstream stages run).
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )

    surviving: list[RepairProposal] = []
    for p in proposals:
        result = validate_survival_contract(p)
        if result.is_valid:
            surviving.append(p)
            continue
        reason = (
            "missing_required_fields_" + "_".join(result.missing_fields)
        )
        emit_patch_outcome(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            intent_id=p.intent_id or "<empty>",
            outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
            terminal_reason=reason,
        )
    proposals = surviving

    if not proposals:
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason="synth_none",
        )

    # ClusterSynthesisResult.proposal is the legacy dict shape; surface
    # the first proposal as a dict so PR 2 wiring slots into the same
    # downstream pipeline as the archetype path.
    return ClusterSynthesisResult(
        proposal=proposals[0].to_json(),
        attempted_archetypes=(),
        skipped_reason="",
    )


# ─── Plan v3 state-machine integration ─────────────────────────────────


class StageThreeContractError(ValueError):
    """Raised when Stage 3 synthesis output is missing fields required by the state machine."""


_STAGE3_REQUIRED_FIELDS: tuple[str, ...] = (
    "intent_id",
    "patch_type",
    "target_objects",
    "target_qids",
    "rca_card_id",
    "causal_target",
    "original_patch_body",
)


def validate_synthesis_output_for_state_machine(proposal_payload: dict) -> None:
    """Raise StageThreeContractError if any state-machine-required field is missing or empty.

    Called at Stage 3 exit. The L6 lane writes a ProposalAttempt to the
    per-QID state ONLY when this validation passes; otherwise the
    transformer emits a contract_failed ProposalAttempt and the run-level
    invariant SM7 catches it.
    """
    missing: list[str] = []
    for field in _STAGE3_REQUIRED_FIELDS:
        value = proposal_payload.get(field, None)
        if value in (None, "", (), [], {}):
            missing.append(field)
    if missing:
        raise StageThreeContractError(
            f"Stage 3 RepairProposal missing required field(s): {missing}. "
            f"State machine requires {_STAGE3_REQUIRED_FIELDS}."
        )


# ─── Escalation rung dispatcher (Plan v3 step §M) ──────────────────────


from enum import StrEnum  # noqa: E402 — placed near consumers


class EscalationRungHint(StrEnum):
    """Closed set of escalation rung shapes the unified dispatcher
    emits. Rungs 1, 3, 4 of the v3 escalation ladder route through
    ``synthesize_escalation_for_state``; rung 2 uses the existing
    ``narrow_replacement_with_llm``."""

    SCOPED_L6 = "scoped_l6"                  # rung 1 — narrower structural fix
    ADD_EXAMPLE_SQL = "add_example_sql"      # rung 3 — teaching artifact
    NARROWED_EXAMPLE_SQL = "narrowed_example_sql"  # rung 4 — single-QID example


def synthesize_escalation_for_state(
    *,
    rung_hint: EscalationRungHint,
    failed_proposal,
    failure_reason: str,
    cluster,
    schema_slice: dict,
    history: list,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    w,
):
    """Unified LLM dispatch for escalation rungs 1, 3, 4.

    Reuses ``run_plan11_synthesis_for_single_cluster``'s prompt-building
    + LLM-call machinery by appending a rung-conditional hint marker
    to the cluster's ``repair_hypothesis``. The LLM picks up the hint
    in the prompt and shapes its proposal accordingly:

      * ``SCOPED_L6`` — ask for a narrower structural patch than the
        rejected one.
      * ``ADD_EXAMPLE_SQL`` — ask for a question-scoped teaching
        artifact in place of a structural fix.
      * ``NARROWED_EXAMPLE_SQL`` — ask for an example SQL targeting
        ONLY the failed QID (single-QID member projection).

    Returns the same ``ClusterSynthesisResult`` shape as
    ``run_plan11_synthesis_for_single_cluster`` so the adapter
    callers in ``escalation_ladder.py`` consume one type.

    Parameters
    ----------
    failed_proposal
        The ``RepairProposal`` that the gate just rejected — used to
        derive context for the hint (its patch_type, target_objects,
        blame_set inform what "narrower" means).
    failure_reason
        The rejection reason string. Surfaced into the
        ``repair_hypothesis`` so the LLM sees *why* the prior attempt
        failed and avoids re-emitting the same shape.
    """
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    hint_marker = f"escalation_rung:{rung_hint.value}"
    reason_marker = f"prior_failure:{failure_reason}"
    enriched_hypothesis = (
        f"{cluster.repair_hypothesis} | {hint_marker} | {reason_marker}"
    ).strip(" |")

    # NARROWED_EXAMPLE_SQL projects the cluster down to the single
    # failed QID — the rung's whole point is to avoid collateral
    # exposure to co-members.
    if rung_hint == EscalationRungHint.NARROWED_EXAMPLE_SQL:
        member_qids = tuple(failed_proposal.target_qids[:1]) or (
            cluster.member_qids[:1]
        )
    else:
        member_qids = cluster.member_qids

    escalation_cluster = FailureCluster(
        cluster_id=cluster.cluster_id,
        semantic_theme=cluster.semantic_theme,
        member_qids=member_qids,
        unifying_evidence=cluster.unifying_evidence,
        repair_hypothesis=enriched_hypothesis,
        primary_blame_set=cluster.primary_blame_set,
        confidence=cluster.confidence,
    )

    return run_plan11_synthesis_for_single_cluster(
        escalation_cluster,
        dict(schema_slice),
        list(history),
        member_qid_evidence=None,
        optimization_run_id=optimization_run_id,
        iteration=iteration,
        ag_id=ag_id,
        w=w,
    )
