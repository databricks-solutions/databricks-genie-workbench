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


def _safe_patch_type(raw: str) -> PatchType | None:
    """Resolve a free-form patch_type string to the closed enum.

    Returns ``None`` if the LLM emitted an unknown value (Step 0 of
    ``validate_patch.py`` would reject the proposal anyway; we drop it
    here so the repair loop in Task 9 has a typed surface).
    """
    try:
        return PatchType(raw)
    except (ValueError, TypeError):
        return None


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
    for idx, item in enumerate(raw_proposals):
        pt = _safe_patch_type(str(item.get("patch_type", "")))
        if pt is None:
            # Unknown patch_type — skip (the repair loop will pick it up
            # when validate_patch returns patch_type_unknown).
            continue
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
                blame_set=tuple(str(b) for b in (item.get("blame_set") or [])),
                repair_hypothesis=str(item.get("repair_hypothesis", "")),
                target_qids=tuple(
                    str(q) for q in (item.get("target_qids") or [])
                ),
            )
        )

    print(
        plan11_stage3_synthesis_marker(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            outcome="synthesized" if proposals else "empty_synthesis",
            proposals_count=len(proposals),
            proposal_ids=[p.intent_id for p in proposals],
            patch_types=[p.patch_type.value for p in proposals],
            target_qids_union=sorted(
                {q for p in proposals for q in p.target_qids}
            ),
            duration_ms=duration_ms,
            tokens_input=tokens_in,
            tokens_output=tokens_out,
        )
    )

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
