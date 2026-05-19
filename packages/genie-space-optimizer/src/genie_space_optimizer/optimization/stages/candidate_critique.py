"""Plan 6 — candidate-critique stage.

Public surface:
  * ``STAGE_KEY``                — registry key (string constant).
  * ``CritiqueInput``            — typed input dataclass (Task 8).
  * ``CritiqueOutcome``          — typed output dataclass (Task 8).
  * ``execute(ctx, inp)``        — stage entry (Task 9).
  * ``critique_candidate_for_proposal(...)`` — per-proposal LLM
    driver (this task, Task 6).

Private helpers:
  * ``_build_request(...)``      — pure Plan-2 LlmReasoningRequest builder.
  * ``_filter_predicted_regressions(...)`` — strip qids not in
    passing_qids_at_risk (data hygiene).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.llm_reasoning_call import (
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    extract_repair_intent_from_proposal,
)
from genie_space_optimizer.skills._loader import _SKILL_LOADER

logger = logging.getLogger(__name__)

STAGE_KEY: str = "candidate_critique"

_SKILL_ID = "candidate_critique"
_PROMPT_CONST = "CANDIDATE_CRITIQUE_PROMPT"
_HYPHEN_SKILL_ID = "candidate-critique"


def _evidence_to_prompt_dict(ev: PerQidRcaEvidence) -> dict[str, Any]:
    """Project PerQidRcaEvidence to the prompt-facing dict shape.
    Mirrors the SKILL.md ``<context_inputs>.per_qid_evidence`` schema."""
    return {
        "qid": ev.qid,
        "observed_failure": ev.observed_failure,
        "generated_sql_issue": ev.generated_sql_issue,
        "expected_sql_shape": ev.expected_sql_shape,
        "blame_set": list(ev.blame_set),
        "confidence": ev.confidence,
    }


def _render_user_prompt(
    *,
    proposal: dict[str, Any],
    cluster_id: str,
    ag_id: str,
    iteration: int,
    cluster_semantic_theme: str,
    per_qid_evidence: dict[str, PerQidRcaEvidence],
    passing_qids_at_risk: tuple[str, ...],
) -> str:
    """Render the per-proposal user prompt as one JSON-shaped block."""
    intent = extract_repair_intent_from_proposal(proposal)
    repair_intent_payload: dict | None = None
    if intent is not None:
        repair_intent_payload = {
            "intent_name": intent.intent_name,
            "intent_description": intent.intent_description,
            "repair_shape": intent.repair_shape.value,
            "patch_type": intent.patch_type.value,
            "rationale": intent.rationale,
            "source": intent.source,
        }
    patch_body = {
        k: v for k, v in proposal.items()
        if k not in {
            "repair_intent", "intent_id", "proposal_id",
            "content_fingerprint", "cross_lever_override",
            "rca_id", "primary_cluster_id", "source_cluster_ids",
            "critique_verdict",
        }
    }
    payload = {
        "proposal_id": str(proposal.get("proposal_id") or ""),
        "cluster_id": cluster_id,
        "ag_id": ag_id,
        "iteration": int(iteration),
        "repair_intent": repair_intent_payload,
        "patch_body": patch_body,
        "blame_set": list(intent.blame_set) if intent is not None else [],
        "per_qid_evidence": [
            _evidence_to_prompt_dict(per_qid_evidence[qid])
            for qid in sorted(per_qid_evidence.keys())
        ],
        "passing_qids_at_risk": sorted(passing_qids_at_risk),
        "cluster_semantic_theme": cluster_semantic_theme,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _build_request(
    *,
    proposal: dict[str, Any],
    cluster_id: str,
    ag_id: str,
    iteration: int,
    cluster_semantic_theme: str,
    per_qid_evidence: dict[str, PerQidRcaEvidence],
    passing_qids_at_risk: tuple[str, ...],
) -> LlmReasoningRequest:
    """Build a Plan-2 LlmReasoningRequest. Pure — no LLM dispatch.
    Exposed for testability."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    user_prompt = _render_user_prompt(
        proposal=proposal,
        cluster_id=cluster_id, ag_id=ag_id, iteration=iteration,
        cluster_semantic_theme=cluster_semantic_theme,
        per_qid_evidence=per_qid_evidence,
        passing_qids_at_risk=passing_qids_at_risk,
    )
    proposal_id = str(proposal.get("proposal_id") or "")
    call_id = (
        f"candidate_critique.iter_{int(iteration)}.{proposal_id}"
    )
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=_HYPHEN_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
        model_override=rsm.model_override,
    )


def _filter_predicted_regressions(
    predicted: tuple[str, ...],
    *,
    passing_qids_at_risk: tuple[str, ...],
) -> tuple[str, ...]:
    """Drop predicted qids not in passing_qids_at_risk (hygiene).

    Preserves input order of the at-risk list (deterministic for
    postmortem comparison). Logs a debug line when filtering fires."""
    risk_set = set(passing_qids_at_risk)
    filtered = tuple(q for q in predicted if q in risk_set)
    if len(filtered) != len(predicted):
        unknown = sorted(set(predicted) - risk_set)
        logger.debug(
            "critique.dropped_unknown_qids predicted=%s unknown=%s "
            "kept=%s",
            list(predicted), unknown, list(filtered),
        )
    return filtered


def critique_candidate_for_proposal(
    *,
    w: Any,
    proposal: dict[str, Any],
    cluster_id: str,
    ag_id: str,
    iteration: int,
    cluster_semantic_theme: str,
    per_qid_evidence: dict[str, PerQidRcaEvidence],
    passing_qids_at_risk: tuple[str, ...],
) -> CritiqueVerdict | None:
    """Dispatch one LLM critique call for ONE proposal.

    Returns:
      CritiqueVerdict on success (envelope parsed; predicted
        regressions filtered to passing_qids_at_risk).
      None on: missing repair_intent stamp (short-circuit, no LLM
        call); LLM decline; LLM error; envelope-parse failure. Caller
        records the None outcome as a typed iteration outcome rather
        than blocking the proposal.
    """
    intent = extract_repair_intent_from_proposal(proposal)
    if intent is None:
        logger.info(
            "critique.short_circuit reason=no_intent proposal_id=%s",
            proposal.get("proposal_id"),
        )
        return None

    request = _build_request(
        proposal=proposal,
        cluster_id=cluster_id, ag_id=ag_id, iteration=iteration,
        cluster_semantic_theme=cluster_semantic_theme,
        per_qid_evidence=per_qid_evidence,
        passing_qids_at_risk=passing_qids_at_risk,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)

    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "critique.declined proposal_id=%s reason=%s",
                proposal.get("proposal_id"),
                response.declined.reason.value,
            )
        elif response.error is not None:
            logger.warning(
                "critique.error proposal_id=%s err=%s",
                proposal.get("proposal_id"), response.error,
            )
        return None

    parsed = response.parsed_output
    proposal_id = str(proposal.get("proposal_id") or "")
    raw_neighbors = tuple(
        str(q) for q in (parsed.get("likely_neighbor_regressions") or ())
    )
    filtered_neighbors = _filter_predicted_regressions(
        raw_neighbors, passing_qids_at_risk=passing_qids_at_risk,
    )
    return CritiqueVerdict(
        proposal_id=proposal_id,
        addresses_target_failure=bool(parsed["addresses_target_failure"]),
        is_overgeneralized=bool(parsed["is_overgeneralized"]),
        likely_neighbor_regressions=filtered_neighbors,
        matches_intended_shape=bool(parsed["matches_intended_shape"]),
        overall_recommendation=parsed["overall_recommendation"],
        rationale=str(parsed["rationale"]),
    )
