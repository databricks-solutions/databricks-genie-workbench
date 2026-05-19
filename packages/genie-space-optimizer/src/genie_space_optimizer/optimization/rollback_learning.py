"""Plan 7 — LLM-driven rollback-learning module.

Public surface (filled across Tasks 6-11):
  * ``hypothesize_next_attempts_for_iteration(ctx, ...)`` — iteration
    entry; one LLM call per rolled-back cluster. (Task 9)
  * ``hypothesize_rollback_for_cluster(...)`` — single-cluster driver.
    (Task 7)
  * ``stamp_hypotheses_on_metadata_snapshot(...)`` — writes
    ``_last_attempt_hypothesis_by_cluster`` side-channel. (Task 10)
  * ``apply_forbidden_signatures_to_rollback_fingerprints(...)`` —
    union helper that feeds the existing deterministic
    content-fingerprint dedup gate. (Task 11)

Private helpers (this task, Task 6):
  * ``_validate_revised_blame_set_subset_of_allowlist``
  * ``_validate_forbidden_signatures_subset_of_applied``
  * ``_validate_revised_patch_type_in_closed_enum``

Imports are kept minimal; the public entry's heavyweight imports
(LlmReasoningCall, skill loader) live in the per-cluster driver
which is exercised only when the GSO_PLAN7_ROLLBACK_LEARNING flag is
on.
"""
from __future__ import annotations

import logging

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)

logger = logging.getLogger(__name__)


def _validate_revised_blame_set_subset_of_allowlist(
    hypothesis: NextAttemptHypothesis,
    *,
    identifier_allowlist: set[str],
) -> None:
    """Reject when any revised_blame_set entry is not in the AG's
    identifier allowlist.

    None blame_set is vacuously valid — the LLM chose not to revise.
    Case-sensitive — UC identifiers in Genie Spaces are case-sensitive."""
    if hypothesis.revised_blame_set is None:
        return
    unknown = [
        b for b in hypothesis.revised_blame_set
        if b not in identifier_allowlist
    ]
    if unknown:
        raise ValueError(
            f"hypothesis cluster={hypothesis.cluster_id!r}: "
            f"revised_blame_set entries outside identifier_allowlist: "
            f"{sorted(unknown)}"
        )


def _validate_forbidden_signatures_subset_of_applied(
    hypothesis: NextAttemptHypothesis,
    *,
    applied_patch_fingerprints: set[str],
) -> None:
    """Reject when any forbidden_signature is not in the AG's
    applied-patch fingerprint set.

    The LLM NOMINATES from existing fingerprints; it cannot invent.
    Empty forbidden_signatures is vacuously valid."""
    unknown = [
        s for s in hypothesis.forbidden_signatures
        if s not in applied_patch_fingerprints
    ]
    if unknown:
        raise ValueError(
            f"hypothesis cluster={hypothesis.cluster_id!r}: "
            f"forbidden_signatures entries outside "
            f"applied_patch_fingerprints: {sorted(unknown)}"
        )


def _validate_revised_patch_type_in_closed_enum(
    hypothesis: NextAttemptHypothesis,
) -> None:
    """Runtime double-check that revised_patch_type is a closed-enum
    member. Pydantic already enforces this at parse time; the runtime
    check pins the invariant for callers that build a hypothesis
    dataclass directly (test fixtures, replay).

    None is vacuously valid."""
    if hypothesis.revised_patch_type is None:
        return
    if not isinstance(hypothesis.revised_patch_type, PatchType):
        raise ValueError(
            f"hypothesis cluster={hypothesis.cluster_id!r}: "
            f"revised_patch_type {hypothesis.revised_patch_type!r} "
            f"is not a PatchType enum member"
        )


# ── Plan 7 Task 7 — per-cluster driver ─────────────────────────────────

import json  # noqa: E402
from typing import Any, TYPE_CHECKING  # noqa: E402

from genie_space_optimizer.skills._loader import _SKILL_LOADER  # noqa: E402

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )
    from genie_space_optimizer.optimization.rca_evidence_typed import (
        PerQidRcaEvidence,
    )
    from genie_space_optimizer.optimization.repair_intent import (
        IntentOutcome, RepairIntent, RepairShape,
    )

_SKILL_ID = "rollback_learning"
_PROMPT_CONST = "ROLLBACK_LEARNING_PROMPT"
_HYPHEN_SKILL_ID = "rollback-learning"


_AVAILABLE_REPAIR_SHAPES = (
    "top_n_by_metric", "ordered_list_by_metric", "rank_within_group",
    "period_over_period", "filter_compose", "filter_remove",
    "join_discovery", "sql_expression", "column_description",
    "instruction", "metric_view_refinement", "other",
)
_AVAILABLE_PATCH_TYPES = (
    "add_example_sql", "add_sql_snippet_expression",
    "add_sql_snippet_filter", "add_sql_snippet_measure",
    "add_instruction", "update_instruction",
    "add_join_spec", "add_column_description",
)


def _evidence_to_prompt_dict(ev: "PerQidRcaEvidence") -> dict[str, Any]:
    return {
        "qid": ev.qid,
        "observed_failure": ev.observed_failure,
        "generated_sql_issue": ev.generated_sql_issue,
        "expected_sql_shape": ev.expected_sql_shape,
        "blame_set": list(ev.blame_set),
        "confidence": ev.confidence,
    }


def _intent_to_prompt_dict(intent: "RepairIntent") -> dict[str, Any]:
    return {
        "intent_id": intent.intent_id,
        "intent_name": intent.intent_name,
        "intent_description": intent.intent_description,
        "repair_shape": intent.repair_shape.value,
        "patch_type": intent.patch_type.value,
        "rationale": intent.rationale,
        "confidence": intent.confidence,
        "source": intent.source,
        "blame_set": list(intent.blame_set),
        "target_qids": list(intent.target_qids),
    }


def _intent_outcome_to_prompt_dict(io_: "IntentOutcome") -> dict[str, Any]:
    return {
        "intent_id": io_.intent_id,
        "ag_id": io_.ag_id,
        "outcome": io_.outcome,
        "applied_signature": io_.applied_signature,
        "applied_at_iter": io_.applied_at_iter,
        "rollback_reason": io_.rollback_reason or "",
    }


def _critique_to_prompt_dict(
    critique_verdict: Any | None,
) -> dict[str, Any] | None:
    if critique_verdict is None:
        return None
    return {
        "addresses_target_failure": bool(
            critique_verdict.addresses_target_failure
        ),
        "is_overgeneralized": bool(critique_verdict.is_overgeneralized),
        "likely_neighbor_regressions": list(
            critique_verdict.likely_neighbor_regressions
        ),
        "matches_intended_shape": bool(
            critique_verdict.matches_intended_shape
        ),
        "overall_recommendation": critique_verdict.overall_recommendation,
        "rationale": critique_verdict.rationale,
    }


def _render_user_prompt(
    *,
    cluster_id: str,
    ag_id: str,
    iteration: int,
    rolled_back_repair_intent: "RepairIntent",
    intent_outcome: "IntentOutcome",
    per_qid_evidence: dict[str, "PerQidRcaEvidence"],
    critique_verdict: Any | None,
    eval_diffs_for_cluster: tuple[dict[str, Any], ...],
    identifier_allowlist: set[str],
    applied_patch_fingerprints: set[str],
) -> str:
    payload = {
        "cluster_id": cluster_id,
        "ag_id": ag_id,
        "iteration": int(iteration),
        "rolled_back_intent": _intent_to_prompt_dict(
            rolled_back_repair_intent
        ),
        "intent_outcome": _intent_outcome_to_prompt_dict(intent_outcome),
        "per_qid_evidence": [
            _evidence_to_prompt_dict(per_qid_evidence[qid])
            for qid in sorted(per_qid_evidence.keys())
        ],
        "critique_verdict": _critique_to_prompt_dict(critique_verdict),
        "eval_diffs": list(eval_diffs_for_cluster or ()),
        "identifier_allowlist": sorted(identifier_allowlist),
        "applied_patch_fingerprints": sorted(applied_patch_fingerprints),
        "available_repair_shapes": list(_AVAILABLE_REPAIR_SHAPES),
        "available_patch_types": list(_AVAILABLE_PATCH_TYPES),
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _build_request(
    *,
    cluster_id: str,
    ag_id: str,
    iteration: int,
    rolled_back_repair_intent: "RepairIntent",
    intent_outcome: "IntentOutcome",
    per_qid_evidence: dict[str, "PerQidRcaEvidence"],
    critique_verdict: Any | None,
    eval_diffs_for_cluster: tuple[dict[str, Any], ...],
    identifier_allowlist: set[str],
    applied_patch_fingerprints: set[str],
):
    """Build a Plan-2 LlmReasoningRequest. Pure — no LLM dispatch."""
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )
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
        cluster_id=cluster_id, ag_id=ag_id, iteration=iteration,
        rolled_back_repair_intent=rolled_back_repair_intent,
        intent_outcome=intent_outcome,
        per_qid_evidence=per_qid_evidence,
        critique_verdict=critique_verdict,
        eval_diffs_for_cluster=eval_diffs_for_cluster,
        identifier_allowlist=identifier_allowlist,
        applied_patch_fingerprints=applied_patch_fingerprints,
    )
    return LlmReasoningRequest(
        call_id=f"rollback_learning.iter_{int(iteration)}.{cluster_id}",
        skill_id=_HYPHEN_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
        model_override=rsm.model_override,
    )


def _build_hypothesis_from_parsed_dict(
    parsed: dict[str, Any],
    *,
    rolled_back_intent_id: str,
    cluster_id: str,
    ag_id: str,
    iteration: int,
) -> NextAttemptHypothesis:
    """Build NextAttemptHypothesis from response.parsed_output dict
    (Plan 2 returns parsed_output as model_dump()-ed dict)."""
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    rrs = parsed.get("revised_repair_shape")
    rpt = parsed.get("revised_patch_type")
    rbs = parsed.get("revised_blame_set")
    return NextAttemptHypothesis(
        rolled_back_intent_id=str(rolled_back_intent_id),
        cluster_id=str(cluster_id),
        ag_id=str(ag_id),
        iteration=int(iteration),
        why_failed=str(parsed["why_failed"]),
        failure_mode=str(parsed["failure_mode"]),
        revised_repair_shape=(
            RepairShape(rrs) if rrs is not None else None
        ),
        revised_patch_type=(
            PatchType(rpt) if rpt is not None else None
        ),
        revised_blame_set=(
            tuple(str(b) for b in rbs) if rbs is not None else None
        ),
        additional_evidence_needed=tuple(
            str(e) for e in parsed.get("additional_evidence_needed") or ()
        ),
        forbidden_signatures=tuple(
            str(s) for s in parsed.get("forbidden_signatures") or ()
        ),
        confidence=parsed["confidence"],
    )


def hypothesize_rollback_for_cluster(
    *,
    w: Any,
    cluster_id: str,
    ag_id: str,
    iteration: int,
    rolled_back_repair_intent: "RepairIntent",
    intent_outcome: "IntentOutcome",
    per_qid_evidence: dict[str, "PerQidRcaEvidence"],
    critique_verdict: Any | None,
    eval_diffs_for_cluster: tuple[dict[str, Any], ...],
    identifier_allowlist: set[str],
    applied_patch_fingerprints: set[str],
) -> NextAttemptHypothesis | None:
    """Dispatch one LLM hypothesis call for ONE rolled-back cluster.

    Returns:
      NextAttemptHypothesis on success (envelope parsed; validators
        accepted blame_set + forbidden_signatures + patch_type).
      None on: LLM decline; LLM error; envelope-parse failure;
        validator rejection.
    """
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        LlmReasoningCall,
    )
    request = _build_request(
        cluster_id=cluster_id, ag_id=ag_id, iteration=iteration,
        rolled_back_repair_intent=rolled_back_repair_intent,
        intent_outcome=intent_outcome,
        per_qid_evidence=per_qid_evidence,
        critique_verdict=critique_verdict,
        eval_diffs_for_cluster=eval_diffs_for_cluster,
        identifier_allowlist=identifier_allowlist,
        applied_patch_fingerprints=applied_patch_fingerprints,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)

    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "rollback_learning.declined cluster=%s reason=%s",
                cluster_id, response.declined.reason.value,
            )
        elif response.error is not None:
            logger.warning(
                "rollback_learning.error cluster=%s err=%s",
                cluster_id, response.error,
            )
        return None

    hypothesis = _build_hypothesis_from_parsed_dict(
        response.parsed_output,
        rolled_back_intent_id=rolled_back_repair_intent.intent_id,
        cluster_id=cluster_id,
        ag_id=ag_id,
        iteration=iteration,
    )

    try:
        _validate_revised_blame_set_subset_of_allowlist(
            hypothesis, identifier_allowlist=identifier_allowlist,
        )
        _validate_forbidden_signatures_subset_of_applied(
            hypothesis, applied_patch_fingerprints=applied_patch_fingerprints,
        )
        _validate_revised_patch_type_in_closed_enum(hypothesis)
    except ValueError as exc:
        logger.warning(
            "rollback_learning.validation_rejected cluster=%s err=%s",
            cluster_id, exc,
        )
        return None

    return hypothesis
