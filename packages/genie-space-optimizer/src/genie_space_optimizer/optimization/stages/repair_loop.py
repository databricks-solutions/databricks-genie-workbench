"""Plan 11 — LLM repair loop for validation-failing patches.

Entry point: :func:`repair_patch_with_llm`.

Bounded at ``max_attempts=2`` (worst case: 2 LLM calls per patch).
Emits ``GSO_PLAN11_REPAIR_LOOP_V1`` on each attempt; on final exhaustion
also emits a ``GSO_LLM_CONTRACT_FAILURE_V1`` marker so postmortems
catch the failure even if no other Plan 11 marker family is wired.

Dormant during PR 1 (flag-off); PR 2 wires it in.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningRequest
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.run_analysis_contract import (
    llm_contract_failure_marker,
    plan11_repair_loop_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
    ValidationError,
    ValidationResult,
)
from genie_space_optimizer.optimization.stages.validate_patch import validate_patch
from genie_space_optimizer.skills._loader import _SKILL_LOADER


_SKILL_ID = "plan11_repair"
_PROMPT_CONST = "PLAN11_REPAIR_PROMPT"
_DEFAULT_MAX_ATTEMPTS = 2


def _max_attempts() -> int:
    try:
        return int(
            os.environ.get(
                "GSO_PLAN11_REPAIR_LOOP_MAX_ATTEMPTS",
                str(_DEFAULT_MAX_ATTEMPTS),
            )
        )
    except (ValueError, TypeError):
        return _DEFAULT_MAX_ATTEMPTS


def _safe_patch_type(raw: Any) -> Any:
    """Coerce a string to :class:`PatchType` if possible; otherwise
    pass through unchanged. The downstream validate_patch dispatcher
    will reject unknown values with ``patch_type_unknown``, so the
    repair loop's next attempt sees a typed error.
    """
    if isinstance(raw, PatchType):
        return raw
    try:
        return PatchType(str(raw))
    except ValueError:
        return raw


def _proposal_from_output(
    *,
    patch: RepairProposal,
    out: dict[str, Any],
) -> RepairProposal:
    """Build a revised :class:`RepairProposal` from the LLM's parsed_output
    dict. Preserves the original ``intent_id`` (the framework's deterministic
    stamp) and the legacy ``repair_shape`` (deprecated but still on the
    wire until PR 4).
    """
    return RepairProposal(
        intent_id=patch.intent_id,
        intent_name=str(out.get("intent_name", patch.intent_name))[:80],
        intent_description=str(out.get("intent_description", "")),
        repair_shape=patch.repair_shape if isinstance(patch.repair_shape, RepairShape) else RepairShape.OTHER,
        patch_type=_safe_patch_type(out.get("patch_type", patch.patch_type)),
        rationale=str(out.get("rationale", "")),
        confidence=out.get("confidence", "low"),  # type: ignore[arg-type]
        patch_body=dict(out.get("patch_body") or {}),
        blame_set=tuple(str(b) for b in (out.get("blame_set") or [])),
        repair_hypothesis=str(out.get("repair_hypothesis", patch.repair_hypothesis)),
        target_qids=tuple(
            str(q) for q in (out.get("target_qids") or patch.target_qids)
        ),
    )


def _build_request(
    *,
    patch: RepairProposal,
    errors: tuple[ValidationError, ...],
    cluster: FailureCluster,
    attempt: int,
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
            "attempt": attempt,
            "original_patch": {
                "intent_id": patch.intent_id,
                "intent_name": patch.intent_name,
                "patch_type": (
                    patch.patch_type.value
                    if isinstance(patch.patch_type, PatchType)
                    else str(patch.patch_type)
                ),
                "patch_body": patch.patch_body,
                "target_qids": list(patch.target_qids),
            },
            "validator_errors": [e.to_json() for e in errors],
            "cluster": cluster.to_json(),
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=f"plan11_repair.{patch.intent_id}.attempt_{attempt}",
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def repair_patch_with_llm(
    patch: RepairProposal,
    errors: tuple[ValidationError, ...],
    cluster: FailureCluster,
    *,
    w: Any,
    optimization_run_id: str = "",
    iteration: int = 0,
    ag_id: str = "",
    validate_kwargs: dict[str, Any] | None = None,
    attempt: int = 1,
    max_attempts: int | None = None,
) -> RepairProposal | None:
    """Plan 11 — one narrow LLM call: 'your patch failed validation, here
    are the typed errors, fix it.'

    Bounded retries (``attempt`` <= ``max_attempts``); emits a per-attempt
    ``GSO_PLAN11_REPAIR_LOOP_V1`` marker. On exhaustion or LLM
    decline/error, also emits ``GSO_LLM_CONTRACT_FAILURE_V1`` so
    postmortems pick it up.

    Returns the revised :class:`RepairProposal` on success, ``None`` on
    exhaustion / decline / unrecoverable error.
    """
    max_att = max_attempts if max_attempts is not None else _max_attempts()

    if attempt > max_att:
        print(
            plan11_repair_loop_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                patch_id=patch.intent_id,
                attempt=attempt,
                max_attempts=max_att,
                outcome="exhausted",
                error_kinds=[e.error_kind for e in errors],
                error_count=len(errors),
            )
        )
        print(
            llm_contract_failure_marker(
                schema_name="Plan11RepairOutput",
                failing_fields=[e.error_kind for e in errors],
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                cluster_id=cluster.cluster_id,
                ag_id=ag_id,
                skill_name=_SKILL_ID,
                error_repr=f"repair_loop_exhausted patch_id={patch.intent_id}",
            )
        )
        return None

    request = _build_request(
        patch=patch, errors=errors, cluster=cluster, attempt=attempt,
    )

    t0 = time.monotonic()
    resp = LlmReasoningCall().invoke(w=w, request=request)
    duration_ms = int((time.monotonic() - t0) * 1000)
    tokens_in = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out = int(getattr(resp, "tokens_output", 0) or 0)

    if not resp.succeeded or resp.parsed_output is None:
        outcome = "llm_declined" if resp.declined is not None else "llm_error"
        print(
            plan11_repair_loop_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                patch_id=patch.intent_id,
                attempt=attempt,
                max_attempts=max_att,
                outcome=outcome,
                error_kinds=[e.error_kind for e in errors],
                error_count=len(errors),
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )
        print(
            llm_contract_failure_marker(
                schema_name="Plan11RepairOutput",
                failing_fields=[e.error_kind for e in errors],
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                cluster_id=cluster.cluster_id,
                ag_id=ag_id,
                skill_name=_SKILL_ID,
                error_repr=f"repair_attempt_{attempt}_{outcome}",
            )
        )
        return None

    out_dict = resp.parsed_output
    revised = _proposal_from_output(patch=patch, out=out_dict)

    # Re-validate the revised proposal. validate_kwargs carries the
    # context (metadata_snapshot, spark, w, catalog, gold_schema,
    # warehouse_id). Callers MUST pass it; the dispatcher tests mock
    # validate_patch directly so the kwarg fan-out doesn't matter there.
    vkw = validate_kwargs or {}
    result: ValidationResult = validate_patch(revised, **vkw)

    if result.is_valid:
        print(
            plan11_repair_loop_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                patch_id=patch.intent_id,
                attempt=attempt,
                max_attempts=max_att,
                outcome="repaired",
                error_kinds=[e.error_kind for e in errors],
                error_count=len(errors),
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )
        return revised

    print(
        plan11_repair_loop_marker(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            patch_id=patch.intent_id,
            attempt=attempt,
            max_attempts=max_att,
            outcome="still_invalid",
            error_kinds=[e.error_kind for e in result.errors],
            error_count=len(result.errors),
            duration_ms=duration_ms,
            tokens_input=tokens_in,
            tokens_output=tokens_out,
        )
    )
    return repair_patch_with_llm(
        revised,
        result.errors,
        cluster,
        w=w,
        optimization_run_id=optimization_run_id,
        iteration=iteration,
        ag_id=ag_id,
        validate_kwargs=validate_kwargs,
        attempt=attempt + 1,
        max_attempts=max_att,
    )
