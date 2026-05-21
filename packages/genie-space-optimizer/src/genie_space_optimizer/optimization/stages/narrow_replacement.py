"""Plan 11 — LLM narrow-replacement loop for blast-radius colliders.

Entry point: :func:`narrow_replacement_with_llm`. Fires when a
structurally valid patch would break currently-passing QIDs
(``collateral_qids``). Same bounded shape as the repair loop, different
prompt and context. Emits ``GSO_PLAN11_NARROW_REPLACEMENT_V1`` per
attempt, and ``GSO_LLM_CONTRACT_FAILURE_V1`` on exhaustion.

Dormant during PR 1 (flag-off); PR 2 wires it in.
"""
from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from typing import Any

from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningRequest
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.run_analysis_contract import (
    llm_contract_failure_marker,
    plan11_narrow_replacement_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster
from genie_space_optimizer.skills._loader import _SKILL_LOADER


def narrow_replacement_from_drop_record(
    *,
    drop_record: Any,
    cluster: Any,
    w: Any,
    optimization_run_id: str = "",
    iteration: int = 0,
    attempt: int = 1,
    max_attempts: int | None = None,
) -> Any:
    """Plan 12 — narrow-replacement entry point that accepts a typed
    :class:`BlastRadiusDropRecord` (the only supported entry going
    forward — closes the ``narrow_skipped_no_original_patch_type``
    failure mode both 2026-05-20 postmortems hit).

    Reconstructs a :class:`RepairProposal` from the drop record's
    ``original_patch_body`` + ``original_patch_type``, then dispatches
    to :func:`narrow_replacement_with_llm`. Returns ``None`` when the
    drop record's ``original_patch_type`` is empty or unknown
    (pre-Plan-12 records) — the caller is responsible for emitting a
    typed terminal outcome in that case.
    """
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )

    if not isinstance(drop_record, BlastRadiusDropRecord):
        raise TypeError(
            "drop_record must be a BlastRadiusDropRecord; got "
            f"{type(drop_record).__name__}"
        )
    try:
        ptype = PatchType(drop_record.original_patch_type)
    except (ValueError, TypeError):
        return None

    reconstructed = RepairProposal(
        intent_id=drop_record.intent_id,
        intent_name="narrow_candidate",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=ptype,
        rationale="",
        confidence="medium",
        patch_body=dict(drop_record.original_patch_body),
        blame_set=(),
        target_objects=(),
        required_constructs=(),
        repair_hypothesis="",
        target_qids=tuple(drop_record.target_qids),
    )
    return narrow_replacement_with_llm(
        reconstructed,
        collateral_qids=tuple(drop_record.collateral_qids),
        protected_sql=dict(drop_record.protected_sql_by_qid),
        cluster=cluster,
        w=w,
        optimization_run_id=optimization_run_id,
        iteration=iteration,
        ag_id=drop_record.ag_id,
        attempt=attempt,
        max_attempts=max_attempts,
    )


_SKILL_ID = "plan11_narrow"
_PROMPT_CONST = "PLAN11_NARROW_PROMPT"
_DEFAULT_MAX_ATTEMPTS = 2


def _max_attempts() -> int:
    try:
        return int(
            os.environ.get(
                "GSO_PLAN11_NARROW_LOOP_MAX_ATTEMPTS",
                str(_DEFAULT_MAX_ATTEMPTS),
            )
        )
    except (ValueError, TypeError):
        return _DEFAULT_MAX_ATTEMPTS


def _safe_patch_type(raw: Any) -> Any:
    if isinstance(raw, PatchType):
        return raw
    try:
        return PatchType(str(raw))
    except ValueError:
        return raw


def _build_request(
    *,
    patch: RepairProposal,
    collateral_qids: tuple[str, ...],
    protected_sql: Mapping[str, str],
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
            "target_qids": list(patch.target_qids),
            "collateral_qids": list(collateral_qids),
            "protected_sql": dict(protected_sql),
            "cluster": cluster.to_json(),
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=f"plan11_narrow.{patch.intent_id}.attempt_{attempt}",
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def narrow_replacement_with_llm(
    patch: RepairProposal,
    *,
    collateral_qids: tuple[str, ...],
    protected_sql: Mapping[str, str],
    cluster: FailureCluster,
    w: Any,
    optimization_run_id: str = "",
    iteration: int = 0,
    ag_id: str = "",
    attempt: int = 1,
    max_attempts: int | None = None,
) -> RepairProposal | None:
    """Plan 11 — narrow a blast-radius-colliding patch so it no longer
    breaks ``collateral_qids``.

    Bounded retries; emits per-attempt ``GSO_PLAN11_NARROW_REPLACEMENT_V1``
    markers and a ``GSO_LLM_CONTRACT_FAILURE_V1`` on exhaustion or LLM
    decline/error.

    Returns the narrowed :class:`RepairProposal` on success, ``None`` on
    exhaustion / decline / unrecoverable error.
    """
    max_att = max_attempts if max_attempts is not None else _max_attempts()

    if attempt > max_att:
        print(
            plan11_narrow_replacement_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                patch_id=patch.intent_id,
                attempt=attempt,
                max_attempts=max_att,
                outcome="exhausted",
                collateral_qids_count=len(collateral_qids),
                target_qids=list(patch.target_qids),
            )
        )
        print(
            llm_contract_failure_marker(
                schema_name="Plan11NarrowOutput",
                failing_fields=["blast_radius_collateral"],
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                cluster_id=cluster.cluster_id,
                ag_id=ag_id,
                skill_name=_SKILL_ID,
                error_repr=f"narrow_loop_exhausted patch_id={patch.intent_id}",
            )
        )
        return None

    request = _build_request(
        patch=patch,
        collateral_qids=collateral_qids,
        protected_sql=protected_sql,
        cluster=cluster,
        attempt=attempt,
    )

    t0 = time.monotonic()
    resp = LlmReasoningCall().invoke(w=w, request=request)
    duration_ms = int((time.monotonic() - t0) * 1000)
    tokens_in = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out = int(getattr(resp, "tokens_output", 0) or 0)

    if not resp.succeeded or resp.parsed_output is None:
        outcome = "llm_declined" if resp.declined is not None else "llm_error"
        print(
            plan11_narrow_replacement_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                patch_id=patch.intent_id,
                attempt=attempt,
                max_attempts=max_att,
                outcome=outcome,
                collateral_qids_count=len(collateral_qids),
                target_qids=list(patch.target_qids),
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )
        print(
            llm_contract_failure_marker(
                schema_name="Plan11NarrowOutput",
                failing_fields=["blast_radius_collateral"],
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                cluster_id=cluster.cluster_id,
                ag_id=ag_id,
                skill_name=_SKILL_ID,
                error_repr=f"narrow_attempt_{attempt}_{outcome}",
            )
        )
        return None

    out = resp.parsed_output
    narrowed = RepairProposal(
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

    print(
        plan11_narrow_replacement_marker(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            patch_id=patch.intent_id,
            attempt=attempt,
            max_attempts=max_att,
            outcome="narrowed",
            collateral_qids_count=len(collateral_qids),
            target_qids=list(narrowed.target_qids),
            duration_ms=duration_ms,
            tokens_input=tokens_in,
            tokens_output=tokens_out,
        )
    )
    return narrowed
