"""Plan 5 — LLM-driven repair intent synthesizer.

Public surface (filled by Tasks 7-8):
  * ``synthesize_repair_intent_for_cluster(...)`` — one-LLM-call-per-
    cluster driver returning ``RepairProposal | None``.

Private helpers (this task, Task 6):
  * ``_validate_patch_body_against_patch_type``
  * ``_validate_blame_set_in_identifier_allowlist``
  * ``_validate_benchmark_leakage_relaxed_for_other``
  * ``_stamp_intent_id``
"""
from __future__ import annotations

import logging
import re
from typing import Any

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    PatchBodyValidationError,
    RepairProposal,
    required_patch_body_fields,
)

logger = logging.getLogger(__name__)


def _validate_patch_body_against_patch_type(
    proposal: RepairProposal,
) -> None:
    """Reject when patch_body is missing a required field for its
    patch_type. Per-patch-type required fields live in
    ``repair_proposal_typed._REQUIRED_PATCH_BODY_FIELDS``.

    Permissive pass-through for patch_types Plan 5 has not enumerated
    (the cross-lever router rejects unsupported patch_types later via
    the compatible-shape check)."""
    required = required_patch_body_fields(proposal.patch_type)
    missing = sorted(required - proposal.patch_body.keys())
    if missing:
        raise PatchBodyValidationError(
            f"patch_body for patch_type={proposal.patch_type.value!r} "
            f"missing required field(s): {missing}"
        )


def _validate_blame_set_in_identifier_allowlist(
    proposal: RepairProposal,
    *,
    identifier_allowlist: set[str],
) -> None:
    """Reject when any blame_set entry is not in the allowlist.

    Empty blame_set is vacuously valid (acceptable for prose patches
    like ADD_INSTRUCTION). Case-sensitive — UC identifiers in Genie
    Spaces are case-sensitive."""
    unknown = [b for b in proposal.blame_set if b not in identifier_allowlist]
    if unknown:
        raise ValueError(
            f"intent {proposal.intent_id!r}: blame_set entries outside "
            f"identifier_allowlist: {sorted(unknown)}"
        )


_LEAKAGE_NGRAM_SIZE = 5


def _normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _ngrams(s: str, n: int) -> set[tuple[str, ...]]:
    tokens = _normalize_text(s).split()
    if len(tokens) < n:
        return set()
    return {tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)}


def _validate_benchmark_leakage_relaxed_for_other(
    proposal: RepairProposal,
    *,
    benchmarks: list[dict[str, Any]] | None,
) -> None:
    """Relaxed n-gram firewall — only fires when ``repair_shape == OTHER``
    AND ``patch_type == ADD_EXAMPLE_SQL`` (the closed-gate bypass case).

    Closed-shape leakage gating runs INSIDE the L5b synthesis pipeline
    (synthesis.py's full firewall). This relaxed gate is the catch-all
    for the OTHER bypass — the cluster's repair_shape was outside the
    catalog, so the closed gate was skipped, and we still want a
    leakage check.

    No-op for non-OTHER shapes (closed gate handles them) and for
    non-ADD_EXAMPLE_SQL patch types (no example_question to check)."""
    if proposal.repair_shape is not RepairShape.OTHER:
        return
    if proposal.patch_type is not PatchType.ADD_EXAMPLE_SQL:
        return
    if not benchmarks:
        return

    question = str(proposal.patch_body.get("example_question") or "")
    if not question:
        return

    proposal_ngrams = _ngrams(question, _LEAKAGE_NGRAM_SIZE)
    if not proposal_ngrams:
        return

    for bm in benchmarks:
        bm_q = str((bm or {}).get("question") or "")
        if not bm_q:
            continue
        bm_ngrams = _ngrams(bm_q, _LEAKAGE_NGRAM_SIZE)
        if proposal_ngrams & bm_ngrams:
            overlap = next(iter(proposal_ngrams & bm_ngrams))
            raise ValueError(
                f"intent {proposal.intent_id!r}: relaxed leakage gate "
                f"rejected example_question — shares {_LEAKAGE_NGRAM_SIZE}-gram "
                f"{' '.join(overlap)!r} with a benchmark question"
            )


def _stamp_intent_id(*, cluster_id: str, ag_id: str, seq: int) -> str:
    """Build a deterministic intent_id.

    Format: ``intent_<cluster_id>_<ag_id>_<seq:03d>``.
    Mirrors the format Plan 1's ``intent_from_archetype`` uses (see
    ``repair_intent.py:292-294``) so postmortem can group intents from
    both producers under the same key.
    """
    if not cluster_id:
        raise ValueError("cluster_id must be non-empty")
    if not ag_id:
        raise ValueError("ag_id must be non-empty")
    if seq < 1:
        raise ValueError(f"seq must be ≥ 1; got {seq}")
    return f"intent_{cluster_id}_{ag_id}_{seq:03d}"


# ── Plan 5 Task 7 — synthesizer driver (success path) ─────────────────

import json  # noqa: E402

from genie_space_optimizer.optimization.cluster_typed import LlmCluster  # noqa: E402
from genie_space_optimizer.optimization.llm_reasoning_call import (  # noqa: E402
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (  # noqa: E402
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (  # noqa: E402
    PerQidRcaEvidence,
)
from genie_space_optimizer.skills._loader import _SKILL_LOADER  # noqa: E402


_SKILL_ID = "repair_intent_synthesis"
_PROMPT_CONST = "REPAIR_INTENT_SYNTHESIS_PROMPT"
_HYPHEN_SKILL_ID = "repair-intent-synthesis"


def _evidence_to_prompt_dict(ev: PerQidRcaEvidence) -> dict[str, Any]:
    """Project PerQidRcaEvidence to the prompt-facing dict shape.
    Mirrors the SKILL.md ``<context_inputs>.per_qid_evidence`` schema."""
    return {
        "qid": ev.qid,
        "observed_failure": ev.observed_failure,
        "generated_sql_issue": ev.generated_sql_issue,
        "expected_sql_shape": ev.expected_sql_shape,
        "blame_set": list(ev.blame_set),
        "suggested_repair_family": ev.suggested_repair_family,
        "repair_hint_patch_type": str(ev.repair_hint_patch_type.value),
        "confidence": ev.confidence,
        "quoted_evidence": list(ev.quoted_evidence),
    }


def _render_user_prompt(
    *,
    cluster: LlmCluster,
    rca_evidence_typed: dict[str, PerQidRcaEvidence],
    identifier_allowlist: set[str],
    ag_id: str,
    iteration: int,
    existing_examples_preview: str,
) -> str:
    """Render the per-cluster user prompt as one JSON-shaped block.

    Per Anthropic context-engineering: smallest set of high-signal
    tokens. We include only the fields SKILL.md promises in
    ``<context_inputs>``; nothing else.
    """
    payload = {
        "cluster_id": cluster.cluster_id,
        "ag_id": ag_id,
        "iteration": int(iteration),
        "cluster_semantic_theme": cluster.semantic_theme,
        "cluster_suggested_repair_shape": cluster.suggested_repair_shape.value,
        "per_qid_evidence": [
            _evidence_to_prompt_dict(rca_evidence_typed[qid])
            for qid in cluster.member_qids
            if qid in rca_evidence_typed
        ],
        "identifier_allowlist": sorted(identifier_allowlist),
        "available_patch_types": [
            PatchType.ADD_EXAMPLE_SQL.value,
            PatchType.ADD_SQL_SNIPPET_EXPRESSION.value,
            PatchType.ADD_SQL_SNIPPET_FILTER.value,
            PatchType.ADD_SQL_SNIPPET_MEASURE.value,
            PatchType.ADD_INSTRUCTION.value,
            PatchType.UPDATE_INSTRUCTION.value,
            PatchType.ADD_JOIN_SPEC.value,
            PatchType.ADD_COLUMN_DESCRIPTION.value,
        ],
        "available_repair_shapes": [s.value for s in RepairShape],
        "existing_examples_preview": existing_examples_preview,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _build_request(
    *,
    cluster: LlmCluster,
    rca_evidence_typed: dict[str, PerQidRcaEvidence],
    identifier_allowlist: set[str],
    ag_id: str,
    iteration: int,
    existing_examples_preview: str,
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
        cluster=cluster, rca_evidence_typed=rca_evidence_typed,
        identifier_allowlist=identifier_allowlist,
        ag_id=ag_id, iteration=iteration,
        existing_examples_preview=existing_examples_preview,
    )
    call_id = (
        f"repair_intent_synthesis.iter_{int(iteration)}.{cluster.cluster_id}"
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


def synthesize_repair_intent_for_cluster(
    *,
    w: Any,
    cluster: LlmCluster,
    rca_evidence_typed: dict[str, PerQidRcaEvidence],
    identifier_allowlist: set[str],
    ag_id: str,
    iteration: int,
    seq: int,
    existing_examples_preview: str,
    benchmarks: list[dict[str, Any]] | None,
) -> RepairProposal | None:
    """Dispatch one LLM repair-intent synthesis call for ONE cluster.

    Returns:
      RepairProposal on success (all 3 validators pass, intent_id
      framework-stamped).
      None on: LLM decline, LLM error, validator rejection. Caller
      (Task 12 wire-in) falls back to ``intent_from_archetype`` against
      the deterministically picked Archetype.
    """
    request = _build_request(
        cluster=cluster, rca_evidence_typed=rca_evidence_typed,
        identifier_allowlist=identifier_allowlist,
        ag_id=ag_id, iteration=iteration,
        existing_examples_preview=existing_examples_preview,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)

    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "synthesize_repair_intent.declined cluster_id=%s reason=%s needed=%s",
                cluster.cluster_id,
                response.declined.reason.value,
                list(response.declined.needed_evidence),
            )
        elif response.error is not None:
            logger.warning(
                "synthesize_repair_intent.error cluster_id=%s err=%s",
                cluster.cluster_id, response.error,
            )
        return None

    intent_id = _stamp_intent_id(
        cluster_id=cluster.cluster_id, ag_id=ag_id, seq=seq,
    )
    parsed = response.parsed_output
    proposal = RepairProposal(
        intent_id=intent_id,
        intent_name=str(parsed["intent_name"]),
        intent_description=str(parsed["intent_description"]),
        repair_shape=RepairShape(parsed["repair_shape"]),
        patch_type=PatchType(parsed["patch_type"]),
        rationale=str(parsed["rationale"]),
        confidence=parsed["confidence"],
        patch_body=dict(parsed.get("patch_body") or {}),
        blame_set=tuple(str(b) for b in parsed.get("blame_set") or ()),
    )

    try:
        _validate_patch_body_against_patch_type(proposal)
    except PatchBodyValidationError as exc:
        logger.warning(
            "synthesize_repair_intent.patch_body_rejected intent_id=%s err=%s",
            proposal.intent_id, exc,
        )
        return None
    try:
        _validate_blame_set_in_identifier_allowlist(
            proposal, identifier_allowlist=identifier_allowlist,
        )
    except ValueError as exc:
        logger.warning(
            "synthesize_repair_intent.blame_set_rejected intent_id=%s err=%s",
            proposal.intent_id, exc,
        )
        return None
    try:
        _validate_benchmark_leakage_relaxed_for_other(
            proposal, benchmarks=benchmarks,
        )
    except ValueError as exc:
        logger.warning(
            "synthesize_repair_intent.leakage_rejected intent_id=%s err=%s",
            proposal.intent_id, exc,
        )
        return None

    return proposal
