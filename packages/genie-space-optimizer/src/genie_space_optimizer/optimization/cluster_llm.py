"""Plan 4 — LLM clustering driver + deterministic post-validation.

Public surface (Task 9 / Task 10):
  * ``cluster_failures_llm(...)`` — one-LLM-call-per-iteration
    driver that returns ``list[LlmCluster]`` or ``None``.

Private helpers (Task 8):
  * ``_validate_member_qids_in_input``
  * ``_validate_no_qid_collision``
  * ``_validate_blame_set_in_schema``
  * ``_stamp_cluster_id``
"""
from __future__ import annotations

from collections.abc import Iterable

from genie_space_optimizer.optimization.cluster_typed import (
    ClusterValidationError,
    LlmCluster,
)


def _validate_member_qids_in_input(
    cluster: LlmCluster,
    *,
    input_qids: set[str],
) -> None:
    """Reject when any member_qid is not in the input batch.

    The LLM was given a fixed set of qids; emitting a member outside
    that set indicates hallucination. We reject the cluster and route
    its qids back through the deterministic path.
    """
    unknown = [
        qid for qid in cluster.member_qids if qid not in input_qids
    ]
    if unknown:
        raise ClusterValidationError(
            f"cluster {cluster.cluster_id!r}: member_qids include "
            f"qid(s) not in input batch: {sorted(unknown)}"
        )


def _validate_no_qid_collision(clusters: Iterable[LlmCluster]) -> None:
    """Reject when the same qid appears in two clusters.

    Raised at the SET level — the LLM violated the "no qid in two
    clusters" rule and the entire LLM output is untrustworthy for
    partitioning. The driver falls through to the deterministic
    body in this case.
    """
    seen: dict[str, str] = {}
    for cluster in clusters:
        for qid in cluster.member_qids:
            if qid in seen and seen[qid] != cluster.cluster_id:
                raise ClusterValidationError(
                    f"qid {qid!r} appears in two clusters: "
                    f"{seen[qid]!r} and {cluster.cluster_id!r}"
                )
            seen[qid] = cluster.cluster_id


def _validate_blame_set_in_schema(
    cluster: LlmCluster,
    *,
    schema_columns: set[str],
) -> None:
    """Reject when any primary_blame_set entry is not present in the
    schema. Empty primary_blame_set is vacuously valid
    (metadata-level failure). Case-sensitive — UC identifiers are
    case-sensitive in Genie Spaces.
    """
    unknown = [
        col for col in cluster.primary_blame_set
        if col not in schema_columns
    ]
    if unknown:
        raise ClusterValidationError(
            f"cluster {cluster.cluster_id!r}: primary_blame_set "
            f"includes column(s) not in schema: {sorted(unknown)}"
        )


def _stamp_cluster_id(*, namespace: str, index: int) -> str:
    """Build a deterministic cluster_id.

    Format: ``<namespace><000-padded index>``. Examples:
      _stamp_cluster_id(namespace="H", index=1)  → "H001"
      _stamp_cluster_id(namespace="S", index=42) → "S042"
      _stamp_cluster_id(namespace="H", index=1234) → "H1234"

    Namespace must be non-empty; index must be ≥ 1.
    """
    if not namespace:
        raise ValueError("namespace must be non-empty")
    if index < 1:
        raise ValueError(f"index must be ≥ 1; got {index}")
    return f"{namespace}{index:03d}"


import json as _json
import logging
from typing import Any

from genie_space_optimizer.optimization.llm_reasoning_call import (  # noqa: E402
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (  # noqa: E402
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (  # noqa: E402
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import RepairShape  # noqa: E402
from genie_space_optimizer.skills._loader import _SKILL_LOADER  # noqa: E402

logger = logging.getLogger(__name__)

_SKILL_ID = "failure_clustering"
_PROMPT_CONST = "FAILURE_CLUSTERING_PROMPT"
_HYPHEN_SKILL_ID = "failure-clustering"

# Namespace label resolution for the rendered prompt — the SKILL.md
# accepts "hard" / "soft" rather than the namespace code letters.
_NAMESPACE_LABEL = {"H": "hard", "S": "soft"}


def _evidence_to_prompt_dict(ev: PerQidRcaEvidence) -> dict[str, Any]:
    """Project PerQidRcaEvidence to the prompt-facing dict shape.

    Mirrors the SKILL.md ``<context_inputs>.per_qid_evidence`` schema
    exactly — field names match, sequence fields become lists, the
    PatchType enum becomes its string value.
    """
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
    rca_evidence_typed: dict[str, PerQidRcaEvidence],
    schema_columns: set[str],
    iteration: int,
    namespace: str,
) -> str:
    """Render the per-iteration user prompt as one JSON-shaped block."""
    payload = {
        "iteration": int(iteration),
        "namespace": _NAMESPACE_LABEL.get(namespace, "hard"),
        "per_qid_evidence": [
            _evidence_to_prompt_dict(ev)
            for qid, ev in sorted(rca_evidence_typed.items())
        ],
        "schema_columns": sorted(schema_columns),
        "available_repair_shapes": [s.value for s in RepairShape],
    }
    return _json.dumps(payload, indent=2, sort_keys=True)


def _build_request(
    *,
    rca_evidence_typed: dict[str, PerQidRcaEvidence],
    schema_columns: set[str],
    iteration: int,
    namespace: str,
) -> LlmReasoningRequest:
    """Build a Plan-2 LlmReasoningRequest for the clustering call.

    Pure function — no LLM dispatch. Exposed for testability.
    """
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
    user_prompt = _render_user_prompt(
        rca_evidence_typed=rca_evidence_typed,
        schema_columns=schema_columns,
        iteration=iteration,
        namespace=namespace,
    )
    call_id = f"failure_clustering.iter_{int(iteration)}.{namespace}"
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=_HYPHEN_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def cluster_failures_llm(
    *,
    w: Any,
    rca_evidence_typed: dict[str, PerQidRcaEvidence],
    schema_columns: set[str],
    iteration: int,
    namespace: str,
) -> list[LlmCluster] | None:
    """Dispatch one LLM clustering call for one iteration.

    Returns:
      list[LlmCluster] on success (every cluster has a framework-
        stamped cluster_id; passes all validators).
      None on:
        - empty input (no qids to cluster)
        - LLM decline / error
        - validator rejection of the entire set (no-qid-collision)

    Per-cluster validator failures (member_qids out of input,
    blame_set outside schema) reject ONLY that cluster — the others
    pass through. Caller is responsible for falling back per dropped
    qid.

    Plan 11: the historical ``< 2`` early-exit gate is removed. The
    only remaining caller (``optimizer.py``) still pre-gates on
    ``len(rca_evidence_typed) >= 2``, so production behavior is
    unchanged. The new Plan 11 dispatch path needs the ability to
    cluster single-QID sets when an iteration only has one failure
    left in the namespace.
    """
    if not rca_evidence_typed:
        logger.info("cluster_failures_llm.skip empty_input")
        return None

    request = _build_request(
        rca_evidence_typed=rca_evidence_typed,
        schema_columns=schema_columns,
        iteration=iteration,
        namespace=namespace,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)

    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "cluster_failures_llm.declined reason=%s needed=%s",
                response.declined.reason.value,
                list(response.declined.needed_evidence),
            )
        elif response.error is not None:
            logger.warning(
                "cluster_failures_llm.error err=%s", response.error,
            )
        return None

    parsed_clusters = response.parsed_output.get("clusters") or []
    if not parsed_clusters:
        logger.info("cluster_failures_llm.empty_result_clusters")
        return None

    input_qids = set(rca_evidence_typed.keys())

    stamped: list[LlmCluster] = []
    for idx, pc in enumerate(parsed_clusters, start=1):
        candidate = LlmCluster(
            cluster_id=_stamp_cluster_id(namespace=namespace, index=idx),
            semantic_theme=str(pc["semantic_theme"]),
            member_qids=tuple(str(q) for q in pc["member_qids"]),
            unifying_evidence=str(pc["unifying_evidence"]),
            suggested_repair_shape=RepairShape(pc["suggested_repair_shape"]),
            primary_blame_set=tuple(
                str(b) for b in pc.get("primary_blame_set") or []
            ),
            confidence=pc["confidence"],
            # Plan 11: prefer free-text repair_hypothesis if the LLM
            # supplied one; legacy responses leave this blank and
            # downstream code falls back to suggested_repair_shape.value.
            repair_hypothesis=str(pc.get("repair_hypothesis") or ""),
        )
        try:
            _validate_member_qids_in_input(
                candidate, input_qids=input_qids,
            )
            _validate_blame_set_in_schema(
                candidate, schema_columns=schema_columns,
            )
        except ClusterValidationError as exc:
            logger.warning(
                "cluster_failures_llm.cluster_rejected cluster_id=%s err=%s",
                candidate.cluster_id, exc,
            )
            continue
        stamped.append(candidate)

    if not stamped:
        return None

    try:
        _validate_no_qid_collision(stamped)
    except ClusterValidationError as exc:
        logger.warning(
            "cluster_failures_llm.set_rejected err=%s (falling back)",
            exc,
        )
        return None

    return stamped
