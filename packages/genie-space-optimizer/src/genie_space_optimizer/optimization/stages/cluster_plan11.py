"""Plan 11 — Stage 2: LLM-driven failure clustering.

Fresh skill (``plan11_cluster``) rather than modifying the legacy
``failure_clustering`` so the deprecation window has a clean rollback.

Entry point: :func:`cluster_diagnoses`. The handler is dormant during
PR 1 (flag-off); PR 2 wires it in.
"""
from __future__ import annotations

import json
import time
from typing import Any

from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningRequest
from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_stage2_clustering_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
    PerQidDiagnosis,
)
from genie_space_optimizer.skills._loader import _SKILL_LOADER
from genie_space_optimizer.skills.plan11_cluster.output_schema import (
    Plan11ClusterOutput,
)


_SKILL_ID = "plan11_cluster"
_PROMPT_CONST = "PLAN11_CLUSTER_PROMPT"


def _build_request(
    *,
    diagnoses: list[PerQidDiagnosis],
    schema_columns: list[str],
    iteration: int,
    namespace: str,
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
            "namespace": namespace,
            "per_qid_diagnosis": [d.to_json() for d in diagnoses],
            "schema_columns": schema_columns,
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=f"plan11_stage2_cluster.iter_{int(iteration)}.{namespace}",
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def _namespace_prefix(namespace: str) -> str:
    """Match the failure_clustering convention: H001/H002 for hard,
    S001/S002 for soft. Any other namespace gets its first uppercase letter."""
    lowered = (namespace or "").strip().lower()
    if lowered == "hard":
        return "H"
    if lowered == "soft":
        return "S"
    return (namespace[:1] or "X").upper()


def cluster_diagnoses(
    *,
    diagnoses: list[PerQidDiagnosis],
    schema_columns: list[str],
    optimization_run_id: str,
    iteration: int,
    namespace: str,
    w: Any,
) -> list[FailureCluster]:
    """Plan 11 Stage 2 — cluster :class:`PerQidDiagnosis` into
    :class:`FailureCluster` objects.

    Stamps cluster_ids (H001, H002, …) post-LLM. Returns ``[]`` on LLM
    decline or empty input. The empty-input path returns ``[]`` with no
    marker; the decline / error path emits a Stage-2 marker with the
    failure outcome.
    """
    if not diagnoses:
        return []

    request = _build_request(
        diagnoses=diagnoses,
        schema_columns=schema_columns,
        iteration=iteration,
        namespace=namespace,
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
            plan11_stage2_clustering_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                namespace=namespace,
                outcome=outcome,
                input_qids_count=len(diagnoses),
                clusters_count=0,
                abstain_reason=abstain_reason,
                abstain_explanation=abstain_explanation,
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )
        return []

    raw_clusters = resp.parsed_output.get("clusters", []) or []
    prefix = _namespace_prefix(namespace)
    input_qid_set = {d.qid for d in diagnoses}
    fallback_qids = tuple(d.qid for d in diagnoses)
    # Trial 13g — index diagnoses by QID so we can backfill an empty
    # ``primary_blame_set`` from the per-QID Stage 1 evidence.
    diagnosis_by_qid: dict[str, PerQidDiagnosis] = {d.qid: d for d in diagnoses}

    clusters: list[FailureCluster] = []
    primary_blame_set_backfilled = 0
    for idx, item in enumerate(raw_clusters, start=1):
        cluster_id = f"{prefix}{idx:03d}"
        raw_members = item.get("member_qids") or []
        # Drop member_qids the LLM hallucinated outside the input set.
        valid_members = tuple(str(q) for q in raw_members if str(q) in input_qid_set)
        if not valid_members:
            # No valid members — fall back to ALL input qids so downstream
            # stages still have something to act on; better to over-include
            # than to drop the cluster silently.
            valid_members = fallback_qids
        llm_blame_set = tuple(
            str(b) for b in (item.get("primary_blame_set") or [])
        )
        if llm_blame_set:
            primary_blame_set = llm_blame_set
        else:
            # Trial 13g — Stage 2 LLM omitted ``primary_blame_set``.
            # Union the member QIDs' Stage 1 diagnosis blame_sets so
            # downstream Stage 3 still has a cluster-level blame seed
            # to ground its proposals in. Preserves arrival order and
            # deduplicates.
            seen: set[str] = set()
            unioned: list[str] = []
            for qid in valid_members:
                d = diagnosis_by_qid.get(qid)
                if d is None:
                    continue
                for blame in d.blame_set or ():
                    s = str(blame)
                    if s and s not in seen:
                        seen.add(s)
                        unioned.append(s)
            primary_blame_set = tuple(unioned)
            if primary_blame_set:
                primary_blame_set_backfilled += 1
        clusters.append(
            FailureCluster(
                cluster_id=cluster_id,
                semantic_theme=str(item.get("semantic_theme", "")),
                member_qids=valid_members,
                unifying_evidence=str(item.get("unifying_evidence", "")),
                repair_hypothesis=str(item.get("repair_hypothesis", "")),
                primary_blame_set=primary_blame_set,
                confidence=item.get("confidence", "low"),  # type: ignore[arg-type]
            )
        )

    cluster_ids = [c.cluster_id for c in clusters]
    print(
        plan11_stage2_clustering_marker(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            namespace=namespace,
            outcome="clustered" if clusters else "empty_clusters",
            input_qids_count=len(diagnoses),
            clusters_count=len(clusters),
            cluster_ids=cluster_ids,
            duration_ms=duration_ms,
            tokens_input=tokens_in,
            tokens_output=tokens_out,
            primary_blame_set_backfilled=primary_blame_set_backfilled,
        )
    )
    return clusters
