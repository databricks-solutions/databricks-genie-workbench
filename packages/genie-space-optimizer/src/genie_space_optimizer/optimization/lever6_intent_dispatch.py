"""Plan 8 Task 3 — Plan 5 LLM intent short-circuit for Lever 6.

Mirrors the shape of ``_dispatch_lever_5b_for_cluster``'s Plan 5
prelude. When ``rca_evidence_typed`` + ``llm_cluster`` are populated,
call ``synthesize_repair_intent_for_cluster`` to get a typed
``RepairProposal``. If the proposal's ``patch_type`` is one of the
three Lever-6 snippet types, dispatch to the existing Lever-6 legacy
body as the per-lever generator and stamp the typed ``RepairIntent``
on the returned proposal dict.

Returns ``None`` when the LLM declines, when the proposal's
patch_type does not route to L6, or when the legacy generator
returns ``None`` (validation failure). Caller falls back to the
existing heuristic body of ``_generate_lever6_proposal``.
"""
from __future__ import annotations

import logging
from typing import Any

from genie_space_optimizer.optimization.failure_cluster import (
    FailureCluster,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    stamp_repair_intent_on_proposal,
)
from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    synthesize_repair_intent_for_cluster,
)

logger = logging.getLogger(__name__)

_L6_PATCH_TYPES: frozenset[PatchType] = frozenset({
    PatchType.ADD_SQL_SNIPPET_MEASURE,
    PatchType.ADD_SQL_SNIPPET_FILTER,
    PatchType.ADD_SQL_SNIPPET_EXPRESSION,
})


def _generate_lever6_proposal_legacy(*args, **kwargs):
    """Lazy import wrapper for the legacy generator. Indirection lets
    tests monkeypatch this symbol without touching the optimizer
    module's giant import surface."""
    from genie_space_optimizer.optimization.optimizer import (
        _generate_lever6_proposal_legacy_body,
    )
    return _generate_lever6_proposal_legacy_body(*args, **kwargs)


def dispatch_lever_6_with_intent(
    *,
    cluster: dict,
    metadata_snapshot: dict,
    w: Any,
    rca_evidence_typed: dict,
    llm_cluster: Any,
    ag_id: str | None,
    iteration: int,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
    raw_evidence: tuple[dict, ...] = (),
    strategist_hints: list[dict] | None = None,
) -> dict | None:
    """Plan 5 intent-aware short-circuit for Lever 6.

    Returns the stamped proposal dict on success; ``None`` when the
    LLM declines, the patch_type is not an L6 snippet type, or the
    legacy generator returns ``None``. Caller takes the heuristic
    fallback in that case.
    """
    if not (rca_evidence_typed and llm_cluster is not None and ag_id):
        return None

    identifier_allowlist: set[str] = set(
        metadata_snapshot.get("schema_columns") or []
    )
    if not identifier_allowlist:
        for ev in rca_evidence_typed.values():
            identifier_allowlist.update(ev.blame_set)

    existing_questions: list[str] = []
    for ex in (metadata_snapshot.get("instructions", {}) or {}).get(
        "example_question_sqls", []
    ) or []:
        q = (ex or {}).get("question")
        if isinstance(q, str) and q.strip():
            existing_questions.append(q.strip())
    existing_preview = "; ".join(
        f"({i+1}) '{q}'" for i, q in enumerate(existing_questions[:5])
    )

    proposal = synthesize_repair_intent_for_cluster(
        w=w,
        cluster=llm_cluster,
        rca_evidence_typed=rca_evidence_typed,
        identifier_allowlist=identifier_allowlist,
        ag_id=ag_id,
        iteration=int(iteration or 0),
        seq=1,
        existing_examples_preview=existing_preview,
        benchmarks=benchmarks,
    )
    if proposal is None:
        return None
    if proposal.patch_type not in _L6_PATCH_TYPES:
        return None

    proposal_dict = _generate_lever6_proposal_legacy(
        cluster=cluster,
        metadata_snapshot=metadata_snapshot,
        strategist_hints=strategist_hints,
        w=w,
        spark=spark,
        catalog=catalog,
        gold_schema=gold_schema,
        warehouse_id=warehouse_id,
        benchmarks=benchmarks,
        raw_evidence=raw_evidence,
    )
    if proposal_dict is None:
        return None

    fc = FailureCluster.from_legacy(cluster)
    intent = proposal.to_repair_intent(cluster=fc, ag_id=ag_id)
    stamp_repair_intent_on_proposal(proposal_dict, intent)
    logger.info(
        "plan5_l6.intent_dispatch cluster_id=%s ag_id=%s intent_id=%s "
        "patch_type=%s",
        getattr(llm_cluster, "cluster_id", "?"),
        ag_id,
        intent.intent_id,
        proposal.patch_type.value,
    )
    return proposal_dict
