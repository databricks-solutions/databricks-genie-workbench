"""Stage 2: RCA Evidence shaping (Phase F2).

Produces a typed RcaEvidenceBundle dataclass from raw eval rows + judge
metadata. F2 is observability-only — the algorithms inside
``cluster_failures`` and ``rca._asi_finding_from_metadata`` stay where
they are. F3 (clustering) reads this typed surface to populate
``ClusterFindings.rca_evidence_by_qid``.

The class is named ``RcaEvidenceBundle`` (not ``RcaEvidence``) to avoid
the existing ``rca.RcaEvidence`` (a single-evidence-atom frozen
dataclass) and to match sibling stage output naming
(``ClusterFindings``, ``ProposalSlate``, ``GateOutcome``,
``AppliedPatchSet``, ``LearningUpdate``) — natural noun for the role,
no stage-prefix or process-order numbering.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.rca import (
    _asi_finding_from_metadata,
    _safe_rca_kind,
    _top_n_collapse_metadata_override,
)


STAGE_KEY: str = "rca_evidence"


@dataclass
class RcaEvidenceInput:
    """Input to stages.rca_evidence.collect.

    ``eval_rows`` is the per-qid eval result list (used for SQL
    extraction). ``hard_failure_qids`` and ``soft_signal_qids`` are the
    partitions from F1 EvaluationResult. ``per_qid_judge`` is the judge
    verdict dict keyed by qid (e.g. ``{"q2": {"verdict": "wrong_join_spec"}}``).
    ``asi_metadata`` is the per-qid metadata dict the judge / ASI
    pipeline produced.
    """

    eval_rows: tuple[dict[str, Any], ...]
    hard_failure_qids: tuple[str, ...]
    soft_signal_qids: tuple[str, ...]
    per_qid_judge: dict[str, dict[str, Any]] = field(default_factory=dict)
    asi_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class RcaEvidenceBundle:
    """Per-qid evidence record after Phase C grounding + PR-D top-N routing.

    ``per_qid_evidence[qid]`` is a dict carrying judge verdict, sql_diff,
    counterfactual_fix, ASI features, and the resolved rca_kind enum value.
    ``rca_kinds_by_qid[qid]`` is the resolved rca_kind string (the
    ``RcaKind`` enum's ``.value``) used by F3 clustering.
    ``evidence_refs[qid]`` is the tuple of trace/eval references the
    DecisionRecord field requires.
    ``promoted_to_top_n_qids`` records which qids the PR-D override
    re-routed to TOP_N_CARDINALITY_COLLAPSE.
    """

    per_qid_evidence: dict[str, dict[str, Any]]
    rca_kinds_by_qid: dict[str, str]
    evidence_refs: dict[str, tuple[str, ...]]
    promoted_to_top_n_qids: tuple[str, ...]


def _row_qid(row: dict[str, Any]) -> str:
    return str(
        row.get("question_id")
        or row.get("qid")
        or row.get("inputs.question_id")
        or ""
    )


def _row_sql(row: dict[str, Any]) -> str:
    return str(
        row.get("genie_sql")
        or row.get("generated_sql")
        or row.get("sql")
        or ""
    )


def _build_metadata(
    *,
    judge: dict[str, Any],
    asi: dict[str, Any],
    sql: str,
) -> tuple[dict[str, Any], str]:
    """Merge judge + ASI metadata, preferring ASI but filling failure_type
    from the judge verdict when ASI doesn't carry it.

    Returns ``(metadata, failure_type)``.
    """
    metadata: dict[str, Any] = dict(asi or {})
    failure_type = str(
        metadata.get("failure_type")
        or judge.get("failure_type")
        or judge.get("verdict")
        or ""
    ).strip()
    if failure_type and not metadata.get("failure_type"):
        metadata["failure_type"] = failure_type
    if sql and not metadata.get("genie_sql"):
        metadata["genie_sql"] = sql
    return metadata, failure_type


def collect(ctx, inp: RcaEvidenceInput) -> RcaEvidenceBundle:
    """Stage 2 entry. Build per-qid evidence using the existing
    ``rca._asi_finding_from_metadata`` primitive, plus PR-D's top-N
    promotion tracking.

    F2 is observability-only: it does NOT modify any harness call
    sites. F3 will consume the produced RcaEvidenceBundle as part of
    its ClusteringInput.
    """
    rows_by_qid: dict[str, dict[str, Any]] = {
        _row_qid(r): r for r in (inp.eval_rows or []) if _row_qid(r)
    }

    per_qid_evidence: dict[str, dict[str, Any]] = {}
    rca_kinds_by_qid: dict[str, str] = {}
    evidence_refs: dict[str, tuple[str, ...]] = {}
    promoted: list[str] = []

    qids = tuple(inp.hard_failure_qids) + tuple(inp.soft_signal_qids)
    for qid in qids:
        qstr = str(qid)
        if not qstr:
            continue

        row = rows_by_qid.get(qstr) or {}
        judge = inp.per_qid_judge.get(qstr) or {}
        asi = inp.asi_metadata.get(qstr) or {}
        sql = _row_sql(row)

        metadata, failure_type = _build_metadata(judge=judge, asi=asi, sql=sql)

        # Detect PR-D top-N promotion BEFORE _asi_finding_from_metadata
        # consumes the metadata. The override only fires for eligible
        # failure types when SQL + intent agree.
        promoted_kind = _top_n_collapse_metadata_override(
            failure_type.lower(), metadata,
        )
        if promoted_kind is not None:
            promoted.append(qstr)

        finding = _asi_finding_from_metadata(
            qstr,
            str(judge.get("judge_name") or "judge_asi"),
            metadata,
        )
        if finding is None:
            # Defensive: empty failure_type → no finding. Skip silently;
            # the qid simply won't appear in per_qid_evidence.
            continue

        rca_kind_value = finding.rca_kind.value
        rca_kinds_by_qid[qstr] = rca_kind_value
        per_qid_evidence[qstr] = {
            "rca_kind": rca_kind_value,
            "judge_verdict": str(judge.get("verdict") or failure_type),
            "sql_diff": sql,
            "counterfactual_fix": metadata.get("counterfactual_fix"),
            "asi_features": dict(asi),
            "expected_objects": list(finding.expected_objects),
            "actual_objects": list(finding.actual_objects),
            "recommended_levers": list(finding.recommended_levers),
            "rca_id": finding.rca_id,
        }
        evidence_refs[qstr] = (
            f"trace://{ctx.run_id}/iter/{ctx.iteration}/judge/{qstr}",
        )

    return RcaEvidenceBundle(
        per_qid_evidence=per_qid_evidence,
        rca_kinds_by_qid=rca_kinds_by_qid,
        evidence_refs=evidence_refs,
        promoted_to_top_n_qids=tuple(promoted),
    )


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = collect
