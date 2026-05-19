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

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.rca import (
    _asi_finding_from_metadata,
    _safe_rca_kind,
    _top_n_collapse_metadata_override,
)


STAGE_KEY: str = "rca_evidence"


@dataclass
class RcaEvidenceInput(JsonRoundTrip):
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
class RcaEvidenceBundle(JsonRoundTrip):
    """Per-qid evidence record after Phase C grounding + PR-D top-N
    routing.

    ``per_qid_evidence[qid]`` is a dict carrying judge verdict,
    sql_diff, counterfactual_fix, ASI features, and the resolved
    rca_kind enum value.
    ``rca_kinds_by_qid[qid]`` is the resolved rca_kind string (the
    ``RcaKind`` enum's ``.value``) used by F3 clustering.
    ``evidence_refs[qid]`` is the tuple of trace/eval references the
    DecisionRecord field requires.
    ``promoted_to_top_n_qids`` records which qids the PR-D override
    re-routed to TOP_N_CARDINALITY_COLLAPSE.

    Plan 3: ``per_qid_evidence_typed[qid]`` is the typed sidecar
    populated when the LLM-driven extractor produced evidence for
    that qid. Absence from this dict means the deterministic
    ``_asi_finding_from_metadata`` fallback supplied this qid's
    entry in ``per_qid_evidence`` (the legacy dict is still
    populated for both paths, so downstream consumers stay
    byte-stable).

    Field type ``dict[str, Any]`` (not ``dict[str, PerQidRcaEvidence]``)
    is intentional — a direct import would create a cycle via
    rca.py. Per-instance typing is enforced by the dispatch code in
    rca_evidence.collect(), not at the dataclass field level.
    """

    per_qid_evidence: dict[str, dict[str, Any]]
    rca_kinds_by_qid: dict[str, str]
    evidence_refs: dict[str, tuple[str, ...]]
    promoted_to_top_n_qids: tuple[str, ...]
    per_qid_evidence_typed: dict[str, Any] = field(default_factory=dict)


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
    """Stage 2 entry. Build per-qid evidence.

    Plan 3 dispatch order (flag-gated):

      1. If ``GSO_PLAN3_LLM_RCA_EVIDENCE`` is true (default-on),
         dispatch per-qid through the rca-evidence-extraction skill
         via Plan 2's LlmReasoningCall. Successful LLM extractions
         populate both the typed sidecar AND the legacy dict (via
         ``PerQidRcaEvidence.to_legacy_dict``).
      2. For every qid the LLM declined / errored / was skipped for
         (or every qid when the flag is off), run the existing
         ``_asi_finding_from_metadata`` deterministic path and
         populate ONLY the legacy dict.

    PR-D's top-N override is detected BEFORE per-qid dispatch — the
    ``promoted_to_top_n_qids`` list is populated regardless of which
    extraction path was used.
    """
    from genie_space_optimizer.common.config import (
        plan3_llm_rca_evidence_enabled,
    )
    from genie_space_optimizer.optimization.rca_evidence_extractor import (
        extract_evidence_for_all_qids,
    )

    rows_by_qid: dict[str, dict[str, Any]] = {
        _row_qid(r): r for r in (inp.eval_rows or []) if _row_qid(r)
    }

    per_qid_evidence: dict[str, dict[str, Any]] = {}
    rca_kinds_by_qid: dict[str, str] = {}
    evidence_refs: dict[str, tuple[str, ...]] = {}
    promoted: list[str] = []
    per_qid_evidence_typed: dict[str, Any] = {}

    qids = tuple(inp.hard_failure_qids) + tuple(inp.soft_signal_qids)

    # ── Step 1: PR-D top-N promotion detection (both paths) ───────────
    metadata_by_qid: dict[str, tuple[dict[str, Any], str]] = {}
    for qid in qids:
        qstr = str(qid)
        if not qstr:
            continue
        row = rows_by_qid.get(qstr) or {}
        judge = inp.per_qid_judge.get(qstr) or {}
        asi = inp.asi_metadata.get(qstr) or {}
        sql = _row_sql(row)
        metadata, failure_type = _build_metadata(
            judge=judge, asi=asi, sql=sql,
        )
        metadata_by_qid[qstr] = (metadata, failure_type)
        promoted_kind = _top_n_collapse_metadata_override(
            failure_type.lower(), metadata,
        )
        if promoted_kind is not None:
            promoted.append(qstr)

    # ── Step 2: LLM-driven typed extraction (flag-gated) ──────────────
    iteration = int(getattr(ctx, "iteration", 0) or 0)
    if plan3_llm_rca_evidence_enabled() and qids:
        sql_by_qid = {
            qstr: _row_sql(rows_by_qid.get(qstr) or {})
            for qstr in (str(q) for q in qids if str(q))
        }
        typed_by_qid = extract_evidence_for_all_qids(
            w=getattr(ctx, "w", None),
            qids=tuple(str(q) for q in qids if str(q)),
            judge_by_qid={
                str(q): inp.per_qid_judge.get(str(q)) or {} for q in qids
            },
            asi_by_qid={
                str(q): inp.asi_metadata.get(str(q)) or {} for q in qids
            },
            sql_by_qid=sql_by_qid,
            iteration=iteration,
        )
        for qstr, evidence in typed_by_qid.items():
            judge = inp.per_qid_judge.get(qstr) or {}
            asi = inp.asi_metadata.get(qstr) or {}
            sql = _row_sql(rows_by_qid.get(qstr) or {})
            legacy_dict = evidence.to_legacy_dict(
                judge=judge, asi=asi, sql=sql,
            )
            per_qid_evidence[qstr] = legacy_dict
            rca_kinds_by_qid[qstr] = legacy_dict["rca_kind"]
            evidence_refs[qstr] = (
                f"trace://{ctx.run_id}/iter/{ctx.iteration}/judge/{qstr}",
            )
            per_qid_evidence_typed[qstr] = evidence

    # ── Step 3: Deterministic fallback for every qid not covered. ─────
    for qid in qids:
        qstr = str(qid)
        if not qstr:
            continue
        if qstr in per_qid_evidence_typed:
            continue
        judge = inp.per_qid_judge.get(qstr) or {}
        metadata, failure_type = metadata_by_qid.get(qstr, ({}, ""))
        finding = _asi_finding_from_metadata(
            qstr,
            str(judge.get("judge_name") or "judge_asi"),
            metadata,
        )
        if finding is None:
            continue
        rca_kind_value = finding.rca_kind.value
        rca_kinds_by_qid[qstr] = rca_kind_value
        per_qid_evidence[qstr] = {
            "rca_kind": rca_kind_value,
            "judge_verdict": str(judge.get("verdict") or failure_type),
            "sql_diff": _row_sql(rows_by_qid.get(qstr) or {}),
            "counterfactual_fix": metadata.get("counterfactual_fix"),
            "asi_features": dict(inp.asi_metadata.get(qstr) or {}),
            "expected_objects": list(finding.expected_objects),
            "actual_objects": list(finding.actual_objects),
            "recommended_levers": list(finding.recommended_levers),
            "rca_id": finding.rca_id,
        }
        evidence_refs[qstr] = (
            f"trace://{ctx.run_id}/iter/{ctx.iteration}/judge/{qstr}",
        )
        # Plan 8 Task 6 — populate the typed sidecar from the same
        # metadata so Plan 4 LLM clustering and Plan 5 LLM intent
        # synthesis see fallback'd qids.
        from genie_space_optimizer.optimization.rca import (
            _typed_evidence_from_metadata,
        )
        sql_for_qid = _row_sql(rows_by_qid.get(qstr) or {})
        typed_fallback = _typed_evidence_from_metadata(
            qstr,
            str(judge.get("judge_name") or "judge_asi"),
            metadata,
            sql_for_qid,
        )
        if typed_fallback is not None:
            per_qid_evidence_typed[qstr] = typed_fallback

    return RcaEvidenceBundle(
        per_qid_evidence=per_qid_evidence,
        rca_kinds_by_qid=rca_kinds_by_qid,
        evidence_refs=evidence_refs,
        promoted_to_top_n_qids=tuple(promoted),
        per_qid_evidence_typed=per_qid_evidence_typed,
    )


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = RcaEvidenceInput
OUTPUT_CLASS = RcaEvidenceBundle


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = collect
