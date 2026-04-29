"""Pure causal-control-plane helpers for the lever loop.

The helpers in this module define the shared contract between clustering,
RCA, proposal grounding, and acceptance. They intentionally avoid Spark,
WorkspaceClient, LLM calls, and Genie API calls so they can be unit tested
without a Databricks workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from genie_space_optimizer.common.config import (
    IGNORED_OPTIMIZATION_JUDGES as _CONFIG_IGNORED_OPTIMIZATION_JUDGES,
)
from genie_space_optimizer.optimization.eval_row_access import (
    row_qid as _row_qid,
    rows_by_qid,
    rows_for_qids,
)
from genie_space_optimizer.optimization.evaluation import (
    get_failed_judges,
    has_individual_judge_failure,
    row_is_hard_failure,
)

IGNORED_OPTIMIZATION_JUDGES: frozenset[str] = frozenset(
    _CONFIG_IGNORED_OPTIMIZATION_JUDGES
)
"""Judges that may be logged but must not drive optimization work.

Sourced from ``common.config.IGNORED_OPTIMIZATION_JUDGES`` so the
``GSO_IGNORED_OPTIMIZATION_JUDGES`` env var is the single source of
truth across the optimizer engine. Re-exported here as a frozenset for
fast membership checks in the control-plane path.
"""


def actionable_failed_judges(row: dict) -> tuple[str, ...]:
    """Return failed judges that are allowed to drive optimizer action."""
    failed = tuple(get_failed_judges(row or {}))
    return tuple(j for j in failed if j not in IGNORED_OPTIMIZATION_JUDGES)


def is_actionable_soft_signal_row(row: dict) -> bool:
    """Return true when a non-hard row has actionable non-text judge failures."""
    if row_is_hard_failure(row or {}):
        return False
    if not has_individual_judge_failure(row or {}):
        return False
    return bool(actionable_failed_judges(row or {}))


def _qid_from_question_ref(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(
            value.get("question_id")
            or value.get("id")
            or value.get("qid")
            or ""
        ).strip()
    return ""


def target_qids_from_action_group(
    action_group: dict,
    source_clusters: Iterable[dict],
) -> tuple[str, ...]:
    """Resolve the qids an action group claims to fix.

    Explicit ``affected_questions`` is accepted only when its entries
    match known source-cluster qids. LLMs sometimes emit the natural
    language question text in this field; that must fall back to
    ``source_cluster_ids`` rather than scoping grounding to zero rows.
    """
    source_ids = {
        str(cid)
        for cid in action_group.get("source_cluster_ids", []) or []
        if str(cid)
    }
    known_qids: list[str] = []
    for cluster in source_clusters or []:
        if source_ids and str(cluster.get("cluster_id", "")) not in source_ids:
            continue
        for qid in cluster.get("question_ids", []) or []:
            if qid:
                known_qids.append(str(qid))

    known_set = set(known_qids)
    explicit: list[str] = []
    for ref in action_group.get("affected_questions") or []:
        qid = _qid_from_question_ref(ref)
        if qid and (not known_set or qid in known_set):
            explicit.append(qid)

    if explicit:
        return tuple(dict.fromkeys(explicit))
    return tuple(dict.fromkeys(known_qids))


def _cluster_judges(cluster: dict) -> tuple[str, ...]:
    raw = (
        cluster.get("affected_judges")
        or cluster.get("dominant_failed_judges")
        or [cluster.get("affected_judge")]
        or []
    )
    if isinstance(raw, str):
        raw = [raw]
    return tuple(str(j) for j in raw if str(j))


def _is_response_quality_only_cluster(cluster: dict) -> bool:
    judges = tuple(j for j in _cluster_judges(cluster) if j)
    return bool(judges) and all(j in IGNORED_OPTIMIZATION_JUDGES for j in judges)


def clusters_for_strategy(
    hard_clusters: list[dict],
    soft_clusters: list[dict],
    *,
    hard_only_threshold: int = 3,
    soft_min_questions: int = 5,
    max_soft_clusters: int = 1,
) -> tuple[list[dict], list[dict]]:
    """Return clusters that may drive the strategist.

    Hard failures remain first priority. When the hard set is small and a
    soft cluster covers many questions, include a bounded soft lane so the
    optimizer can learn broad corpus guidance without starving hard fixes.
    """
    hard = list(hard_clusters or [])
    soft = [
        c for c in (soft_clusters or [])
        if isinstance(c, dict) and not _is_response_quality_only_cluster(c)
    ]
    if not hard:
        return [], soft
    if len(hard) > int(hard_only_threshold):
        return hard, []

    large_soft = sorted(
        [
            c for c in soft
            if len(c.get("question_ids", []) or []) >= int(soft_min_questions)
        ],
        key=lambda c: len(c.get("question_ids", []) or []),
        reverse=True,
    )
    return hard, large_soft[: int(max_soft_clusters)]


def hard_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Return qids whose rows are hard failures under the shared predicate."""
    qids: list[str] = []
    for row in rows or []:
        if isinstance(row, dict) and row_is_hard_failure(row):
            qid = _row_qid(row)
            if qid:
                qids.append(qid)
    return tuple(dict.fromkeys(qids))


def _arbiter_value(row: dict) -> str:
    return str(
        row.get("feedback/arbiter/value")
        or row.get("arbiter/value")
        or row.get("arbiter")
        or ""
    ).strip().lower()


def _result_correctness_value(row: dict) -> str:
    return str(
        row.get("feedback/result_correctness/value")
        or row.get("result_correctness/value")
        or row.get("result_correctness")
        or ""
    ).strip().lower()


def _cluster_qids(cluster: dict) -> set[str]:
    return {str(q) for q in cluster.get("question_ids", []) or [] if str(q)}


def uncovered_patchable_clusters(
    source_clusters: list[dict],
    action_groups: list[dict],
) -> list[dict]:
    """Return patchable hard clusters not covered by strategist output."""
    covered_cluster_ids: set[str] = set()
    covered_qids: set[str] = set()
    for ag in action_groups or []:
        covered_cluster_ids.update(
            str(cid) for cid in ag.get("source_cluster_ids", []) or [] if str(cid)
        )
        covered_qids.update(
            str(q) for q in ag.get("affected_questions", []) or [] if str(q)
        )

    uncovered: list[dict] = []
    for cluster in source_clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        qids = _cluster_qids(cluster)
        if cid and cid in covered_cluster_ids:
            continue
        if qids and qids <= covered_qids:
            continue
        uncovered.append(cluster)
    return uncovered


def diagnostic_action_group_for_cluster(cluster: dict) -> dict:
    """Build a deterministic AG when the strategist omits a hard cluster."""
    cid = str(cluster.get("cluster_id") or "H_UNKNOWN")
    qids = [str(q) for q in cluster.get("question_ids", []) or [] if str(q)]
    root = str(cluster.get("root_cause") or cluster.get("asi_failure_type") or "unknown")
    fixes = [
        str(f) for f in cluster.get("asi_counterfactual_fixes", []) or []
        if str(f)
    ]
    fix_text = fixes[0] if fixes else "Use the cluster RCA evidence to produce a targeted metadata change."
    return {
        "id": f"AG_COVERAGE_{cid}",
        "root_cause_summary": f"{root}: {fix_text}",
        "affected_questions": qids,
        "source_cluster_ids": [cid],
        "coverage_reason": "strategist_omitted_patchable_hard_cluster",
        "lever_directives": {},
    }


def patchable_hard_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Rows where GT is confirmed correct and Genie should be patched."""
    qids: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if _result_correctness_value(row) not in {"no", "false", "0", "0.0"}:
            continue
        if _arbiter_value(row) != "ground_truth_correct":
            continue
        qid = _row_qid(row)
        if qid:
            qids.append(qid)
    return tuple(dict.fromkeys(qids))


def ambiguous_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Rows where neither answer is endorsed and benchmark review is safer."""
    qids: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if _result_correctness_value(row) not in {"no", "false", "0", "0.0"}:
            continue
        if _arbiter_value(row) != "neither_correct":
            continue
        qid = _row_qid(row)
        if qid:
            qids.append(qid)
    return tuple(dict.fromkeys(qids))


@dataclass(frozen=True)
class ControlPlaneAcceptance:
    accepted: bool
    reason_code: str
    baseline_accuracy: float
    candidate_accuracy: float
    delta_pp: float
    target_qids: tuple[str, ...]
    target_fixed_qids: tuple[str, ...]
    target_still_hard_qids: tuple[str, ...]
    out_of_target_regressed_qids: tuple[str, ...]


def _fmt_qids(qids: Iterable[str]) -> str:
    values = tuple(str(q) for q in qids or () if str(q))
    return ", ".join(values) if values else "(none)"


def format_control_plane_acceptance_detail(
    decision: ControlPlaneAcceptance,
) -> str:
    """Return a compact operator-facing reason for control-plane rejection."""
    return (
        f"reason={decision.reason_code}; "
        f"target_qids={_fmt_qids(decision.target_qids)}; "
        f"target_fixed_qids={_fmt_qids(decision.target_fixed_qids)}; "
        f"target_still_hard_qids={_fmt_qids(decision.target_still_hard_qids)}; "
        f"out_of_target_regressed_qids={_fmt_qids(decision.out_of_target_regressed_qids)}"
    )


def decide_control_plane_acceptance(
    *,
    baseline_accuracy: float,
    candidate_accuracy: float,
    target_qids: Iterable[str],
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
) -> ControlPlaneAcceptance:
    """Accept only causal post-arbiter improvement with no hard regressions.

    The global objective is post-arbiter accuracy. Target-qid checks prevent
    accepting unrelated gains when the proposed causal target did not improve.

    The unified causal contract requires every iteration to declare which
    hard-failure QIDs it intends to fix. An empty ``target_qids`` set means
    the iteration's patches were never grounded to a causal target, so the
    decision rejects with ``missing_target_qids`` even when global accuracy
    rose — that prevents un-targeted, lucky gains from masking the loss of
    causal grounding.
    """
    targets = tuple(dict.fromkeys(str(q) for q in target_qids or [] if str(q)))
    pre_hard = set(hard_failure_qids(pre_rows))
    post_hard = set(hard_failure_qids(post_rows))
    target_set = set(targets)
    target_fixed = tuple(sorted((pre_hard & target_set) - post_hard))
    target_still = tuple(sorted(post_hard & target_set))
    out_of_target_regressed = tuple(sorted((post_hard - pre_hard) - target_set))
    delta = round(float(candidate_accuracy) - float(baseline_accuracy), 1)

    if not targets:
        reason = "missing_target_qids"
        accepted = False
    elif delta <= 0:
        reason = "post_arbiter_not_improved"
        accepted = False
    elif not target_fixed:
        reason = "target_qids_not_improved"
        accepted = False
    elif out_of_target_regressed:
        reason = "out_of_target_hard_regression"
        accepted = False
    else:
        reason = "accepted"
        accepted = True

    return ControlPlaneAcceptance(
        accepted=accepted,
        reason_code=reason,
        baseline_accuracy=round(float(baseline_accuracy), 1),
        candidate_accuracy=round(float(candidate_accuracy), 1),
        delta_pp=delta,
        target_qids=targets,
        target_fixed_qids=target_fixed,
        target_still_hard_qids=target_still,
        out_of_target_regressed_qids=out_of_target_regressed,
    )
