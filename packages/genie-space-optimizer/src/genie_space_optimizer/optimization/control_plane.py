"""Pure causal-control-plane helpers for the lever loop.

The helpers in this module define the shared contract between clustering,
RCA, proposal grounding, and acceptance. They intentionally avoid Spark,
WorkspaceClient, LLM calls, and Genie API calls so they can be unit tested
without a Databricks workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from genie_space_optimizer.optimization.evaluation import (
    get_failed_judges,
    has_individual_judge_failure,
    row_is_hard_failure,
)

IGNORED_OPTIMIZATION_JUDGES: frozenset[str] = frozenset({"response_quality"})
"""Judges that may be logged but must not drive optimization work."""


def _row_qid(row: dict) -> str:
    inputs = row.get("inputs")
    if isinstance(inputs, dict):
        nested_qid = inputs.get("question_id") or inputs.get("id")
    else:
        nested_qid = ""
    return str(
        row.get("inputs.question_id")
        or row.get("inputs/question_id")
        or row.get("question_id")
        or row.get("qid")
        or row.get("id")
        or nested_qid
        or ""
    )


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


def rows_by_qid(rows: Iterable[dict]) -> dict[str, dict]:
    """Index eval rows by question ID, dropping rows without an ID."""
    out: dict[str, dict] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        qid = _row_qid(row)
        if qid:
            out[qid] = row
    return out


def rows_for_qids(rows: Iterable[dict], qids: Iterable[str]) -> list[dict]:
    """Return eval rows matching qids in qid order."""
    index = rows_by_qid(rows)
    out: list[dict] = []
    for qid in qids or []:
        row = index.get(str(qid))
        if row is not None:
            out.append(row)
    return out


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

    Explicit ``affected_questions`` wins. If absent, use source clusters.
    Order is stable and duplicates are removed.
    """
    qids: list[str] = []
    for ref in action_group.get("affected_questions") or []:
        qid = _qid_from_question_ref(ref)
        if qid:
            qids.append(qid)
    if not qids:
        source_ids = {
            str(cid)
            for cid in action_group.get("source_cluster_ids", []) or []
            if str(cid)
        }
        for cluster in source_clusters or []:
            if str(cluster.get("cluster_id", "")) not in source_ids:
                continue
            for qid in cluster.get("question_ids", []) or []:
                if qid:
                    qids.append(str(qid))
    return tuple(dict.fromkeys(qids))


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
) -> tuple[list[dict], list[dict]]:
    """Return clusters that may drive the strategist.

    Hard failures are the optimization target. When any hard cluster exists,
    soft clusters are withheld from action-group generation so the strategist
    cannot spend the iteration on soft text-quality work. When no hard
    cluster remains, only non-response-quality soft clusters are eligible.
    """
    hard = list(hard_clusters or [])
    soft = [
        c for c in (soft_clusters or [])
        if isinstance(c, dict) and not _is_response_quality_only_cluster(c)
    ]
    if hard:
        return hard, []
    return [], soft


def hard_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Return qids whose rows are hard failures under the shared predicate."""
    qids: list[str] = []
    for row in rows or []:
        if isinstance(row, dict) and row_is_hard_failure(row):
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
    """
    targets = tuple(dict.fromkeys(str(q) for q in target_qids or [] if str(q)))
    pre_hard = set(hard_failure_qids(pre_rows))
    post_hard = set(hard_failure_qids(post_rows))
    target_set = set(targets)
    target_fixed = tuple(sorted((pre_hard & target_set) - post_hard))
    target_still = tuple(sorted(post_hard & target_set))
    out_of_target_regressed = tuple(sorted((post_hard - pre_hard) - target_set))
    delta = round(float(candidate_accuracy) - float(baseline_accuracy), 1)

    if delta <= 0:
        reason = "post_arbiter_not_improved"
        accepted = False
    elif targets and not target_fixed:
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
