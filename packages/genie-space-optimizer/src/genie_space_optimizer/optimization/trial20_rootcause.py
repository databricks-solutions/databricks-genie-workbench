"""Trial 20 — A1 merge-blocking root-cause replay helper.

Pure offline helper that replays the EXACT full-eval inputs through
:func:`decide_control_plane_acceptance` and
:func:`decide_pre_arbiter_regression_guardrail` and emits
``GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1`` capturing every input the two
helpers consume.

Operators consume this via:

* :func:`build_full_eval_root_cause_marker` — pure helper, takes the
  EXACT pre_rows / post_rows / target_qids / baselines an MLflow run
  recorded and returns the marker payload as a dict.

* :func:`format_marker_line` — wraps the marker dict into the
  ``GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1 <json>`` line shape that
  postmortem joins consume.

A1 is a diagnostic; no production callsite. The
``test_trial20_rootcause_marker.py`` and integration replay fixtures
exercise the helper.
"""
from __future__ import annotations

import json
from typing import Any, Iterable

from genie_space_optimizer.optimization.control_plane import (
    decide_control_plane_acceptance,
    decide_pre_arbiter_regression_guardrail,
    hard_failure_qids,
)


_ROOT_CAUSE_MARKER = "GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1"


def _row_has_arbiter_field(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    for key in (
        "feedback/arbiter/value",
        "arbiter/value",
        "arbiter_verdict",
    ):
        v = row.get(key)
        if isinstance(v, str) and v.strip():
            return True
    return False


def _classify_root_cause(
    *,
    target_fixed: tuple[str, ...],
    target_qids: tuple[str, ...],
    pre_hard: set[str],
    post_hard: set[str],
    arbiter_field_missing_count: int,
    arbiter_field_present_count: int,
    baseline_source: str,
    candidate_source: str,
    decision_today_accepted: bool,
    pre_arbiter_blocked: bool,
) -> str:
    """Return the most likely fix surface label.

    Labels (one of):

    * ``arbiter_field_stripped`` — at least one full-eval row is
      missing the arbiter field even though sliced eval emitted it.
      Row-projection path needs the fix.
    * ``target_attribution_drift`` — the rescued QID is NOT in
      ``target_qids`` declared by the AG; target attribution needs
      to be widened or the guardrail needs to fall back on
      post-arbiter delta.
    * ``baseline_source_mismatch`` — the baseline rows fed to the
      gate come from a different source than the metrics; baseline
      selection at ``control_plane.select_control_plane_baseline_rows``
      needs the fix.
    * ``post_arbiter_gain_absorbs_pre_arbiter_regression`` —
      ``target_fixed`` is empty AND post-arbiter delta is positive;
      the guardrail itself is the fix surface (add a post-arbiter
      delta early-accept symmetric to the existing
      ``accepted_pre_arbiter_improvement`` branch).
    * ``unclear`` — none of the structural signals fire; manual
      inspection required.
    """
    if not pre_arbiter_blocked:
        return "not_pre_arbiter_blocked"
    if arbiter_field_missing_count > 0 and arbiter_field_present_count == 0:
        return "arbiter_field_stripped"
    rescued = pre_hard - post_hard
    rescued_outside_targets = rescued - set(target_qids)
    if rescued_outside_targets:
        return "target_attribution_drift"
    if baseline_source != candidate_source and baseline_source != "unknown":
        return "baseline_source_mismatch"
    if not target_fixed:
        return "post_arbiter_gain_absorbs_pre_arbiter_regression"
    return "unclear"


def build_full_eval_root_cause_marker(
    *,
    run_id: str,
    ag_id: str,
    iteration: int,
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
    target_qids: Iterable[str],
    baseline_accuracy: float,
    candidate_accuracy: float,
    baseline_pre_arbiter_accuracy: float,
    candidate_pre_arbiter_accuracy: float,
    baseline_source: str = "unknown",
    candidate_source: str = "unknown",
    max_pre_arbiter_regression_pp: float = 5.0,
) -> dict[str, Any]:
    """Pure replay; returns the marker payload as a dict.

    The marker payload is the source of truth for A1; A2's fix surface
    is determined by inspecting the ``identified_fix_surface`` field
    and reading the corroborating signals (arbiter field counts,
    target attribution, baseline source).
    """
    pre_list = list(pre_rows or [])
    post_list = list(post_rows or [])
    targets = tuple(dict.fromkeys(str(q) for q in target_qids or [] if str(q)))

    pre_hard = set(hard_failure_qids(pre_list))
    post_hard = set(hard_failure_qids(post_list))
    target_set = set(targets)
    target_fixed = tuple(sorted((pre_hard & target_set) - post_hard))

    arbiter_field_present_count = sum(
        1 for r in pre_list if _row_has_arbiter_field(r)
    ) + sum(1 for r in post_list if _row_has_arbiter_field(r))
    arbiter_field_missing_count = (
        len(pre_list) + len(post_list) - arbiter_field_present_count
    )

    control_decision = decide_control_plane_acceptance(
        baseline_accuracy=float(baseline_accuracy),
        candidate_accuracy=float(candidate_accuracy),
        target_qids=targets,
        pre_rows=pre_list,
        post_rows=post_list,
        baseline_pre_arbiter_accuracy=float(baseline_pre_arbiter_accuracy),
        candidate_pre_arbiter_accuracy=float(candidate_pre_arbiter_accuracy),
    )
    pre_arbiter_decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=float(baseline_pre_arbiter_accuracy),
        candidate_pre_arbiter_accuracy=float(candidate_pre_arbiter_accuracy),
        target_fixed_qids=tuple(
            sorted(set(control_decision.target_fixed_qids or ()))
        ),
        max_pre_arbiter_regression_pp=float(max_pre_arbiter_regression_pp),
    )

    decision_today_accepted = bool(
        control_decision.accepted and pre_arbiter_decision.accepted
    )
    pre_arbiter_blocked = not pre_arbiter_decision.accepted

    post_arbiter_delta = round(
        float(candidate_accuracy) - float(baseline_accuracy), 1
    )
    pre_arbiter_delta = round(
        float(candidate_pre_arbiter_accuracy)
        - float(baseline_pre_arbiter_accuracy),
        1,
    )

    identified = _classify_root_cause(
        target_fixed=target_fixed,
        target_qids=targets,
        pre_hard=pre_hard,
        post_hard=post_hard,
        arbiter_field_missing_count=arbiter_field_missing_count,
        arbiter_field_present_count=arbiter_field_present_count,
        baseline_source=baseline_source,
        candidate_source=candidate_source,
        decision_today_accepted=decision_today_accepted,
        pre_arbiter_blocked=pre_arbiter_blocked,
    )

    # Shadow decision with the post-arbiter-delta early-accept branch.
    decision_with_post_arbiter_absorbs = bool(
        post_arbiter_delta > 0.0 and control_decision.accepted
    )

    return {
        "marker": _ROOT_CAUSE_MARKER,
        "run_id": str(run_id),
        "ag_id": str(ag_id),
        "iteration": int(iteration),
        "target_qids": list(targets),
        "target_fixed_qids": list(target_fixed),
        "pre_hard": sorted(pre_hard),
        "post_hard": sorted(post_hard),
        "rescued_qids": sorted(pre_hard - post_hard),
        "regressed_qids": sorted(post_hard - pre_hard),
        "arbiter_field_present_count": int(arbiter_field_present_count),
        "arbiter_field_missing_count": int(arbiter_field_missing_count),
        "baseline_source": str(baseline_source),
        "candidate_source": str(candidate_source),
        "post_arbiter_delta_pp": float(post_arbiter_delta),
        "pre_arbiter_delta_pp": float(pre_arbiter_delta),
        "decision_today_accepted": decision_today_accepted,
        "control_plane_reason": str(control_decision.reason_code or ""),
        "pre_arbiter_reason": str(pre_arbiter_decision.reason_code or ""),
        "decision_with_post_arbiter_absorbs": decision_with_post_arbiter_absorbs,
        "identified_fix_surface": identified,
    }


def format_marker_line(payload: dict[str, Any]) -> str:
    """Format the marker dict as the ``MARKER <json>`` log line."""
    return f"{_ROOT_CAUSE_MARKER} {json.dumps(payload, sort_keys=True)}"


__all__ = [
    "build_full_eval_root_cause_marker",
    "format_marker_line",
]
