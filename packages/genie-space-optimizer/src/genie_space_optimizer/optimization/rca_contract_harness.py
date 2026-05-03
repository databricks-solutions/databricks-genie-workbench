"""Pure regression harness for the RCA optimizer control contract.

Composes canonical eval-row access, the RCA ledger, RCA execution plans,
proposal grounding, terminal state classification, and rejection-to-next-action
mapping over frozen fixtures so historical failure shapes can be replayed
end-to-end without Databricks services.
"""

from __future__ import annotations

from typing import Any


def _as_patch_for_grounding(plan: Any) -> dict:
    patch: dict = {
        "patch_type": "add_instruction",
        "target": "QUERY PATTERNS",
        "new_text": str(getattr(plan, "evidence_summary", "") or ""),
        "target_qids": list(getattr(plan, "target_qids", ()) or ()),
        "_rca_grounding_terms": list(getattr(plan, "grounding_terms", ()) or ()),
    }
    intents = list(getattr(plan, "patch_intents", ()) or ())
    if intents:
        first = intents[0]
        # Phase C Task 2: ``patch_intents`` is now ``tuple[ExpectedFix, ...]``
        # but historical fixtures + LLM output paths still emit dicts.
        # Accept both.
        if hasattr(first, "as_dict"):
            first_dict = first.as_dict()
        elif isinstance(first, dict):
            first_dict = first
        else:
            first_dict = {}
        patch.update({
            "patch_type": (
                first_dict.get("type")
                or first_dict.get("patch_type")
                or patch["patch_type"]
            ),
            "target": (
                first_dict.get("target")
                or first_dict.get("column")
                or patch["target"]
            ),
            "new_text": first_dict.get("intent") or patch["new_text"],
        })
    return patch


def evaluate_frozen_rca_contract(
    *,
    rows: list[dict],
    source_clusters: list[dict],
    action_group: dict,
    post_arbiter_accuracy: float,
    iteration_counter: int,
    max_iterations: int,
    min_relevance: float = 0.5,
) -> dict[str, Any]:
    """Run the pure RCA control-contract stages over frozen fixtures."""
    from genie_space_optimizer.optimization.control_plane import (
        target_qids_from_action_group,
    )
    from genie_space_optimizer.optimization.proposal_grounding import (
        explain_causal_relevance,
    )
    from genie_space_optimizer.optimization.rca import (
        build_rca_ledger,
        rca_findings_from_clusters,
    )
    from genie_space_optimizer.optimization.rca_execution import (
        build_rca_execution_plans,
        plans_for_action_group,
        required_levers_for_action_group,
    )
    from genie_space_optimizer.optimization.rca_terminal import (
        classify_terminal_state,
    )

    extra_findings = rca_findings_from_clusters(source_clusters)
    ledger = build_rca_ledger(rows, extra_findings=extra_findings)
    execution_plans = build_rca_execution_plans(ledger.get("themes") or [])
    target_qids = target_qids_from_action_group(action_group, source_clusters)
    ag_plans = plans_for_action_group(
        action_group,
        execution_plans,
        source_clusters=source_clusters,
    )
    required_levers = required_levers_for_action_group(
        action_group,
        execution_plans,
        source_clusters=source_clusters,
    )

    grounding: dict[str, Any] = {
        "score": 0.0,
        "failure_category": "no_plans",
        "scoped_row_count": 0,
    }
    if ag_plans:
        grounding = explain_causal_relevance(
            _as_patch_for_grounding(ag_plans[0]),
            rows,
            target_qids=target_qids,
            min_relevance=min_relevance,
        )

    terminal = classify_terminal_state(
        post_arbiter_accuracy=post_arbiter_accuracy,
        max_iterations=max_iterations,
        iteration_counter=iteration_counter,
        actionable_plan_count=len(ag_plans),
        repeated_failure_count=0,
        judge_failure_count=0,
        benchmark_issue_count=0,
        unpatchable_count=0,
    )

    return {
        "target_qids": list(target_qids),
        "finding_count": int(ledger.get("finding_count", 0)),
        "theme_count": int(ledger.get("theme_count", 0)),
        "execution_plan_count": len(execution_plans),
        "action_group_plan_count": len(ag_plans),
        "required_levers": list(required_levers),
        "grounding": grounding,
        "terminal": {
            "status": terminal.status.value,
            "should_continue": terminal.should_continue,
            "reason": terminal.reason,
        },
    }
