"""Golden snapshot test for map_stages_to_steps().

Updated for the 6-task DAG split: verifies that all stages map to
their correct step after the enrichment extraction.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.backend.routes.runs import (
    _STEP_DEFINITIONS,
    map_stages_to_steps,
)


def _make_stage(stage: str, status: str, task_key: str = "", detail: dict | None = None) -> dict:
    return {
        "stage": stage,
        "status": status,
        "task_key": task_key,
        "started_at": "2026-03-01T00:00:00Z",
        "completed_at": "2026-03-01T00:05:00Z",
        "detail_json": detail,
    }


COMPLETED_RUN_STAGES = [
    _make_stage("PREFLIGHT_STARTED", "STARTED", "preflight"),
    _make_stage("PREFLIGHT_METADATA_COLLECTION", "COMPLETE", "preflight"),
    _make_stage("PREFLIGHT_STARTED", "COMPLETE", "preflight"),
    _make_stage("BASELINE_EVAL_STARTED", "STARTED", "baseline_eval"),
    _make_stage("BASELINE_EVAL_STARTED", "COMPLETE", "baseline_eval"),
    _make_stage("ENRICHMENT_STARTED", "STARTED", "enrichment"),
    _make_stage("PROMPT_MATCHING_SETUP", "COMPLETE", "enrichment"),
    _make_stage("DESCRIPTION_ENRICHMENT", "COMPLETE", "enrichment"),
    _make_stage("JOIN_DISCOVERY", "COMPLETE", "enrichment"),
    _make_stage("SPACE_METADATA_ENRICHMENT", "COMPLETE", "enrichment"),
    _make_stage("ENRICHMENT_COMPLETE", "COMPLETE", "enrichment"),
    _make_stage("LEVER_1_STARTED", "STARTED", "lever_loop"),
    _make_stage("AG_ag1_FULL_EVAL", "COMPLETE", "lever_loop"),
    _make_stage("LEVER_1_STARTED", "COMPLETE", "lever_loop"),
    _make_stage("FINALIZE_STARTED", "STARTED", "finalize"),
    _make_stage("REPEATABILITY_STARTED", "COMPLETE", "finalize"),
    _make_stage("FINALIZE_STARTED", "COMPLETE", "finalize"),
    _make_stage("DEPLOY_STARTED", "STARTED", "deploy"),
    _make_stage("DEPLOY_STARTED", "COMPLETE", "deploy"),
    _make_stage("COMPLETE", "COMPLETE", "deploy"),
]

COMPLETED_RUN_DATA = {
    "status": "CONVERGED",
    "best_accuracy": 85.0,
    "best_repeatability": 90.0,
    "baseline_accuracy": 70.0,
    "best_iteration": 1,
    "levers": [1],
}

ITERATIONS_ROWS = [
    {
        "iteration": 0,
        "eval_scope": "full",
        "overall_accuracy": 70.0,
        "total_questions": 20,
        "correct_count": 14,
    },
]


class TestGoldenSnapshotPostSplit:
    """Asserts the 6-step mapping after the 6-task DAG split."""

    def test_six_steps_returned(self):
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        assert len(steps) == 6

    def test_step_names(self):
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        names = [s.name for s in steps]
        assert names == [
            "Preflight",
            "Baseline Evaluation",
            "Proactive Enrichment",
            "Adaptive Optimization",
            "Finalization",
            "Deploy",
        ]

    def test_preflight_stages_map_to_step_1(self):
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        step1 = steps[0]
        assert step1.name == "Preflight"
        assert step1.stepNumber == 1

    def test_enrichment_stages_map_to_step_3(self):
        """Enrichment stages (DESCRIPTION_ENRICHMENT, JOIN_DISCOVERY, etc.)
        now map to step 3 'Proactive Enrichment'."""
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        step3 = steps[2]
        assert step3.name == "Proactive Enrichment"
        assert step3.stepNumber == 3

    def test_lever_stages_map_to_step_4(self):
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        step4 = steps[3]
        assert step4.name == "Adaptive Optimization"
        assert step4.stepNumber == 4

    def test_deploy_stages_map_to_step_6(self):
        """DEPLOY and COMPLETE stages now map to step 6."""
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        step6 = steps[5]
        assert step6.name == "Deploy"
        assert step6.stepNumber == 6

    def test_all_steps_completed_for_converged_run(self):
        steps = map_stages_to_steps(COMPLETED_RUN_STAGES, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        for step in steps:
            assert step.status == "completed", f"Step {step.stepNumber} '{step.name}' is {step.status}, expected completed"

    def test_step_definitions_count(self):
        assert len(_STEP_DEFINITIONS) == 6

    def test_old_runs_without_enrichment_task_still_map(self):
        """Old runs (pre-split) had enrichment stages with task_key='lever_loop'.
        They should still map to step 3 based on stage name prefix matching."""
        old_stages = [
            _make_stage("PREFLIGHT_STARTED", "COMPLETE", "preflight"),
            _make_stage("BASELINE_EVAL_STARTED", "COMPLETE", "baseline_eval"),
            _make_stage("DESCRIPTION_ENRICHMENT", "COMPLETE", "lever_loop"),
            _make_stage("JOIN_DISCOVERY", "COMPLETE", "lever_loop"),
            _make_stage("LEVER_1_STARTED", "COMPLETE", "lever_loop"),
            _make_stage("FINALIZE_STARTED", "COMPLETE", "finalize"),
            _make_stage("DEPLOY_STARTED", "COMPLETE", "deploy"),
            _make_stage("COMPLETE", "COMPLETE", "deploy"),
        ]
        steps = map_stages_to_steps(old_stages, ITERATIONS_ROWS, dict(COMPLETED_RUN_DATA))
        assert len(steps) == 6
        assert steps[2].name == "Proactive Enrichment"
        assert steps[2].status == "completed"
