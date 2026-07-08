"""Unit tests for backend/routes/runs.py pipeline step mapping."""

from __future__ import annotations

from genie_space_optimizer.backend.routes.runs import map_stages_to_steps


def test_downstream_stage_overrides_stale_started_rows() -> None:
    stages = [
        {"stage": "INTAKE_AND_SNAPSHOT", "status": "STARTED"},
        {"stage": "INTAKE_AND_SNAPSHOT", "status": "COMPLETE"},
        {"stage": "PREFLIGHT_STARTED", "status": "STARTED"},
        {"stage": "BENCHMARK_QC_AND_REPAIR", "status": "COMPLETE"},
        {"stage": "OPTIMIZE", "status": "STARTED"},
    ]

    steps = map_stages_to_steps(stages, [], {"status": "IN_PROGRESS"})
    by_num = {step.stepNumber: step for step in steps}

    assert by_num[1].status == "completed"
    assert by_num[2].status == "completed"
    assert by_num[3].status == "running"
