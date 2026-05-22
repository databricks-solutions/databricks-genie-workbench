"""Trial-readiness tests for the canary helper.

Asserts the canary's new plumbed-from-harness fields land on the
TransformerContext correctly, so the applier_gate and evaluated_gate
have what they need.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import patch as _patch


def _hard_row(qid="q1"):
    return {
        "question_id": qid,
        "feedback/result_correctness/value": "no",
        "question": "How many?",
        "ground_truth_sql": "SELECT COUNT(*) FROM t",
        "generated_sql": "SELECT 1",
        "judge_rationale": "wrong aggregate",
        "sql": "SELECT 1",  # baseline_sql for seen record
    }


def test_canary_passes_metadata_snapshot_through(monkeypatch):
    """The canary helper must propagate metadata_snapshot onto the ctx
    so applier_gate doesn't corrupt the Genie space with an empty
    snapshot."""
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        lambda **kw: [],  # short-circuit at Stage 1
    )

    captured_ctx = {}
    from genie_space_optimizer.optimization.state_machine.orchestrator import (
        StateMachine,
    )
    original_step = StateMachine.step

    def capture_step(self, state, ctx):
        captured_ctx.setdefault("metadata_snapshot", ctx.metadata_snapshot)
        captured_ctx.setdefault("schema_columns", ctx.schema_columns)
        return original_step(self, state, ctx)

    monkeypatch.setattr(StateMachine, "step", capture_step)

    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    maybe_run_state_machine_canary_iteration(
        eval_rows=[_hard_row()],
        iteration=1,
        run_id="r",
        workspace_client=None,
        metadata_snapshot={
            "schema_columns": ["catalog.schema.orders.status"],
            "tables": ["catalog.schema.orders"],
        },
    )

    assert "schema_columns" in captured_ctx
    assert captured_ctx["schema_columns"] == ("catalog.schema.orders.status",)
    assert captured_ctx["metadata_snapshot"]["tables"] == [
        "catalog.schema.orders",
    ]


def test_canary_builds_stage_ctx_when_harness_args_provided(monkeypatch):
    """When predict_fn + scorers + benchmarks are supplied, the helper
    constructs a real StageContext + RunEvaluationKwargs on the ctx."""
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        lambda **kw: [],
    )

    captured = {}
    from genie_space_optimizer.optimization.state_machine.orchestrator import (
        StateMachine,
    )
    original_step = StateMachine.step

    def capture_step(self, state, ctx):
        captured.setdefault("stage_ctx", ctx.stage_ctx)
        captured.setdefault("eval_kwargs", ctx.eval_kwargs)
        return original_step(self, state, ctx)

    monkeypatch.setattr(StateMachine, "step", capture_step)

    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    maybe_run_state_machine_canary_iteration(
        eval_rows=[_hard_row()],
        iteration=1,
        run_id="r",
        workspace_client=None,
        domain="d", catalog="c", schema="s",
        spark=object(), exp_name="e",
        benchmarks=[{"question_id": "q1"}],
        predict_fn=lambda *a, **k: {},
        scorers=[],
        reference_sqls=None,
        uc_schema="c.s",
        max_benchmark_count=1,
    )

    assert captured["stage_ctx"] is not None
    assert captured["eval_kwargs"] is not None
    assert captured["eval_kwargs"]["space_id"] == ""  # default; not passed
    assert captured["eval_kwargs"]["domain"] == "d"
    assert captured["eval_kwargs"]["catalog"] == "c"
    # The stage_ctx is a typed StageContext, not a dict.
    assert captured["stage_ctx"].run_id == "r"
    assert captured["stage_ctx"].iteration == 1


def test_canary_writes_trajectories_to_run_root(monkeypatch):
    """The canary must persist qstate + trajectory JSON so postmortems
    can read what happened."""
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        lambda **kw: [],
    )

    with tempfile.TemporaryDirectory() as td:
        monkeypatch.setenv("GSO_PLAN_V3_RUN_ROOT", td)

        from genie_space_optimizer.optimization.optimizer import (
            maybe_run_state_machine_canary_iteration,
        )
        maybe_run_state_machine_canary_iteration(
            eval_rows=[_hard_row("q1")],
            iteration=1,
            run_id="r_smoke",
            workspace_client=None,
        )

        root = Path(td)
        qstates = list(root.glob("iteration_1/qstate_*.json"))
        trajs = list(root.glob("trajectories/trajectory_*.json"))
        assert len(qstates) == 1
        assert len(trajs) == 1
        assert "q1" in qstates[0].name
