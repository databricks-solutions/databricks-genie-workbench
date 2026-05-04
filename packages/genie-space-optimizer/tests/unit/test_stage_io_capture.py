"""Phase H Task 5: per-stage I/O capture decorator."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.stages import StageContext


@dataclass
class _FakeIn:
    qid: str = "q1"


@dataclass
class _FakeOut:
    decided: bool = True


def _stub_ctx(decision_emit) -> StageContext:
    return StageContext(
        run_id="r1", iteration=2, space_id="s1", domain="d",
        catalog="c", schema="s", apply_mode="real",
        journey_emit=MagicMock(), decision_emit=decision_emit,
        mlflow_anchor_run_id="run_xyz",
        feature_flags={},
    )


def test_capture_decorator_logs_input_output_and_returns_unchanged(
    monkeypatch,
) -> None:
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture,
    )

    captured_logs: list[tuple[str, str, str]] = []

    def _stub_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured_logs.append((run_id, artifact_file, text))

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _stub_log_text,
    )

    def _execute(ctx, inp):
        return _FakeOut(decided=True)

    wrapped = wrap_with_io_capture(
        execute=_execute,
        stage_key="safety_gates",
    )

    ctx = _stub_ctx(decision_emit=lambda r: None)
    inp = _FakeIn(qid="q9")

    out = wrapped(ctx, inp)

    assert out == _FakeOut(decided=True)

    artifact_paths = sorted(p for _, p, _ in captured_logs)
    assert any("input.json" in p for p in artifact_paths)
    assert any("output.json" in p for p in artifact_paths)
    assert any("decisions.json" in p for p in artifact_paths)
    for _, p, _ in captured_logs:
        assert "iterations/iter_02/stages/06_safety_gates" in p

    in_json = next(t for _, p, t in captured_logs if "input.json" in p)
    assert json.loads(in_json) == {"qid": "q9"}

    out_json = next(t for _, p, t in captured_logs if "output.json" in p)
    assert json.loads(out_json) == {"decided": True}


def test_capture_decorator_captures_emitted_decisions(monkeypatch) -> None:
    """Decisions emitted via ctx.decision_emit during the wrapped call
    are written to decisions.json."""
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture,
    )
    captured_logs: list[tuple[str, str, str]] = []

    def _stub_log_text(*, run_id, text, artifact_file):
        captured_logs.append((run_id, artifact_file, text))

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _stub_log_text,
    )

    captured_records: list[Any] = []

    def _execute(ctx, inp):
        ctx.decision_emit({"type": "GATE_DECISION", "outcome": "dropped"})
        ctx.decision_emit({"type": "GATE_DECISION", "outcome": "accepted"})
        return _FakeOut()

    wrapped = wrap_with_io_capture(
        execute=_execute, stage_key="safety_gates",
    )
    ctx = _stub_ctx(decision_emit=captured_records.append)

    wrapped(ctx, _FakeIn())

    assert len(captured_records) == 2
    decisions_text = next(t for _, p, t in captured_logs if "decisions.json" in p)
    decisions = json.loads(decisions_text)
    assert len(decisions) == 2
    assert decisions[0]["outcome"] == "dropped"


def test_capture_decorator_swallows_mlflow_failures(monkeypatch, caplog) -> None:
    """If MLflow log_text raises, the decorator MUST NOT propagate —
    the optimizer must never break because diagnostic capture failed.
    """
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture,
    )

    def _failing_log_text(*, run_id, text, artifact_file):
        raise RuntimeError("MLflow is down")

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _failing_log_text,
    )

    def _execute(ctx, inp):
        return _FakeOut()

    wrapped = wrap_with_io_capture(execute=_execute, stage_key="safety_gates")
    ctx = _stub_ctx(decision_emit=lambda r: None)

    import logging
    with caplog.at_level(logging.WARNING):
        out = wrapped(ctx, _FakeIn())
    assert out == _FakeOut()

    assert any(
        "stage_io_capture" in record.name and record.levelname == "WARNING"
        for record in caplog.records
    )


def test_capture_decorator_skips_when_no_anchor_run() -> None:
    """If ctx.mlflow_anchor_run_id is None, the decorator skips logging
    silently. Used by replay tests where MLflow is not available.
    """
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture,
    )

    def _execute(ctx, inp):
        return _FakeOut()

    wrapped = wrap_with_io_capture(execute=_execute, stage_key="safety_gates")
    ctx = StageContext(
        run_id="r1", iteration=1, space_id="s1", domain="d",
        catalog="c", schema="s", apply_mode="real",
        journey_emit=MagicMock(), decision_emit=lambda r: None,
        mlflow_anchor_run_id=None,
        feature_flags={},
    )
    out = wrapped(ctx, _FakeIn())
    assert out == _FakeOut()
