"""Pin that the acceptance stage's capture wrapper still flushes
inputs to MLflow even when the inner stage raises.

Run 11110002 captured acceptance_decision/inputs/iter_*.json but no
outputs/ or decisions/ across all 3 iterations. The wrapper logs
input.json BEFORE invoking the wrapped stage, so even if the wrapped
stage raises, the input artifact is logged. This pins the contract
the harness relies on for stage-09 partial resilience.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


def test_wrap_with_io_capture_flushes_inputs_even_when_run_raises() -> None:
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture,
    )

    captured_inputs: list[Any] = []

    def fake_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured_inputs.append((artifact_file, text))

    def boom(ctx: Any, inp: Any) -> Any:
        raise RuntimeError("inner stage failed")

    wrapped = wrap_with_io_capture(execute=boom, stage_key="acceptance_decision")

    fake_ctx = MagicMock()
    fake_ctx.run_id = "run-x"
    fake_ctx.iteration = 1
    fake_ctx.mlflow_anchor_run_id = "anchor-y"
    fake_ctx.decision_emit = lambda *_a, **_kw: None

    fake_inp = MagicMock()
    fake_inp.to_dict = lambda: {"k": "v"}

    # Patch the module-level _log_text so the wrapper can 'flush' inputs
    # without touching real MLflow.
    import genie_space_optimizer.optimization.stage_io_capture as sio
    original = sio._log_text
    try:
        sio._log_text = fake_log_text
        with pytest.raises(RuntimeError):
            wrapped(fake_ctx, fake_inp)
    finally:
        sio._log_text = original

    # The wrapper must have flushed the input artifact before the inner
    # stage raised. If this assertion fails the wrapper changed shape
    # and the harness fix needs re-validation.
    assert any(
        "input" in name
        for (name, _txt) in captured_inputs
    ), f"Expected input flush before inner failure; got {captured_inputs}"
    # Phase H Task 9 — outputs and decisions must also flush on raise
    # so stage 09's audit trail survives an inner failure.
    assert any(
        "output" in name
        for (name, _txt) in captured_inputs
    ), f"Expected output flush after inner failure; got {captured_inputs}"
    assert any(
        "decisions" in name
        for (name, _txt) in captured_inputs
    ), f"Expected decisions flush after inner failure; got {captured_inputs}"
    # The exception must still propagate so the harness can record it.
    output_blob = next(
        text for (name, text) in captured_inputs if "output" in name
    )
    assert "RuntimeError" in output_blob


def test_wrap_with_io_capture_preserves_decisions_emitted_before_raise() -> None:
    """captured_decisions accumulates as the stage emits; on raise, the
    accumulated list must still flush so stage 09 records what the
    acceptance stage attempted before failing."""
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture,
    )

    captured: list[tuple[str, str]] = []

    def fake_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured.append((artifact_file, text))

    def emit_then_boom(ctx: Any, inp: Any) -> Any:
        ctx.decision_emit({"reason_code": "scratch", "iteration": 1})
        raise ValueError("partial work")

    wrapped = wrap_with_io_capture(
        execute=emit_then_boom, stage_key="acceptance_decision"
    )

    fake_ctx = MagicMock()
    fake_ctx.run_id = "r"
    fake_ctx.iteration = 1
    fake_ctx.mlflow_anchor_run_id = "anchor"
    fake_ctx.decision_emit = lambda *_a, **_kw: None

    fake_inp = MagicMock()
    fake_inp.to_dict = lambda: {}

    import genie_space_optimizer.optimization.stage_io_capture as sio
    original = sio._log_text
    try:
        sio._log_text = fake_log_text
        with pytest.raises(ValueError):
            wrapped(fake_ctx, fake_inp)
    finally:
        sio._log_text = original

    decisions_blob = next(
        text for (name, text) in captured if "decisions" in name
    )
    assert "scratch" in decisions_blob
