"""Plan P-A — ``stage_io_capture`` exposes which (iteration, stage_key)
pairs successfully captured ``output.json``. The harness terminate
path consumes this index to build per-iteration ``stages/index.json``
files via ``build_iteration_stage_index_payload``.
"""
from __future__ import annotations

import dataclasses
from typing import Any


@dataclasses.dataclass
class _StubCtx:
    iteration: int
    mlflow_anchor_run_id: str | None = "anchor1"
    decision_emit: Any = lambda r: None  # noqa: E731


def test_consume_stage_capture_index_returns_only_captured_pairs(
    monkeypatch,
) -> None:
    from genie_space_optimizer.optimization import stage_io_capture as sic

    def _stub_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        # success — record nothing extra
        return

    monkeypatch.setattr(sic, "_log_text", _stub_log_text)

    # Drain whatever previous tests left behind.
    sic.consume_stage_capture_index()

    def _exec(_ctx: Any, _inp: Any) -> dict:
        return {"ok": True}

    wrapped = sic.wrap_with_io_capture(
        execute=_exec, stage_key="cluster_formation",
    )
    wrapped(_StubCtx(iteration=2), {"in": "x"})

    index = sic.consume_stage_capture_index()
    assert (2, "cluster_formation") in index
    # Drain semantics: a second consume returns empty.
    assert sic.consume_stage_capture_index() == {}


def test_consume_stage_capture_index_skips_failed_output_writes(
    monkeypatch,
) -> None:
    """If ``output.json`` failed to log, the (iter, stage_key) pair
    must NOT appear in the captured index."""
    from genie_space_optimizer.optimization import stage_io_capture as sic

    def _stub_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        if artifact_file.endswith("/output.json"):
            raise RuntimeError("simulated mlflow failure")

    monkeypatch.setattr(sic, "_log_text", _stub_log_text)
    sic.consume_stage_capture_index()  # drain

    def _exec(_ctx: Any, _inp: Any) -> dict:
        return {"ok": True}

    wrapped = sic.wrap_with_io_capture(
        execute=_exec, stage_key="cluster_formation",
    )
    wrapped(_StubCtx(iteration=3), {"in": "x"})

    assert sic.consume_stage_capture_index() == {}
