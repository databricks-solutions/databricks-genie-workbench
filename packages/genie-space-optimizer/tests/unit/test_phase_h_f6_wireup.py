"""Phase H Completion Task 4: F6 wire-up populates 06_safety_gates."""

from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.run_output_contract import (
    stage_artifact_paths,
)
from genie_space_optimizer.optimization.stage_io_capture import (
    wrap_with_io_capture,
)
from genie_space_optimizer.optimization.stages import (
    StageContext,
    gates as _gates_stage,
)


def _make_ctx(*, anchor: str | None) -> StageContext:
    return StageContext(
        run_id="opt-h6",
        iteration=2,
        space_id="space-x",
        domain="airline",
        catalog="cat",
        schema="gso",
        apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda r: None,
        mlflow_anchor_run_id=anchor,
        feature_flags={},
    )


def test_f6_filter_emits_no_decision_records() -> None:
    inp = _gates_stage.GatesInput(
        proposals_by_ag={"ag1": (
            {"proposal_id": "p1", "patch_text": "x", "noop": False},
        )},
        ags=({"id": "ag1"},),
    )
    emitted: list = []
    ctx = StageContext(
        run_id="t", iteration=1, space_id="s", domain="d",
        catalog="c", schema="s", apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=emitted.append,
        mlflow_anchor_run_id=None, feature_flags={},
    )
    _gates_stage.filter(ctx, inp)
    assert emitted == [], (
        "F6.filter must not emit DecisionRecords; harness inline emit "
        "sites remain authoritative"
    )


def test_f6_capture_wrap_writes_to_anchor_run() -> None:
    captured: dict[str, str] = {}

    def _fake_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured[artifact_file] = text

    inp = _gates_stage.GatesInput(
        proposals_by_ag={"ag1": (
            {"proposal_id": "p1", "patch_text": "x", "noop": False},
        )},
        ags=({"id": "ag1"},),
    )
    ctx = _make_ctx(anchor="anchor-run-6")

    wrapped = wrap_with_io_capture(
        execute=_gates_stage.filter, stage_key="safety_gates",
    )
    with patch(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        side_effect=_fake_log_text,
    ):
        wrapped(ctx, inp)

    paths = stage_artifact_paths(iteration=2, stage_key="safety_gates")
    assert paths["input"] in captured
    assert paths["output"] in captured
    assert "06_safety_gates" in paths["input"]
