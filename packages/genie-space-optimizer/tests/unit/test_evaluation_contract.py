"""C15-P2.1: EvaluationInput / EvaluationResult typed contract tests.

The existing evaluation stage preserves its production-shape classes
(EvaluationInput with space_state / eval_qids / run_role, EvaluationResult
with scoreboard / hard_failure_qids etc.) and adds JsonRoundTrip as a mixin
so boundary-fixture replay can serialize / deserialize stage I/O.

Naming note: test_stage_io_class_declarations.py pins:
  INPUT_CLASS  = EvaluationInput   (not EvaluationInput with eval_rows)
  OUTPUT_CLASS = EvaluationResult  (not EvaluationOutput)
These tests conform to those pinned names.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.evaluation import (
    EvaluationInput,
    EvaluationResult,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_mixes_jsonroundtrip() -> None:
    assert issubclass(EvaluationInput, JsonRoundTrip)


def test_output_mixes_jsonroundtrip() -> None:
    assert issubclass(EvaluationResult, JsonRoundTrip)


def test_input_to_json_round_trips_fields() -> None:
    """EvaluationInput.to_json() / from_json() preserves all fields."""
    inp = EvaluationInput(
        space_state={"genie_space_id": "abc"},
        eval_qids=("gs_001", "gs_007"),
        run_role="baseline",
        iteration_label="iter_01",
        scope="full",
    )
    payload = inp.to_json()
    assert payload["run_role"] == "baseline"
    assert payload["scope"] == "full"
    # eval_qids is a tuple — serializes as list, restores as tuple via from_json
    restored = EvaluationInput.from_json(payload)
    assert restored.eval_qids == ("gs_001", "gs_007")
    assert restored.run_role == "baseline"


def test_output_to_json_round_trips_fields() -> None:
    """EvaluationResult.to_json() / from_json() preserves tuple fields."""
    out = EvaluationResult(
        scoreboard={"overall_accuracy": 0.833},
        hard_failure_qids=("gs_024",),
        soft_signal_qids=(),
        already_passing_qids=("gs_001", "gs_007"),
        gt_correction_candidate_qids=(),
        eval_rows=(),
    )
    payload = out.to_json()
    restored = EvaluationResult.from_json(payload)
    assert restored.hard_failure_qids == ("gs_024",)
    assert restored.already_passing_qids == ("gs_001", "gs_007")
    assert restored.scoreboard["overall_accuracy"] == pytest.approx(0.833)
