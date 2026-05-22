"""GSO_OPTIMIZER_OUTCOME_V1 marker shape."""
import json

from genie_space_optimizer.optimization.state_machine.markers import (
    optimizer_outcome_marker,
)


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_outcome_marker_shape():
    line = optimizer_outcome_marker(
        run_id="run_x",
        outcome="OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
        hard_qids_count=3,
        deepest_stage_by_qid={"gs_009": "applyable", "gs_024": "proposed"},
    )
    name, payload = _parse(line)
    assert name == "GSO_OPTIMIZER_OUTCOME_V1"
    assert payload["outcome"] == "OPTIMIZER_STALLED_NO_APPLIED_PATCHES"
    assert payload["hard_qids_count"] == 3
    assert payload["deepest_stage_by_qid"]["gs_009"] == "applyable"
