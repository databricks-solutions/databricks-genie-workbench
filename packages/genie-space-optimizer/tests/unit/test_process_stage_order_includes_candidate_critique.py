"""Plan 6 Task 12 — PROCESS_STAGE_ORDER includes candidate_critique at
position 7 (between proposal_generation and safety_gates).

The position matters for the operator transcript section ordering and
the per-iteration artifact-path numbering (``stage_artifact_paths``
formats prefixes as ``<NN>_<stage_key>``)."""
from __future__ import annotations

from genie_space_optimizer.optimization.run_output_contract import (
    PROCESS_STAGE_ORDER,
    stage_artifact_paths,
)


def test_candidate_critique_present_in_process_stage_order() -> None:
    keys = [s.key for s in PROCESS_STAGE_ORDER]
    assert "candidate_critique" in keys


def test_candidate_critique_position_is_seven() -> None:
    """Position 7 in the process order: proposal_generation (6),
    candidate_critique (7), safety_gates (8)."""
    keys = [s.key for s in PROCESS_STAGE_ORDER]
    proposal_idx = keys.index("proposal_generation")
    critique_idx = keys.index("candidate_critique")
    safety_idx = keys.index("safety_gates")
    assert critique_idx == proposal_idx + 1
    assert safety_idx == critique_idx + 1


def test_candidate_critique_artifact_path_uses_position_seven_prefix() -> None:
    """``stage_artifact_paths`` formats the prefix as ``<NN>_<stage_key>``.
    candidate_critique at position 7 → ``07_candidate_critique``."""
    paths = stage_artifact_paths(2, "candidate_critique")
    assert "07_candidate_critique" in paths["input"]
    assert "07_candidate_critique" in paths["output"]
    assert "07_candidate_critique" in paths["decisions"]


def test_safety_gates_artifact_path_shifts_to_position_eight() -> None:
    """C15 had safety_gates at position 7; Plan 6 shifts it to 8."""
    paths = stage_artifact_paths(2, "safety_gates")
    assert "08_safety_gates" in paths["input"]


def test_candidate_critique_has_title_and_why_strings() -> None:
    entry = next(s for s in PROCESS_STAGE_ORDER if s.key == "candidate_critique")
    assert entry.title
    assert entry.why
    assert len(entry.why) > 50
