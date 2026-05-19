"""Plan 6 Task 11 — STAGES registry includes candidate_critique at
position 7 (between proposal_generation and safety_gates).

The position matters for the per-iteration MLflow artifact paths
(``stage_artifact_paths`` formats the prefix as ``<NN>_<stage_key>``)
and for the operator transcript section ordering."""
from __future__ import annotations

from genie_space_optimizer.optimization.stages import STAGES
from genie_space_optimizer.optimization.stages._registry import get_stage
from genie_space_optimizer.optimization.stages.candidate_critique import (
    CritiqueInput,
    CritiqueOutcome,
)


def test_candidate_critique_is_registered_under_canonical_key() -> None:
    entry = get_stage("candidate_critique")
    assert entry.stage_key == "candidate_critique"


def test_candidate_critique_entry_wires_input_output_class_and_execute() -> None:
    entry = get_stage("candidate_critique")
    assert entry.input_class is CritiqueInput
    assert entry.output_class is CritiqueOutcome
    assert callable(entry.execute)
    assert entry.execute.__name__ == "execute"


def test_candidate_critique_position_is_seven() -> None:
    """Position 7: between proposal_generation (6) and safety_gates (8)."""
    keys = [e.stage_key for e in STAGES]
    proposal_idx = keys.index("proposal_generation")
    critique_idx = keys.index("candidate_critique")
    safety_idx = keys.index("safety_gates")
    assert critique_idx == proposal_idx + 1
    assert safety_idx == critique_idx + 1


def test_total_stage_count_is_thirteen() -> None:
    """C15 baseline = 12; Plan 6 adds one → 13."""
    assert len(STAGES) == 13


def test_registry_is_dispatchable_via_get_stage_for_every_key() -> None:
    """Smoke: every registered key resolves."""
    for entry in STAGES:
        resolved = get_stage(entry.stage_key)
        assert resolved is entry
