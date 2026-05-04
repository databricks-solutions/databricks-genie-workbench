"""Phase G-lite Task 5: stage registry shape and lookup tests."""

from __future__ import annotations

import pytest


_EXPECTED_KEYS_IN_ORDER: tuple[str, ...] = (
    "evaluation_state",
    "rca_evidence",
    "cluster_formation",
    "action_group_selection",
    "proposal_generation",
    "safety_gates",
    "applied_patches",
    "acceptance_decision",
    "learning_next_action",
)


def test_stages_registry_has_nine_entries_in_process_order() -> None:
    """G-lite Task 5: STAGES is a 9-tuple in the canonical process order."""
    from genie_space_optimizer.optimization.stages import STAGES

    assert len(STAGES) == 9
    actual_keys = tuple(entry.stage_key for entry in STAGES)
    assert actual_keys == _EXPECTED_KEYS_IN_ORDER, (
        f"STAGES order drift: {actual_keys!r}"
    )


def test_each_stage_entry_carries_module_and_execute() -> None:
    """G-lite Task 5: each StageEntry exposes module and execute callable."""
    from genie_space_optimizer.optimization.stages import STAGES

    for entry in STAGES:
        assert entry.module is not None, f"{entry.stage_key}: module is None"
        assert callable(entry.execute), (
            f"{entry.stage_key}: execute is not callable"
        )
        # The execute on the entry must be the same object as the
        # module's execute alias.
        assert entry.execute is entry.module.execute


def test_get_stage_returns_entry_for_known_key() -> None:
    from genie_space_optimizer.optimization.stages import get_stage, STAGES

    entry = get_stage("evaluation_state")
    assert entry is STAGES[0]
    assert entry.stage_key == "evaluation_state"


def test_get_stage_raises_for_unknown_key() -> None:
    from genie_space_optimizer.optimization.stages import get_stage

    with pytest.raises(KeyError, match="unknown_stage"):
        get_stage("unknown_stage")
