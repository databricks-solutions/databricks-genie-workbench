from genie_space_optimizer.optimization.stages.action_groups import (
    _TERMINATIONS_REQUIRING_PIVOT,
)


def test_kit_forced_inert_reroute_requires_pivot():
    assert "kit_forced_inert_reroute" in _TERMINATIONS_REQUIRING_PIVOT


def test_existing_pivot_members_preserved():
    for member in (
        "kept_insufficient",
        "no_applied_patches",
        "structural_gate_dropped_instruction_only",
        "applyability_rejected",
    ):
        assert member in _TERMINATIONS_REQUIRING_PIVOT
