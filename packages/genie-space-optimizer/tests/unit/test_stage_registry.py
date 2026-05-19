"""Phase G-lite Task 5: stage registry shape and lookup tests."""

from __future__ import annotations

import pytest


_EXPECTED_KEYS_IN_ORDER: tuple[str, ...] = (
    "evaluation_state",
    "rca_evidence",
    "cluster_formation",
    # C15 Phase 2 new stage:
    "strategist_context",
    "action_group_selection",
    "proposal_generation",
    # Plan 6 new stage (between proposal_generation and safety_gates):
    "candidate_critique",
    "safety_gates",
    "applied_patches",
    "acceptance_decision",
    "learning_next_action",
    # C15 Phase 1 new stages:
    "bundle_assembly",
    "run_manifest",
)

# For tests that check the original 9-stage core set:
_ORIGINAL_9_KEYS: tuple[str, ...] = _EXPECTED_KEYS_IN_ORDER[:9]


def test_stages_registry_has_nine_entries_in_process_order() -> None:
    """C15 Phase 2: STAGES tuple has 12 entries — 9 original stages +
    bundle_assembly + run_manifest (Phase 1) + strategist_context (Phase 2).
    The original 9-stage core order is preserved (with strategist_context
    inserted at position 4 between cluster_formation and action_group_selection)."""
    from genie_space_optimizer.optimization.stages import STAGES

    # At minimum, original 9 must be present in order
    assert len(STAGES) >= 9
    actual_keys = tuple(entry.stage_key for entry in STAGES)
    assert actual_keys[:3] == _ORIGINAL_9_KEYS[:3], (
        f"First 3-stage order drift: {actual_keys[:3]!r}"
    )
    # Plan 6 adds candidate_critique → 13 entries
    assert len(STAGES) == 13, f"Expected 13 STAGES entries, got {len(STAGES)}"
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


def test_each_stage_entry_carries_input_and_output_class() -> None:
    """Phase H Task 3: registry exposes input_class and output_class so
    the capture decorator can serialize stage I/O without runtime
    introspection."""
    from genie_space_optimizer.optimization.stages import STAGES

    for entry in STAGES:
        assert isinstance(entry.input_class, type), (
            f"{entry.stage_key}: input_class not a type"
        )
        assert isinstance(entry.output_class, type), (
            f"{entry.stage_key}: output_class not a type"
        )
        # The classes must be the same objects as the module's declarations.
        assert entry.input_class is entry.module.INPUT_CLASS
        assert entry.output_class is entry.module.OUTPUT_CLASS
