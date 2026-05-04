"""Phase G-lite Task 2 + 5 + 6: stage conformance tests."""

from __future__ import annotations

import importlib

import pytest


_EXPECTED_STAGES: tuple[tuple[str, str, str], ...] = (
    # (module_path, STAGE_KEY, named_verb_attr)
    ("genie_space_optimizer.optimization.stages.evaluation",
     "evaluation_state",        "evaluate_post_patch"),
    ("genie_space_optimizer.optimization.stages.rca_evidence",
     "rca_evidence",            "collect"),
    ("genie_space_optimizer.optimization.stages.clustering",
     "cluster_formation",       "form"),
    ("genie_space_optimizer.optimization.stages.action_groups",
     "action_group_selection",  "select"),
    ("genie_space_optimizer.optimization.stages.proposals",
     "proposal_generation",     "generate"),
    ("genie_space_optimizer.optimization.stages.gates",
     "safety_gates",            "filter"),
    ("genie_space_optimizer.optimization.stages.application",
     "applied_patches",         "apply"),
    ("genie_space_optimizer.optimization.stages.acceptance",
     "acceptance_decision",     "decide"),
    ("genie_space_optimizer.optimization.stages.learning",
     "learning_next_action",    "update"),
)


@pytest.mark.parametrize(
    "module_path, expected_key, named_verb",
    _EXPECTED_STAGES,
    ids=[entry[1] for entry in _EXPECTED_STAGES],
)
def test_stage_module_has_execute_alias(
    module_path: str, expected_key: str, named_verb: str,
) -> None:
    """G-lite Task 2: every stage module exposes a uniform ``execute``
    callable that aliases the named verb. The named verb is also still
    exposed for human-readable harness call sites."""
    module = importlib.import_module(module_path)

    assert hasattr(module, "execute"), (
        f"{module_path}: missing module-level execute() alias "
        f"(expected to alias {named_verb!r})"
    )
    assert hasattr(module, named_verb), (
        f"{module_path}: missing named verb {named_verb!r}"
    )
    # The alias and the named verb must be the SAME callable identity.
    assert module.execute is getattr(module, named_verb), (
        f"{module_path}: execute is not aliased to {named_verb!r}"
    )


@pytest.mark.parametrize(
    "module_path, expected_key, named_verb",
    _EXPECTED_STAGES,
    ids=[entry[1] for entry in _EXPECTED_STAGES],
)
def test_stage_module_stage_key_matches_expected(
    module_path: str, expected_key: str, named_verb: str,
) -> None:
    """G-lite Task 6: STAGE_KEY constant on each stage module matches
    the canonical 9-stage process key. STAGE_KEY drift is a contract
    bug that the registry assumes is impossible."""
    module = importlib.import_module(module_path)
    assert hasattr(module, "STAGE_KEY"), (
        f"{module_path}: missing STAGE_KEY constant"
    )
    assert module.STAGE_KEY == expected_key, (
        f"{module_path}: STAGE_KEY={module.STAGE_KEY!r} does not match "
        f"canonical {expected_key!r}"
    )


@pytest.mark.parametrize(
    "module_path, expected_key, named_verb",
    _EXPECTED_STAGES,
    ids=[entry[1] for entry in _EXPECTED_STAGES],
)
def test_stage_module_satisfies_stage_handler_protocol(
    module_path: str, expected_key: str, named_verb: str,
) -> None:
    """G-lite Task 6: every stage module satisfies the runtime-checkable
    StageHandler Protocol via isinstance check on the module-level
    ``execute`` callable.
    """
    from genie_space_optimizer.optimization.stages import StageHandler

    module = importlib.import_module(module_path)
    # @runtime_checkable Protocols check method presence; ``execute``
    # is the method in the StageHandler contract.
    assert isinstance(module, StageHandler), (
        f"{module_path}: does not satisfy StageHandler Protocol "
        f"(execute missing or not callable)"
    )


def test_registry_stage_keys_match_module_stage_keys() -> None:
    """G-lite Task 6: the STAGES registry's stage_keys agree with the
    STAGE_KEY constants on each module. Catches drift at import time."""
    from genie_space_optimizer.optimization.stages import STAGES

    for entry in STAGES:
        assert entry.module.STAGE_KEY == entry.stage_key, (
            f"{entry.stage_key}: registry/module key drift "
            f"(module.STAGE_KEY={entry.module.STAGE_KEY!r})"
        )
