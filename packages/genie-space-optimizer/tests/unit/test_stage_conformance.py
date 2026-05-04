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
