"""Phase H Task 3: every stage module declares INPUT_CLASS + OUTPUT_CLASS."""

from __future__ import annotations

import importlib

import pytest


_EXPECTED: tuple[tuple[str, str, str], ...] = (
    # (stage_module, input_class_name, output_class_name)
    ("genie_space_optimizer.optimization.stages.evaluation",
     "EvaluationInput",     "EvaluationResult"),
    ("genie_space_optimizer.optimization.stages.rca_evidence",
     "RcaEvidenceInput",    "RcaEvidenceBundle"),
    ("genie_space_optimizer.optimization.stages.clustering",
     "ClusteringInput",     "ClusterFindings"),
    ("genie_space_optimizer.optimization.stages.action_groups",
     "ActionGroupsInput",   "ActionGroupSlate"),
    ("genie_space_optimizer.optimization.stages.proposals",
     "ProposalsInput",      "ProposalSlate"),
    ("genie_space_optimizer.optimization.stages.gates",
     "GatesInput",          "GateOutcome"),
    ("genie_space_optimizer.optimization.stages.application",
     "ApplicationInput",    "AppliedPatchSet"),
    ("genie_space_optimizer.optimization.stages.acceptance",
     "AcceptanceInput",     "AgOutcome"),
    ("genie_space_optimizer.optimization.stages.learning",
     "LearningInput",       "LearningUpdate"),
)


@pytest.mark.parametrize(
    "module_path, input_name, output_name",
    _EXPECTED,
    ids=[entry[0].rsplit(".", 1)[-1] for entry in _EXPECTED],
)
def test_stage_module_declares_input_class_and_output_class(
    module_path: str, input_name: str, output_name: str,
) -> None:
    module = importlib.import_module(module_path)
    assert hasattr(module, "INPUT_CLASS"), (
        f"{module_path}: missing INPUT_CLASS declaration"
    )
    assert hasattr(module, "OUTPUT_CLASS"), (
        f"{module_path}: missing OUTPUT_CLASS declaration"
    )
    assert module.INPUT_CLASS.__name__ == input_name, (
        f"{module_path}: INPUT_CLASS={module.INPUT_CLASS.__name__!r}, "
        f"expected {input_name!r}"
    )
    assert module.OUTPUT_CLASS.__name__ == output_name, (
        f"{module_path}: OUTPUT_CLASS={module.OUTPUT_CLASS.__name__!r}, "
        f"expected {output_name!r}"
    )
