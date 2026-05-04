"""Stage-aligned package for the lever-loop process.

Each module under stages/ corresponds to one of the 9 canonical
stage keys (locked in stages/_registry.py and later promoted to
Phase H's run_output_contract.PROCESS_STAGE_ORDER). Modules expose
a typed StageInput, StageOutput, and a uniform execute() entry point.

The harness composes stages by importing the package and iterating
over STAGES in process order. Phase H wraps each execute() with a
capture decorator that writes I/O to MLflow under
``gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/``.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages._protocol import StageHandler
from genie_space_optimizer.optimization.stages._registry import (
    STAGES,
    StageEntry,
    get_stage,
)
from genie_space_optimizer.optimization.stages._run_evaluation_kwargs import (
    RunEvaluationKwargs,
)

__all__ = [
    "RunEvaluationKwargs",
    "STAGES",
    "StageContext",
    "StageEntry",
    "StageHandler",
    "get_stage",
]
