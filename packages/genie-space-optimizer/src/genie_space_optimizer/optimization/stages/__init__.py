"""Stage-aligned package for the lever-loop process.

Each module under stages/ corresponds to one PROCESS_STAGE_ORDER entry
from run_output_contract.PROCESS_STAGE_ORDER (defined by the Phase H
plan; the keys are pinned today for forward compatibility). Modules
expose a typed StageInput, StageOutput, and a single execute() entry
point.

The harness composes stages by importing the package and calling
execute() in PROCESS_STAGE_ORDER. Phase H wraps each call with a
capture decorator that writes I/O to MLflow under
``gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/``.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages._protocol import StageHandler
from genie_space_optimizer.optimization.stages._run_evaluation_kwargs import (
    RunEvaluationKwargs,
)

__all__ = ["RunEvaluationKwargs", "StageContext", "StageHandler"]
