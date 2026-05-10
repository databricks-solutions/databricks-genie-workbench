"""Stage-aligned package for the lever-loop process.

Each module under stages/ corresponds to one of the 12 canonical
stage keys (locked in stages/_registry.py and later promoted to
Phase H's run_output_contract.PROCESS_STAGE_ORDER). Modules expose
a typed StageInput, StageOutput, and a uniform execute() entry point.

The harness composes stages by importing the package and iterating
over STAGES in process order. Phase H wraps each execute() with a
capture decorator that writes I/O to MLflow under
``gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/``.

C15 Phase 1: STAGES grew from 9 to 11 entries — bundle_assembly
and run_manifest added at positions 10 and 11.
C15 Phase 2: STAGES grew from 11 to 12 entries — strategist_context
added at position 4 (between cluster_formation and action_group_selection).
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
