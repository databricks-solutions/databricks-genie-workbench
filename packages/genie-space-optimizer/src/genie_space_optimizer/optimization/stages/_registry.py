"""Stage registry: canonical 12-entry tuple in process order.

The registry is the single source of truth for "what stages exist
and in what order" until Phase H promotes the keys to
``run_output_contract.PROCESS_STAGE_ORDER``. After that, this
registry imports the order from there to stay in lockstep.

Each ``StageEntry`` carries:

- ``stage_key``: one of the 12 canonical keys.
- ``module``: the imported stage module (e.g. ``stages.evaluation``).
- ``execute``: the uniform ``execute`` callable on the module
  (added by Phase G-lite Task 2; aliases the named verb).
- ``input_class`` / ``output_class``: the stage's typed Input/Output
  dataclasses (declared by Phase H Task 3 as ``INPUT_CLASS`` /
  ``OUTPUT_CLASS`` on each stage module). The capture decorator reads
  these so it can serialize stage I/O to MLflow without runtime
  introspection.

The harness reads this registry to drive its iteration tape (after
Phase H wires the capture decorator). Phase G-lite does not migrate
the harness — it only provides the registry surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType
from typing import Any, Callable

from genie_space_optimizer.optimization.stages import (
    acceptance,
    action_groups,
    application,
    bundle_assembly,
    clustering,
    evaluation,
    gates,
    learning,
    proposals,
    rca_evidence,
    run_manifest,
    strategist_context,
)


@dataclass(frozen=True)
class StageEntry:
    """A single entry in the stage registry.

    Frozen because the registry is read-only — adding a new stage
    requires editing this module deliberately, not mutating at
    runtime. (G-lite freezes ONLY this small dataclass; it does not
    freeze the per-stage Input/Output dataclasses, which carry the
    real risk.)
    """

    stage_key: str
    module: ModuleType
    execute: Callable[..., Any]
    input_class: type
    output_class: type


# Canonical process order: 12 executable stages.
# PROCESS_STAGE_ORDER (run_output_contract) has 14 entries — the 12 here
# plus ``post_patch_evaluation`` and ``contract_health`` (transcript-only).
# C15 Phase 1 adds ``bundle_assembly`` (position 10) and ``run_manifest``
# (position 11) to the executable registry.
# C15 Phase 2 adds ``strategist_context`` (position 4, between
# ``cluster_formation`` and ``action_group_selection``).
# Phase H's PROCESS_STAGE_ORDER must agree with this tuple's keys
# (the conformance test in tests/unit/test_stage_registry.py pins the order).
STAGES: tuple[StageEntry, ...] = (
    StageEntry("evaluation_state",       evaluation,      evaluation.execute,
               evaluation.INPUT_CLASS,    evaluation.OUTPUT_CLASS),
    StageEntry("rca_evidence",           rca_evidence,    rca_evidence.execute,
               rca_evidence.INPUT_CLASS,  rca_evidence.OUTPUT_CLASS),
    StageEntry("cluster_formation",      clustering,      clustering.execute,
               clustering.INPUT_CLASS,    clustering.OUTPUT_CLASS),
    StageEntry("strategist_context",     strategist_context, strategist_context.execute,
               strategist_context.INPUT_CLASS, strategist_context.OUTPUT_CLASS),
    StageEntry("action_group_selection", action_groups,   action_groups.execute,
               action_groups.INPUT_CLASS, action_groups.OUTPUT_CLASS),
    StageEntry("proposal_generation",    proposals,       proposals.execute,
               proposals.INPUT_CLASS,     proposals.OUTPUT_CLASS),
    StageEntry("safety_gates",           gates,           gates.execute,
               gates.INPUT_CLASS,         gates.OUTPUT_CLASS),
    StageEntry("applied_patches",        application,     application.execute,
               application.INPUT_CLASS,   application.OUTPUT_CLASS),
    StageEntry("acceptance_decision",    acceptance,      acceptance.execute,
               acceptance.INPUT_CLASS,    acceptance.OUTPUT_CLASS),
    StageEntry("learning_next_action",   learning,        learning.execute,
               learning.INPUT_CLASS,      learning.OUTPUT_CLASS),
    StageEntry("bundle_assembly",        bundle_assembly, bundle_assembly.execute,
               bundle_assembly.INPUT_CLASS, bundle_assembly.OUTPUT_CLASS),
    StageEntry("run_manifest",           run_manifest,    run_manifest.execute,
               run_manifest.INPUT_CLASS,  run_manifest.OUTPUT_CLASS),
)


_STAGE_BY_KEY: dict[str, StageEntry] = {
    entry.stage_key: entry for entry in STAGES
}


def get_stage(stage_key: str) -> StageEntry:
    """Return the StageEntry for ``stage_key``.

    Raises ``KeyError`` if ``stage_key`` is not one of the 11 canonical
    keys.
    """
    if stage_key not in _STAGE_BY_KEY:
        raise KeyError(
            f"unknown stage_key: {stage_key!r}. "
            f"Known keys: {sorted(_STAGE_BY_KEY)}"
        )
    return _STAGE_BY_KEY[stage_key]
