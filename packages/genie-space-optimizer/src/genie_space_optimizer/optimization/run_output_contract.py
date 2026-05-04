"""Stable GSO Run Output Contract vocabulary.

Reads as the canonical vocabulary; never imports MLflow, Spark, or
Databricks SDK. All callers (transcript renderer, bundle assembler,
evidence_bundle, mlflow_audit, gso-postmortem skill) share these
constants and path builders.

Reconciliation with the G-lite stage registry (stages/_registry.py):

  - STAGES (9 entries) is the executable iteration target.
  - PROCESS_STAGE_ORDER (11 entries) is the human-readable transcript
    ordering.
  - The reconciliation rule is locked by
    tests/unit/test_process_stage_order_matches_stages_registry.py:
    every STAGES.stage_key must appear as a PROCESS_STAGE_ORDER.key.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


GSO_BUNDLE_ROOT = "gso_postmortem_bundle"


class RunRole(str, Enum):
    """Run roles tagged on MLflow runs to make ``mlflow_audit`` and the
    ``gso-postmortem`` skill discover the right run from a flat
    experiment listing."""
    LEVER_LOOP = "lever_loop"
    ITERATION_EVAL = "iteration_eval"
    STRATEGY = "strategy"
    FINALIZE = "finalize"
    ENRICHMENT_SNAPSHOT = "enrichment_snapshot"
    LOGGED_MODEL = "logged_model"


@dataclass(frozen=True)
class ProcessStage:
    """One row of the process transcript ordering.

    ``key`` matches a stage_key. ``title`` is a human heading. ``why``
    is a one-paragraph educational explanation rendered into the
    transcript so a new operator can follow the optimizer end-to-end.
    """
    key: str
    title: str
    why: str


PROCESS_STAGE_ORDER: tuple[ProcessStage, ...] = (
    ProcessStage(
        key="evaluation_state",
        title="Evaluation State",
        why=(
            "The optimizer first evaluates the current Genie Space so it "
            "knows which questions are hard failures, which are soft "
            "signals, and which passing questions must be protected."
        ),
    ),
    ProcessStage(
        key="rca_evidence",
        title="RCA Evidence",
        why=(
            "The optimizer must diagnose why each question failed before "
            "it proposes changes; judge output, ASI metadata, SQL diffs, "
            "and counterfactual fixes form that evidence."
        ),
    ),
    ProcessStage(
        key="cluster_formation",
        title="Cluster Formation",
        why=(
            "Related failures are grouped so the optimizer targets causal "
            "patterns instead of isolated symptoms."
        ),
    ),
    ProcessStage(
        key="action_group_selection",
        title="Action Group Selection",
        why=(
            "The strategist chooses the cluster scope and lever mix to "
            "attempt in this iteration."
        ),
    ),
    ProcessStage(
        key="proposal_generation",
        title="Proposal Generation",
        why=(
            "The action group becomes concrete candidate patches that can "
            "be validated before the Genie Space is changed."
        ),
    ),
    ProcessStage(
        key="safety_gates",
        title="Safety Gates",
        why=(
            "RCA-groundedness, blast-radius, patch-cap, and applyability "
            "gates keep patches causal, bounded, and safe."
        ),
    ),
    ProcessStage(
        key="applied_patches",
        title="Applied Patches",
        why=(
            "Only surviving patches are applied to the candidate Genie "
            "Space; this is the state-changing step."
        ),
    ),
    ProcessStage(
        key="post_patch_evaluation",
        title="Post-Patch Evaluation",
        why=(
            "The patched space is evaluated again to measure target fixes, "
            "protected-question regressions, and overall score movement. "
            "Today this re-uses the same evaluation primitive as Stage 1; "
            "Phase H captures it as a separate transcript entry for "
            "process clarity."
        ),
    ),
    ProcessStage(
        key="acceptance_decision",
        title="Acceptance / Rollback",
        why=(
            "The control plane keeps changes that safely improve the "
            "objective and rolls back changes that do not."
        ),
    ),
    ProcessStage(
        key="learning_next_action",
        title="Learning / Next Action",
        why=(
            "The loop records what worked, what failed, and what the next "
            "iteration or human operator should do."
        ),
    ),
    ProcessStage(
        key="contract_health",
        title="Contract Health",
        why=(
            "The run reports whether journey validation, decision records, "
            "and artifact persistence are complete enough for postmortem "
            "analysis."
        ),
    ),
)


def iteration_bundle_prefix(iteration: int) -> str:
    return f"{GSO_BUNDLE_ROOT}/iterations/iter_{int(iteration):02d}"


_STAGE_INDEX_BY_KEY: dict[str, int] = {
    stage.key: idx + 1 for idx, stage in enumerate(PROCESS_STAGE_ORDER)
}


def stage_artifact_paths(iteration: int, stage_key: str) -> dict[str, str]:
    """Return per-stage artifact paths for a given iteration.

    The directory name is ``<NN>_<stage_key>`` (e.g.
    ``06_safety_gates``) so a ``ls`` of the iteration directory is
    naturally process-ordered.

    Raises ``KeyError`` if ``stage_key`` is not in PROCESS_STAGE_ORDER.
    """
    if stage_key not in _STAGE_INDEX_BY_KEY:
        raise KeyError(
            f"unknown stage_key: {stage_key!r}. "
            f"Known keys: {sorted(_STAGE_INDEX_BY_KEY)}"
        )
    idx = _STAGE_INDEX_BY_KEY[stage_key]
    base = f"{iteration_bundle_prefix(iteration)}/stages/{idx:02d}_{stage_key}"
    return {
        "input":     f"{base}/input.json",
        "output":    f"{base}/output.json",
        "decisions": f"{base}/decisions.json",
    }


def bundle_artifact_paths(*, iterations: list[int]) -> dict[str, Any]:
    """Return the full parent-bundle path map for the given iterations."""
    paths: dict[str, Any] = {
        "manifest":               f"{GSO_BUNDLE_ROOT}/manifest.json",
        "run_summary":            f"{GSO_BUNDLE_ROOT}/run_summary.json",
        "artifact_index":         f"{GSO_BUNDLE_ROOT}/artifact_index.json",
        "operator_transcript":    f"{GSO_BUNDLE_ROOT}/operator_transcript.md",
        "decision_trace_all":     f"{GSO_BUNDLE_ROOT}/decision_trace_all.json",
        "journey_validation_all": f"{GSO_BUNDLE_ROOT}/journey_validation_all.json",
        "replay_fixture":         f"{GSO_BUNDLE_ROOT}/replay_fixture.json",
        "scoreboard":             f"{GSO_BUNDLE_ROOT}/scoreboard.json",
        "failure_buckets":        f"{GSO_BUNDLE_ROOT}/failure_buckets.json",
        "iterations": {},
    }
    for iteration in iterations:
        prefix = iteration_bundle_prefix(iteration)
        paths["iterations"][int(iteration)] = {
            "summary":             f"{prefix}/summary.json",
            "operator_transcript": f"{prefix}/operator_transcript.md",
            "decision_trace":      f"{prefix}/decision_trace.json",
            "journey_validation":  f"{prefix}/journey_validation.json",
            "rca_ledger":          f"{prefix}/rca_ledger.json",
            "proposal_inventory":  f"{prefix}/proposal_inventory.json",
            "patch_survival":      f"{prefix}/patch_survival.json",
            "stages":              f"{prefix}/stages",
        }
    return paths
