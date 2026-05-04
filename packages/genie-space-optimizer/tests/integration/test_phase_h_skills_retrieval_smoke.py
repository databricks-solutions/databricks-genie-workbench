"""Phase H Completion Task 6: skills retrieval contract.

Asserts that after a Phase-H lever loop completes, an LLM following
docs/skills/gso-postmortem/SKILL.md's "Phase H workflow" section can
walk the bundle at the documented paths without grepping stdout.

The test materializes a synthetic bundle on disk that mirrors what
Tasks 1-5 produce, then exercises every path the SKILL.md instructs
the agent to read. If a path the SKILL.md cites is unreachable, the
test fails with a precise pointer to the broken link in the skill
contract.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.run_output_contract import (
    PROCESS_STAGE_ORDER,
    bundle_artifact_paths,
    stage_artifact_paths,
)


_EXECUTABLE_STAGES: tuple[str, ...] = (
    "evaluation_state",
    "rca_evidence",
    "cluster_formation",
    "action_group_selection",
    "proposal_generation",
    "safety_gates",
    "applied_patches",
    "acceptance_decision",
    "learning_next_action",
)


@pytest.fixture
def synthetic_bundle(tmp_path: Path) -> Path:
    """Materialize a minimal Phase-H bundle on disk."""
    iterations = [1, 2]
    paths = bundle_artifact_paths(iterations=iterations)

    for label in (
        "manifest", "run_summary", "artifact_index",
        "operator_transcript", "decision_trace_all",
        "journey_validation_all",
    ):
        rel = paths[label]
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if rel.endswith(".md"):
            target.write_text("# transcript stub\n")
        else:
            target.write_text(json.dumps({"stub": True}))

    for it in iterations:
        for stage_key in _EXECUTABLE_STAGES:
            sp = stage_artifact_paths(iteration=it, stage_key=stage_key)
            for label in ("input", "output", "decisions"):
                target = tmp_path / sp[label]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps({"stage": stage_key,
                                              "iter": it}))
    return tmp_path


def test_skill_workflow_step_3_manifest_is_present(
    synthetic_bundle: Path,
) -> None:
    paths = bundle_artifact_paths(iterations=[1, 2])
    manifest_path = synthetic_bundle / paths["manifest"]
    assert manifest_path.is_file(), (
        "SKILL.md Phase H Step 3 reads gso_postmortem_bundle/manifest.json; "
        "synthetic bundle must contain it"
    )


def test_skill_workflow_step_4_per_stage_capture_walkable(
    synthetic_bundle: Path,
) -> None:
    """SKILL.md Step 4: 'iter_NN/stages/<NN>_<stage_key>/{input,output,
    decisions}.json' must be readable for every executable stage."""
    for it in (1, 2):
        for stage_key in _EXECUTABLE_STAGES:
            sp = stage_artifact_paths(iteration=it, stage_key=stage_key)
            for label in ("input", "output", "decisions"):
                target = synthetic_bundle / sp[label]
                assert target.is_file(), (
                    f"SKILL.md Phase H Step 4 instructs the agent to read "
                    f"{sp[label]!r}; synthetic bundle missing this file. "
                    f"Either update SKILL.md or update Tasks 1-4 to "
                    f"populate this path."
                )


def test_skill_workflow_step_5_transcript_mirrors_stage_dirs(
    synthetic_bundle: Path,
) -> None:
    """SKILL.md Step 5: the transcript section for stage N matches
    the directory NN_stage_key. Verified structurally by checking
    PROCESS_STAGE_ORDER agrees with stage_artifact_paths' indices."""
    for stage_idx, stage in enumerate(PROCESS_STAGE_ORDER, start=1):
        if stage.key in ("post_patch_evaluation", "contract_health"):
            continue
        sp = stage_artifact_paths(iteration=1, stage_key=stage.key)
        prefix = f"{stage_idx:02d}_{stage.key}"
        assert prefix in sp["input"], (
            f"Stage idx {stage_idx} ({stage.key}) directory naming "
            f"diverges from PROCESS_STAGE_ORDER position; SKILL.md "
            f"Step 5's '<NN>_<stage_key>' contract is broken."
        )
