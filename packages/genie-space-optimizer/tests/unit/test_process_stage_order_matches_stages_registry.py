"""Phase H Task 2: PROCESS_STAGE_ORDER ↔ STAGES reconciliation.

The 11 STAGES registry keys must appear in PROCESS_STAGE_ORDER in the
same relative order. PROCESS_STAGE_ORDER may have additional entries
(post_patch_evaluation, contract_health) that don't correspond to
distinct STAGES entries — those are transcript-only.

C15 Phase 1: STAGES grew from 9 to 11 (bundle_assembly + run_manifest
added). PROCESS_STAGE_ORDER grew from 11 to 13. Transcript-only keys
remain {post_patch_evaluation, contract_health}.
"""

from __future__ import annotations


_TRANSCRIPT_ONLY_KEYS = {
    "post_patch_evaluation",
    "contract_health",
}


def test_every_stages_key_appears_in_process_stage_order_in_order() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )
    from genie_space_optimizer.optimization.stages import STAGES

    process_keys = [stage.key for stage in PROCESS_STAGE_ORDER]
    stages_keys = [entry.stage_key for entry in STAGES]

    process_keys_excl_transcript_only = [
        k for k in process_keys if k not in _TRANSCRIPT_ONLY_KEYS
    ]
    assert process_keys_excl_transcript_only == stages_keys, (
        "PROCESS_STAGE_ORDER's executable subset must match STAGES order:\n"
        f"  PROCESS (executable subset): {process_keys_excl_transcript_only}\n"
        f"  STAGES                     : {stages_keys}"
    )


def test_transcript_only_keys_are_documented() -> None:
    """Catches the case where a new transcript-only key is added without
    updating _TRANSCRIPT_ONLY_KEYS in this test."""
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )
    from genie_space_optimizer.optimization.stages import STAGES

    stages_keys = {entry.stage_key for entry in STAGES}
    transcript_only = {
        s.key for s in PROCESS_STAGE_ORDER if s.key not in stages_keys
    }
    assert transcript_only == _TRANSCRIPT_ONLY_KEYS, (
        f"transcript-only keys drift: actual={transcript_only}, "
        f"expected={_TRANSCRIPT_ONLY_KEYS}"
    )


# ── Phase H Fidelity Task 6 — manifest stage-order contract ──────


def test_manifest_stage_keys_in_process_order_uses_full_stage_contract() -> None:
    """Run ``3b050ec5`` showed ``manifest.stage_keys_in_process_order``
    contained only 9 entries (the executable ``STAGES`` registry) while
    the 11-stage transcript contract includes ``post_patch_evaluation``
    and ``contract_health``. The mismatch made postmortem skills walk
    only 9/11 stages and report missing pieces for stages that never
    had an executable producer.

    Phase H Fidelity Task 6: ``manifest.stage_keys_in_process_order``
    MUST mirror ``PROCESS_STAGE_ORDER`` so the transcript and
    manifest agree on what a complete iteration looks like.

    C15 Phase 1: PROCESS_STAGE_ORDER grew to 13 (bundle_assembly + run_manifest added).
    """
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_manifest,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )

    manifest = build_manifest(
        optimization_run_id="r1",
        databricks_job_id="job1",
        databricks_parent_run_id="parent1",
        lever_loop_task_run_id="task1",
        iterations=[1],
        missing_pieces=[],
    )
    expected_keys = [s.key for s in PROCESS_STAGE_ORDER]
    # C15 P2: PROCESS_STAGE_ORDER now has 14 entries (was 13; strategist_context added)
    assert len(expected_keys) == 14
    assert manifest["stage_keys_in_process_order"] == expected_keys


def test_manifest_executable_stage_keys_field_lists_executable_stages() -> None:
    """For consumers that need the executable subset (e.g. the bundle
    walker that reads stage I/O artifacts), the manifest exposes a
    separate ``executable_stage_keys`` field sourced from ``STAGES``.

    C15 Phase 1: STAGES now has 11 entries (was 9; bundle_assembly + run_manifest added).
    """
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_manifest,
    )
    from genie_space_optimizer.optimization.stages import STAGES

    manifest = build_manifest(
        optimization_run_id="r1",
        databricks_job_id="job1",
        databricks_parent_run_id="parent1",
        lever_loop_task_run_id="task1",
        iterations=[1],
        missing_pieces=[],
    )
    expected_executable = [e.stage_key for e in STAGES]
    # C15 P2: now 12 stages (9 original + bundle_assembly + run_manifest + strategist_context)
    assert len(expected_executable) == 12
    assert manifest["executable_stage_keys"] == expected_executable
