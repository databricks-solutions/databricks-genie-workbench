from genie_space_optimizer.optimization.run_output_contract import (
    GSO_BUNDLE_ROOT,
    PROCESS_STAGE_ORDER,
    RunRole,
    bundle_artifact_paths,
    iteration_bundle_prefix,
    stage_artifact_paths,
)


def test_bundle_root_constant() -> None:
    assert GSO_BUNDLE_ROOT == "gso_postmortem_bundle"


def test_iteration_bundle_prefix_zero_pads() -> None:
    assert iteration_bundle_prefix(3) == "gso_postmortem_bundle/iterations/iter_03"


def test_stage_artifact_paths_for_iter_3_safety_gates() -> None:
    paths = stage_artifact_paths(3, "safety_gates")
    assert paths["input"] == (
        "gso_postmortem_bundle/iterations/iter_03/stages/06_safety_gates/input.json"
    )
    assert paths["output"] == (
        "gso_postmortem_bundle/iterations/iter_03/stages/06_safety_gates/output.json"
    )
    assert paths["decisions"] == (
        "gso_postmortem_bundle/iterations/iter_03/stages/06_safety_gates/decisions.json"
    )


def test_run_role_values() -> None:
    assert RunRole.LEVER_LOOP.value == "lever_loop"
    assert RunRole.ITERATION_EVAL.value == "iteration_eval"
    assert RunRole.STRATEGY.value == "strategy"
    assert RunRole.LOGGED_MODEL.value == "logged_model"


def test_process_stage_order_has_eleven_entries_in_canonical_order() -> None:
    keys = [stage.key for stage in PROCESS_STAGE_ORDER]
    assert keys == [
        "evaluation_state",
        "rca_evidence",
        "cluster_formation",
        "action_group_selection",
        "proposal_generation",
        "safety_gates",
        "applied_patches",
        "post_patch_evaluation",
        "acceptance_decision",
        "learning_next_action",
        "contract_health",
    ]
    for stage in PROCESS_STAGE_ORDER:
        assert stage.title
        assert stage.why


def test_bundle_artifact_paths_covers_iterations() -> None:
    paths = bundle_artifact_paths(iterations=[1, 2])
    assert paths["manifest"] == "gso_postmortem_bundle/manifest.json"
    assert paths["operator_transcript"] == "gso_postmortem_bundle/operator_transcript.md"
    assert paths["decision_trace_all"] == "gso_postmortem_bundle/decision_trace_all.json"
    assert paths["iterations"][1]["operator_transcript"] == (
        "gso_postmortem_bundle/iterations/iter_01/operator_transcript.md"
    )


def test_stage_artifact_paths_rejects_unknown_stage() -> None:
    import pytest
    with pytest.raises(KeyError):
        stage_artifact_paths(1, "no_such_stage")
