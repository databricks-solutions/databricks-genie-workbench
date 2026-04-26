"""Unit tests for the Tier 4 MLflow run-name helper.

The v3 scheme is ``iter_NN / stage [/ detail] / run_xxxxxxxx`` — descriptive
context first, run id last, with zero-padded iteration indices so MLflow's
lexicographic sort within an iteration produces chronological order.
"""

from __future__ import annotations

from genie_space_optimizer.common.mlflow_names import (
    RUN_NAME_VERSION,
    baseline_run_name,
    default_tags,
    deploy_run_name,
    enrichment_run_name,
    finalize_run_name,
    full_eval_run_name,
    iteration_outcome_run_name,
    labeling_run_name,
    p0_eval_run_name,
    preflight_run_name,
    slice_eval_run_name,
    strategy_run_name,
)


_RUN_ID = "e9c0b491-abcd-1234-5678-deadbeefcafe"
_SHORT = "e9c0b491"
_RUN = f"run_{_SHORT}"


def test_baseline_run_name_uses_v3_format():
    assert baseline_run_name(_RUN_ID) == f"iter_00 / baseline / {_RUN}"


def test_enrichment_run_name_defaults_to_snapshot():
    assert enrichment_run_name(_RUN_ID) == f"iter_00 / enrichment / snapshot / {_RUN}"


def test_enrichment_run_name_with_detail():
    assert (
        enrichment_run_name(_RUN_ID, detail="post_eval")
        == f"iter_00 / enrichment / post_eval / {_RUN}"
    )


def test_strategy_run_name_zero_pads_iteration():
    """MLflow sorts lexically; ``iter_01`` must come before ``iter_10``."""
    assert strategy_run_name(_RUN_ID, 1, "AG1") == f"iter_01 / strategy / AG1 / {_RUN}"
    assert strategy_run_name(_RUN_ID, 12, "AG5") == f"iter_12 / strategy / AG5 / {_RUN}"


def test_slice_eval_run_name_zero_pads():
    assert slice_eval_run_name(_RUN_ID, 3) == f"iter_03 / slice_eval / {_RUN}"


def test_p0_eval_run_name_zero_pads():
    assert p0_eval_run_name(_RUN_ID, 3) == f"iter_03 / p0_eval / {_RUN}"


def test_full_eval_run_name_pass_index():
    """Pass 1 reads ``pass_1``; pass >1 reads ``pass_N_confirm``."""
    assert (
        full_eval_run_name(_RUN_ID, 2, pass_index=1)
        == f"iter_02 / full_eval / pass_1 / {_RUN}"
    )
    assert (
        full_eval_run_name(_RUN_ID, 2, pass_index=2)
        == f"iter_02 / full_eval / pass_2_confirm / {_RUN}"
    )


def test_finalize_run_name_defaults_to_repeat_pass_1():
    assert finalize_run_name(_RUN_ID) == f"iter_00 / finalize / repeat_pass_1 / {_RUN}"


def test_finalize_run_name_held_out_threads_iteration():
    assert (
        finalize_run_name(_RUN_ID, detail="held_out", iteration=3)
        == f"iter_03 / finalize / held_out / {_RUN}"
    )


def test_deploy_run_name_defaults_to_uc():
    assert deploy_run_name(_RUN_ID) == f"iter_00 / deploy / uc / {_RUN}"


def test_preflight_run_name_default():
    assert (
        preflight_run_name(_RUN_ID)
        == f"iter_00 / preflight / benchmark_generation / {_RUN}"
    )


def test_labeling_run_name():
    assert labeling_run_name(_RUN_ID) == f"iter_00 / labeling_session / {_RUN}"


def test_retry_suffix_appended_idempotently():
    """Retries append ``/ retry_{k}`` so names remain unique per attempt."""
    assert baseline_run_name(_RUN_ID, retry=2) == f"iter_00 / baseline / {_RUN} / retry_2"
    assert (
        strategy_run_name(_RUN_ID, 1, "AG1", retry=1)
        == f"iter_01 / strategy / AG1 / {_RUN} / retry_1"
    )


def test_iteration_outcome_run_name_shape():
    """Outcome names tag accepted vs rolled_back per iteration + AG."""
    assert (
        iteration_outcome_run_name(_RUN_ID, 1, "accepted", "AG1")
        == f"iter_01 / accepted / AG1 / {_RUN}"
    )
    assert (
        iteration_outcome_run_name(_RUN_ID, 1, "rolled_back", "AG1")
        == f"iter_01 / rolled_back / AG1 / {_RUN}"
    )


def test_default_tags_always_include_run_id_and_version():
    tags = default_tags(_RUN_ID)
    assert tags["genie.run_id"] == _RUN_ID
    assert tags["genie.run_name_version"] == RUN_NAME_VERSION


def test_run_name_version_bumped_to_v3():
    assert RUN_NAME_VERSION == "v3"


def test_default_tags_zero_pad_iteration():
    tags = default_tags(_RUN_ID, iteration=3)
    assert tags["genie.iteration"] == "03"


def test_default_tags_include_stage_and_ag_when_provided():
    tags = default_tags(
        _RUN_ID, space_id="space-abc", stage="full_eval", iteration=7, ag_id="AG2"
    )
    assert tags["genie.stage"] == "full_eval"
    assert tags["genie.ag_id"] == "AG2"
    assert tags["genie.space_id"] == "space-abc"
    assert tags["genie.iteration"] == "07"


def test_empty_run_id_falls_back_to_placeholder():
    """Helper must not crash on an empty run id; returns a sortable placeholder."""
    assert baseline_run_name("") == "iter_00 / baseline / run_run"


def test_lex_sort_orders_iterations_numerically():
    """Names that lex-sort correctly must sort iter_01 < iter_02 < iter_10."""
    names = [
        strategy_run_name(_RUN_ID, i, "AG1")
        for i in (10, 2, 1)
    ]
    names.sort()
    assert names == [
        f"iter_01 / strategy / AG1 / {_RUN}",
        f"iter_02 / strategy / AG1 / {_RUN}",
        f"iter_10 / strategy / AG1 / {_RUN}",
    ]
