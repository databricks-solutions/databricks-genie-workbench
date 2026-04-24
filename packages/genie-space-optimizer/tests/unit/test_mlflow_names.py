"""Unit tests for the Tier 4 MLflow run-name helper.

The v2 scheme is ``<run_short>/<stage>/<detail>`` with zero-padded
iteration indices so MLflow's lexicographic sort produces the expected
chronological order within a run.
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
    p0_eval_run_name,
    slice_eval_run_name,
    strategy_run_name,
)


_RUN_ID = "e9c0b491-abcd-1234-5678-deadbeefcafe"
_SHORT = "e9c0b491"


def test_baseline_run_name_uses_short_prefix():
    assert baseline_run_name(_RUN_ID) == f"{_SHORT}/baseline"


def test_enrichment_run_name_defaults_to_snapshot():
    assert enrichment_run_name(_RUN_ID) == f"{_SHORT}/enrichment/snapshot"


def test_enrichment_run_name_with_detail():
    assert enrichment_run_name(_RUN_ID, detail="post_eval") == f"{_SHORT}/enrichment/post_eval"


def test_strategy_run_name_zero_pads_iteration():
    """MLflow sorts lexically; ``iter_01_*`` must come before ``iter_10_*``."""
    assert strategy_run_name(_RUN_ID, 1, "AG1") == f"{_SHORT}/iter_01_strategy/AG1"
    assert strategy_run_name(_RUN_ID, 12, "AG5") == f"{_SHORT}/iter_12_strategy/AG5"


def test_slice_eval_run_name_zero_pads():
    assert slice_eval_run_name(_RUN_ID, 3) == f"{_SHORT}/iter_03_slice_eval"


def test_p0_eval_run_name_zero_pads():
    assert p0_eval_run_name(_RUN_ID, 3) == f"{_SHORT}/iter_03_p0_eval"


def test_full_eval_run_name_pass_index():
    """Pass 1 reads ``run_1``; pass >1 reads ``run_N_confirm``."""
    assert full_eval_run_name(_RUN_ID, 2, pass_index=1) == f"{_SHORT}/iter_02_full_eval/run_1"
    assert (
        full_eval_run_name(_RUN_ID, 2, pass_index=2)
        == f"{_SHORT}/iter_02_full_eval/run_2_confirm"
    )


def test_finalize_run_name_defaults_to_repeat_pass_1():
    assert finalize_run_name(_RUN_ID) == f"{_SHORT}/finalize/repeat_pass_1"


def test_finalize_run_name_held_out():
    assert finalize_run_name(_RUN_ID, detail="held_out") == f"{_SHORT}/finalize/held_out"


def test_deploy_run_name_defaults_to_uc():
    assert deploy_run_name(_RUN_ID) == f"{_SHORT}/deploy/uc"


def test_retry_suffix_appended_idempotently():
    """Retries append ``/retry_{k}`` so names remain unique per attempt."""
    assert baseline_run_name(_RUN_ID, retry=2) == f"{_SHORT}/baseline/retry_2"
    assert (
        strategy_run_name(_RUN_ID, 1, "AG1", retry=1)
        == f"{_SHORT}/iter_01_strategy/AG1/retry_1"
    )


def test_iteration_outcome_run_name_shape():
    """Outcome names tag accepted vs rolled_back per iteration + AG."""
    assert (
        iteration_outcome_run_name(_RUN_ID, 1, "accepted", "AG1")
        == f"{_SHORT}/iter_01_accepted/AG1"
    )
    assert (
        iteration_outcome_run_name(_RUN_ID, 1, "rolled_back", "AG1")
        == f"{_SHORT}/iter_01_rolled_back/AG1"
    )


def test_default_tags_always_include_run_id_and_version():
    tags = default_tags(_RUN_ID)
    assert tags["genie.run_id"] == _RUN_ID
    assert tags["genie.run_name_version"] == RUN_NAME_VERSION


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
    assert baseline_run_name("") == "run/baseline"


def test_lex_sort_orders_iterations_numerically():
    """Names that lex-sort correctly must sort iter_01 < iter_02 < iter_10."""
    names = [
        strategy_run_name(_RUN_ID, i, "AG1")
        for i in (10, 2, 1)
    ]
    names.sort()
    assert names == [
        f"{_SHORT}/iter_01_strategy/AG1",
        f"{_SHORT}/iter_02_strategy/AG1",
        f"{_SHORT}/iter_10_strategy/AG1",
    ]
