"""Phase E.0 Task 4 — anchor resolver picks a stable run for decision-trail artifacts."""

from unittest.mock import MagicMock


def test_resolve_anchor_prefers_lever_loop_run_when_present() -> None:
    from genie_space_optimizer.tools.mlflow_artifact_anchor import resolve_anchor_run_id

    client = MagicMock()
    # search_runs returns multiple sibling runs; one has run_type=lever_loop.
    run_lever_loop = MagicMock()
    run_lever_loop.info.run_id = "lever_loop_run_id"
    run_lever_loop.data.tags = {"genie.run_type": "lever_loop"}
    run_strategy = MagicMock()
    run_strategy.info.run_id = "strategy_run_id"
    run_strategy.data.tags = {"genie.run_type": "strategy"}
    client.search_runs.return_value = [run_strategy, run_lever_loop]

    anchor = resolve_anchor_run_id(
        client=client,
        opt_run_id="opt_1",
        experiment_ids=["exp_1"],
    )
    assert anchor == "lever_loop_run_id"


def test_resolve_anchor_prefers_lever_loop_run_tagged_run_role() -> None:
    """The canonical parent-run vocabulary stamps ``genie.run_role``.
    The resolver must recognize it in addition to the legacy
    ``genie.run_type`` tag so artifacts land on the same parent run no
    matter which code path created it."""
    from genie_space_optimizer.tools.mlflow_artifact_anchor import resolve_anchor_run_id

    client = MagicMock()
    run_lever_loop = MagicMock()
    run_lever_loop.info.run_id = "lever_loop_run_role"
    run_lever_loop.data.tags = {"genie.run_role": "lever_loop"}
    run_strategy = MagicMock()
    run_strategy.info.run_id = "strategy_run_id"
    run_strategy.data.tags = {"genie.run_role": "strategy"}
    client.search_runs.return_value = [run_strategy, run_lever_loop]

    anchor = resolve_anchor_run_id(
        client=client,
        opt_run_id="opt_1",
        experiment_ids=["exp_1"],
    )
    assert anchor == "lever_loop_run_role"


def test_resolve_anchor_falls_back_to_first_run_when_no_lever_loop() -> None:
    from genie_space_optimizer.tools.mlflow_artifact_anchor import resolve_anchor_run_id

    client = MagicMock()
    run_a = MagicMock()
    run_a.info.run_id = "first_run"
    run_a.info.start_time = 100
    run_a.data.tags = {"genie.run_type": "strategy"}
    run_b = MagicMock()
    run_b.info.run_id = "second_run"
    run_b.info.start_time = 200
    run_b.data.tags = {"genie.run_type": "full_eval"}
    client.search_runs.return_value = [run_a, run_b]

    anchor = resolve_anchor_run_id(
        client=client,
        opt_run_id="opt_1",
        experiment_ids=["exp_1"],
    )
    # Earliest start_time wins — typically the parent.
    assert anchor == "first_run"


def test_resolve_anchor_returns_empty_when_no_siblings_found() -> None:
    from genie_space_optimizer.tools.mlflow_artifact_anchor import resolve_anchor_run_id

    client = MagicMock()
    client.search_runs.return_value = []

    anchor = resolve_anchor_run_id(
        client=client,
        opt_run_id="opt_unknown",
        experiment_ids=["exp_1"],
    )
    assert anchor == ""
