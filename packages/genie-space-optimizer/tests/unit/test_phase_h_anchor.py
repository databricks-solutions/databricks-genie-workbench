"""Unit tests for ``resolve_or_create_phase_h_anchor``.

The helper must:
- Prefer an existing parent run tagged ``genie.run_role=lever_loop``.
- Also recognize legacy ``genie.run_type=lever_loop`` tags.
- Create a new run using the canonical lever-loop parent name/tags when
  no existing parent is found.
- Return ``None`` on any MLflow/client failure without raising, so
  observability never breaks the optimizer.
"""

from unittest.mock import MagicMock

from genie_space_optimizer.common.mlflow_names import (
    lever_loop_parent_run_name,
)
from genie_space_optimizer.optimization.phase_h_anchor import (
    resolve_or_create_phase_h_anchor,
)


def _make_client():
    client = MagicMock()
    exp = MagicMock()
    exp.experiment_id = "exp_1"
    client.get_experiment_by_name.return_value = exp
    return client


def test_returns_existing_parent_tagged_run_role():
    client = _make_client()
    parent = MagicMock()
    parent.info.run_id = "parent_run"
    parent.data.tags = {
        "genie.optimization_run_id": "opt_1",
        "genie.run_role": "lever_loop",
    }
    other = MagicMock()
    other.info.run_id = "strategy_run"
    other.data.tags = {"genie.run_role": "strategy"}
    client.search_runs.return_value = [other, parent]

    anchor = resolve_or_create_phase_h_anchor(
        experiment_name="/exp",
        optimization_run_id="opt_1",
        client=client,
    )
    assert anchor == "parent_run"
    assert not client.create_run.called


def test_returns_existing_parent_tagged_run_type_legacy():
    client = _make_client()
    parent = MagicMock()
    parent.info.run_id = "parent_run_legacy"
    parent.data.tags = {
        "genie.optimization_run_id": "opt_1",
        "genie.run_type": "lever_loop",
    }
    client.search_runs.return_value = [parent]

    anchor = resolve_or_create_phase_h_anchor(
        experiment_name="/exp",
        optimization_run_id="opt_1",
        client=client,
    )
    assert anchor == "parent_run_legacy"
    assert not client.create_run.called


def test_creates_parent_run_when_none_exists():
    client = _make_client()
    client.search_runs.return_value = []
    created = MagicMock()
    created.info.run_id = "newly_created"
    client.create_run.return_value = created

    anchor = resolve_or_create_phase_h_anchor(
        experiment_name="/exp",
        optimization_run_id="opt_42",
        databricks_job_id="job_1",
        databricks_parent_run_id="dbr_1",
        lever_loop_task_run_id="task_1",
        client=client,
    )
    assert anchor == "newly_created"
    client.create_run.assert_called_once()
    kwargs = client.create_run.call_args.kwargs
    assert kwargs["experiment_id"] == "exp_1"
    assert kwargs["run_name"] == lever_loop_parent_run_name("opt_42")
    tags = kwargs["tags"]
    assert tags["genie.run_role"] == "lever_loop"
    assert tags["genie.run_type"] == "lever_loop"
    assert tags["genie.optimization_run_id"] == "opt_42"
    assert tags["genie.databricks.job_id"] == "job_1"


def test_returns_none_when_experiment_missing():
    client = _make_client()
    client.get_experiment_by_name.return_value = None

    anchor = resolve_or_create_phase_h_anchor(
        experiment_name="/missing",
        optimization_run_id="opt_x",
        client=client,
    )
    assert anchor is None
    assert not client.create_run.called


def test_returns_none_when_create_run_fails():
    client = _make_client()
    client.search_runs.return_value = []
    client.create_run.side_effect = RuntimeError("boom")

    anchor = resolve_or_create_phase_h_anchor(
        experiment_name="/exp",
        optimization_run_id="opt_x",
        client=client,
    )
    assert anchor is None


def test_graceful_degradation_when_search_raises():
    """search_runs failure should not abort the helper; it falls
    through to create_run which succeeds under normal operation."""
    client = _make_client()
    client.search_runs.side_effect = RuntimeError("boom")
    created = MagicMock()
    created.info.run_id = "created_after_search_failed"
    client.create_run.return_value = created

    anchor = resolve_or_create_phase_h_anchor(
        experiment_name="/exp",
        optimization_run_id="opt_y",
        client=client,
    )
    assert anchor == "created_after_search_failed"


def test_returns_none_when_required_inputs_missing():
    client = _make_client()
    assert resolve_or_create_phase_h_anchor(
        experiment_name="",
        optimization_run_id="opt_1",
        client=client,
    ) is None
    assert resolve_or_create_phase_h_anchor(
        experiment_name="/exp",
        optimization_run_id="",
        client=client,
    ) is None
