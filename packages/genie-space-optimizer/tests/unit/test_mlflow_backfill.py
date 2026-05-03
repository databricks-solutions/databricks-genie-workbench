"""Phase E.0 Task 6 — mlflow_backfill reconstructs phase_a/+phase_b/ artifacts."""

from unittest.mock import MagicMock


def test_backfill_uploads_phase_a_for_each_iteration_with_validation_in_fixture() -> None:
    from genie_space_optimizer.tools.mlflow_backfill import backfill_artifacts

    fixture = {
        "iterations": [
            {
                "iteration": 0,
                "journey_validation": {"is_valid": True, "violations": []},
                "decision_records": [],
            },
            {
                "iteration": 1,
                "journey_validation": {"is_valid": True, "violations": []},
                "decision_records": [
                    {
                        "run_id": "r", "iteration": 1,
                        "decision_type": "eval_classified",
                        "outcome": "info", "reason_code": "hard_failure",
                    },
                ],
            },
        ],
    }

    client = MagicMock()
    summary = backfill_artifacts(
        client=client,
        anchor_run_id="anchor_1",
        replay_fixture=fixture,
    )
    log_text_calls = client.log_text.call_args_list
    artifact_paths = [c.kwargs.get("artifact_file") or c.args[2] for c in log_text_calls]
    assert "phase_a/journey_validation/iter_0.json" in artifact_paths
    assert "phase_a/journey_validation/iter_1.json" in artifact_paths
    assert "phase_b/decision_trace/iter_1.json" in artifact_paths
    assert "phase_b/operator_transcript/iter_1.txt" in artifact_paths
    # Iteration 0 has no decision_records → no phase_b/ artifacts.
    assert "phase_b/decision_trace/iter_0.json" not in artifact_paths
    assert summary.uploaded == 4
    assert summary.skipped_iterations == [0]


def test_backfill_returns_zero_when_fixture_has_no_iterations() -> None:
    from genie_space_optimizer.tools.mlflow_backfill import backfill_artifacts

    client = MagicMock()
    summary = backfill_artifacts(
        client=client,
        anchor_run_id="anchor_2",
        replay_fixture={"iterations": []},
    )
    assert summary.uploaded == 0
    assert client.log_text.called is False
