from __future__ import annotations

import json


def _json_payload(line: str) -> dict:
    _prefix, payload = line.split(" ", 1)
    return json.loads(payload)


def test_marker_line_is_compact_sorted_json() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import marker_line

    line = marker_line("GSO_TEST_V1", {"b": 2, "a": 1})

    assert line == 'GSO_TEST_V1 {"a":1,"b":2}'


def test_run_manifest_marker_has_required_fields() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        run_manifest_marker,
    )

    line = run_manifest_marker(
        optimization_run_id="opt_run_1",
        databricks_job_id="123",
        databricks_parent_run_id="456",
        lever_loop_task_run_id="789",
        mlflow_experiment_id="42",
        space_id="space_1",
        event="start",
    )

    assert line.startswith("GSO_RUN_MANIFEST_V1 ")
    payload = _json_payload(line)
    assert payload == {
        "databricks_job_id": "123",
        "databricks_parent_run_id": "456",
        "event": "start",
        "lever_loop_task_run_id": "789",
        "mlflow_experiment_id": "42",
        "optimization_run_id": "opt_run_1",
        "space_id": "space_1",
    }


def test_phase_b_marker_reports_trace_artifacts() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_marker,
    )

    line = phase_b_marker(
        optimization_run_id="opt_run_1",
        iteration=3,
        decision_record_count=12,
        decision_validation_count=0,
        transcript_chars=2000,
        decision_trace_artifact="phase_b/decision_trace/iter_3.json",
        operator_transcript_artifact="phase_b/operator_transcript/iter_3.txt",
        persist_ok=True,
    )

    assert line.startswith("GSO_PHASE_B_V1 ")
    payload = _json_payload(line)
    assert payload["decision_record_count"] == 12
    assert payload["decision_validation_count"] == 0
    assert payload["persist_ok"] is True


def test_phase_b_no_records_marker_carries_reason_and_producer_exceptions() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_no_records_marker,
    )

    line = phase_b_no_records_marker(
        optimization_run_id="opt_run_1",
        iteration=2,
        reason="all_ags_dropped_at_grounding",
        producer_exceptions={"eval_classification": 0, "cluster": 1},
        contract_version="v1",
    )

    assert line.startswith("GSO_PHASE_B_NO_RECORDS_V1 ")
    payload = _json_payload(line)
    assert payload["optimization_run_id"] == "opt_run_1"
    assert payload["iteration"] == 2
    assert payload["reason"] == "all_ags_dropped_at_grounding"
    assert payload["producer_exceptions"] == {"cluster": 1, "eval_classification": 0}
    assert payload["contract_version"] == "v1"


def test_phase_b_no_records_marker_handles_empty_producer_exceptions() -> None:
    """Default ``producer_exceptions=None`` becomes an empty dict so the
    JSON payload is always present."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_no_records_marker,
    )

    line = phase_b_no_records_marker(
        optimization_run_id="opt_run_1",
        iteration=1,
        reason="no_clusters",
    )

    payload = _json_payload(line)
    assert payload["producer_exceptions"] == {}


def test_phase_b_end_marker_carries_per_iter_counts() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_end_marker,
    )

    line = phase_b_end_marker(
        optimization_run_id="opt_run_1",
        total_records=120,
        iter_record_counts=[24, 24, 24, 24, 24],
        iter_violation_counts=[0, 0, 0, 0, 0],
        no_records_iterations=[],
        contract_version="v1",
    )

    assert line.startswith("GSO_PHASE_B_END_V1 ")
    payload = _json_payload(line)
    assert payload["total_records"] == 120
    assert payload["iter_record_counts"] == [24, 24, 24, 24, 24]
    assert payload["iter_violation_counts"] == [0, 0, 0, 0, 0]
    assert payload["no_records_iterations"] == []
    assert payload["contract_version"] == "v1"


def test_phase_b_end_marker_carries_no_records_iterations_list() -> None:
    """Cycle-9 reality: 5 iters with 0 records each."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_end_marker,
    )

    line = phase_b_end_marker(
        optimization_run_id="opt_run_1",
        total_records=0,
        iter_record_counts=[0, 0, 0, 0, 0],
        iter_violation_counts=[0, 0, 0, 0, 0],
        no_records_iterations=[1, 2, 3, 4, 5],
        contract_version="v1",
    )

    payload = _json_payload(line)
    assert payload["total_records"] == 0
    assert payload["no_records_iterations"] == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# Plan N4 — invariant violation marker
# ---------------------------------------------------------------------------


def test_invariant_violation_marker_round_trip() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        gso_invariant_violation_marker,
    )

    line = gso_invariant_violation_marker(
        optimization_run_id="r1",
        iteration=3,
        invariant_name="quarantine_attribution_drift",
        offending_qids=("gs_009",),
        degradation="released_from_quarantine",
    )
    assert line.startswith("GSO_INVARIANT_VIOLATION_V1")
    payload = _json_payload(line)
    assert payload["optimization_run_id"] == "r1"
    assert payload["iteration"] == 3
    assert payload["invariant_name"] == "quarantine_attribution_drift"
    assert payload["offending_qids"] == ["gs_009"]
    assert payload["degradation"] == "released_from_quarantine"


def test_invariant_violation_marker_handles_all_five_invariant_names() -> None:
    """The marker is the single canonical line postmortems pivot on;
    all five closed-vocabulary ``invariant_name`` values must
    round-trip cleanly."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        gso_invariant_violation_marker,
    )

    names = [
        "quarantine_attribution_drift",
        "regression_debt_partition_incomplete",
        "soft_cluster_currency_drift",
        "cap_conservation_violated",
        "non_canonical_judge_row",
    ]
    for name in names:
        line = gso_invariant_violation_marker(
            optimization_run_id="r1",
            iteration=2,
            invariant_name=name,
        )
        payload = _json_payload(line)
        assert payload["invariant_name"] == name
        assert payload["iteration"] == 2


def test_invariant_violation_marker_carries_payload_dict() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        gso_invariant_violation_marker,
    )

    line = gso_invariant_violation_marker(
        optimization_run_id="r1",
        iteration=1,
        invariant_name="cap_conservation_violated",
        payload={"decisions_in": 3, "decisions_out": 2, "input_count": 2},
    )
    payload = _json_payload(line)
    assert payload["payload"]["decisions_in"] == 3
    assert payload["payload"]["decisions_out"] == 2
    assert payload["payload"]["input_count"] == 2


# Cycle 12-T1 — marker_line accepts any _V<N> suffix
import pytest


def test_marker_line_accepts_v2_and_higher() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import marker_line

    line_v2 = marker_line("GSO_FOO_V2", {"a": 1})
    line_v42 = marker_line("GSO_FOO_V42", {"a": 1})

    assert line_v2.startswith("GSO_FOO_V2 ")
    assert line_v42.startswith("GSO_FOO_V42 ")


def test_marker_line_rejects_v0_and_unversioned() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import marker_line

    with pytest.raises(ValueError):
        marker_line("GSO_FOO_V0", {})
    with pytest.raises(ValueError):
        marker_line("GSO_FOO", {})
    with pytest.raises(ValueError):
        marker_line("FOO_V1", {})
