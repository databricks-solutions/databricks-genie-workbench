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


def test_collect_effective_flags_returns_known_flags() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        collect_effective_flags,
    )

    flags = collect_effective_flags()

    assert isinstance(flags, dict)
    # Production-locked flags must always appear and always be True.
    assert flags.get("target_aware_acceptance") is True
    assert flags.get("regression_debt_invariant") is True
    assert flags.get("lever_qualified_patch_ids") is True


def test_collect_effective_flags_strips_enabled_suffix_from_keys() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        collect_effective_flags,
    )

    flags = collect_effective_flags()

    # No key ends in "_enabled" — that suffix is stripped for compactness.
    for key in flags:
        assert not key.endswith("_enabled"), key


def test_collect_effective_flags_payload_under_8kb() -> None:
    """Defensive ceiling: if a future PR pushes flag count past this, the
    test surfaces it and the team can decide between truncation or
    splitting into a sidecar marker."""
    import json

    from genie_space_optimizer.optimization.run_analysis_contract import (
        collect_effective_flags,
    )

    flags = collect_effective_flags()
    encoded = json.dumps(flags, sort_keys=True, separators=(",", ":"))

    assert len(encoded.encode("utf-8")) < 8 * 1024, len(encoded)


def test_collect_effective_flags_swallows_accessor_exceptions(monkeypatch) -> None:
    """Any accessor that raises is recorded as ``None`` (not the boolean
    truthy/falsy of the exception object)."""
    from genie_space_optimizer.common import config as _config
    from genie_space_optimizer.optimization.run_analysis_contract import (
        collect_effective_flags,
    )

    def _boom() -> bool:  # pragma: no cover - shape only
        raise RuntimeError("simulated")

    _boom.__name__ = "boom_enabled"
    monkeypatch.setattr(_config, "boom_enabled", _boom, raising=False)

    flags = collect_effective_flags()

    assert flags.get("boom") is None


def test_run_manifest_v2_marker_has_all_v1_and_v2_fields() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        run_manifest_v2_marker,
    )

    line = run_manifest_v2_marker(
        optimization_run_id="opt_run_1",
        databricks_job_id="123",
        databricks_parent_run_id="456",
        lever_loop_task_run_id="789",
        mlflow_experiment_id="42",
        space_id="space_1",
        event="start",
        wheel_sha="1.2.3+g0a1b2c3",
        git_sha="0a1b2c3d" * 5,
        effective_flags={"target_aware_acceptance": True, "regression_debt_invariant": True},
        python_version="3.11.6",
        domain="airline",
    )

    assert line.startswith("GSO_RUN_MANIFEST_V2 ")
    payload = _json_payload(line)
    # V1 fields preserved.
    assert payload["optimization_run_id"] == "opt_run_1"
    assert payload["databricks_job_id"] == "123"
    assert payload["space_id"] == "space_1"
    assert payload["event"] == "start"
    # V2 fields added.
    assert payload["wheel_sha"] == "1.2.3+g0a1b2c3"
    assert payload["git_sha"] == "0a1b2c3d" * 5
    assert payload["python_version"] == "3.11.6"
    assert payload["domain"] == "airline"
    assert payload["effective_flags"] == {
        "regression_debt_invariant": True,
        "target_aware_acceptance": True,
    }


def test_run_manifest_v2_marker_v2_fields_default_to_empty() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        run_manifest_v2_marker,
    )

    line = run_manifest_v2_marker(
        optimization_run_id="opt_run_1",
        event="end",
    )

    payload = _json_payload(line)
    assert payload["wheel_sha"] == ""
    assert payload["git_sha"] == ""
    assert payload["python_version"] == ""
    assert payload["domain"] == ""
    assert payload["effective_flags"] == {}


def test_run_manifest_v2_marker_payload_under_16kb() -> None:
    """Defensive size check on the assembled V2 line. The known
    production-flag count keeps this well under 16KB."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        collect_effective_flags,
        run_manifest_v2_marker,
    )

    line = run_manifest_v2_marker(
        optimization_run_id="opt_run_1",
        event="end",
        effective_flags=collect_effective_flags(),
    )

    assert len(line.encode("utf-8")) < 16 * 1024


def test_assemble_run_manifest_v2_payload_uses_build_metadata(monkeypatch) -> None:
    """Verify the small assembly helper used by the harness produces a line
    with the expected V2 fields populated from build_metadata + config."""
    monkeypatch.setenv("GSO_GIT_SHA", "ab" * 20)
    monkeypatch.setenv("GSO_DOMAIN", "airline_test")
    monkeypatch.delenv("GSO_RUN_MANIFEST_V2_ENABLED", raising=False)

    from genie_space_optimizer.optimization.run_analysis_contract import (
        assemble_run_manifest_v2_line,
    )

    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt_run_1",
        databricks_job_id="j1",
        databricks_parent_run_id="r1",
        lever_loop_task_run_id="t1",
        mlflow_experiment_id="e1",
        space_id="s1",
        event="start",
    )

    assert line.startswith("GSO_RUN_MANIFEST_V2 ")
    payload = _json_payload(line)
    assert payload["git_sha"] == "ab" * 20
    assert payload["domain"] == "airline_test"
    assert payload["python_version"]  # non-empty
    assert payload["effective_flags"]  # non-empty (config has many flags)
    assert payload["effective_flags"]["target_aware_acceptance"] is True


def test_v2_manifest_satisfies_binary_criterion(monkeypatch) -> None:
    """Cycle 12-T1 binary criterion: every postmortem can answer
    "what wheel/git_sha/flags/python ran?" by reading exactly one record."""
    monkeypatch.setenv("GSO_GIT_SHA", "deadbeefcafe" + "0" * 28)
    monkeypatch.setenv("GSO_DOMAIN", "test_domain")
    monkeypatch.delenv("GSO_RUN_MANIFEST_V2_ENABLED", raising=False)

    from genie_space_optimizer.optimization.run_analysis_contract import (
        assemble_run_manifest_v2_line,
    )
    from genie_space_optimizer.tools.marker_parser import parse_markers

    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt_run_1",
        space_id="s1",
        event="start",
    )
    log = parse_markers(line + "\n")

    assert log.run_manifest_v2 is not None
    payload = log.run_manifest_v2
    # All four binary-criterion fields are present and non-empty.
    assert payload["git_sha"], payload
    assert payload["python_version"], payload
    assert payload["domain"] == "test_domain"
    assert payload["wheel_sha"], payload
    assert payload["effective_flags"], payload
