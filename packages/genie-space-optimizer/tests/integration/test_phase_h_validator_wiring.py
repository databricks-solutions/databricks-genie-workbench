"""Cycle 12-T2 — integration test for the Phase H strict validator wiring.

Drives ``_run_phase_h_strict_validation`` through its three exit paths
(ok / listing_failed / validator_failed) and asserts the returned payload
matches the expected typed marker structure.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class _Artifact:
    path: str
    is_dir: bool = False


class _StubMlflowClient:
    """Minimal stand-in for MlflowClient. Returns artifacts based on a
    pre-supplied flat list of paths."""

    def __init__(self, materialized_paths: list[str]) -> None:
        self._paths = list(materialized_paths)

    def list_artifacts(self, run_id: str, prefix: str):
        # Non-recursive behaviour: emit each path one segment past `prefix/`,
        # marking deeper paths as directories.
        prefix = prefix.rstrip("/") + "/"
        seen: dict[str, _Artifact] = {}
        for p in self._paths:
            if not p.startswith(prefix):
                continue
            rest = p[len(prefix):]
            head, sep, _ = rest.partition("/")
            full = f"{prefix}{head}"
            if sep:
                seen.setdefault(full, _Artifact(path=full, is_dir=True))
            else:
                seen[full] = _Artifact(path=full, is_dir=False)
        return list(seen.values())


def _stub_bundle_artifact_paths(*, iterations):
    return {
        "manifest":               "gso_postmortem_bundle/manifest.json",
        "run_summary":            "gso_postmortem_bundle/run_summary.json",
        "artifact_index":         "gso_postmortem_bundle/artifact_index.json",
        "operator_transcript":    "gso_postmortem_bundle/operator_transcript.md",
        "decision_trace_all":     "gso_postmortem_bundle/decision_trace_all.json",
        "iterations": {},
    }


def test_validator_path_ok_with_self_write_exclusion() -> None:
    """Listing returns the 4 self-write paths as missing (because they're
    written AFTER listing); validator excludes them via self_write_paths;
    the only missing entry is decision_trace_all.json."""
    from genie_space_optimizer.optimization.harness import (
        _run_phase_h_strict_validation,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    materialized = []  # nothing in MLflow yet
    new_missing, payload = _run_phase_h_strict_validation(
        optimization_run_id="opt1",
        iterations_completed=[],
        anchor_run_id="anchor1",
        bundle_artifact_paths_fn=_stub_bundle_artifact_paths,
        validate_paths_fn=validate_phase_h_manifest_paths,
        flag_enabled_fn=lambda: True,
        mlflow_client_factory=lambda: _StubMlflowClient(materialized),
    )

    assert payload["flag_enabled"] is True
    assert payload["declared_count"] == 5
    assert payload["self_write_count"] == 4
    assert payload["materialized_count"] == 0
    assert payload["listing_status"] == "ok"
    assert payload["validator_status"] == "ok"
    assert payload["missing_count"] == 1
    assert payload["exception_class"] == ""
    assert len(new_missing) == 1
    assert new_missing[0]["artifact_path"] == "gso_postmortem_bundle/decision_trace_all.json"


def test_validator_path_listing_failed_emits_typed_status() -> None:
    """When MLflow listing raises, the marker carries listing_status=failed
    and validator_status=skipped, with the exception class named."""
    from genie_space_optimizer.optimization.harness import (
        _run_phase_h_strict_validation,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    class _BoomClient:
        def list_artifacts(self, run_id, prefix):
            raise RuntimeError("simulated MLflow outage")

    new_missing, payload = _run_phase_h_strict_validation(
        optimization_run_id="opt1",
        iterations_completed=[],
        anchor_run_id="anchor1",
        bundle_artifact_paths_fn=_stub_bundle_artifact_paths,
        validate_paths_fn=validate_phase_h_manifest_paths,
        flag_enabled_fn=lambda: True,
        mlflow_client_factory=_BoomClient,
    )

    assert payload["listing_status"] == "failed"
    assert payload["validator_status"] == "skipped"
    assert payload["exception_class"] == "RuntimeError"
    assert payload["missing_count"] == 0
    assert new_missing == []


def test_validator_path_flag_off_emits_skipped_status() -> None:
    from genie_space_optimizer.optimization.harness import (
        _run_phase_h_strict_validation,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    new_missing, payload = _run_phase_h_strict_validation(
        optimization_run_id="opt1",
        iterations_completed=[],
        anchor_run_id="anchor1",
        bundle_artifact_paths_fn=_stub_bundle_artifact_paths,
        validate_paths_fn=validate_phase_h_manifest_paths,
        flag_enabled_fn=lambda: False,
        mlflow_client_factory=lambda: _StubMlflowClient([]),
    )

    assert payload["flag_enabled"] is False
    assert payload["listing_status"] == "skipped"
    assert payload["validator_status"] == "skipped"
    assert new_missing == []


def test_validator_path_validator_raised_is_captured() -> None:
    """If validate_phase_h_manifest_paths itself raises, the marker carries
    validator_status=failed and the exception class."""
    from genie_space_optimizer.optimization.harness import (
        _run_phase_h_strict_validation,
    )

    def _boom_validator(**kwargs):
        raise ValueError("simulated validator bug")

    new_missing, payload = _run_phase_h_strict_validation(
        optimization_run_id="opt1",
        iterations_completed=[],
        anchor_run_id="anchor1",
        bundle_artifact_paths_fn=_stub_bundle_artifact_paths,
        validate_paths_fn=_boom_validator,
        flag_enabled_fn=lambda: True,
        mlflow_client_factory=lambda: _StubMlflowClient([]),
    )

    assert payload["listing_status"] == "ok"
    assert payload["validator_status"] == "failed"
    assert payload["exception_class"] == "ValueError"
    assert new_missing == []


def test_validator_path_no_anchor_skips_listing() -> None:
    """When ``anchor_run_id`` is None, the listing is skipped; the marker
    records listing_status=skipped without raising."""
    from genie_space_optimizer.optimization.harness import (
        _run_phase_h_strict_validation,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        validate_phase_h_manifest_paths,
    )

    new_missing, payload = _run_phase_h_strict_validation(
        optimization_run_id="opt1",
        iterations_completed=[],
        anchor_run_id=None,
        bundle_artifact_paths_fn=_stub_bundle_artifact_paths,
        validate_paths_fn=validate_phase_h_manifest_paths,
        flag_enabled_fn=lambda: True,
    )

    assert payload["listing_status"] == "skipped"
    assert payload["validator_status"] == "skipped"
    assert new_missing == []
