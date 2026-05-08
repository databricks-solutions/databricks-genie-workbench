"""Cycle 12-T3 — extended audit_parent_bundle verifies all 9 parent paths."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import patch


@dataclass
class _Artifact:
    path: str


def _make_runs(run_id: str) -> list:
    class _Run:
        class _Info:
            def __init__(self, rid: str) -> None:
                self.run_id = rid

        def __init__(self, rid: str) -> None:
            self.info = self._Info(rid)

    return [_Run(run_id)]


def test_audit_parent_bundle_reports_all_9_parent_paths_when_present() -> None:
    """When every parent-level path is in the listing, the report is clean."""
    from genie_space_optimizer.tools import mlflow_audit

    declared_parent = [
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/run_summary.json",
        "gso_postmortem_bundle/artifact_index.json",
        "gso_postmortem_bundle/operator_transcript.md",
        "gso_postmortem_bundle/decision_trace_all.json",
        "gso_postmortem_bundle/journey_validation_all.json",
        "gso_postmortem_bundle/replay_fixture.json",
        "gso_postmortem_bundle/scoreboard.json",
        "gso_postmortem_bundle/failure_buckets.json",
    ]

    class _StubClient:
        def search_runs(self, **kwargs):
            return _make_runs("run_abc")

        def list_artifacts(self, run_id, path=None):
            # Recursive listing of every declared parent path.
            return [_Artifact(p) for p in declared_parent]

    with patch.object(mlflow_audit, "MlflowClient", _StubClient):
        report = mlflow_audit.audit_parent_bundle(
            optimization_run_id="opt_run_1",
        )

    assert report.has_manifest is True
    assert report.missing_artifacts == ()
    assert report.missing_parent_paths == ()


def test_audit_parent_bundle_reports_missing_parent_paths() -> None:
    from genie_space_optimizer.tools import mlflow_audit

    class _StubClient:
        def search_runs(self, **kwargs):
            return _make_runs("run_abc")

        def list_artifacts(self, run_id, path=None):
            # Manifest present, but replay_fixture and scoreboard missing.
            return [
                _Artifact("gso_postmortem_bundle/manifest.json"),
                _Artifact("gso_postmortem_bundle/run_summary.json"),
                _Artifact("gso_postmortem_bundle/artifact_index.json"),
                _Artifact("gso_postmortem_bundle/operator_transcript.md"),
                _Artifact("gso_postmortem_bundle/decision_trace_all.json"),
                _Artifact("gso_postmortem_bundle/journey_validation_all.json"),
                _Artifact("gso_postmortem_bundle/failure_buckets.json"),
            ]

    with patch.object(mlflow_audit, "MlflowClient", _StubClient):
        report = mlflow_audit.audit_parent_bundle(
            optimization_run_id="opt_run_1",
        )

    assert report.has_manifest is True
    assert sorted(report.missing_parent_paths) == [
        "gso_postmortem_bundle/replay_fixture.json",
        "gso_postmortem_bundle/scoreboard.json",
    ]


def test_audit_parent_bundle_back_compat_no_runs_found() -> None:
    """Legacy behaviour: no runs found → has_manifest=False, missing list
    contains the manifest path."""
    from genie_space_optimizer.tools import mlflow_audit

    class _StubClient:
        def search_runs(self, **kwargs):
            return []

        def list_artifacts(self, run_id, path=None):
            return []

    with patch.object(mlflow_audit, "MlflowClient", _StubClient):
        report = mlflow_audit.audit_parent_bundle(
            optimization_run_id="opt_run_1",
        )

    assert report.has_manifest is False
    assert "gso_postmortem_bundle/manifest.json" in report.missing_artifacts
    # missing_parent_paths reflects the same gap.
    assert "gso_postmortem_bundle/manifest.json" in report.missing_parent_paths
