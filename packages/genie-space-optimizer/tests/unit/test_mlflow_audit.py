"""Phase H Task 10: audit_parent_bundle reports manifest presence."""

from __future__ import annotations


class _FakeRun:
    def __init__(self, run_id, tags, *, start_time: int = 0, artifact_paths=()):
        self.info = type("Info", (), {
            "run_id": run_id,
            "start_time": start_time,
        })()
        self.data = type("Data", (), {"tags": tags})()
        # Used by Track A — exposed so the test fake can drive
        # _list_artifacts_recursive without monkeypatching the helper.
        self._artifact_paths = tuple(artifact_paths)


class _Artifact:
    def __init__(self, path):
        self.path = path


def test_audit_reports_missing_parent_bundle(monkeypatch) -> None:
    from genie_space_optimizer.tools.mlflow_audit import audit_parent_bundle

    class _FakeClient:
        def search_runs(self, *a, **k):
            return [_FakeRun("br1", {
                "genie.run_role": "lever_loop",
                "genie.optimization_run_id": "opt-1",
            })]

        def list_artifacts(self, run_id, path=None):
            return []  # no manifest

    monkeypatch.setattr(
        "genie_space_optimizer.tools.mlflow_audit.MlflowClient",
        lambda: _FakeClient(),
    )

    report = audit_parent_bundle(optimization_run_id="opt-1")
    assert report.parent_run_id == "br1"
    assert not report.has_manifest
    assert any(
        "manifest.json" in p for p in report.missing_artifacts
    ), f"expected manifest.json in missing_artifacts; got {report.missing_artifacts!r}"


def test_audit_succeeds_when_manifest_present(monkeypatch) -> None:
    from genie_space_optimizer.tools.mlflow_audit import audit_parent_bundle

    class _FakeClient:
        def search_runs(self, *a, **k):
            return [_FakeRun("br1", {
                "genie.run_role": "lever_loop",
                "genie.optimization_run_id": "opt-1",
            })]

        def list_artifacts(self, run_id, path=None):
            if path == "gso_postmortem_bundle":
                return [_Artifact("gso_postmortem_bundle/manifest.json")]
            return []

    monkeypatch.setattr(
        "genie_space_optimizer.tools.mlflow_audit.MlflowClient",
        lambda: _FakeClient(),
    )

    report = audit_parent_bundle(optimization_run_id="opt-1")
    assert report.parent_run_id == "br1"
    assert report.has_manifest
    assert report.missing_artifacts == ()


def test_audit_reports_missing_when_no_runs_found(monkeypatch) -> None:
    """No matching parent run → has_manifest=False, parent_run_id=None,
    notes records the search-fallback path."""
    from genie_space_optimizer.tools.mlflow_audit import audit_parent_bundle

    class _FakeClient:
        def search_runs(self, *a, **k):
            return []

        def list_artifacts(self, run_id, path=None):
            return []

    monkeypatch.setattr(
        "genie_space_optimizer.tools.mlflow_audit.MlflowClient",
        lambda: _FakeClient(),
    )

    report = audit_parent_bundle(optimization_run_id="opt-missing")
    assert report.parent_run_id is None
    assert not report.has_manifest
    assert "manifest.json" in report.missing_artifacts[0]
    assert "parent run not found" in report.notes


def test_audit_anchor_prefers_latest_lever_loop_with_captures(monkeypatch) -> None:
    """When two lever_loop siblings share an opt_run_id, the anchor
    must be the one with the most recent start_time AND a
    ``gso_trial_captures/`` artifact prefix. The earliest-by-start_time
    pick is wrong because ``MlflowClient.log_text`` overwrites
    ``gso_trial_captures/*`` and the bundler needs the latest upload."""
    from genie_space_optimizer.tools import mlflow_audit

    earliest = _FakeRun(
        "run_old",
        {"genie.run_type": "lever_loop"},
        start_time=1_000_000,
        artifact_paths=("gso_trial_captures/narrowing_v1.ndjson",),
    )
    latest = _FakeRun(
        "run_new",
        {"genie.run_type": "lever_loop"},
        start_time=2_000_000,
        artifact_paths=("gso_trial_captures/narrowing_v1.ndjson",),
    )

    class _FakeClient:
        def search_runs(self, *a, **k):
            # Return earliest first so the bug (first-match wins) would
            # pick the wrong sibling.
            return [earliest, latest]

        def list_artifacts(self, run_id, path=None):
            run = earliest if run_id == "run_old" else latest
            class _A:
                def __init__(self, p): self.path, self.is_dir = p, False
            return [_A(p) for p in run._artifact_paths if p.startswith(path or "")]

    monkeypatch.setattr(
        mlflow_audit, "_list_artifacts_recursive",
        lambda client, run_id: list(
            (earliest if run_id == "run_old" else latest)._artifact_paths
        ),
    )

    result = mlflow_audit.audit_optimization_run(
        optimization_run_id="opt-shared",
        experiment_id="exp1",
        client=_FakeClient(),
    )
    assert result["anchor_run_id"] == "run_new", result
