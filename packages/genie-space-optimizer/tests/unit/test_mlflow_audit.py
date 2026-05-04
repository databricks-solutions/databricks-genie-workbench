"""Phase H Task 10: audit_parent_bundle reports manifest presence."""

from __future__ import annotations


class _FakeRun:
    def __init__(self, run_id, tags):
        self.info = type("Info", (), {"run_id": run_id})()
        self.data = type("Data", (), {"tags": tags})()


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
