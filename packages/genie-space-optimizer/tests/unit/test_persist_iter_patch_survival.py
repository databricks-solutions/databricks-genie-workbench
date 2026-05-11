"""Cycle 15.2 / C12-T5 part 2 — patch_survival.json persist helper.

Tests use a captured-call fake of mlflow.tracking.MlflowClient.log_text
so the helper's upload path can be asserted without a Databricks
workspace.
"""
from __future__ import annotations


class _FakeMlflowClient:
    """Capture `log_text(run_id, text, artifact_file)` invocations."""
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def log_text(self, *, run_id: str, text: str, artifact_file: str) -> None:
        self.calls.append({
            "run_id": str(run_id),
            "text": str(text),
            "artifact_file": str(artifact_file),
        })


def test_persist_helper_writes_to_canonical_contract_path(monkeypatch) -> None:
    """When the anchor run is resolved, the helper writes a single
    artifact at `gso_postmortem_bundle/iterations/iter_03/patch_survival.json`
    containing the aggregate JSON, and records the iteration in the
    materialized set.
    """
    from genie_space_optimizer.optimization import harness as _h
    from genie_space_optimizer.optimization.patch_survival import (
        PatchSurvivalSnapshot,
    )

    fake = _FakeMlflowClient()

    monkeypatch.setattr(
        _h, "_resolve_anchor_run_id_for_persist",
        lambda opt_run_id: "anchor_run_xyz",
    )
    monkeypatch.setattr(_h, "_build_mlflow_client_for_persist", lambda: fake)
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()

    snap = PatchSurvivalSnapshot(
        ag_id="AG1",
        proposed=[{"proposal_id": "P", "cluster_id": "H1"}],
        normalized=[{"proposal_id": "P", "cluster_id": "H1"}],
        applyable=[{"proposal_id": "P", "cluster_id": "H1"}],
        capped=[{"proposal_id": "P", "cluster_id": "H1"}],
        applied=[{"proposal_id": "P", "cluster_id": "H1"}],
    )
    result = _h._persist_iter_patch_survival_to_anchor(
        opt_run_id="opt_run_abc",
        iteration=3,
        per_ag_snapshots=[snap],
    )
    assert result.success is True
    assert result.anchor_run_id == "anchor_run_xyz"
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["run_id"] == "anchor_run_xyz"
    assert call["artifact_file"] == (
        "gso_postmortem_bundle/iterations/iter_03/patch_survival.json"
    )
    import json
    body = json.loads(call["text"])
    assert body["iteration"] == 3
    assert body["ags"][0]["ag_id"] == "AG1"
    assert 3 in _h._PATCH_SURVIVAL_MATERIALIZED_ITERS


def test_persist_helper_returns_failure_when_anchor_unresolved(monkeypatch) -> None:
    """When resolve_anchor_run_id returns "", the helper does not
    attempt log_text, returns success=False with exception_class
    "NoSiblingRun", and does NOT record the iteration in the
    materialized set.
    """
    from genie_space_optimizer.optimization import harness as _h
    from genie_space_optimizer.optimization.patch_survival import (
        PatchSurvivalSnapshot,
    )

    fake = _FakeMlflowClient()

    monkeypatch.setattr(
        _h, "_resolve_anchor_run_id_for_persist", lambda opt_run_id: "",
    )
    monkeypatch.setattr(_h, "_build_mlflow_client_for_persist", lambda: fake)
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()

    result = _h._persist_iter_patch_survival_to_anchor(
        opt_run_id="opt_run_abc",
        iteration=5,
        per_ag_snapshots=[PatchSurvivalSnapshot(ag_id="AG1")],
    )
    assert result.success is False
    assert result.exception_class == "NoSiblingRun"
    assert fake.calls == []
    assert 5 not in _h._PATCH_SURVIVAL_MATERIALIZED_ITERS


def test_persist_helper_records_iteration_in_materialized_set_on_success(
    monkeypatch,
) -> None:
    """On a successful single-call upload, the iteration appears in
    the materialized set exactly once. A subsequent log_text raise
    on a DIFFERENT iteration leaves that other iteration absent.
    """
    from genie_space_optimizer.optimization import harness as _h
    from genie_space_optimizer.optimization.patch_survival import (
        PatchSurvivalSnapshot,
    )

    class _RaisingClient:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def log_text(self, *, run_id, text, artifact_file) -> None:
            self.calls.append({"artifact_file": artifact_file})
            raise RuntimeError("network glitch")

    fake_ok = _FakeMlflowClient()
    fake_bad = _RaisingClient()

    monkeypatch.setattr(
        _h, "_resolve_anchor_run_id_for_persist",
        lambda opt_run_id: "anchor_run_xyz",
    )
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()

    monkeypatch.setattr(_h, "_build_mlflow_client_for_persist", lambda: fake_ok)
    ok = _h._persist_iter_patch_survival_to_anchor(
        opt_run_id="opt_run_abc", iteration=1,
        per_ag_snapshots=[PatchSurvivalSnapshot(ag_id="AG1")],
    )
    assert ok.success is True
    assert 1 in _h._PATCH_SURVIVAL_MATERIALIZED_ITERS

    monkeypatch.setattr(_h, "_build_mlflow_client_for_persist", lambda: fake_bad)
    bad = _h._persist_iter_patch_survival_to_anchor(
        opt_run_id="opt_run_abc", iteration=2,
        per_ag_snapshots=[PatchSurvivalSnapshot(ag_id="AG1")],
    )
    assert bad.success is False
    assert bad.exception_class == "RuntimeError"
    assert 2 not in _h._PATCH_SURVIVAL_MATERIALIZED_ITERS
    # Iteration 1 stays recorded — the failure on iteration 2 must
    # not retroactively remove a successful prior persist.
    assert 1 in _h._PATCH_SURVIVAL_MATERIALIZED_ITERS
