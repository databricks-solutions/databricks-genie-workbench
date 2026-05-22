"""The canary's trajectory files must land in the Phase H bundle
directory so the postmortem bundle assembler can read them. /tmp/gso/
is reaped on Databricks Apps and is not a durable artifact."""

def test_persistence_root_prefers_phase_h_bundle_path(monkeypatch, tmp_path):
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path / "phase_h"))
    from genie_space_optimizer.optimization.optimizer import (
        _os_env_run_root,
    )
    root = _os_env_run_root()
    assert str(tmp_path / "phase_h") in root


def test_persistence_root_falls_back_to_explicit_override(monkeypatch, tmp_path):
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)
    monkeypatch.setenv("GSO_PLAN_V3_RUN_ROOT", str(tmp_path / "custom"))
    from genie_space_optimizer.optimization.optimizer import (
        _os_env_run_root,
    )
    root = _os_env_run_root()
    assert str(tmp_path / "custom") in root
