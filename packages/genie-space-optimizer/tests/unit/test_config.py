"""Unit tests for the feature-flag accessors in ``common/config.py``.

The accessors that ship default-on via ``_flag_default_on`` are the
production rollback escape hatches; this file covers the Defect Plan 1
(2026-05-12) accessors (grounding gate + cluster-signature collision)
introduced by ``docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md``.
"""
from __future__ import annotations

import importlib


def _fresh_config(monkeypatch, env_vars: tuple[str, ...]):
    """Reload ``common.config`` with the given env vars unset, so the
    default-ON behavior is observable. Mirrors the pattern used by
    ``tests/unit/test_rco4b_trial_preflight_flag_inventory.py``.
    """
    for v in env_vars:
        monkeypatch.delenv(v, raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    return config


def test_ag_emit_grounding_gate_enabled_defaults_to_true(monkeypatch):
    cfg = _fresh_config(monkeypatch, ("GSO_AG_EMIT_GROUNDING_GATE",))
    assert cfg.ag_emit_grounding_gate_enabled() is True


def test_ag_emit_grounding_gate_disabled_when_falsy(monkeypatch):
    from genie_space_optimizer.common.config import (
        ag_emit_grounding_gate_enabled,
    )
    for v in ("0", "false", "FALSE", "off", "no"):
        monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", v)
        assert ag_emit_grounding_gate_enabled() is False, (
            f"falsy value {v!r} did not disable the flag"
        )


def test_forbidden_ag_collision_by_cluster_signature_enabled_defaults_to_true(
    monkeypatch,
):
    cfg = _fresh_config(
        monkeypatch, ("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE",),
    )
    assert cfg.forbidden_ag_collision_by_cluster_signature_enabled() is True


def test_forbidden_ag_collision_by_cluster_signature_disabled_when_falsy(
    monkeypatch,
):
    from genie_space_optimizer.common.config import (
        forbidden_ag_collision_by_cluster_signature_enabled,
    )
    for v in ("0", "false", "FALSE", "off", "no"):
        monkeypatch.setenv(
            "GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", v
        )
        assert forbidden_ag_collision_by_cluster_signature_enabled() is False, (
            f"falsy value {v!r} did not disable the flag"
        )
