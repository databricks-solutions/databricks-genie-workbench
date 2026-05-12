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


def test_rca_regen_recovery_policy_enabled_defaults_to_true(monkeypatch):
    monkeypatch.delenv("GSO_RCA_REGEN_RECOVERY_POLICY", raising=False)
    from genie_space_optimizer.common.config import (
        rca_regen_recovery_policy_enabled,
    )

    assert rca_regen_recovery_policy_enabled() is True


def test_rca_regen_recovery_policy_disabled_when_falsy(monkeypatch):
    from genie_space_optimizer.common.config import (
        rca_regen_recovery_policy_enabled,
    )

    for v in ("0", "false", "FALSE", "off", "no"):
        monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", v)
        assert rca_regen_recovery_policy_enabled() is False


def test_rca_regen_policy_overrides_returns_empty_when_no_env(monkeypatch):
    for var in (
        "GSO_RCA_REGEN_MAX_ATTEMPTS_NO_PARENT_RCA",
        "GSO_RCA_REGEN_MAX_ATTEMPTS_NO_FINDINGS",
        "GSO_RCA_REGEN_MAX_ATTEMPTS_NO_TERM_OVERLAP",
        "GSO_RCA_REGEN_MAX_ATTEMPTS_NO_CAUSAL_TARGET",
        "GSO_RCA_REGEN_MAX_ATTEMPTS_MISSING_TARGET_QIDS",
        "GSO_RCA_REGEN_MAX_ATTEMPTS_NO_EVIDENCE_AVAILABLE",
        "GSO_RCA_REGEN_MAX_ATTEMPTS_UNKNOWN",
    ):
        monkeypatch.delenv(var, raising=False)
    from genie_space_optimizer.common.config import (
        rca_regen_policy_overrides,
    )

    assert rca_regen_policy_overrides() == {}


def test_rca_regen_policy_overrides_reads_env_per_reason(monkeypatch):
    from genie_space_optimizer.common.config import (
        rca_regen_policy_overrides,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RcaUngroundedReason,
    )

    monkeypatch.setenv("GSO_RCA_REGEN_MAX_ATTEMPTS_NO_FINDINGS", "2")
    monkeypatch.setenv("GSO_RCA_REGEN_MAX_ATTEMPTS_NO_PARENT_RCA", "0")
    overrides = rca_regen_policy_overrides()
    assert overrides[RcaUngroundedReason.NO_FINDINGS] == 2
    assert overrides[RcaUngroundedReason.NO_PARENT_RCA] == 0
    assert RcaUngroundedReason.NO_TERM_OVERLAP not in overrides


def test_rca_regen_policy_overrides_skips_garbage(monkeypatch):
    from genie_space_optimizer.common.config import (
        rca_regen_policy_overrides,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RcaUngroundedReason,
    )

    monkeypatch.setenv("GSO_RCA_REGEN_MAX_ATTEMPTS_NO_FINDINGS", "not-a-number")
    monkeypatch.setenv("GSO_RCA_REGEN_MAX_ATTEMPTS_NO_TERM_OVERLAP", " 3 ")
    overrides = rca_regen_policy_overrides()
    assert RcaUngroundedReason.NO_FINDINGS not in overrides
    assert overrides[RcaUngroundedReason.NO_TERM_OVERLAP] == 3
