"""Trial 20 — master + sub-flag default-ON semantics.

Pins:

* Default ON (env unset) for master and every sub-flag.
* Off-values ``0`` / ``false`` / ``no`` / ``off`` (case-insensitive)
  disable.
* Master OFF forces every sub-flag OFF regardless of its own env var
  (single emergency rollback knob).
* Sub-flag OFF leaves master and siblings ON.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.trial20_flags import (
    trial20_blast_radius_mandatory_enabled,
    trial20_enforce_enabled,
    trial20_family_pivot_graph_enabled,
    trial20_kept_insufficient_terminal_enabled,
    trial20_multi_lever_bundle_default_enabled,
    trial20_pre_arbiter_veto_fix_enabled,
)


_SUB_FLAG_HELPERS = [
    (
        trial20_pre_arbiter_veto_fix_enabled,
        "GSO_TRIAL20_PRE_ARBITER_VETO_FIX",
    ),
    (
        trial20_kept_insufficient_terminal_enabled,
        "GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL",
    ),
    (
        trial20_family_pivot_graph_enabled,
        "GSO_TRIAL20_FAMILY_PIVOT_GRAPH",
    ),
    (
        trial20_multi_lever_bundle_default_enabled,
        "GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT",
    ),
    (
        trial20_blast_radius_mandatory_enabled,
        "GSO_TRIAL20_BLAST_RADIUS_MANDATORY",
    ),
]


def test_master_default_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL20_ENFORCE", raising=False)
    assert trial20_enforce_enabled() is True


@pytest.mark.parametrize("off_value", ["0", "false", "no", "off", "FALSE", "OFF"])
def test_master_off_values_disable(monkeypatch, off_value):
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", off_value)
    assert trial20_enforce_enabled() is False


def test_master_off_forces_all_sub_flags_off(monkeypatch):
    """Master is the single emergency rollback knob."""
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "0")
    for helper, _env in _SUB_FLAG_HELPERS:
        monkeypatch.setenv(_env, "1")
        assert helper() is False, (
            f"{helper.__name__} must be OFF when master is OFF, even "
            f"with {_env}=1"
        )


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_default_on(monkeypatch, helper, env_name):
    monkeypatch.delenv("GSO_TRIAL20_ENFORCE", raising=False)
    monkeypatch.delenv(env_name, raising=False)
    assert helper() is True


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_off_leaves_master_on(monkeypatch, helper, env_name):
    """Disabling one sub-flag doesn't disable the master or siblings."""
    monkeypatch.delenv("GSO_TRIAL20_ENFORCE", raising=False)
    monkeypatch.setenv(env_name, "0")
    assert trial20_enforce_enabled() is True
    assert helper() is False
    for sibling_helper, sibling_env in _SUB_FLAG_HELPERS:
        if sibling_env == env_name:
            continue
        monkeypatch.delenv(sibling_env, raising=False)
        assert sibling_helper() is True, (
            f"{sibling_helper.__name__} must remain ON when only "
            f"{env_name} is OFF"
        )
