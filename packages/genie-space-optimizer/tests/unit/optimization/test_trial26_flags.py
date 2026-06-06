"""Trial 26 — master + sub-flag default-ON semantics.

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

from genie_space_optimizer.optimization.trial26_flags import (
    trial26_applier_snippet_name_fix_enabled,
    trial26_kit_gate_reachable_enabled,
    trial26_kit_map_expanded_enabled,
    trial26_rca_kind_canonical_normalise_enabled,
)


_SUB_FLAG_HELPERS = [
    (
        trial26_rca_kind_canonical_normalise_enabled,
        "GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE",
    ),
    (
        trial26_kit_map_expanded_enabled,
        "GSO_TRIAL26_KIT_MAP_EXPANDED",
    ),
    (
        trial26_applier_snippet_name_fix_enabled,
        "GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX",
    ),
]


def test_master_default_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    assert trial26_kit_gate_reachable_enabled() is True


@pytest.mark.parametrize("off_value", ["0", "false", "no", "off", "FALSE", "OFF", "No"])
def test_master_off_values_disable(monkeypatch, off_value):
    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", off_value)
    assert trial26_kit_gate_reachable_enabled() is False


def test_master_off_forces_all_sub_flags_off(monkeypatch):
    """Master is the single emergency rollback knob."""
    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "0")
    for helper, env in _SUB_FLAG_HELPERS:
        monkeypatch.setenv(env, "1")
        assert helper() is False, (
            f"{helper.__name__} must be OFF when master is OFF, even "
            f"with {env}=1"
        )


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_default_on(monkeypatch, helper, env_name):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv(env_name, raising=False)
    assert helper() is True


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_off_leaves_master_on(monkeypatch, helper, env_name):
    """Disabling one sub-flag doesn't disable the master or siblings."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.setenv(env_name, "0")
    assert trial26_kit_gate_reachable_enabled() is True
    assert helper() is False
    for sibling_helper, sibling_env in _SUB_FLAG_HELPERS:
        if sibling_env == env_name:
            continue
        monkeypatch.delenv(sibling_env, raising=False)
        assert sibling_helper() is True, (
            f"{sibling_helper.__name__} must remain ON when only "
            f"{env_name} is OFF"
        )


def test_unknown_value_treated_as_on(monkeypatch):
    """Any value not in the explicit OFF vocabulary is treated as ON.

    Mirrors trial24_flags semantics. This protects against typos like
    ``GSO_TRIAL26_KIT_GATE_REACHABLE=enabled`` accidentally disabling
    the trial.
    """
    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "enabled")
    assert trial26_kit_gate_reachable_enabled() is True

    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "")
    assert trial26_kit_gate_reachable_enabled() is True

    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "true")
    assert trial26_kit_gate_reachable_enabled() is True
