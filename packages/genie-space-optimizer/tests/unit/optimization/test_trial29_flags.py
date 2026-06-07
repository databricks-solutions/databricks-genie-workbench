"""Trial 29 — master + sub-flag default-ON semantics.

Mirrors :mod:`tests.unit.optimization.test_trial27_flags`. Pins:

* Default ON (env unset) for master and every sub-flag.
* Off-values ``0`` / ``false`` / ``no`` / ``off`` (case-insensitive)
  disable.
* Master OFF forces every sub-flag OFF regardless of its own env var
  (single emergency rollback knob).
* Sub-flag OFF leaves master and siblings ON.
* Unknown / typo values treated as ON.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.trial29_flags import (
    trial29_behavior_delta_enabled,
    trial29_inert_reroute_enabled,
)


_SUB_FLAG_HELPERS = [
    (trial29_inert_reroute_enabled, "GSO_TRIAL29_INERT_REROUTE"),
]


def test_master_default_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    assert trial29_behavior_delta_enabled() is True


@pytest.mark.parametrize(
    "off_value", ["0", "false", "no", "off", "FALSE", "OFF", "No"]
)
def test_master_off_values_disable(monkeypatch, off_value):
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", off_value)
    assert trial29_behavior_delta_enabled() is False


def test_master_off_forces_all_sub_flags_off(monkeypatch):
    """Master is the single emergency rollback knob."""
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "0")
    for helper, env in _SUB_FLAG_HELPERS:
        monkeypatch.setenv(env, "1")
        assert helper() is False, (
            f"{helper.__name__} must be OFF when master is OFF, even "
            f"with {env}=1"
        )


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_default_on(monkeypatch, helper, env_name):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv(env_name, raising=False)
    assert helper() is True


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_off_leaves_master_on(monkeypatch, helper, env_name):
    """Disabling one sub-flag doesn't disable the master."""
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.setenv(env_name, "0")
    assert trial29_behavior_delta_enabled() is True
    assert helper() is False


def test_unknown_value_treated_as_on(monkeypatch):
    """Any value not in the explicit OFF vocabulary is treated as ON.

    Protects against typos like ``GSO_TRIAL29_BEHAVIOR_DELTA=enabled``
    accidentally disabling the trial.
    """
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "enabled")
    assert trial29_behavior_delta_enabled() is True

    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "")
    assert trial29_behavior_delta_enabled() is True

    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "true")
    assert trial29_behavior_delta_enabled() is True
