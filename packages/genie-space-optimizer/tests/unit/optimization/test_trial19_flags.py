"""Trial 19 — master + sub-flag default-ON semantics.

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

from genie_space_optimizer.optimization.trial19_flags import (
    trial19_already_correct_filter_enabled,
    trial19_enforce_enabled,
    trial19_enforce_insufficient_enabled,
    trial19_gt_pending_review_enabled,
    trial19_llm_first_rca_enabled,
)


_SUB_FLAG_HELPERS = [
    (
        trial19_enforce_insufficient_enabled,
        "GSO_TRIAL19_ENFORCE_INSUFFICIENT",
    ),
    (
        trial19_llm_first_rca_enabled,
        "GSO_TRIAL19_LLM_FIRST_RCA",
    ),
    (
        trial19_already_correct_filter_enabled,
        "GSO_TRIAL19_ALREADY_CORRECT_FILTER",
    ),
    (
        trial19_gt_pending_review_enabled,
        "GSO_TRIAL19_GT_PENDING_REVIEW",
    ),
]


def test_master_default_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL19_ENFORCE", raising=False)
    assert trial19_enforce_enabled() is True


@pytest.mark.parametrize("off_value", ["0", "false", "no", "off", "FALSE", "OFF"])
def test_master_off_values_disable(monkeypatch, off_value):
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", off_value)
    assert trial19_enforce_enabled() is False


def test_master_off_forces_all_sub_flags_off(monkeypatch):
    """Master is the single emergency rollback knob."""
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "0")
    for helper, _env in _SUB_FLAG_HELPERS:
        # Even if a sub-flag is explicitly ON, master OFF wins.
        monkeypatch.setenv(_env, "1")
        assert helper() is False, (
            f"{helper.__name__} must be OFF when master is OFF, even "
            f"with {_env}=1"
        )


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_default_on(monkeypatch, helper, env_name):
    monkeypatch.delenv("GSO_TRIAL19_ENFORCE", raising=False)
    monkeypatch.delenv(env_name, raising=False)
    assert helper() is True


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_off_leaves_master_on(monkeypatch, helper, env_name):
    """Disabling one sub-flag doesn't disable the master or siblings."""
    monkeypatch.delenv("GSO_TRIAL19_ENFORCE", raising=False)
    monkeypatch.setenv(env_name, "0")
    assert trial19_enforce_enabled() is True
    assert helper() is False
    # Siblings remain ON.
    for sibling_helper, sibling_env in _SUB_FLAG_HELPERS:
        if sibling_env == env_name:
            continue
        monkeypatch.delenv(sibling_env, raising=False)
        assert sibling_helper() is True, (
            f"{sibling_helper.__name__} must remain ON when only "
            f"{env_name} is OFF"
        )
