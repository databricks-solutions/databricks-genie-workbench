"""Permanent regression test — V1 flag default posture for Plan 5.

Plans 1, 2, and 4 are default-on as of Plan 5. Plan 3 stays default-off
until the second trial run produces commit-ready three-stage fixtures.

If any of these assertions ever flips, either:
  * Plan 5 was reverted (intentional rollback — update this test),
  * Plan 3 was activated without the operator-confirmed second trial
    (REGRESSION — block the merge), or
  * The flag helpers stopped using the documented defaults.
"""
from __future__ import annotations

import importlib
import os
import sys

import pytest


_PLAN5_ENV_KEYS = (
    "GSO_RCA_CONTRACT_NARROW_V1",
    "GSO_LEVER5_SPLIT_V1",
    "GSO_LEVER5_SHADOW_V1",
    "GSO_THREE_STAGE_V1",
    "GSO_THREE_STAGE_SHADOW_V1",
    "GSO_RAW_EVIDENCE_V1",
    "GSO_RAW_EVIDENCE_SHADOW_V1",
    "GSO_NARROWING_CAPTURE_PATH",
    "GSO_LEVER5_SPLIT_CAPTURE_PATH",
    "GSO_THREE_STAGE_CAPTURE_PATH",
    "GSO_RAW_EVIDENCE_CAPTURE_PATH",
    "GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE",
    "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE",
    "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE",
    "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env_overrides: dict[str, str]):
    """Reset env to a known baseline, apply overrides, re-import config."""
    for key in _PLAN5_ENV_KEYS:
        os.environ.pop(key, None)
    for key, value in env_overrides.items():
        os.environ[key] = value
    sys.modules.pop("genie_space_optimizer.common.config", None)
    return importlib.import_module("genie_space_optimizer.common.config")


# ── Plan 1: rca_contract_narrowed_enabled ─────────────────────────────


def test_plan_1_default_on_no_env():
    """Plan 1 — narrowing the RCA contract — defaults ON."""
    cfg = _reload_config_with_env({})
    assert cfg.rca_contract_narrowed_enabled() is True


def test_plan_1_emergency_rollback_via_env():
    """Operator override: setting GSO_RCA_CONTRACT_NARROW_V1=0 disables."""
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "0"})
    assert cfg.rca_contract_narrowed_enabled() is False


# ── Plan 2: lever5_split_enabled ─────────────────────────────────────


def test_plan_2_default_on_no_env():
    """Plan 2 — L5 split — defaults ON."""
    cfg = _reload_config_with_env({})
    assert cfg.lever5_split_enabled() is True


def test_plan_2_emergency_rollback_via_env():
    cfg = _reload_config_with_env({"GSO_LEVER5_SPLIT_V1": "0"})
    assert cfg.lever5_split_enabled() is False


# ── Plan 4: raw_evidence_v1_enabled ──────────────────────────────────


def test_plan_4_default_on_no_env():
    """Plan 4 — raw-evidence — defaults ON."""
    cfg = _reload_config_with_env({})
    assert cfg.raw_evidence_v1_enabled() is True


def test_plan_4_emergency_rollback_via_env():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_V1": "0"})
    assert cfg.raw_evidence_v1_enabled() is False


# ── Plan 3: three_stage_enabled — locked default-off (Task 4) ────────


def test_plan_3_default_off_no_env():
    """Plan 3 stays flag-gated until the second trial produces clean
    three-stage fixtures. Operator must explicitly set the env var to
    activate it; do not flip the default in code without an explicit
    follow-up plan."""
    cfg = _reload_config_with_env({})
    assert cfg.three_stage_enabled() is False


def test_plan_3_opt_in_via_env():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_V1": "1"})
    assert cfg.three_stage_enabled() is True


# ── Task 5: require-coverage helpers always False in production ──────


@pytest.mark.parametrize("helper_name,env_key", [
    ("narrowing_capture_require_coverage_enabled",
     "GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE"),
    ("lever5_split_capture_require_coverage_enabled",
     "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE"),
    ("three_stage_capture_require_coverage_enabled",
     "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE"),
    ("raw_evidence_capture_require_coverage_enabled",
     "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE"),
])
def test_require_coverage_helpers_always_false_in_prod(
    helper_name: str, env_key: str,
):
    """Plan 5 forces all four *_require_coverage helpers to always return
    False so the atexit gates never fire inside a notebook task.

    Even when the operator sets the env var to a truthy value, the helper
    returns False. The postmortem-driven workflow reads the same coverage
    data from harness summary logs, so the loud-fail behavior is no longer
    needed in production.
    """
    cfg = _reload_config_with_env({env_key: "1"})
    helper = getattr(cfg, helper_name)
    assert helper() is False, (
        f"{helper_name} returned True with {env_key}=1; the atexit gate "
        f"would fire inside the notebook task and turn a successful "
        f"optimization into a failed task."
    )
