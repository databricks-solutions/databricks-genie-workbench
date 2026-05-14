"""Unit tests for Plan 2 (Lever 5 Split). Mirrors Plan 1's structure
for the capture-sink + coverage-gate machinery.

Test sections:
  1. Flag helpers (this file's first three tests).
  2. LEVER_5A_INSTRUCTION_PROMPT shape + format renders.
  3. _validate_lever_5a_no_sql_output gate.
  4. _dispatch_lever_5_split routing under split / shadow / off.
  5. _LeverFiveCaptureSink + atexit coverage gate.
"""
from __future__ import annotations

import importlib
import os


_PLAN2_ENV_KEYS = (
    "GSO_LEVER5_SPLIT_V1",
    "GSO_LEVER5_SHADOW_V1",
    "GSO_LEVER5_SPLIT_CAPTURE_PATH",
    "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env. Mirrors Plan 1's helper of
    the same name in test_rca_contract_narrow_v1.py.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg.lever5_*_enabled()`` after this function returns see the same
    env. Plan-2 env keys not in ``env`` are cleared so tests stay isolated.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN2_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


# ── Section 1: flag helpers ──────────────────────────────────────────


def test_lever5_split_default_off():
    cfg = _reload_config_with_env({})
    assert cfg.lever5_split_enabled() is False
    assert cfg.lever5_shadow_enabled() is False
    assert cfg.lever5_split_capture_require_coverage_enabled() is False


def test_lever5_split_flag_on():
    cfg = _reload_config_with_env({"GSO_LEVER5_SPLIT_V1": "1"})
    assert cfg.lever5_split_enabled() is True
    # Shadow is independent:
    assert cfg.lever5_shadow_enabled() is False


def test_lever5_shadow_flag_on():
    cfg = _reload_config_with_env({"GSO_LEVER5_SHADOW_V1": "1"})
    assert cfg.lever5_split_enabled() is False
    assert cfg.lever5_shadow_enabled() is True


def test_lever5_split_and_shadow_are_mutually_exclusive_in_practice():
    """Both flags can be set at the OS level, but the dispatcher MUST
    treat split-on as authoritative (split wins, shadow ignored). This
    test pins that contract; the dispatcher implementation in Task 10
    must honor it."""
    cfg = _reload_config_with_env({
        "GSO_LEVER5_SPLIT_V1": "1",
        "GSO_LEVER5_SHADOW_V1": "1",
    })
    # Both helpers return True; the dispatcher resolves the precedence:
    assert cfg.lever5_split_enabled() is True
    assert cfg.lever5_shadow_enabled() is True
