"""Unit tests for Plan 3 (Three-Stage Pipeline). Sections:
  1. Flag helpers.
  1b. _THREE_STAGE_SKILL_NAMES registry.
  2. STAGE_1_DISCOVERY_PROMPT shape + render.
  3. _ThreeStageCaptureSink + atexit coverage gate.
  4. ActivationBundle dataclass + merge_skill_picks helper.
  5. build_activation_bundle.
  6. _call_llm_for_stage_1_discovery.
  7. Stage-2 dispatcher (_stage_2_for_skill + per-skill adapters).
  8. run_three_stage_pipeline_for_ag orchestrator.
  9. Shadow-mode emission + harness wiring.
"""
from __future__ import annotations

import importlib
import os


_PLAN3_ENV_KEYS = (
    "GSO_THREE_STAGE_V1",
    "GSO_THREE_STAGE_SHADOW_V1",
    "GSO_THREE_STAGE_CAPTURE_PATH",
    "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env. Mirrors Plans 1 + 2 pattern.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg.three_stage_*_enabled()`` after this function returns see the
    same env. Plan-3 env keys not in ``env`` are cleared so tests stay
    isolated.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN3_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


# ── Section 1: flag helpers ──────────────────────────────────────────


def test_three_stage_default_off():
    cfg = _reload_config_with_env({})
    assert cfg.three_stage_enabled() is False
    assert cfg.three_stage_shadow_enabled() is False
    assert cfg.three_stage_capture_path_set() is False
    assert cfg.three_stage_capture_require_coverage_enabled() is False


def test_three_stage_pipeline_flag_on():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_V1": "1"})
    assert cfg.three_stage_enabled() is True
    assert cfg.three_stage_shadow_enabled() is False


def test_three_stage_shadow_flag_on():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_SHADOW_V1": "1"})
    assert cfg.three_stage_enabled() is False
    assert cfg.three_stage_shadow_enabled() is True


def test_three_stage_capture_path_set_when_var_present():
    cfg = _reload_config_with_env({"GSO_THREE_STAGE_CAPTURE_PATH": "/tmp/x.ndjson"})
    assert cfg.three_stage_capture_path_set() is True
