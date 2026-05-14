"""Unit tests for Plan 4 (Raw Evidence + Consolidation). Sections:
  1. Flag helpers + GSO_RAW_EVIDENCE_N parsing.
  2. _RAW_EVIDENCE_ANTI_ANCHORING_HEADER constant.
  3. raw_evidence module: extraction.
  4. raw_evidence module: per-skill projection.
  5. raw_evidence module: diverse sampling.
  6. _RawEvidenceCaptureSink + atexit gate.
  7. build_activation_bundle integration.
  8. _stage_2_* adapter wiring.
  9. _call_llm_for_proposal raw_evidence kwarg + slot rendering.
  10. Shadow-mode emission + harness wiring.
  11. Anti-anchoring framing rendered in every wired prompt.
"""
from __future__ import annotations

import importlib
import os


_PLAN4_ENV_KEYS = (
    "GSO_RAW_EVIDENCE_V1",
    "GSO_RAW_EVIDENCE_SHADOW_V1",
    "GSO_RAW_EVIDENCE_CAPTURE_PATH",
    "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE",
    "GSO_RAW_EVIDENCE_N",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with patched env. Mirrors Plans 1-3 pattern.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg.raw_evidence_*_enabled()`` see the same env. Plan-4 env keys
    not in ``env`` are cleared so tests stay isolated.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN4_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


# ── Section 1: flag helpers ──────────────────────────────────────────


def test_raw_evidence_v1_default_off():
    cfg = _reload_config_with_env({})
    assert cfg.raw_evidence_v1_enabled() is False
    assert cfg.raw_evidence_v1_shadow_enabled() is False
    assert cfg.raw_evidence_capture_path_set() is False
    assert cfg.raw_evidence_capture_require_coverage_enabled() is False


def test_raw_evidence_v1_pipeline_flag_on():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_V1": "1"})
    assert cfg.raw_evidence_v1_enabled() is True
    assert cfg.raw_evidence_v1_shadow_enabled() is False


def test_raw_evidence_v1_shadow_flag_on():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_SHADOW_V1": "1"})
    assert cfg.raw_evidence_v1_enabled() is False
    assert cfg.raw_evidence_v1_shadow_enabled() is True


def test_raw_evidence_n_default_3():
    cfg = _reload_config_with_env({})
    assert cfg.raw_evidence_n() == 3


def test_raw_evidence_n_zero_disables():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "0"})
    assert cfg.raw_evidence_n() == 0


def test_raw_evidence_n_clamped_to_10():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "1000"})
    assert cfg.raw_evidence_n() == 10


def test_raw_evidence_n_negative_falls_back_to_default():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "-5"})
    assert cfg.raw_evidence_n() == 3


def test_raw_evidence_n_garbage_falls_back_to_default():
    cfg = _reload_config_with_env({"GSO_RAW_EVIDENCE_N": "not-a-number"})
    assert cfg.raw_evidence_n() == 3
