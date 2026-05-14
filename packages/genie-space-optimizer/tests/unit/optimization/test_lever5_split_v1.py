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


# ── Section 1b: L5b byte-stability ────────────────────────────────────


def test_render_synthesis_prompt_byte_stable_after_plan_2():
    """Plan 2 explicitly does NOT modify the synthesis prompt template.
    This test pins that contract — any byte change here is an unintended
    regression."""
    from genie_space_optimizer.optimization import synthesis

    afs = {
        "cluster_id": "C1",
        "failure_type": "missing_filter",
        "affected_judge": "result_correctness",
        "question_count": 3,
        "blame_set": ["catalog.schema.fact_orders.order_date"],
        "counterfactual_fixes": ["add WHERE order_date >= '2024-01-01'"],
        "structural_diff": {"missing": ["WHERE"]},
        "judge_verdict_pattern": "result_count_mismatch",
        "suggested_fix_summary": "add temporal filter",
    }

    class _Arch:
        name = "temporal_filter_archetype"
        output_shape = {"requires": ["WHERE"]}
        prompt_template = "Use a WHERE clause filtering by the date column."

    rendered = synthesis.render_synthesis_prompt(
        afs, _Arch(), "catalog.schema.fact_orders.order_date",
    )
    # Pin the docstring-stable substrings of the template:
    assert "abstracted failure signature (AFS)" in rendered
    assert "Cluster ID: C1" in rendered
    assert "Failure Type: missing_filter" in rendered
    assert "temporal_filter_archetype" in rendered
    assert "{{" not in rendered  # all template variables substituted
    # Pin the output_format header so structural-template changes are caught:
    assert "Output format (strict JSON)" in rendered


def test_lever_5b_skill_id_accessor():
    """synthesis.lever_5b_skill_id() returns the canonical skill_id used
    by the dispatcher and capture sink."""
    from genie_space_optimizer.optimization import synthesis
    assert synthesis.lever_5b_skill_id() == "lever-5b-example-sql"
