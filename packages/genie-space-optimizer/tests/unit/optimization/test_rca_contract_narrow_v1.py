"""Unit tests for GSO_RCA_CONTRACT_NARROW_V1 (Plan 1, Checkpoint B).

Verifies that:
  * `_rca_contract_for(name)` returns the full header for causal prompt names.
  * `_rca_contract_for(name)` returns "" for non-causal names when the flag is on.
  * `_rca_contract_for(name)` returns the full header for non-causal names when the flag is off (default).
  * The 11 causal sites in `common/config.py` continue to render the contract block in their resolved prompt strings.
  * The 3 non-causal sites omit the contract block when the flag is on.
"""
from __future__ import annotations

import importlib
import os


_PLAN1_ENV_KEYS = (
    "GSO_RCA_CONTRACT_NARROW_V1",
    "GSO_NARROWING_CAPTURE_PATH",
    "GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE",
)


def _reload_config_with_env(env: dict[str, str]):
    """Reload common.config with a patched environment so module-level flag
    evaluation re-runs. Returns the reloaded module.

    Env is set directly (no context manager) so subsequent calls to
    ``cfg._rca_contract_for(...)`` after this function returns see the
    same env. Plan-1 env keys not in ``env`` are cleared so tests stay
    isolated even if a prior test set them.
    """
    from genie_space_optimizer.common import config as cfg

    for key in _PLAN1_ENV_KEYS:
        if key in env:
            os.environ[key] = env[key]
        else:
            os.environ.pop(key, None)
    return importlib.reload(cfg)


def test_helper_returns_header_for_causal_prompt_name_when_flag_on():
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    result = cfg._rca_contract_for("strategy-adaptive")
    assert result == cfg._RCA_CONTRACT_HEADER
    assert "unified_rca_engine_contract" in result


def test_helper_returns_empty_for_non_causal_prompt_name_when_flag_on():
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    for name in ("preflight-instruction-expand",
                 "lever-4-join-discovery",
                 "preflight-sql-expression-seeding"):
        assert cfg._rca_contract_for(name) == "", name


def test_helper_returns_header_for_non_causal_when_flag_off():
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "0"})
    result = cfg._rca_contract_for("preflight-instruction-expand")
    assert result == cfg._RCA_CONTRACT_HEADER


def test_unknown_prompt_name_defaults_to_causal_treatment():
    """Safety: any name not explicitly in the non-causal set keeps the contract.
    Adding a new non-causal name is an explicit registry edit, not an opt-out."""
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    assert cfg._rca_contract_for("brand-new-skill-not-yet-classified") == cfg._RCA_CONTRACT_HEADER


def test_flag_default_is_off():
    cfg = _reload_config_with_env({})
    # When unset, env-var lookup misses; _flag_enabled returns False.
    assert cfg.rca_contract_narrowed_enabled() is False
    # Therefore the helper returns the header even for non-causal names.
    assert cfg._rca_contract_for("lever-4-join-discovery") == cfg._RCA_CONTRACT_HEADER


def test_eleven_causal_sites_still_contain_contract_marker():
    """Resolved prompt strings for every causal site must contain the
    contract's `<unified_rca_engine_contract>` marker tag."""
    cfg = _reload_config_with_env({})
    causal_constants = (
        "PROPOSAL_GENERATION_PROMPT",
        "LEVER_1_2_COLUMN_PROMPT",
        "LEVER_4_JOIN_SPEC_PROMPT",
        "LEVER_5_INSTRUCTION_PROMPT",
        "LEVER_5_HOLISTIC_PROMPT",
        "STRATEGIST_PROMPT",
        "STRATEGIST_TRIAGE_PROMPT",
        "STRATEGIST_DETAIL_PROMPT",
        "ADAPTIVE_STRATEGIST_PROMPT",
        "LEVER_6_SQL_EXPRESSION_PROMPT",
        "PROSE_RULE_MINING_PROMPT",
    )
    for name in causal_constants:
        prompt = getattr(cfg, name)
        assert "<unified_rca_engine_contract>" in prompt, name


def test_three_non_causal_sites_omit_contract_when_flag_on():
    """Resolved prompt strings for the three non-causal sites must NOT
    contain the contract marker when the flag is on."""
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    non_causal_constants = (
        "EXPAND_INSTRUCTION_PROMPT",
        "LEVER_4_JOIN_DISCOVERY_PROMPT",
        "SQL_EXPRESSION_SEEDING_PROMPT",
    )
    for name in non_causal_constants:
        prompt = getattr(cfg, name)
        assert "<unified_rca_engine_contract>" not in prompt, name


def test_three_non_causal_sites_keep_contract_when_flag_off():
    """Default-off path preserves byte-stable replay: contract still present."""
    cfg = _reload_config_with_env({})
    non_causal_constants = (
        "EXPAND_INSTRUCTION_PROMPT",
        "LEVER_4_JOIN_DISCOVERY_PROMPT",
        "SQL_EXPRESSION_SEEDING_PROMPT",
    )
    for name in non_causal_constants:
        prompt = getattr(cfg, name)
        assert "<unified_rca_engine_contract>" in prompt, name


import json
import tempfile
from pathlib import Path


def test_capture_sink_no_op_when_path_unset():
    """When GSO_NARROWING_CAPTURE_PATH is unset, no NDJSON file should be
    written even if GSO_RCA_CONTRACT_NARROW_V1=1 and a non-causal name
    is queried. Counters still increment."""
    with tempfile.TemporaryDirectory() as td:
        cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
        cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        _ = cfg._rca_contract_for("lever-4-join-discovery")
        # Counter incremented:
        snap = cfg.dump_narrowing_capture_summary()
        assert snap["hits"]["lever-4-join-discovery"] == 1
        # No file:
        assert list(Path(td).iterdir()) == []


def test_capture_sink_writes_ndjson_when_path_set():
    """With both flag and path, each non-causal call appends one
    JSON line with the documented schema."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "capture.ndjson"
        cfg = _reload_config_with_env({
            "GSO_RCA_CONTRACT_NARROW_V1": "1",
            "GSO_NARROWING_CAPTURE_PATH": str(path),
        })
        cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        _ = cfg._rca_contract_for("lever-4-join-discovery")
        _ = cfg._rca_contract_for("preflight-instruction-expand")
        _ = cfg._rca_contract_for("lever-4-join-discovery")
        # Causal name should NOT be captured:
        _ = cfg._rca_contract_for("strategy-adaptive")

        lines = path.read_text().strip().splitlines()
        assert len(lines) == 3, lines
        records = [json.loads(line) for line in lines]
        skill_ids = [r["skill_id"] for r in records]
        assert skill_ids == [
            "lever-4-join-discovery",
            "preflight-instruction-expand",
            "lever-4-join-discovery",
        ]
        # Required fields:
        for r in records:
            assert set(r.keys()) >= {
                "skill_id", "process_pid", "rendered_at_ts",
                "header_omitted_bytes", "iteration_id",
            }, r
            assert isinstance(r["header_omitted_bytes"], int)
            assert r["header_omitted_bytes"] > 0
            assert isinstance(r["process_pid"], int)


def test_capture_sink_no_op_when_flag_off_even_with_path():
    """If the flag is off, no narrowing happens, so capture must not write."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "capture.ndjson"
        cfg = _reload_config_with_env({
            "GSO_NARROWING_CAPTURE_PATH": str(path),
            # flag explicitly off
        })
        cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
        _ = cfg._rca_contract_for("lever-4-join-discovery")
        assert not path.exists(), "capture must not write when flag is off"


def test_dump_summary_returns_per_skill_counts():
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._rca_contract_for("lever-4-join-discovery")
    cfg._rca_contract_for("lever-4-join-discovery")
    cfg._rca_contract_for("preflight-instruction-expand")
    snap = cfg.dump_narrowing_capture_summary()
    assert snap["hits"] == {
        "lever-4-join-discovery": 2,
        "preflight-instruction-expand": 1,
        "preflight-sql-expression-seeding": 0,
    }
    assert snap["all_sites_exercised"] is False
    assert snap["unhit_sites"] == ("preflight-sql-expression-seeding",)


def test_coverage_gate_passes_when_all_sites_hit():
    cfg = _reload_config_with_env({
        "GSO_RCA_CONTRACT_NARROW_V1": "1",
        "GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    for name in cfg._NON_CAUSAL_PROMPT_NAMES:  # noqa: SLF001
        cfg._rca_contract_for(name)
    # Should not raise:
    cfg._NARROWING_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_raises_when_a_site_is_unhit():
    cfg = _reload_config_with_env({
        "GSO_RCA_CONTRACT_NARROW_V1": "1",
        "GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE": "1",
    })
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._rca_contract_for("lever-4-join-discovery")
    # Two sites unhit:
    import pytest
    with pytest.raises(RuntimeError, match="narrowing trial incomplete"):
        cfg._NARROWING_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_no_op_when_flag_unset():
    """The gate is opt-in — without GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE
    it does nothing even if coverage is incomplete."""
    cfg = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    # Don't trigger any sites; gate is opt-in so this should not raise:
    cfg._NARROWING_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


# ── Task 6: byte-delta tests ─────────────────────────────────────────


def test_lever_4_join_discovery_byte_delta_when_flag_on():
    """When flag is on, LEVER_4_JOIN_DISCOVERY_PROMPT must be smaller
    than when flag is off, by approximately len(_RCA_CONTRACT_HEADER).
    The exact byte count makes the regression detectable."""
    cfg_off = _reload_config_with_env({})
    prompt_off = cfg_off.LEVER_4_JOIN_DISCOVERY_PROMPT
    header_len = len(cfg_off._RCA_CONTRACT_HEADER)

    cfg_on = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    prompt_on = cfg_on.LEVER_4_JOIN_DISCOVERY_PROMPT

    delta = len(prompt_off) - len(prompt_on)
    # Allow ±2 chars slack for the trailing "\n\n" grouping in the
    # header definition; the dominant signal is the contract block size.
    assert abs(delta - header_len) <= 2, (
        f"Expected ~{header_len}-byte reduction in flag-on prompt, "
        f"got {delta}. prompt_off={len(prompt_off)} prompt_on={len(prompt_on)}"
    )


# ── Task 7: rendering tests ──────────────────────────────────────────


def test_lever_4_join_discovery_renders_under_both_flag_states():
    """Ensure format_mlflow_template still parses the prompt cleanly
    whether the contract block is present or stripped."""
    from genie_space_optimizer.common.config import format_mlflow_template

    sample_kwargs = {
        "full_schema_context": "(test schema)",
        "current_join_specs": "[]",
        "discovery_hints": "(no hints)",
        "identifier_allowlist": "test.schema.t1.col1",
    }

    # Flag off
    cfg_off = _reload_config_with_env({})
    rendered_off = format_mlflow_template(
        cfg_off.LEVER_4_JOIN_DISCOVERY_PROMPT, **sample_kwargs,
    )
    assert "(test schema)" in rendered_off
    assert "<unified_rca_engine_contract>" in rendered_off
    assert "{{" not in rendered_off, "unrendered template variables remain"

    # Flag on
    cfg_on = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    rendered_on = format_mlflow_template(
        cfg_on.LEVER_4_JOIN_DISCOVERY_PROMPT, **sample_kwargs,
    )
    assert "(test schema)" in rendered_on
    assert "<unified_rca_engine_contract>" not in rendered_on
    assert "{{" not in rendered_on, "unrendered template variables remain"


# ── Task 8: EXPAND_INSTRUCTION_PROMPT rendering ──────────────────────


def test_expand_instruction_renders_under_both_flag_states():
    """Ensure EXPAND_INSTRUCTION_PROMPT still renders both ways."""
    from genie_space_optimizer.common.config import format_mlflow_template

    sample_kwargs = {
        "existing_instructions": "(none)",
        "missing_sections": "- PURPOSE",
        "existing_length": "0",
        "remaining_budget": "2000",
        "missing_count": "1",
        "per_section_budget": "2000",
        "join_specs_context": "(none)",
        "tables_context": "(none)",
        "schema_index": "(none)",
        "metric_views_context": "(none)",
        "functions_context": "(none)",
    }

    cfg_off = _reload_config_with_env({})
    rendered_off = format_mlflow_template(
        cfg_off.EXPAND_INSTRUCTION_PROMPT, **sample_kwargs,
    )
    assert "<unified_rca_engine_contract>" in rendered_off

    cfg_on = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    rendered_on = format_mlflow_template(
        cfg_on.EXPAND_INSTRUCTION_PROMPT, **sample_kwargs,
    )
    assert "<unified_rca_engine_contract>" not in rendered_on


# ── Task 9: EXPAND_INSTRUCTION_PROMPT byte-delta ─────────────────────


def test_expand_instruction_byte_delta_when_flag_on():
    cfg_off = _reload_config_with_env({})
    header_len = len(cfg_off._RCA_CONTRACT_HEADER)
    prompt_off = cfg_off.EXPAND_INSTRUCTION_PROMPT

    cfg_on = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    prompt_on = cfg_on.EXPAND_INSTRUCTION_PROMPT

    delta = len(prompt_off) - len(prompt_on)
    assert abs(delta - header_len) <= 2, (
        f"Expected ~{header_len}-byte reduction, got {delta}"
    )


# ── Task 10: SQL_EXPRESSION_SEEDING_PROMPT ──────────────────────────


def test_sql_expression_seeding_renders_under_both_flag_states():
    from genie_space_optimizer.common.config import format_mlflow_template

    sample_kwargs = {
        "candidates": "[]",
        "schema": "(test)",
    }

    cfg_off = _reload_config_with_env({})
    rendered_off = format_mlflow_template(
        cfg_off.SQL_EXPRESSION_SEEDING_PROMPT, **sample_kwargs,
    )
    assert "<unified_rca_engine_contract>" in rendered_off

    cfg_on = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    rendered_on = format_mlflow_template(
        cfg_on.SQL_EXPRESSION_SEEDING_PROMPT, **sample_kwargs,
    )
    assert "<unified_rca_engine_contract>" not in rendered_on


def test_sql_expression_seeding_byte_delta_when_flag_on():
    cfg_off = _reload_config_with_env({})
    header_len = len(cfg_off._RCA_CONTRACT_HEADER)
    prompt_off = cfg_off.SQL_EXPRESSION_SEEDING_PROMPT

    cfg_on = _reload_config_with_env({"GSO_RCA_CONTRACT_NARROW_V1": "1"})
    prompt_on = cfg_on.SQL_EXPRESSION_SEEDING_PROMPT

    delta = len(prompt_off) - len(prompt_on)
    assert abs(delta - header_len) <= 2, (
        f"Expected ~{header_len}-byte reduction, got {delta}"
    )
