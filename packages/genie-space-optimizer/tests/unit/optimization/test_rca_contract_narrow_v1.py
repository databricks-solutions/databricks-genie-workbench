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
