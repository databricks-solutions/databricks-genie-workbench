"""Unit tests for Plan 1's contract-narrowing behavior.

The historical ``GSO_RCA_CONTRACT_NARROW_V1`` flag was retired by the
2026-05-16 dead-flag cleanup. Plan 1 is now unconditionally on:
non-causal prompts render without the ``<unified_rca_engine_contract>``
block; causal prompts keep it.

This file is the byte-shape regression suite for the narrowed posture.
The historical rollback-branch tests were deleted because the rollback
branch no longer exists; the grep-guard at
``tests/unit/test_dead_flags_removed.py`` blocks re-introduction of the
env var or its helper.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path


def test_helper_returns_header_for_causal_prompt_name():
    from genie_space_optimizer.common import config as cfg

    result = cfg._rca_contract_for("strategy-adaptive")
    assert result == cfg._RCA_CONTRACT_HEADER
    assert "unified_rca_engine_contract" in result


def test_helper_returns_empty_for_non_causal_prompt_names():
    from genie_space_optimizer.common import config as cfg

    for name in ("preflight-instruction-expand",
                 "lever-4-join-discovery",
                 "preflight-sql-expression-seeding"):
        assert cfg._rca_contract_for(name) == "", name


def test_unknown_prompt_name_defaults_to_causal_treatment():
    """Safety: any name not explicitly in the non-causal set keeps the contract.
    Adding a new non-causal name is an explicit registry edit, not an opt-out."""
    from genie_space_optimizer.common import config as cfg

    assert cfg._rca_contract_for("brand-new-skill-not-yet-classified") == cfg._RCA_CONTRACT_HEADER


def test_eleven_causal_sites_still_contain_contract_marker():
    """Resolved prompt strings for every causal site must contain the
    contract's ``<unified_rca_engine_contract>`` marker tag."""
    from genie_space_optimizer.common import config as cfg

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


def test_three_non_causal_sites_omit_contract():
    """Resolved prompt strings for the three non-causal sites must NOT
    contain the contract marker. This is the always-narrowed posture
    that replaced the historical flag-gated rollback path."""
    from genie_space_optimizer.common import config as cfg

    non_causal_constants = (
        "EXPAND_INSTRUCTION_PROMPT",
        "LEVER_4_JOIN_DISCOVERY_PROMPT",
        "SQL_EXPRESSION_SEEDING_PROMPT",
    )
    for name in non_causal_constants:
        prompt = getattr(cfg, name)
        assert "<unified_rca_engine_contract>" not in prompt, name


# ── Capture-sink behavior ────────────────────────────────────────────


def test_capture_sink_no_op_when_path_unset():
    """When GSO_NARROWING_CAPTURE_PATH is unset, no NDJSON file is
    written. The in-memory hit counter still increments."""
    import os

    os.environ.pop("GSO_NARROWING_CAPTURE_PATH", None)
    import importlib
    from genie_space_optimizer.common import config as cfg
    importlib.reload(cfg)
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001

    with tempfile.TemporaryDirectory() as td:
        _ = cfg._rca_contract_for("lever-4-join-discovery")
        # Counter incremented:
        snap = cfg.dump_narrowing_capture_summary()
        assert snap["hits"]["lever-4-join-discovery"] == 1
        # No file:
        assert list(Path(td).iterdir()) == []


def test_capture_sink_writes_ndjson_when_path_set():
    """With the path env set, each non-causal call appends one JSON
    line with the documented schema. Causal names are not captured."""
    import os
    import importlib

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "capture.ndjson"
        os.environ["GSO_NARROWING_CAPTURE_PATH"] = str(path)
        try:
            from genie_space_optimizer.common import config as cfg
            importlib.reload(cfg)
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
            for r in records:
                assert set(r.keys()) >= {
                    "skill_id", "process_pid", "rendered_at_ts",
                    "header_omitted_bytes", "iteration_id",
                }, r
                assert isinstance(r["header_omitted_bytes"], int)
                assert r["header_omitted_bytes"] > 0
                assert isinstance(r["process_pid"], int)
        finally:
            os.environ.pop("GSO_NARROWING_CAPTURE_PATH", None)


def test_dump_summary_returns_per_skill_counts():
    from genie_space_optimizer.common import config as cfg

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
    from genie_space_optimizer.common import config as cfg

    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    for name in cfg._NON_CAUSAL_PROMPT_NAMES:  # noqa: SLF001
        cfg._rca_contract_for(name)
    # Should not raise:
    cfg._NARROWING_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_raises_when_a_site_is_unhit(monkeypatch):
    """Plan 5 makes the gate inert in production (helper returns False).
    Dev tests that want to exercise the gate path monkeypatch the helper
    back to True."""
    from genie_space_optimizer.common import config as cfg

    monkeypatch.setattr(
        cfg, "narrowing_capture_require_coverage_enabled", lambda: True,
    )
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._rca_contract_for("lever-4-join-discovery")
    # Two sites unhit:
    import pytest
    with pytest.raises(RuntimeError, match="narrowing trial incomplete"):
        cfg._NARROWING_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001


def test_coverage_gate_no_op_when_flag_unset():
    """The gate is opt-in — without GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE
    it does nothing even if coverage is incomplete."""
    from genie_space_optimizer.common import config as cfg

    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    # Don't trigger any sites; gate is opt-in so this should not raise:
    cfg._NARROWING_CAPTURE_SINK.enforce_coverage_or_raise()  # noqa: SLF001
