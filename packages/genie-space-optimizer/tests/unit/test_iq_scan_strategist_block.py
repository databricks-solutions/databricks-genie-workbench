"""Tests for the IQ Scan findings block in the strategist prompt.

Guards:

- Flag-gated: disabled by default, does NOT leak into ``context_data`` when
  ``GSO_ENABLE_IQ_SCAN_STRATEGIST`` is unset.
- All 4 signal categories render when present (score, ceilings, RLS tables,
  coverage gaps, recommended levers).
- Sections are omitted individually when the corresponding field is empty.
- ``_build_context_data`` carries the rendered text under
  ``iq_scan_findings``.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_flag(monkeypatch):
    """Every test starts with the flag off. Individual tests opt in."""
    monkeypatch.delenv("GSO_ENABLE_IQ_SCAN_STRATEGIST", raising=False)


# ---------------------------------------------------------------------------
# Flag gating
# ---------------------------------------------------------------------------

class TestFlagGating:
    def test_disabled_returns_empty_string(self):
        from genie_space_optimizer.optimization.optimizer import _format_iq_scan_findings

        summary = {
            "score": 7, "total": 12, "maturity": "Ready to Optimize",
            "ceilings": ["too many sources"],
            "rls_tables": [],
            "coverage_gaps": ["no filter snippets"],
            "recommended_levers": [1, 4],
        }
        assert _format_iq_scan_findings(summary) == ""

    def test_enabled_with_none_summary_returns_empty(self, monkeypatch):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_STRATEGIST", "true")
        from genie_space_optimizer.optimization.optimizer import _format_iq_scan_findings
        assert _format_iq_scan_findings(None) == ""

    def test_enabled_with_empty_dict_returns_empty(self, monkeypatch):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_STRATEGIST", "true")
        from genie_space_optimizer.optimization.optimizer import _format_iq_scan_findings
        assert _format_iq_scan_findings({}) == ""


# ---------------------------------------------------------------------------
# Rendering all sections
# ---------------------------------------------------------------------------

class TestRendering:
    def test_all_sections_present(self, monkeypatch):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_STRATEGIST", "true")
        from genie_space_optimizer.optimization.optimizer import _format_iq_scan_findings

        text = _format_iq_scan_findings({
            "score": 7, "total": 12, "maturity": "Ready to Optimize",
            "ceilings": ["15 data sources exceeds the 12 limit"],
            "rls_tables": ["main.analytics.orders"],
            "coverage_gaps": ["missing filter snippets", "example_sqls missing usage_guidance"],
            "recommended_levers": [1, 4, 6],
        })
        assert "IQ Score: 7/12 (Ready to Optimize)" in text
        assert "WARNING: 15 data sources" in text
        assert "row-level security" in text.lower()
        assert "main.analytics.orders" in text
        assert "Coverage gaps:" in text
        assert "missing filter snippets" in text
        assert "Scan-recommended levers:" in text
        assert "1 (Tables & Columns)" in text

    def test_omits_empty_sections(self, monkeypatch):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_STRATEGIST", "true")
        from genie_space_optimizer.optimization.optimizer import _format_iq_scan_findings

        text = _format_iq_scan_findings({
            "score": 12, "total": 12, "maturity": "Trusted",
            "ceilings": [],
            "rls_tables": [],
            "coverage_gaps": [],
            "recommended_levers": [],
        })
        assert text == "IQ Score: 12/12 (Trusted)"
        assert "WARNING" not in text
        assert "Coverage gaps" not in text
        assert "levers" not in text


# ---------------------------------------------------------------------------
# _build_context_data integration
# ---------------------------------------------------------------------------

class TestContextDataIntegration:
    def _minimal_kwargs(self) -> dict:
        """Arguments needed to call _build_context_data without touching the LLM.

        Most sections are allowed to be empty or trivial.
        """
        return {
            "clusters": [],
            "soft_signal_clusters": [],
            "metadata_snapshot": {"data_sources": {"tables": [], "metric_views": []}},
            "reflection_buffer": [],
            "priority_ranking": [],
            "blame_set": None,
            "success_summary": "(no benchmarks)",
            "reflection_text": "",
            "persistence_text": "",
            "proven_patterns_text": "",
            "suggestions_text": "",
        }

    def test_adds_iq_scan_findings_key_when_text_provided(self):
        from genie_space_optimizer.optimization.optimizer import _build_context_data

        ctx = _build_context_data(
            **self._minimal_kwargs(),
            iq_scan_text="IQ Score: 7/12 (Ready to Optimize)",
        )
        assert ctx["iq_scan_findings"] == "IQ Score: 7/12 (Ready to Optimize)"

    def test_key_is_none_when_text_empty(self):
        from genie_space_optimizer.optimization.optimizer import _build_context_data

        ctx = _build_context_data(**self._minimal_kwargs(), iq_scan_text="")
        assert "iq_scan_findings" in ctx
        assert ctx["iq_scan_findings"] is None

    def test_default_is_backward_compatible(self):
        """Existing call sites that don't pass iq_scan_text still work."""
        from genie_space_optimizer.optimization.optimizer import _build_context_data

        ctx = _build_context_data(**self._minimal_kwargs())
        assert "iq_scan_findings" in ctx
        assert ctx["iq_scan_findings"] is None

    def test_strategist_prompt_includes_typed_rca_themes(self):
        from genie_space_optimizer.optimization.optimizer import (
            _build_context_data,
            _format_rca_themes_for_strategy,
        )
        from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

        block = _format_rca_themes_for_strategy([
            RcaPatchTheme(
                rca_id="rca_avg_txn",
                rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
                patch_family="contrastive_metric_routing",
                patches=(),
                target_qids=("retail_010", "retail_027"),
                touched_objects=("mv_esr_store_sales", "mv_7now_store_sales"),
            )
        ], [])

        assert "Typed RCA Themes" in block
        assert "contrastive_metric_routing" in block
        assert "retail_010" in block

        ctx = _build_context_data(
            **self._minimal_kwargs(),
            rca_theme_context=block,
        )
        assert ctx["rca_theme_context"] == block
