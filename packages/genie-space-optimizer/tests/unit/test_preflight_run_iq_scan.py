"""Tests for preflight_run_iq_scan — the IQ Scan preflight sub-step.

Note: ``write_scan_snapshot`` is imported lazily inside the function under
test so it is patched at the *source* module
``genie_space_optimizer.optimization.scan_snapshots``.

Guards:

- Flag-gated no-op when GSO_ENABLE_IQ_SCAN_PREFLIGHT is unset.
- Hard-block on Check 1 (no data sources).
- Warn-only on Check 10 (<10 benchmarks) — never raises.
- Persists a phase='preflight' row via write_scan_snapshot.
- Translates failing/warning config checks into recommended levers via
  SCAN_CHECK_TO_LEVERS and unions with caller-supplied CTA levers.
- Degrades gracefully when the snapshot writer raises.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def capturing_spark():
    """Minimal Spark stub; the scan sub-step only uses ``spark.sql`` via writers."""
    return MagicMock(name="spark")


# ---------------------------------------------------------------------------
# Flag gating
# ---------------------------------------------------------------------------

class TestFlagGating:
    def test_disabled_returns_noop(self, capturing_spark, monkeypatch):
        monkeypatch.delenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", raising=False)
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        out = preflight_run_iq_scan(
            capturing_spark, "run-1", "space-1", "cat", "gold", {},
        )
        assert out == {
            "scan": None,
            "scan_summary_for_strategist": None,
            "recommended_levers": [],
        }

    def test_disabled_preserves_cta_levers(self, capturing_spark, monkeypatch):
        monkeypatch.delenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", raising=False)
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        out = preflight_run_iq_scan(
            capturing_spark, "run-1", "space-1", "cat", "gold", {},
            recommended_levers_from_cta=[2, 4],
        )
        assert out["recommended_levers"] == [2, 4]

    @pytest.mark.parametrize("flag_value", ["1", "true", "yes", "on", "TRUE"])
    def test_enabled_values(self, flag_value, capturing_spark, monkeypatch):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", flag_value)
        from genie_space_optimizer.optimization.preflight import _iq_scan_preflight_enabled
        assert _iq_scan_preflight_enabled() is True


# ---------------------------------------------------------------------------
# Check 1 hard-block
# ---------------------------------------------------------------------------

class TestCheck1HardBlock:
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_raises_when_no_data_sources(
        self, mock_write_stage, capturing_spark, monkeypatch,
    ):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        empty_config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            return_value=True,
        ):
            with pytest.raises(RuntimeError, match="No tables or metric views configured"):
                preflight_run_iq_scan(
                    capturing_spark, "run-1", "space-1", "cat", "gold", empty_config,
                )

        # Snapshot must be written first so failure has an audit row.
        stages = [c.args[2] for c in mock_write_stage.call_args_list]
        assert "PREFLIGHT_IQ_SCAN_CHECK1_FAILED" in stages

    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_snapshot_persisted_before_hard_block(
        self, _mock_write_stage, capturing_spark, monkeypatch,
    ):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        empty_config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            return_value=True,
        ) as mock_writer:
            with pytest.raises(RuntimeError):
                preflight_run_iq_scan(
                    capturing_spark, "run-1", "space-1", "cat", "gold", empty_config,
                )

        mock_writer.assert_called_once()
        _args, kwargs = mock_writer.call_args
        positional = mock_writer.call_args.args
        # Positional: spark, run_id, space_id, phase, scan_result, catalog, schema
        assert positional[1] == "run-1"
        assert positional[3] == "preflight"


# ---------------------------------------------------------------------------
# Check 10 warn-only (no raise)
# ---------------------------------------------------------------------------

class TestCheck10WarnOnly:
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_few_benchmarks_warns_never_raises(
        self, mock_write_stage, capturing_spark, monkeypatch,
    ):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        # 1 table with good metadata BUT < 10 example_questions — Check 10 fails.
        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [{
                        "identifier": "main.s.t",
                        "description": "orders",
                        "comment": "orders fact",
                        "columns": [
                            {"name": "id", "type": "INT", "description": "order id"},
                            {"name": "amt", "type": "DOUBLE", "description": "amount"},
                        ],
                    }],
                    "metric_views": [],
                },
                "example_questions": [],
                "sample_questions": [],
                "general_instructions": "",
            }
        }

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            return_value=True,
        ):
            out = preflight_run_iq_scan(
                capturing_spark, "run-1", "space-1", "cat", "gold", cfg,
            )

        assert out["scan"] is not None
        stages = [c.args[2] for c in mock_write_stage.call_args_list]
        assert "PREFLIGHT_IQ_SCAN_BENCHMARK_WARN" in stages
        assert "PREFLIGHT_IQ_SCAN_CHECK1_FAILED" not in stages


# ---------------------------------------------------------------------------
# Lever recommendation
# ---------------------------------------------------------------------------

class TestRecommendedLevers:
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_failing_checks_translate_to_levers(
        self, _mock_write_stage, capturing_spark, monkeypatch,
    ):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.common.config import SCAN_CHECK_TO_LEVERS
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        # 1 table with no descriptions — triggers failure on checks 2 and 3.
        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [{
                        "identifier": "main.s.t",
                        "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "amt", "type": "DOUBLE"},
                        ],
                    }],
                    "metric_views": [],
                },
                "example_questions": [],
                "sample_questions": [],
            }
        }

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            return_value=True,
        ):
            out = preflight_run_iq_scan(
                capturing_spark, "run-1", "space-1", "cat", "gold", cfg,
            )

        expected_levers = set(SCAN_CHECK_TO_LEVERS.get(2, [])) | set(SCAN_CHECK_TO_LEVERS.get(3, []))
        assert expected_levers.issubset(set(out["recommended_levers"]))

    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_cta_levers_merged_and_deduped(
        self, _mock_write_stage, capturing_spark, monkeypatch,
    ):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [{
                        "identifier": "main.s.t",
                        "columns": [{"name": "id", "type": "INT"}],
                    }],
                    "metric_views": [],
                },
            }
        }

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            return_value=True,
        ):
            out = preflight_run_iq_scan(
                capturing_spark, "run-1", "space-1", "cat", "gold", cfg,
                recommended_levers_from_cta=[1, 1, 2],
            )

        # Sorted + deduped, includes CTA levers.
        assert 1 in out["recommended_levers"]
        assert 2 in out["recommended_levers"]
        assert out["recommended_levers"] == sorted(set(out["recommended_levers"]))


# ---------------------------------------------------------------------------
# Snapshot-write soft-fail
# ---------------------------------------------------------------------------

class TestSnapshotWriteResilience:
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_writer_exception_does_not_abort(
        self, _mock_write_stage, capturing_spark, monkeypatch,
    ):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [{
                        "identifier": "main.s.t",
                        "description": "x",
                        "columns": [{"name": "id", "type": "INT", "description": "y"}],
                    }],
                    "metric_views": [],
                },
            }
        }

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            side_effect=RuntimeError("transient UC outage"),
        ):
            out = preflight_run_iq_scan(
                capturing_spark, "run-1", "space-1", "cat", "gold", cfg,
            )

        assert out["scan"] is not None


# ---------------------------------------------------------------------------
# Strategist summary shape
# ---------------------------------------------------------------------------

class TestStrategistSummaryShape:
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_summary_keys(self, _mock_write_stage, capturing_spark, monkeypatch):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
        from genie_space_optimizer.optimization.preflight import preflight_run_iq_scan

        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [{
                        "identifier": "main.s.t",
                        "description": "desc",
                        "columns": [{"name": "id", "type": "INT", "description": "y"}],
                    }],
                    "metric_views": [],
                },
            }
        }

        with patch(
            "genie_space_optimizer.optimization.scan_snapshots.write_scan_snapshot",
            return_value=True,
        ):
            out = preflight_run_iq_scan(
                capturing_spark, "run-1", "space-1", "cat", "gold", cfg,
            )

        summary = out["scan_summary_for_strategist"]
        assert set(summary.keys()) == {
            "score", "total", "maturity",
            "ceilings", "rls_tables", "coverage_gaps", "recommended_levers",
        }
