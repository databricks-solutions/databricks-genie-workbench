"""Unit tests for per-question failure persistence, TVF confidence scoring,
escalation dispatch, and flag_for_review mechanisms."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.harness import (
    VerdictEntry,
    _build_question_persistence_summary,
    _build_reflection_entry,
    _handle_escalation,
    _score_tvf_removal_confidence,
    _validate_tvf_removal_coverage,
)
from genie_space_optimizer.common.uc_metadata import check_tvf_schema_overlap


# ── Helpers ──────────────────────────────────────────────────────────


def _ve(iteration: int, verdict: str, question_text: str = "What is X?") -> VerdictEntry:
    return VerdictEntry(
        iteration=iteration,
        verdict=verdict,
        genie_sql="SELECT 1",
        expected_sql="SELECT 2",
        question_text=question_text,
        rationale="test",
    )


def _reflection(iteration: int, accepted: bool, affected: list[str], action: str = "add_instruction on tbl") -> dict:
    return {
        "iteration": iteration,
        "ag_id": f"AG{iteration}",
        "accepted": accepted,
        "action": action,
        "levers": [1],
        "target_objects": [],
        "score_deltas": {},
        "accuracy_delta": 0.0,
        "new_failures": None,
        "rollback_reason": None if accepted else "gate",
        "do_not_retry": [],
        "affected_question_ids": affected,
        "fixed_questions": [],
        "still_failing": affected,
        "new_regressions": [],
        "reflection_text": "",
        "refinement_mode": "",
    }


# ═══════════════════════════════════════════════════════════════════════
# _build_reflection_entry
# ═══════════════════════════════════════════════════════════════════════


class TestBuildReflectionEntry:
    def test_includes_question_tracking_fields(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=True,
            levers=[1], target_objects=["tbl"],
            prev_scores={"s": 50.0}, new_scores={"s": 60.0},
            rollback_reason=None, patches=[],
            affected_question_ids=["q1", "q2"],
            prev_failure_qids={"q1", "q2", "q3"},
            new_failure_qids={"q2", "q4"},
        )
        assert entry["affected_question_ids"] == ["q1", "q2"]
        assert sorted(entry["fixed_questions"]) == ["q1", "q3"]
        assert entry["still_failing"] == ["q2"]
        assert entry["new_regressions"] == ["q4"]

    def test_empty_sets_default_gracefully(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=False,
            levers=[], target_objects=[], prev_scores={}, new_scores={},
            rollback_reason="no_proposals", patches=[],
        )
        assert entry["affected_question_ids"] == []
        assert entry["fixed_questions"] == []
        assert entry["still_failing"] == []
        assert entry["new_regressions"] == []

    def test_reflection_text_and_refinement_mode(self):
        entry = _build_reflection_entry(
            iteration=2, ag_id="AG2", accepted=False,
            levers=[1, 5], target_objects=["tbl"],
            prev_scores={"s": 80.0}, new_scores={"s": 75.0},
            rollback_reason="regression", patches=[],
            reflection_text="Rollback: instructions caused regressions.",
            refinement_mode="in_plan",
        )
        assert entry["reflection_text"] == "Rollback: instructions caused regressions."
        assert entry["refinement_mode"] == "in_plan"

    def test_defaults_for_new_fields(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=True,
            levers=[1], target_objects=["tbl"],
            prev_scores={"s": 50.0}, new_scores={"s": 60.0},
            rollback_reason=None, patches=[],
        )
        assert entry["reflection_text"] == ""
        assert entry["refinement_mode"] == ""


# ═══════════════════════════════════════════════════════════════════════
# _build_question_persistence_summary
# ═══════════════════════════════════════════════════════════════════════


class TestBuildQuestionPersistenceSummary:
    def test_no_data_returns_placeholder(self):
        text, structured = _build_question_persistence_summary({}, [])
        assert "No cross-iteration" in text
        assert structured == {}

    def test_below_threshold_not_shown(self):
        history = {"q1": [_ve(1, "both_correct")]}
        text, structured = _build_question_persistence_summary(history, [], min_failures=2)
        assert "No persistent" in text
        assert structured == {}

    def test_persistent_question_shown(self):
        history = {
            "q1": [
                _ve(1, "ground_truth_correct"),
                _ve(2, "ground_truth_correct"),
                _ve(3, "ground_truth_correct"),
            ],
        }
        text, structured = _build_question_persistence_summary(history, [], min_failures=2)
        assert "q1" in text
        assert "3/3" in text
        assert "PERSISTENT" in text
        assert "q1" in structured
        assert structured["q1"]["classification"] == "PERSISTENT"
        assert structured["q1"]["fail_count"] == 3

    def test_additive_exhausted_classification(self):
        history = {
            "q1": [
                _ve(1, "ground_truth_correct"),
                _ve(2, "ground_truth_correct"),
                _ve(3, "ground_truth_correct"),
            ],
        }
        reflection_buffer = [
            _reflection(1, False, ["q1"], "add_instruction on tbl"),
            _reflection(2, False, ["q1"], "add_example_sql on tbl"),
            _reflection(3, False, ["q1"], "add_instruction on tbl, add_example_sql on tbl"),
        ]
        text, structured = _build_question_persistence_summary(history, reflection_buffer, min_failures=2)
        assert "ADDITIVE_LEVERS_EXHAUSTED" in text
        assert structured["q1"]["classification"] == "ADDITIVE_LEVERS_EXHAUSTED"

    def test_intermittent_classification(self):
        history = {
            "q1": [
                _ve(1, "ground_truth_correct"),
                _ve(2, "both_correct"),
                _ve(3, "ground_truth_correct"),
            ],
        }
        text, structured = _build_question_persistence_summary(history, [], min_failures=2)
        assert "INTERMITTENT" in text


# ═══════════════════════════════════════════════════════════════════════
# check_tvf_schema_overlap
# ═══════════════════════════════════════════════════════════════════════


class TestCheckTvfSchemaOverlap:
    def test_full_coverage(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            MagicMock(__getitem__=lambda s, i: "Return Type:"),
            MagicMock(__getitem__=lambda s, i: "col_a STRING"),
            MagicMock(__getitem__=lambda s, i: "col_b INT"),
            MagicMock(__getitem__=lambda s, i: ""),
        ]

        metadata = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.tbl",
                        "column_configs": [
                            {"column_name": "col_a"},
                            {"column_name": "col_b"},
                            {"column_name": "col_c"},
                        ],
                    }
                ],
                "metric_views": [],
            }
        }

        result = check_tvf_schema_overlap(mock_spark, "cat.sch.my_tvf", metadata)
        assert result["full_coverage"] is True
        assert result["coverage_ratio"] == 1.0
        assert result["uncovered_columns"] == []

    def test_partial_coverage(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            MagicMock(__getitem__=lambda s, i: "Return Type:"),
            MagicMock(__getitem__=lambda s, i: "col_a STRING"),
            MagicMock(__getitem__=lambda s, i: "col_x INT"),
            MagicMock(__getitem__=lambda s, i: ""),
        ]

        metadata = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.tbl",
                        "column_configs": [{"column_name": "col_a"}],
                    }
                ],
                "metric_views": [],
            }
        }

        result = check_tvf_schema_overlap(mock_spark, "cat.sch.my_tvf", metadata)
        assert result["full_coverage"] is False
        assert result["coverage_ratio"] == 0.5
        assert "col_x" in result["uncovered_columns"]


# ═══════════════════════════════════════════════════════════════════════
# _score_tvf_removal_confidence
# ═══════════════════════════════════════════════════════════════════════


class TestScoreTvfRemovalConfidence:
    def _provenance(self, tvf: str, iteration: int, qid: str = "q1") -> dict:
        return {
            "question_id": qid,
            "blame_set": json.dumps([tvf]),
            "iteration": iteration,
        }

    def test_iteration_gate_not_met_returns_none(self):
        history = {"q1": [_ve(1, "ground_truth_correct")]}
        result = _score_tvf_removal_confidence(
            "cat.sch.my_tvf",
            benchmarks=[],
            verdict_history=history,
            reflection_buffer=[],
            schema_overlap={"full_coverage": True, "coverage_ratio": 1.0, "uncovered_columns": []},
            asi_provenance=[self._provenance("cat.sch.my_tvf", 1)],
            min_iterations=2,
        )
        assert result is None

    def test_gt_reference_returns_low(self):
        history = {
            "q1": [_ve(1, "ground_truth_correct"), _ve(2, "ground_truth_correct")],
        }
        result = _score_tvf_removal_confidence(
            "cat.sch.my_tvf",
            benchmarks=[{"expected_asset": "cat.sch.my_tvf", "expected_sql": "SELECT 1"}],
            verdict_history=history,
            reflection_buffer=[],
            schema_overlap={"full_coverage": True, "coverage_ratio": 1.0, "uncovered_columns": []},
            asi_provenance=[
                self._provenance("cat.sch.my_tvf", 1),
                self._provenance("cat.sch.my_tvf", 2),
            ],
            min_iterations=2,
        )
        assert result == "low"

    def test_high_confidence_full_coverage_and_blame(self):
        history = {
            "q1": [_ve(1, "ground_truth_correct"), _ve(2, "ground_truth_correct")],
        }
        result = _score_tvf_removal_confidence(
            "cat.sch.my_tvf",
            benchmarks=[{"expected_asset": "", "expected_sql": "SELECT 1 FROM tbl"}],
            verdict_history=history,
            reflection_buffer=[],
            schema_overlap={"full_coverage": True, "coverage_ratio": 1.0, "uncovered_columns": []},
            asi_provenance=[
                self._provenance("cat.sch.my_tvf", 1),
                self._provenance("cat.sch.my_tvf", 2),
            ],
            min_iterations=2,
        )
        assert result == "high"

    def test_medium_confidence_partial_coverage(self):
        history = {
            "q1": [_ve(1, "ground_truth_correct"), _ve(2, "ground_truth_correct")],
        }
        result = _score_tvf_removal_confidence(
            "cat.sch.my_tvf",
            benchmarks=[{"expected_asset": "", "expected_sql": "SELECT col_a FROM tbl"}],
            verdict_history=history,
            reflection_buffer=[],
            schema_overlap={
                "full_coverage": False,
                "coverage_ratio": 0.5,
                "uncovered_columns": ["col_x"],
            },
            asi_provenance=[self._provenance("cat.sch.my_tvf", 1)],
            min_iterations=2,
        )
        assert result == "medium"

    def test_low_confidence_uncovered_in_benchmarks(self):
        history = {
            "q1": [_ve(1, "ground_truth_correct"), _ve(2, "ground_truth_correct")],
        }
        result = _score_tvf_removal_confidence(
            "cat.sch.my_tvf",
            benchmarks=[{"expected_asset": "", "expected_sql": "SELECT col_x FROM tbl"}],
            verdict_history=history,
            reflection_buffer=[],
            schema_overlap={
                "full_coverage": False,
                "coverage_ratio": 0.5,
                "uncovered_columns": ["col_x"],
            },
            asi_provenance=[self._provenance("cat.sch.my_tvf", 1)],
            min_iterations=2,
        )
        assert result == "low"


# ═══════════════════════════════════════════════════════════════════════
# _handle_escalation
# ═══════════════════════════════════════════════════════════════════════


class TestHandleEscalation:
    def test_flag_for_review_calls_labeling(self):
        mock_spark = MagicMock()
        ag = {
            "affected_questions": ["q1", "q2"],
            "root_cause_summary": "Persistent failure",
        }

        with patch(
            "genie_space_optimizer.optimization.labeling.flag_for_human_review",
            return_value=2,
        ) as mock_flag:
            result = _handle_escalation(
                "flag_for_review", ag,
                w=MagicMock(), spark=mock_spark, run_id="run1",
                catalog="cat", schema="sch", domain="test",
                iteration=3, benchmarks=[], verdict_history={},
                reflection_buffer=[], metadata_snapshot={},
            )

        assert result["handled"] is True
        assert result["detail"]["flagged_count"] == 2
        mock_flag.assert_called_once()

    @patch("genie_space_optimizer.optimization.harness._run_arbiter_corrections")
    def test_gt_repair_delegated(self, mock_arbiter):
        mock_arbiter.return_value = {
            "gc_applied": 1, "gc_skipped": 0,
            "nc_repaired": 0, "nc_quarantined": 0,
            "corrected_qids": {"q1"}, "quarantined_qids": set(),
        }
        result = _handle_escalation(
            "gt_repair",
            {"affected_questions": ["q1"]},
            w=MagicMock(), spark=MagicMock(), run_id="run1",
            catalog="cat", schema="sch", domain="test",
            iteration=3, benchmarks=[], verdict_history={},
            reflection_buffer=[], metadata_snapshot={},
        )
        assert result["handled"] is True
        assert result["detail"]["corrections_applied"] == 1
        mock_arbiter.assert_called_once()

    def test_remove_tvf_no_identifier_returns_unhandled(self):
        ag = {
            "affected_questions": ["q1"],
            "lever_directives": {"3": {"functions": []}},
        }
        result = _handle_escalation(
            "remove_tvf", ag,
            w=MagicMock(), spark=MagicMock(), run_id="run1",
            catalog="cat", schema="sch", domain="test",
            iteration=3, benchmarks=[], verdict_history={},
            reflection_buffer=[], metadata_snapshot={},
        )
        assert result["handled"] is False
        assert "no_tvf_identifier" in str(result["detail"])

    def test_unknown_escalation_not_handled(self):
        result = _handle_escalation(
            "unknown_type",
            {"affected_questions": []},
            w=MagicMock(), spark=MagicMock(), run_id="run1",
            catalog="cat", schema="sch", domain="test",
            iteration=3, benchmarks=[], verdict_history={},
            reflection_buffer=[], metadata_snapshot={},
        )
        assert result["handled"] is False

    def test_remove_tvf_coverage_check_failure_blocks_removal(self):
        """When the TVF target is actually a table, coverage check rejects it."""
        ag = {
            "affected_questions": ["q1"],
            "lever_directives": {
                "3": {"functions": [{"identifier": "cat.sch.my_table"}]}
            },
        }
        metadata = {
            "data_sources": {
                "tables": [{"identifier": "cat.sch.my_table", "column_configs": []}],
                "metric_views": [],
            },
            "instructions": {"sql_functions": []},
        }
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []

        result = _handle_escalation(
            "remove_tvf", ag,
            w=MagicMock(), spark=mock_spark, run_id="run1",
            catalog="cat", schema="sch", domain="test",
            iteration=3, benchmarks=[], verdict_history={},
            reflection_buffer=[], metadata_snapshot=metadata,
        )
        assert result["handled"] is False
        assert result["detail"].get("error") == "coverage_check_failed"
        assert "table" in result["detail"]["reason"].lower()


# ═══════════════════════════════════════════════════════════════════════
# _validate_tvf_removal_coverage
# ═══════════════════════════════════════════════════════════════════════


class TestValidateTvfRemovalCoverage:
    def _metadata(
        self,
        tvf_id: str = "cat.sch.my_tvf",
        tables: list[dict] | None = None,
        mvs: list[dict] | None = None,
    ) -> dict:
        return {
            "instructions": {
                "sql_functions": [{"identifier": tvf_id}] if tvf_id else [],
            },
            "data_sources": {
                "tables": tables or [],
                "metric_views": mvs or [],
            },
        }

    def test_rejects_table_removal(self):
        metadata = {
            "instructions": {"sql_functions": []},
            "data_sources": {
                "tables": [{"identifier": "cat.sch.tbl_a", "column_configs": []}],
                "metric_views": [],
            },
        }
        result = _validate_tvf_removal_coverage(
            "cat.sch.tbl_a", [], {}, metadata,
        )
        assert result["valid"] is False
        assert "table" in result["reason"].lower()

    def test_rejects_metric_view_removal(self):
        metadata = {
            "instructions": {"sql_functions": []},
            "data_sources": {
                "tables": [],
                "metric_views": [{"identifier": "cat.sch.mv_a"}],
            },
        }
        result = _validate_tvf_removal_coverage(
            "cat.sch.mv_a", [], {}, metadata,
        )
        assert result["valid"] is False
        assert "metric view" in result["reason"].lower()

    def test_rejects_unknown_asset(self):
        metadata = self._metadata(tvf_id="")
        result = _validate_tvf_removal_coverage(
            "cat.sch.unknown_thing", [], {}, metadata,
        )
        assert result["valid"] is False
        assert "not found" in result["reason"].lower()

    def test_allows_tvf_with_full_coverage(self):
        metadata = self._metadata("cat.sch.my_tvf")
        schema_overlap = {
            "full_coverage": True,
            "coverage_ratio": 1.0,
            "uncovered_columns": [],
        }
        benchmarks = [{"question_id": "q1", "expected_asset": "cat.sch.my_tvf"}]

        result = _validate_tvf_removal_coverage(
            "cat.sch.my_tvf", benchmarks, schema_overlap, metadata,
        )
        assert result["valid"] is True
        assert result["affected_questions"] == ["q1"]

    def test_allows_tvf_with_no_affected_questions(self):
        metadata = self._metadata("cat.sch.my_tvf")
        schema_overlap = {
            "full_coverage": False,
            "coverage_ratio": 0.0,
            "uncovered_columns": ["col_x"],
        }
        result = _validate_tvf_removal_coverage(
            "cat.sch.my_tvf", [], schema_overlap, metadata,
        )
        assert result["valid"] is True

    def test_rejects_low_coverage_with_affected_questions(self):
        metadata = self._metadata("cat.sch.my_tvf")
        schema_overlap = {
            "full_coverage": False,
            "coverage_ratio": 0.3,
            "uncovered_columns": ["col_x", "col_y"],
        }
        benchmarks = [{"question_id": "q1", "expected_asset": "cat.sch.my_tvf"}]

        result = _validate_tvf_removal_coverage(
            "cat.sch.my_tvf", benchmarks, schema_overlap, metadata,
        )
        assert result["valid"] is False
        assert "Insufficient" in result["reason"]
        assert result["affected_questions"] == ["q1"]
        assert result["coverage_ratio"] == 0.3

    def test_allows_half_coverage_with_affected_questions(self):
        metadata = self._metadata("cat.sch.my_tvf")
        schema_overlap = {
            "full_coverage": False,
            "coverage_ratio": 0.5,
            "uncovered_columns": ["col_y"],
        }
        benchmarks = [{"question_id": "q1", "expected_asset": "cat.sch.my_tvf"}]

        result = _validate_tvf_removal_coverage(
            "cat.sch.my_tvf", benchmarks, schema_overlap, metadata,
        )
        assert result["valid"] is True

    def test_case_insensitive_matching(self):
        metadata = self._metadata("Cat.Sch.My_TVF")
        schema_overlap = {
            "full_coverage": True,
            "coverage_ratio": 1.0,
            "uncovered_columns": [],
        }
        result = _validate_tvf_removal_coverage(
            "cat.sch.my_tvf", [], schema_overlap, metadata,
        )
        assert result["valid"] is True
