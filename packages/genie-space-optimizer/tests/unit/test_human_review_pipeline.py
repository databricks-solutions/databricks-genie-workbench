"""Unit tests for human review pipeline: benchmark truncation, trace enrichment,
judge override processing, and persistence flagging."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch, call

import pytest

from genie_space_optimizer.optimization.evaluation import (
    _truncate_benchmarks,
    _PROVENANCE_PRIORITY,
)


# ── _truncate_benchmarks ─────────────────────────────────────────────


class TestTruncateBenchmarks:
    def test_no_truncation_when_under_limit(self):
        benchmarks = [{"id": str(i), "provenance": "synthetic"} for i in range(10)]
        result = _truncate_benchmarks(benchmarks, 25)
        assert len(result) == 10
        assert result is benchmarks

    def test_truncates_to_max_count(self):
        benchmarks = [{"id": str(i), "provenance": "synthetic"} for i in range(30)]
        result = _truncate_benchmarks(benchmarks, 25)
        assert len(result) == 25

    def test_provenance_priority_order(self):
        benchmarks = [
            {"id": "curated_1", "provenance": "curated"},
            {"id": "gap_fill_1", "provenance": "coverage_gap_fill"},
            {"id": "synthetic_1", "provenance": "synthetic"},
            {"id": "auto_corr_1", "provenance": "auto_corrected"},
            {"id": "curated_2", "provenance": "curated"},
        ]
        result = _truncate_benchmarks(benchmarks, 3)
        assert len(result) == 3
        assert result[0]["id"] == "curated_1"
        assert result[1]["id"] == "curated_2"
        assert result[2]["id"] == "synthetic_1"

    def test_unknown_provenance_last(self):
        benchmarks = [
            {"id": "unknown_1", "provenance": "unknown_type"},
            {"id": "curated_1", "provenance": "curated"},
            {"id": "synthetic_1", "provenance": "synthetic"},
        ]
        result = _truncate_benchmarks(benchmarks, 2)
        assert result[0]["id"] == "curated_1"
        assert result[1]["id"] == "synthetic_1"

    def test_missing_provenance_treated_as_other(self):
        benchmarks = [
            {"id": "no_prov"},
            {"id": "curated_1", "provenance": "curated"},
        ]
        result = _truncate_benchmarks(benchmarks, 1)
        assert result[0]["id"] == "curated_1"

    def test_preserves_within_tier_order(self):
        benchmarks = [
            {"id": "s1", "provenance": "synthetic"},
            {"id": "s2", "provenance": "synthetic"},
            {"id": "s3", "provenance": "synthetic"},
            {"id": "s4", "provenance": "synthetic"},
        ]
        result = _truncate_benchmarks(benchmarks, 3)
        assert [b["id"] for b in result] == ["s1", "s2", "s3"]

    def test_exact_limit_no_truncation(self):
        benchmarks = [{"id": str(i), "provenance": "synthetic"} for i in range(25)]
        result = _truncate_benchmarks(benchmarks, 25)
        assert len(result) == 25
        assert result is benchmarks

    def test_empty_list(self):
        result = _truncate_benchmarks([], 25)
        assert result == []


# ── log_judge_verdicts_on_traces ──────────────────────────────────────


class TestLogJudgeVerdictsOnTraces:
    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_logs_verdicts_for_mapped_traces(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_judge_verdicts_on_traces

        eval_result = {
            "trace_map": {"q1": "t1", "q2": "t2"},
            "rows": [
                {"question_id": "q1", "schema_accuracy/value": "yes", "arbiter": "both_correct"},
                {"question_id": "q2", "schema_accuracy/value": "no", "arbiter": "genie_wrong"},
            ],
        }
        result = log_judge_verdicts_on_traces(eval_result)
        assert result == 2
        assert mock_mlflow.log_feedback.call_count == 2

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_skips_unmapped_traces(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_judge_verdicts_on_traces

        eval_result = {
            "trace_map": {"q1": "t1"},
            "rows": [
                {"question_id": "q1", "schema_accuracy/value": "yes"},
                {"question_id": "q_unmapped", "schema_accuracy/value": "no"},
            ],
        }
        result = log_judge_verdicts_on_traces(eval_result)
        assert result == 1

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_empty_rows(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_judge_verdicts_on_traces

        eval_result = {"trace_map": {}, "rows": []}
        result = log_judge_verdicts_on_traces(eval_result)
        assert result == 0
        mock_mlflow.log_feedback.assert_not_called()


# ── log_persistence_context_on_traces ─────────────────────────────────


class TestLogPersistenceContextOnTraces:
    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_logs_persistence_data(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_persistence_context_on_traces

        eval_result = {"trace_map": {"q1": "t1", "q2": "t2"}}
        persistence_data = {
            "q1": {
                "classification": "PERSISTENT",
                "fail_count": 3,
                "max_consecutive": 3,
                "patches_tried": [(1, "add_instruction")],
                "fail_iterations": [1, 2, 3],
            },
        }
        result = log_persistence_context_on_traces(eval_result, persistence_data)
        assert result == 1
        mock_mlflow.log_feedback.assert_called_once()
        kw = mock_mlflow.log_feedback.call_args
        assert kw.kwargs["name"] == "persistence_context"
        assert kw.kwargs["value"] is True  # PERSISTENT != INTERMITTENT

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_intermittent_value_is_false(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_persistence_context_on_traces

        eval_result = {"trace_map": {"q1": "t1"}}
        persistence_data = {
            "q1": {
                "classification": "INTERMITTENT",
                "fail_count": 2,
                "max_consecutive": 1,
                "patches_tried": [],
                "fail_iterations": [1, 3],
            },
        }
        result = log_persistence_context_on_traces(eval_result, persistence_data)
        assert result == 1
        kw = mock_mlflow.log_feedback.call_args
        assert kw.kwargs["value"] is False

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_skips_unmapped_questions(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_persistence_context_on_traces

        eval_result = {"trace_map": {"q1": "t1"}}
        persistence_data = {
            "q_other": {"classification": "PERSISTENT", "fail_count": 3, "max_consecutive": 3},
        }
        result = log_persistence_context_on_traces(eval_result, persistence_data)
        assert result == 0


# ── _extract_genie_sql_from_trace ────────────────────────────────────


class TestExtractGenieSqlFromTrace:
    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_extracts_genie_sql_from_dict_response(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import _extract_genie_sql_from_trace

        mock_trace = MagicMock()
        mock_trace.data.response = {"genie_sql": "SELECT 1"}
        mock_mlflow.get_trace.return_value = mock_trace

        result = _extract_genie_sql_from_trace("trace_123")
        assert result == "SELECT 1"

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_returns_empty_for_missing_trace(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import _extract_genie_sql_from_trace

        mock_mlflow.get_trace.return_value = None
        result = _extract_genie_sql_from_trace("trace_missing")
        assert result == ""

    def test_returns_empty_for_empty_trace_id(self):
        from genie_space_optimizer.optimization.evaluation import _extract_genie_sql_from_trace

        result = _extract_genie_sql_from_trace("")
        assert result == ""

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_extracts_from_json_string_response(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import _extract_genie_sql_from_trace

        mock_trace = MagicMock()
        mock_trace.data.response = json.dumps({"genie_sql": "SELECT 2"})
        mock_mlflow.get_trace.return_value = mock_trace

        result = _extract_genie_sql_from_trace("trace_456")
        assert result == "SELECT 2"


# ── Judge override processing ─────────────────────────────────────────


class TestJudgeOverrideProcessing:
    """Validate that judge_override entries from ingest_human_feedback include
    question and question_id, and that Ambiguous verdicts are captured."""

    def _make_trace_row(self, verdict_value: str, request_content: str = "What is X?"):
        return {
            "trace_id": "tr-001",
            "request": json.dumps({"messages": [{"content": request_content}]}),
            "request_id": "qid-001",
            "assessments": [
                {
                    "name": "judge_verdict_accuracy",
                    "value": verdict_value,
                    "rationale": "test rationale",
                }
            ],
        }

    @patch("genie_space_optimizer.optimization.labeling._find_session_by_name")
    @patch("genie_space_optimizer.optimization.labeling.mlflow")
    def test_ambiguous_captured_as_judge_override(self, mock_mlflow, mock_find):
        import pandas as pd
        from genie_space_optimizer.optimization.labeling import ingest_human_feedback

        row = self._make_trace_row("Ambiguous — unclear question")
        mock_session = MagicMock()
        mock_find.return_value = mock_session
        mock_mlflow.search_traces.return_value = pd.DataFrame([row])

        result = ingest_human_feedback("test_session")
        corrections = result["corrections"]
        overrides = [c for c in corrections if c["type"] == "judge_override"]
        assert len(overrides) == 1
        assert overrides[0]["feedback"] == "ambiguous"
        assert overrides[0].get("question") == "What is X?"
        assert overrides[0].get("question_id") == "qid-001"

    @patch("genie_space_optimizer.optimization.labeling._find_session_by_name")
    @patch("genie_space_optimizer.optimization.labeling.mlflow")
    def test_correct_verdict_captured(self, mock_mlflow, mock_find):
        import pandas as pd
        from genie_space_optimizer.optimization.labeling import ingest_human_feedback

        row = self._make_trace_row("No — the Genie answer is correct")
        mock_session = MagicMock()
        mock_find.return_value = mock_session
        mock_mlflow.search_traces.return_value = pd.DataFrame([row])

        result = ingest_human_feedback("test_session")
        corrections = result["corrections"]
        overrides = [c for c in corrections if c["type"] == "judge_override"]
        assert len(overrides) == 1
        assert overrides[0]["feedback"] == "genie_correct"


# ── log_patch_history_on_traces ───────────────────────────────────────


class TestLogPatchHistoryOnTraces:
    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_logs_patch_history_for_persistent_questions(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_patch_history_on_traces

        question_trace_map = {
            "q1": ["t1a", "t1b"],
            "q2": ["t2a"],
            "q3": ["t3a", "t3b", "t3c"],
        }
        reflection_buffer = [
            {
                "iteration": 1,
                "accepted": True,
                "action": "add_instruction on tbl_orders",
                "affected_question_ids": ["q1", "q2"],
                "prev_scores": {"s1": 60.0},
                "new_scores": {"s1": 64.2},
            },
            {
                "iteration": 2,
                "accepted": False,
                "action": "rewrite_instruction on tbl_users",
                "affected_question_ids": ["q1", "q3"],
                "prev_scores": {"s1": 64.2},
                "new_scores": {"s1": 62.1},
            },
        ]
        result = log_patch_history_on_traces(
            question_trace_map, reflection_buffer,
            persistent_question_ids={"q1"},
        )
        assert result == 1
        kw = mock_mlflow.log_feedback.call_args
        assert kw.kwargs["trace_id"] == "t1b"
        assert kw.kwargs["name"] == "patch_history"
        assert "Iter 1" in kw.kwargs["rationale"]
        assert "ACCEPTED" in kw.kwargs["rationale"]
        assert "Iter 2" in kw.kwargs["rationale"]
        assert "ROLLED_BACK" in kw.kwargs["rationale"]

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_logs_all_questions_when_no_filter(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_patch_history_on_traces

        question_trace_map = {"q1": ["t1"], "q2": ["t2"]}
        reflection_buffer = [
            {
                "iteration": 1,
                "accepted": True,
                "action": "add_example_sql on tbl_x",
                "affected_question_ids": ["q1", "q2"],
                "prev_scores": {"s1": 50.0},
                "new_scores": {"s1": 55.0},
            },
        ]
        result = log_patch_history_on_traces(question_trace_map, reflection_buffer)
        assert result == 2

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_skips_questions_without_traces(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_patch_history_on_traces

        question_trace_map = {"q1": ["t1"]}
        reflection_buffer = [
            {
                "iteration": 1,
                "accepted": True,
                "action": "add_instruction on tbl_x",
                "affected_question_ids": ["q1", "q_missing"],
                "prev_scores": {},
                "new_scores": {},
            },
        ]
        result = log_patch_history_on_traces(
            question_trace_map, reflection_buffer,
            persistent_question_ids={"q1", "q_missing"},
        )
        assert result == 1

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_empty_reflection_buffer(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_patch_history_on_traces

        result = log_patch_history_on_traces({"q1": ["t1"]}, [])
        assert result == 0
        mock_mlflow.log_feedback.assert_not_called()


# ── Persistent-only session filtering ─────────────────────────────────


class TestPersistentOnlySessionFiltering:
    def test_filters_to_persistent_questions_only(self):
        persistence_data = {
            "q1": {"classification": "PERSISTENT", "fail_count": 3},
            "q2": {"classification": "INTERMITTENT", "fail_count": 1},
            "q3": {"classification": "ADDITIVE_LEVERS_EXHAUSTED", "fail_count": 4},
            "q4": {"classification": "TRANSIENT", "fail_count": 1},
        }
        question_trace_map = {
            "q1": ["t1a", "t1b"],
            "q2": ["t2a"],
            "q3": ["t3a", "t3b", "t3c"],
            "q4": ["t4a"],
        }

        persistent_question_ids = [
            qid for qid, ctx in persistence_data.items()
            if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
        ]
        persistent_trace_ids = [
            question_trace_map[qid][-1]
            for qid in persistent_question_ids
            if qid in question_trace_map
        ]

        assert set(persistent_question_ids) == {"q1", "q3"}
        assert set(persistent_trace_ids) == {"t1b", "t3c"}

    def test_falls_back_when_no_persistence_data(self):
        persistence_data: dict = {}
        all_failure_trace_ids = ["tf1", "tf2"]
        all_failure_question_ids = ["q1", "q2"]

        persistent_question_ids = [
            qid for qid, ctx in persistence_data.items()
            if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
        ]

        if persistent_question_ids:
            session_trace_ids = []
            session_question_ids = persistent_question_ids
        else:
            session_trace_ids = list(dict.fromkeys(all_failure_trace_ids))
            session_question_ids = list(dict.fromkeys(all_failure_question_ids))

        assert session_trace_ids == ["tf1", "tf2"]
        assert session_question_ids == ["q1", "q2"]

    def test_handles_missing_question_in_trace_map(self):
        persistence_data = {
            "q1": {"classification": "PERSISTENT", "fail_count": 3},
            "q_orphan": {"classification": "PERSISTENT", "fail_count": 2},
        }
        question_trace_map = {"q1": ["t1a"]}

        persistent_question_ids = [
            qid for qid, ctx in persistence_data.items()
            if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
        ]
        persistent_trace_ids = [
            question_trace_map[qid][-1]
            for qid in persistent_question_ids
            if qid in question_trace_map
        ]

        assert "t1a" in persistent_trace_ids
        assert len(persistent_trace_ids) == 1


# ── log_persistence_context extra_trace_map ───────────────────────────


class TestLogPersistenceContextExtraTraceMap:
    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_extra_trace_map_logs_on_all_traces(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_persistence_context_on_traces

        persistence_data = {
            "q1": {
                "classification": "PERSISTENT",
                "fail_count": 3,
                "max_consecutive": 3,
                "patches_tried": [],
                "fail_iterations": [1, 2, 3],
            },
        }
        extra_trace_map = {"q1": ["t1a", "t1b", "t1c"]}
        result = log_persistence_context_on_traces(
            {}, persistence_data, extra_trace_map=extra_trace_map,
        )
        assert result == 3
        assert mock_mlflow.log_feedback.call_count == 3
        logged_tids = [c.kwargs["trace_id"] for c in mock_mlflow.log_feedback.call_args_list]
        assert logged_tids == ["t1a", "t1b", "t1c"]

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_falls_back_to_eval_result_trace_map(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_persistence_context_on_traces

        eval_result = {"trace_map": {"q1": "t1_fallback"}}
        persistence_data = {
            "q1": {
                "classification": "PERSISTENT",
                "fail_count": 2,
                "max_consecutive": 2,
                "patches_tried": [],
                "fail_iterations": [1, 2],
            },
        }
        result = log_persistence_context_on_traces(eval_result, persistence_data)
        assert result == 1
        assert mock_mlflow.log_feedback.call_args.kwargs["trace_id"] == "t1_fallback"

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_extra_trace_map_overrides_eval_result(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import log_persistence_context_on_traces

        eval_result = {"trace_map": {"q1": "t1_old"}}
        persistence_data = {
            "q1": {
                "classification": "PERSISTENT",
                "fail_count": 2,
                "max_consecutive": 2,
                "patches_tried": [],
                "fail_iterations": [1, 2],
            },
        }
        extra_trace_map = {"q1": ["t1_new1", "t1_new2"]}
        result = log_persistence_context_on_traces(
            eval_result, persistence_data, extra_trace_map=extra_trace_map,
        )
        assert result == 2
        logged_tids = [c.kwargs["trace_id"] for c in mock_mlflow.log_feedback.call_args_list]
        assert "t1_old" not in logged_tids
        assert logged_tids == ["t1_new1", "t1_new2"]


# ── Finalize merges repeatability traces ──────────────────────────────


class TestFinalizeMergesRepeatabilityTraces:
    def test_repeatability_trace_maps_merged_into_question_trace_map(self):
        question_trace_map: dict[str, list[str]] = {
            "q1": ["t1_iter1"],
            "q2": ["t2_iter1", "t2_iter2"],
        }
        rep_results = [
            {"trace_map": {"q1": "t1_rep1", "q2": "t2_rep1"}, "mlflow_run_id": "run_rep1"},
            {"trace_map": {"q1": "t1_rep2", "q3": "t3_rep2"}, "mlflow_run_id": "run_rep2"},
        ]
        all_eval_mlflow_run_ids: list[str] = ["run_iter1"]

        for rr in rep_results:
            rr_tmap = rr.get("trace_map", {})
            for qid, tid in rr_tmap.items():
                question_trace_map.setdefault(qid, []).append(tid)
            rr_run_id = rr.get("mlflow_run_id", "")
            if rr_run_id:
                all_eval_mlflow_run_ids.append(rr_run_id)

        assert question_trace_map["q1"] == ["t1_iter1", "t1_rep1", "t1_rep2"]
        assert question_trace_map["q2"] == ["t2_iter1", "t2_iter2", "t2_rep1"]
        assert question_trace_map["q3"] == ["t3_rep2"]
        assert all_eval_mlflow_run_ids == ["run_iter1", "run_rep1", "run_rep2"]

    def test_empty_rep_results_no_mutation(self):
        question_trace_map: dict[str, list[str]] = {"q1": ["t1"]}
        original = dict(question_trace_map)
        rep_results: list[dict] = []

        for rr in rep_results:
            rr_tmap = rr.get("trace_map", {})
            for qid, tid in rr_tmap.items():
                question_trace_map.setdefault(qid, []).append(tid)

        assert question_trace_map == original
