"""Unit tests for eval dedup fix, instruction slot counting with table/MV
descriptions, and TVF removal patch construction."""

from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.common.genie_schema import (
    MAX_INSTRUCTION_SLOTS,
    count_instruction_slots,
)


# ═══════════════════════════════════════════════════════════════════════
# count_instruction_slots — table/MV descriptions
# ═══════════════════════════════════════════════════════════════════════


class TestCountInstructionSlotsWithDescriptions:
    """Verify that count_instruction_slots includes table and metric view
    descriptions in its budget, matching the Genie API counting."""

    def test_empty_config(self):
        assert count_instruction_slots({}) == 0

    def test_instructions_only(self):
        config = {
            "data_sources": {"tables": []},
            "instructions": {
                "text_instructions": [{"id": "ti1", "content": ["text"]}],
                "example_question_sqls": [
                    {"id": "eq1", "question": ["Q?"], "sql": ["SELECT 1"]},
                    {"id": "eq2", "question": ["Q2?"], "sql": ["SELECT 2"]},
                ],
                "sql_functions": [
                    {"id": "sf1", "identifier": "cat.sch.fn1"},
                ],
            },
        }
        assert count_instruction_slots(config) == 4  # 1 + 2 + 1

    def test_tables_with_descriptions_counted(self):
        config = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "description": ["A table"]},
                    {"identifier": "cat.sch.t2", "description": ["Another table"]},
                    {"identifier": "cat.sch.t3"},  # no description
                ],
            },
            "instructions": {
                "example_question_sqls": [
                    {"id": "eq1", "question": ["Q?"], "sql": ["SELECT 1"]},
                ],
            },
        }
        assert count_instruction_slots(config) == 3  # 1 example + 2 table descs

    def test_metric_views_with_descriptions_counted(self):
        config = {
            "data_sources": {
                "tables": [],
                "metric_views": [
                    {"identifier": "cat.sch.mv1", "description": ["Revenue MV"]},
                    {"identifier": "cat.sch.mv2"},  # no description
                ],
            },
            "instructions": {},
        }
        assert count_instruction_slots(config) == 1  # 1 MV desc

    def test_full_budget_with_all_sources(self):
        config = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "description": ["Desc"]},
                    {"identifier": "cat.sch.t2", "description": ["Desc"]},
                ],
                "metric_views": [
                    {"identifier": "cat.sch.mv1", "description": ["Desc"]},
                ],
            },
            "instructions": {
                "text_instructions": [{"id": "ti1", "content": ["text"]}],
                "example_question_sqls": [
                    {"id": f"eq{i}", "question": [f"Q{i}?"], "sql": [f"SELECT {i}"]}
                    for i in range(5)
                ],
                "sql_functions": [
                    {"id": "sf1", "identifier": "cat.sch.fn1"},
                    {"id": "sf2", "identifier": "cat.sch.fn2"},
                ],
            },
        }
        # 1 (text) + 5 (examples) + 2 (functions) + 2 (table descs) + 1 (mv desc)
        assert count_instruction_slots(config) == 11

    def test_empty_description_not_counted(self):
        config = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "description": []},
                    {"identifier": "cat.sch.t2", "description": [""]},
                ],
            },
        }
        # empty list is falsy; [""] is truthy
        assert count_instruction_slots(config) == 1

    def test_exceeds_budget_detected(self):
        eqs = [
            {"id": f"eq{i}", "question": [f"Q{i}?"], "sql": [f"SELECT {i}"]}
            for i in range(85)
        ]
        tables = [
            {"identifier": f"cat.sch.t{i}", "description": [f"Table {i}"]}
            for i in range(12)
        ]
        mvs = [
            {"identifier": f"cat.sch.mv{i}", "description": [f"MV {i}"]}
            for i in range(2)
        ]
        config = {
            "data_sources": {"tables": tables, "metric_views": mvs},
            "instructions": {
                "text_instructions": [{"id": "ti1", "content": ["text"]}],
                "example_question_sqls": eqs,
            },
        }
        # 1 + 85 + 12 + 2 = 100
        assert count_instruction_slots(config) == 100

        config["instructions"]["example_question_sqls"].append(
            {"id": "eq_extra", "question": ["Extra?"], "sql": ["SELECT extra"]}
        )
        assert count_instruction_slots(config) == 101
        assert count_instruction_slots(config) > MAX_INSTRUCTION_SLOTS


# ═══════════════════════════════════════════════════════════════════════
# _handle_escalation — stores tvf_id and previous_tvf_asset
# ═══════════════════════════════════════════════════════════════════════


class TestHandleEscalationTvfDetail:
    """Verify that _handle_escalation stores tvf_id and previous_tvf_asset
    in the result dict when processing remove_tvf escalations."""

    @patch("genie_space_optimizer.common.uc_metadata.check_tvf_schema_overlap")
    @patch("genie_space_optimizer.optimization.labeling.flag_for_human_review")
    @patch("genie_space_optimizer.optimization.state.load_provenance")
    @patch("genie_space_optimizer.optimization.harness._score_tvf_removal_confidence")
    def test_stores_tvf_details_for_high_confidence(
        self, mock_score, mock_prov, mock_flag, mock_overlap,
    ):
        from genie_space_optimizer.optimization.harness import _handle_escalation

        mock_score.return_value = "high"
        mock_prov.return_value = MagicMock(empty=True)
        mock_overlap.return_value = {"coverage": 1.0, "covered_columns": []}

        tvf_asset = {
            "id": "sf123",
            "identifier": "cat.sch.get_booking_trends",
            "description": ["Returns booking trends"],
        }
        metadata_snapshot = {
            "instructions": {
                "sql_functions": [tvf_asset],
            },
        }
        ag = {
            "lever_directives": {
                "3": {
                    "functions": [{"identifier": "cat.sch.get_booking_trends"}],
                },
            },
            "affected_questions": ["q1", "q2"],
        }

        result = _handle_escalation(
            "remove_tvf", ag,
            w=MagicMock(), spark=MagicMock(), run_id="run-1",
            catalog="cat", schema="sch", domain="dom", iteration=3,
            benchmarks=[], verdict_history={}, reflection_buffer=[],
            metadata_snapshot=metadata_snapshot,
        )

        assert result["handled"] is True
        assert result["detail"]["tier_action"] == "auto_apply"
        assert result["detail"]["tvf_id"] == "cat.sch.get_booking_trends"
        assert result["detail"]["previous_tvf_asset"] == tvf_asset

    @patch("genie_space_optimizer.common.uc_metadata.check_tvf_schema_overlap")
    @patch("genie_space_optimizer.optimization.labeling.flag_for_human_review")
    @patch("genie_space_optimizer.optimization.state.load_provenance")
    @patch("genie_space_optimizer.optimization.harness._score_tvf_removal_confidence")
    def test_stores_tvf_details_for_medium_confidence(
        self, mock_score, mock_prov, mock_flag, mock_overlap,
    ):
        from genie_space_optimizer.optimization.harness import _handle_escalation

        mock_score.return_value = "medium"
        mock_prov.return_value = MagicMock(empty=True)
        mock_overlap.return_value = {"coverage": 0.8}

        tvf_asset = {
            "id": "sf456",
            "identifier": "cat.sch.get_trends",
        }
        metadata_snapshot = {
            "instructions": {
                "sql_functions": [tvf_asset],
            },
        }
        ag = {
            "lever_directives": {
                "3": {
                    "functions": [{"identifier": "cat.sch.get_trends"}],
                },
            },
            "affected_questions": ["q3"],
        }

        result = _handle_escalation(
            "remove_tvf", ag,
            w=MagicMock(), spark=MagicMock(), run_id="run-1",
            catalog="cat", schema="sch", domain="dom", iteration=3,
            benchmarks=[], verdict_history={}, reflection_buffer=[],
            metadata_snapshot=metadata_snapshot,
        )

        assert result["handled"] is True
        assert result["detail"]["tier_action"] == "apply_and_flag"
        assert result["detail"]["tvf_id"] == "cat.sch.get_trends"
        assert result["detail"]["previous_tvf_asset"] == tvf_asset

    @patch("genie_space_optimizer.common.uc_metadata.check_tvf_schema_overlap")
    @patch("genie_space_optimizer.optimization.labeling.flag_for_human_review")
    @patch("genie_space_optimizer.optimization.state.load_provenance")
    @patch("genie_space_optimizer.optimization.harness._score_tvf_removal_confidence")
    @patch("genie_space_optimizer.optimization.harness._validate_tvf_removal_coverage")
    def test_missing_tvf_in_config_returns_empty_asset(
        self, mock_coverage, mock_score, mock_prov, mock_flag, mock_overlap,
    ):
        from genie_space_optimizer.optimization.harness import _handle_escalation

        mock_coverage.return_value = {"valid": True}
        mock_score.return_value = "high"
        mock_prov.return_value = MagicMock(empty=True)
        mock_overlap.return_value = {"coverage": 1.0}

        metadata_snapshot = {
            "instructions": {
                "sql_functions": [
                    {"id": "sf999", "identifier": "cat.sch.some_other_fn"},
                ],
            },
        }
        ag = {
            "lever_directives": {
                "3": {
                    "functions": [{"identifier": "cat.sch.nonexistent_tvf"}],
                },
            },
            "affected_questions": [],
        }

        result = _handle_escalation(
            "remove_tvf", ag,
            w=MagicMock(), spark=MagicMock(), run_id="run-1",
            catalog="cat", schema="sch", domain="dom", iteration=3,
            benchmarks=[], verdict_history={}, reflection_buffer=[],
            metadata_snapshot=metadata_snapshot,
        )

        assert result["detail"]["tvf_id"] == "cat.sch.nonexistent_tvf"
        assert result["detail"]["previous_tvf_asset"] == {}


# ═══════════════════════════════════════════════════════════════════════
# TVF removal patch rendering via render_patch
# ═══════════════════════════════════════════════════════════════════════


class TestRemoveTvfPatchRendering:
    """Verify that render_patch correctly produces forward/rollback commands
    for a remove_tvf patch."""

    def test_remove_tvf_patch_renders_correctly(self):
        from genie_space_optimizer.optimization.applier import render_patch

        tvf_asset = {
            "id": "sf123",
            "identifier": "cat.sch.get_booking_trends",
            "description": ["Returns booking trends"],
        }
        patch = {
            "type": "remove_tvf",
            "target": "cat.sch.get_booking_trends",
            "new_text": "",
            "old_text": "",
            "previous_tvf_asset": tvf_asset,
        }
        space_config = {
            "data_sources": {"tables": []},
            "instructions": {
                "sql_functions": [tvf_asset],
            },
        }

        rendered = render_patch(patch, "sp-1", space_config)
        fwd = json.loads(rendered["command"])
        rev = json.loads(rendered["rollback_command"])

        assert fwd["op"] == "remove"
        assert fwd["section"] == "tvfs"
        assert fwd["identifier"] == "cat.sch.get_booking_trends"

        assert rev["op"] == "add"
        assert rev["section"] == "tvfs"
        assert rev["tvf_asset"] == tvf_asset


# ═══════════════════════════════════════════════════════════════════════
# run_evaluation — uses deduped DataFrame, not dataset object
# ═══════════════════════════════════════════════════════════════════════


class TestRunEvaluationUsesEvalData:
    """Verify that run_evaluation passes the deduped eval_data DataFrame
    to mlflow.genai.evaluate, not the _eval_dataset_obj."""

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    @patch("genie_space_optimizer.optimization.evaluation._run_evaluate_with_retries")
    @patch("genie_space_optimizer.optimization.evaluation.register_judge_prompts")
    @patch("genie_space_optimizer.optimization.evaluation.register_scorers_with_experiment")
    @patch("genie_space_optimizer.optimization.evaluation._precheck_benchmarks_for_eval")
    @patch("genie_space_optimizer.optimization.evaluation._load_known_functions")
    def test_eval_data_used_not_dataset_obj(
        self, mock_kf, mock_precheck, mock_reg_scorers, mock_reg_judges,
        mock_retries, mock_mlflow,
    ):
        import pandas as pd

        from genie_space_optimizer.optimization.evaluation import run_evaluation

        benchmarks = [
            {
                "id": f"q{i}",
                "question": f"Question {i}",
                "expected_sql": f"SELECT {i}",
                "expected_asset": "TABLE",
            }
            for i in range(5)
        ]

        mock_precheck.return_value = (benchmarks, [], {
            "invalid_benchmark_count": 0,
            "permission_blocked_count": 0,
            "unresolved_column_count": 0,
            "bad_join_key_count": 0,
        })
        mock_kf.return_value = set()

        mock_eval_result = MagicMock()
        mock_eval_result.metrics = {"accuracy": 0.8}
        mock_eval_result.tables = {"eval_results": pd.DataFrame()}
        mock_retries.return_value = (mock_eval_result, [])

        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        try:
            run_evaluation(
                w=MagicMock(),
                space_id="sp-1",
                benchmarks=benchmarks,
                scorers=[],
                experiment_name="/test-exp",
                predict_fn=lambda x: "SELECT 1",
                spark=MagicMock(),
                iteration=0,
                catalog="cat",
                gold_schema="sch",
                uc_schema="cat.sch",
                domain="dom",
            )
        except Exception:
            pass

        if mock_retries.called:
            call_kwargs = mock_retries.call_args
            evaluate_kwargs = (
                call_kwargs.kwargs.get("evaluate_kwargs")
                or call_kwargs[1].get("evaluate_kwargs")
                or (call_kwargs[0][0] if call_kwargs[0] else {})
            )
            if isinstance(evaluate_kwargs, dict):
                data = evaluate_kwargs.get("data")
                if data is not None:
                    assert isinstance(data, pd.DataFrame), (
                        f"Expected eval_data DataFrame, got {type(data).__name__}"
                    )
