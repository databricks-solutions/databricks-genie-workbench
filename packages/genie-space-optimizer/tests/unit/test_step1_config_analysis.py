"""Unit tests for Step 1 Configuration Analysis fallback logic.

Covers _resolve_parsed_space, _collect_all_preflight_detail,
and the Step 1 paths in _build_step_io / _build_step_summary.
"""

from __future__ import annotations

import json

import pytest

from genie_space_optimizer.backend.routes.runs import (
    _build_step_io,
    _build_step_summary,
    _collect_all_preflight_detail,
    _resolve_parsed_space,
    _STEP_DEFINITIONS,
)


STEP1_DEF = next(d for d in _STEP_DEFINITIONS if d["stepNumber"] == 1)


# ── _resolve_parsed_space ────────────────────────────────────────────


class TestResolveParsedSpace:
    def test_parsed_space_key_present(self):
        snap = {"_parsed_space": {"data_sources": {"tables": [{"identifier": "t1"}]}}}
        result = _resolve_parsed_space(snap)
        assert result == snap["_parsed_space"]

    def test_serialized_space_json_string(self):
        inner = {"data_sources": {"tables": [{"identifier": "t1"}]}}
        snap = {"serialized_space": json.dumps(inner)}
        result = _resolve_parsed_space(snap)
        assert result == inner

    def test_serialized_space_already_dict(self):
        inner = {"data_sources": {"tables": [{"identifier": "t1"}]}}
        snap = {"serialized_space": inner}
        result = _resolve_parsed_space(snap)
        assert result == inner

    def test_data_sources_at_top_level(self):
        snap = {"data_sources": {"tables": [{"identifier": "t1"}]}, "instructions": {}}
        result = _resolve_parsed_space(snap)
        assert result is snap

    def test_empty_dict(self):
        assert _resolve_parsed_space({}) == {}

    def test_none_input(self):
        assert _resolve_parsed_space(None) == {}

    def test_malformed_json_string(self):
        snap = {"serialized_space": "{bad json!!!"}
        assert _resolve_parsed_space(snap) == {}

    def test_parsed_space_takes_priority_over_serialized_space(self):
        parsed = {"data_sources": {"tables": [{"identifier": "from_parsed"}]}}
        serialized = {"data_sources": {"tables": [{"identifier": "from_serialized"}]}}
        snap = {"_parsed_space": parsed, "serialized_space": json.dumps(serialized)}
        result = _resolve_parsed_space(snap)
        assert result["data_sources"]["tables"][0]["identifier"] == "from_parsed"


# ── _collect_all_preflight_detail ────────────────────────────────────


class TestCollectAllPreflightDetail:
    def test_merges_started_and_metadata_stages(self):
        stages = [
            {"stage": "PREFLIGHT_STARTED", "status": "STARTED", "detail_json": None},
            {
                "stage": "PREFLIGHT_METADATA_COLLECTION",
                "status": "COMPLETE",
                "detail_json": json.dumps({
                    "table_ref_count": 5,
                    "columns_collected": 42,
                }),
            },
            {
                "stage": "PREFLIGHT_STARTED",
                "status": "COMPLETE",
                "detail_json": json.dumps({
                    "table_count": 5,
                    "instruction_count": 3,
                    "benchmark_count": 10,
                }),
            },
        ]
        result = _collect_all_preflight_detail(stages)
        assert result["table_count"] == 5
        assert result["table_ref_count"] == 5
        assert result["columns_collected"] == 42
        assert result["instruction_count"] == 3
        assert result["benchmark_count"] == 10

    def test_only_metadata_stage_older_harness(self):
        stages = [
            {"stage": "PREFLIGHT_STARTED", "status": "STARTED", "detail_json": None},
            {
                "stage": "PREFLIGHT_METADATA_COLLECTION",
                "status": "COMPLETE",
                "detail_json": json.dumps({"table_ref_count": 7}),
            },
        ]
        result = _collect_all_preflight_detail(stages)
        assert result["table_ref_count"] == 7
        assert "table_count" not in result

    def test_empty_stages(self):
        assert _collect_all_preflight_detail([]) == {}

    def test_non_preflight_stages_ignored(self):
        stages = [
            {"stage": "BASELINE_EVAL", "status": "COMPLETE", "detail_json": '{"foo": 1}'},
            {"stage": "LEVER_1_STARTED", "status": "STARTED", "detail_json": '{"bar": 2}'},
        ]
        assert _collect_all_preflight_detail(stages) == {}

    def test_detail_json_as_json_string(self):
        stages = [
            {
                "stage": "PREFLIGHT_STARTED",
                "status": "COMPLETE",
                "detail_json": '{"table_count": 3}',
            },
        ]
        result = _collect_all_preflight_detail(stages)
        assert result["table_count"] == 3

    def test_detail_json_as_dict(self):
        """Pandas sometimes returns already-parsed dicts."""
        stages = [
            {
                "stage": "PREFLIGHT_STARTED",
                "status": "COMPLETE",
                "detail_json": {"table_count": 3},
            },
        ]
        result = _collect_all_preflight_detail(stages)
        assert result["table_count"] == 3


# ── _build_step_summary for Step 1 ──────────────────────────────────


def _make_stage(stage_name: str, status: str, detail: dict | None = None) -> dict:
    row: dict = {"stage": stage_name, "status": status, "detail_json": None}
    if detail is not None:
        row["detail_json"] = json.dumps(detail)
    return row


class TestBuildStepSummaryStep1:
    def test_detail_has_counts(self):
        matching = [
            _make_stage("PREFLIGHT_STARTED", "STARTED"),
            _make_stage("PREFLIGHT_STARTED", "COMPLETE", {
                "table_count": 8, "instruction_count": 4, "benchmark_count": 20,
            }),
        ]
        summary = _build_step_summary(STEP1_DEF, matching, [], {}, stages_rows=matching)
        assert "8 tables" in summary
        assert "4 instructions" in summary
        assert "20 sample questions" in summary

    def test_fallback_to_metadata_table_ref_count(self):
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        all_stages = matching + [
            _make_stage("PREFLIGHT_METADATA_COLLECTION", "COMPLETE", {
                "table_ref_count": 6,
            }),
        ]
        summary = _build_step_summary(STEP1_DEF, matching, [], {}, stages_rows=all_stages)
        assert "6 tables" in summary

    def test_all_empty_shows_question_marks(self):
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        summary = _build_step_summary(STEP1_DEF, matching, [], {}, stages_rows=matching)
        assert "? tables" in summary
        assert "? instructions" in summary
        assert "? sample questions" in summary

    def test_no_matching_stages(self):
        assert _build_step_summary(STEP1_DEF, [], [], {}) is None


# ── _build_step_io for Step 1 ───────────────────────────────────────


class TestBuildStepIoStep1:
    def test_detail_counts_used(self):
        matching = [
            _make_stage("PREFLIGHT_STARTED", "STARTED"),
            _make_stage("PREFLIGHT_STARTED", "COMPLETE", {
                "table_count": 5, "instruction_count": 2, "benchmark_count": 12,
            }),
        ]
        _inputs, outputs = _build_step_io(
            STEP1_DEF, matching, [], {}, stages_rows=matching,
        )
        assert outputs["tableCount"] == 5
        assert outputs["instructionCount"] == 2
        assert outputs["sampleQuestionCount"] == 12

    def test_fallback_to_all_preflight_detail(self):
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        all_stages = matching + [
            _make_stage("PREFLIGHT_METADATA_COLLECTION", "COMPLETE", {
                "table_ref_count": 9,
            }),
        ]
        _inputs, outputs = _build_step_io(
            STEP1_DEF, matching, [], {}, stages_rows=all_stages,
        )
        assert outputs["tableCount"] == 9

    def test_fallback_to_config_snapshot(self):
        config_snapshot = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [{"identifier": "t1"}, {"identifier": "t2"}],
                    "functions": [{"identifier": "f1"}],
                },
                "instructions": {
                    "text_instructions": [
                        {"id": "i1", "content": ["Do X"]},
                    ],
                    "example_question_sqls": [
                        {"question": "Q1"},
                        {"question": "Q2"},
                    ],
                },
            }
        }
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        run_data = {"config_snapshot": config_snapshot}
        _inputs, outputs = _build_step_io(
            STEP1_DEF, matching, [], run_data, stages_rows=matching,
        )
        assert outputs["tableCount"] == 2
        assert outputs["functionCount"] == 1
        assert outputs["sampleQuestionCount"] == 2

    def test_config_snapshot_serialized_space_string(self):
        inner = {
            "data_sources": {
                "tables": [{"identifier": "t1"}, {"identifier": "t2"}, {"identifier": "t3"}],
                "functions": [],
            },
            "instructions": {"text_instructions": [], "example_question_sqls": []},
        }
        config_snapshot = {"serialized_space": json.dumps(inner)}
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        run_data = {"config_snapshot": config_snapshot}
        _inputs, outputs = _build_step_io(
            STEP1_DEF, matching, [], run_data, stages_rows=matching,
        )
        assert outputs["tableCount"] == 3

    def test_config_snapshot_as_json_string(self):
        """Config snapshot stored as a raw JSON string (not pre-parsed)."""
        snap = {
            "_parsed_space": {
                "data_sources": {"tables": [{"identifier": "t1"}], "functions": []},
                "instructions": {"text_instructions": [], "example_question_sqls": []},
            }
        }
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        run_data = {"config_snapshot": json.dumps(snap)}
        _inputs, outputs = _build_step_io(
            STEP1_DEF, matching, [], run_data, stages_rows=matching,
        )
        assert outputs["tableCount"] == 1

    def test_all_empty_returns_zeros(self):
        matching = [_make_stage("PREFLIGHT_STARTED", "STARTED")]
        _inputs, outputs = _build_step_io(
            STEP1_DEF, matching, [], {}, stages_rows=matching,
        )
        assert outputs["tableCount"] == 0
        assert outputs["functionCount"] == 0
        assert outputs["instructionCount"] == 0
        assert outputs["sampleQuestionCount"] == 0

    def test_no_matching_returns_none(self):
        inputs, outputs = _build_step_io(STEP1_DEF, [], [], {})
        assert inputs is None
        assert outputs is None
