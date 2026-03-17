"""Integration test: rollback — detect regression → rollback → skip lever."""

from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.applier import (
    apply_patch_set,
    rollback,
)
from genie_space_optimizer.optimization.optimizer import detect_regressions


@pytest.fixture
def applied_state(sample_metadata):
    """Apply a patch set and return (metadata, apply_log)."""
    patches = [
        {"type": "add_instruction", "new_text": "Filter by active.", "lever": 5},
        {"type": "add_description", "target": "orders", "new_text": "Revenue orders", "lever": 1},
    ]
    log = apply_patch_set(
        w=None,
        space_id="space-1",
        patches=patches,
        metadata_snapshot=sample_metadata,
    )
    return sample_metadata, log


class TestRegressionDetection:
    def test_detect_regression_after_apply(self):
        prev_scores = {"result_correctness": 82.0, "syntax_validity": 98.0}
        post_scores = {"result_correctness": 76.0, "syntax_validity": 97.0}
        regressions = detect_regressions(post_scores, prev_scores, threshold=2.0)
        assert len(regressions) == 1
        assert regressions[0]["judge"] == "result_correctness"
        assert regressions[0]["drop"] > 2.0

    def test_no_regression_within_tolerance(self):
        prev = {"result_correctness": 82.0}
        post = {"result_correctness": 81.5}
        regressions = detect_regressions(post, prev, threshold=2.0)
        assert regressions == []


class TestRollback:
    def test_rollback_restores_pre_snapshot(self, applied_state):
        metadata, log = applied_state
        assert log["pre_snapshot"] != log["post_snapshot"]

        result = rollback(log, w=None, space_id="space-1", metadata_snapshot=metadata)
        assert result["status"] == "SUCCESS"

    def test_rollback_without_pre_snapshot(self):
        result = rollback({}, w=None, space_id="space-1")
        assert result["status"] == "error"
        assert "No pre_snapshot" in result["errors"][0]

    def test_rollback_api_call(self, applied_state, mock_ws):
        """When ws is provided, rollback should call patch_space_config."""
        metadata, log = applied_state
        with MagicMock() as mock_patch:
            from genie_space_optimizer.optimization import applier
            original_fn = applier.patch_space_config
            applier.patch_space_config = mock_patch
            try:
                result = rollback(log, w=mock_ws, space_id="space-1")
                assert result["status"] == "SUCCESS"
                mock_patch.assert_called_once()
            finally:
                applier.patch_space_config = original_fn


class TestSkipLever:
    def test_regression_leads_to_lever_skip(self):
        """Simulate: apply lever → eval → detect regression → skip lever."""
        prev_scores = {"result_correctness": 85.0, "syntax_validity": 98.0}
        post_scores = {"result_correctness": 78.0, "syntax_validity": 97.0}

        regressions = detect_regressions(post_scores, prev_scores, threshold=2.0)
        assert len(regressions) > 0

        skipped_levers = []
        current_lever = 2
        if regressions:
            skipped_levers.append(current_lever)

        assert current_lever in skipped_levers


class TestRollbackMarkPatches:
    def test_mark_patches_rolled_back(self, mock_spark):
        from genie_space_optimizer.optimization.state import mark_patches_rolled_back

        mark_patches_rolled_back(
            mock_spark,
            run_id="run-1",
            iteration=2,
            reason="regression_detected",
            catalog="cat",
            schema="gold",
        )
        sql = mock_spark.sql.call_args[0][0]
        assert "rolled_back = true" in sql
        assert "regression_detected" in sql
