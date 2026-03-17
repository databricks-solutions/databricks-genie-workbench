"""Unit tests for deploy sub-step functions extracted from _run_deploy().

Guards against regressions when the monolithic _run_deploy() was split into
deploy_check() and deploy_execute().
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestDeployCheck:
    """Tests for deploy_check() — sub-step 5a."""

    def test_returns_should_deploy_true_when_target_set(self):
        from genie_space_optimizer.optimization.harness import deploy_check

        result = deploy_check("dabs://my-target", "mv-123", 3)
        assert result["should_deploy"] is True
        assert result["deploy_target"] == "dabs://my-target"
        assert result["prev_model_id"] == "mv-123"
        assert result["iteration_counter"] == 3

    def test_returns_should_deploy_false_when_target_none(self):
        from genie_space_optimizer.optimization.harness import deploy_check

        result = deploy_check(None, "mv-123", 0)
        assert result["should_deploy"] is False
        assert result["deploy_target"] is None

    def test_returns_should_deploy_false_when_target_empty(self):
        from genie_space_optimizer.optimization.harness import deploy_check

        result = deploy_check("", "mv-123", 0)
        assert result["should_deploy"] is False

    def test_prints_gate_check_block(self, capsys):
        from genie_space_optimizer.optimization.harness import deploy_check

        deploy_check("dabs://target", "mv-abc", 2)
        captured = capsys.readouterr()
        assert "DEPLOY" in captured.out
        assert "GATE CHECK" in captured.out
        assert "dabs://target" in captured.out
        assert "mv-abc" in captured.out

    def test_return_keys_are_stable(self):
        from genie_space_optimizer.optimization.harness import deploy_check

        result = deploy_check("target", "mv-1", 1)
        assert set(result.keys()) == {
            "should_deploy", "deploy_target", "prev_model_id", "iteration_counter",
        }


class TestDeployExecute:
    """Tests for deploy_execute() — sub-step 5b."""

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_skips_when_no_target(self, mock_write_stage):
        from genie_space_optimizer.optimization.harness import deploy_execute

        spark = MagicMock()
        w = MagicMock()
        result = deploy_execute(
            w, spark, "run-1", None, "space-1", "exp", "domain",
            "mv-1", 0, "cat", "gold",
        )
        assert result["status"] == "SKIPPED"
        assert result["reason"] == "no_deploy_target"
        mock_write_stage.assert_called_once()
        call_args = mock_write_stage.call_args
        assert call_args[0][2] == "DEPLOY_SKIPPED"

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_deploys_when_target_set(self, mock_write_stage):
        from genie_space_optimizer.optimization.harness import deploy_execute

        spark = MagicMock()
        w = MagicMock()
        result = deploy_execute(
            w, spark, "run-1", "dabs://target", "space-1", "exp", "domain",
            "mv-1", 3, "cat", "gold",
        )
        assert result["status"] == "PENDING_APPROVAL"
        assert result["deploy_target"] == "dabs://target"

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_prints_result_block(self, mock_write_stage, capsys):
        from genie_space_optimizer.optimization.harness import deploy_execute

        deploy_execute(
            MagicMock(), MagicMock(), "run-1", "target", "sp", "exp", "dom",
            "mv-1", 1, "cat", "gold",
        )
        captured = capsys.readouterr()
        assert "DEPLOY" in captured.out
        assert "RESULT" in captured.out

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_skip_prints_result_block(self, mock_write_stage, capsys):
        from genie_space_optimizer.optimization.harness import deploy_execute

        deploy_execute(
            MagicMock(), MagicMock(), "run-1", None, "sp", "exp", "dom",
            "mv-1", 0, "cat", "gold",
        )
        captured = capsys.readouterr()
        assert "SKIPPED" in captured.out


class TestDeployWrapperEquivalence:
    """Verify _run_deploy() calls both sub-steps and returns identical results."""

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_wrapper_skip_matches_direct_execute(self, mock_write_stage):
        from genie_space_optimizer.optimization.harness import _run_deploy, deploy_execute

        w, spark = MagicMock(), MagicMock()
        args = (w, spark, "run-1", None, "sp", "exp", "dom", "mv-1", 0, "cat", "gold")
        wrapper_result = _run_deploy(*args)
        mock_write_stage.reset_mock()
        direct_result = deploy_execute(*args)
        assert wrapper_result == direct_result

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_wrapper_deploy_matches_direct_execute(self, mock_write_stage):
        from genie_space_optimizer.optimization.harness import _run_deploy, deploy_execute

        w, spark = MagicMock(), MagicMock()
        args = (w, spark, "run-1", "target", "sp", "exp", "dom", "mv-1", 2, "cat", "gold")
        wrapper_result = _run_deploy(*args)
        mock_write_stage.reset_mock()
        direct_result = deploy_execute(*args)
        assert wrapper_result == direct_result
