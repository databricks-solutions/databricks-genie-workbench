"""Unit tests for the simplified bundle-managed job launcher."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.backend.job_launcher import (
    _build_idempotency_token,
    check_job_health,
    ensure_job_run_as,
    get_job_url,
    submit_optimization,
)


class TestBuildIdempotencyToken:
    def test_deterministic(self):
        t1 = _build_idempotency_token(run_id="r1", space_id="s1", triggered_by="u1")
        t2 = _build_idempotency_token(run_id="r1", space_id="s1", triggered_by="u1")
        assert t1 == t2

    def test_prefix(self):
        token = _build_idempotency_token(run_id="r", space_id="s", triggered_by="u")
        assert token.startswith("gso-")

    def test_max_length(self):
        token = _build_idempotency_token(
            run_id="x" * 100, space_id="y" * 100, triggered_by="z" * 100,
        )
        assert len(token) <= 64

    def test_different_inputs_different_tokens(self):
        t1 = _build_idempotency_token(run_id="r1", space_id="s1", triggered_by="u1")
        t2 = _build_idempotency_token(run_id="r2", space_id="s1", triggered_by="u1")
        assert t1 != t2


class TestSubmitOptimization:
    def test_calls_run_now_with_job_id(self):
        ws = MagicMock()
        waiter = MagicMock()
        waiter.run_id = 42
        ws.jobs.run_now.return_value = waiter

        job_run_id, returned_job_id = submit_optimization(
            ws,
            job_id=123,
            run_id="run-abc",
            space_id="space-1",
            domain="default",
            catalog="main",
            schema="genie_optimization",
        )

        ws.jobs.run_now.assert_called_once()
        call_kwargs = ws.jobs.run_now.call_args
        assert call_kwargs.kwargs["job_id"] == 123
        assert call_kwargs.kwargs["job_parameters"]["run_id"] == "run-abc"
        assert call_kwargs.kwargs["job_parameters"]["space_id"] == "space-1"
        assert job_run_id == "42"
        assert returned_job_id == 123

    def test_passes_all_parameters(self):
        ws = MagicMock()
        ws.jobs.run_now.return_value = MagicMock(run_id=1)

        submit_optimization(
            ws,
            job_id=10,
            run_id="r",
            space_id="s",
            domain="d",
            catalog="c",
            schema="sch",
            apply_mode="both",
            levers="[1,2]",
            max_iterations="3",
            triggered_by="user@test",
            experiment_name="exp",
            deploy_target="tgt",
        )

        params = ws.jobs.run_now.call_args.kwargs["job_parameters"]
        assert params["apply_mode"] == "both"
        assert params["levers"] == "[1,2]"
        assert params["max_iterations"] == "3"
        assert params["triggered_by"] == "user@test"
        assert params["experiment_name"] == "exp"
        assert params["deploy_target"] == "tgt"


class TestGetJobUrl:
    def test_with_explicit_job_id(self):
        ws = MagicMock()
        ws.config.host = "https://myworkspace.cloud.databricks.com"
        url = get_job_url(ws, job_id=456)
        assert url == "https://myworkspace.cloud.databricks.com/jobs/456"

    def test_with_no_job_id_falls_back_to_name_lookup(self):
        ws = MagicMock()
        ws.config.host = "https://myworkspace.cloud.databricks.com"
        job = MagicMock()
        job.job_id = 789
        job.settings = MagicMock()
        job.settings.tags = {"managed-by": "databricks-bundle"}
        ws.jobs.list.return_value = [job]

        url = get_job_url(ws, job_id=None)
        assert url == "https://myworkspace.cloud.databricks.com/jobs/789"

    def test_returns_none_when_no_host(self):
        ws = MagicMock()
        ws.config.host = ""
        assert get_job_url(ws, job_id=123) is None

    def test_returns_none_when_no_job_found(self):
        ws = MagicMock()
        ws.config.host = "https://example.com"
        ws.jobs.list.return_value = []
        assert get_job_url(ws, job_id=None) is None


class TestCheckJobHealth:
    def test_healthy_with_matching_run_as(self):
        ws = MagicMock()
        detail = MagicMock()
        detail.run_as_user_name = "sp-abc123"
        ws.jobs.get.return_value = detail

        ok, msg = check_job_health(ws, "sp-abc123", job_id=100)
        assert ok is True
        assert msg == ""

    def test_unhealthy_with_mismatched_run_as(self):
        ws = MagicMock()
        detail = MagicMock()
        detail.run_as_user_name = "old-sp-xyz"
        ws.jobs.get.return_value = detail

        ok, msg = check_job_health(ws, "sp-abc123", job_id=100)
        assert ok is False
        assert "run_as" in msg

    def test_healthy_when_no_sp(self):
        ws = MagicMock()
        detail = MagicMock()
        detail.run_as_user_name = "some-sp"
        ws.jobs.get.return_value = detail

        ok, msg = check_job_health(ws, "", job_id=100)
        assert ok is True

    def test_fallback_to_name_lookup_when_no_job_id(self):
        ws = MagicMock()
        job = MagicMock()
        job.job_id = 42
        job.settings = MagicMock()
        job.settings.tags = {"managed-by": "databricks-bundle"}
        detail = MagicMock()
        detail.run_as_user_name = "sp-1"
        ws.jobs.list.return_value = [job]
        ws.jobs.get.return_value = detail

        ok, msg = check_job_health(ws, "sp-1", job_id=None)
        assert ok is True

    def test_not_found_when_no_job_id_and_no_results(self):
        ws = MagicMock()
        ws.jobs.list.return_value = []

        ok, msg = check_job_health(ws, "sp-1", job_id=None)
        assert ok is False
        assert "not found" in msg.lower()

    def test_tolerates_exceptions(self):
        ws = MagicMock()
        ws.jobs.get.side_effect = RuntimeError("API error")

        ok, msg = check_job_health(ws, "sp-1", job_id=100)
        assert ok is True


class TestEnsureJobRunAs:
    def test_no_op_when_already_correct(self):
        ws = MagicMock()
        detail = MagicMock()
        detail.run_as_user_name = "sp-abc123"
        ws.jobs.get.return_value = detail

        ensure_job_run_as(ws, job_id=100, sp_client_id="sp-abc123")
        ws.jobs.update.assert_not_called()

    def test_updates_when_mismatched(self):
        ws = MagicMock()
        detail = MagicMock()
        detail.run_as_user_name = "old-sp"
        ws.jobs.get.return_value = detail

        ensure_job_run_as(ws, job_id=100, sp_client_id="sp-new")
        ws.jobs.update.assert_called_once()
        call_kwargs = ws.jobs.update.call_args.kwargs
        assert call_kwargs["job_id"] == 100

    def test_updates_when_empty_run_as(self):
        ws = MagicMock()
        detail = MagicMock()
        detail.run_as_user_name = ""
        ws.jobs.get.return_value = detail

        ensure_job_run_as(ws, job_id=100, sp_client_id="sp-new")
        ws.jobs.update.assert_called_once()

    def test_tolerates_api_errors(self):
        ws = MagicMock()
        ws.jobs.get.side_effect = RuntimeError("API error")

        ensure_job_run_as(ws, job_id=100, sp_client_id="sp-1")


class TestAppConfigJobId:
    def test_job_id_defaults_to_none(self):
        from genie_space_optimizer.backend.core._config import AppConfig
        config = AppConfig()
        assert config.job_id is None

    def test_job_id_from_env(self):
        from genie_space_optimizer.backend.core._config import AppConfig
        with patch.dict("os.environ", {"GENIE_SPACE_OPTIMIZER_JOB_ID": "42"}):
            config = AppConfig()
            assert config.job_id == 42
