from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.integration import discard
from genie_space_optimizer.integration.config import IntegrationConfig


def _config() -> IntegrationConfig:
    return IntegrationConfig(
        catalog="main",
        schema_name="gso_test",
        warehouse_id="wh-test",
        job_id=12345,
    )


def test_discard_rejects_selected_active_run_case_insensitively(monkeypatch) -> None:
    monkeypatch.setattr(
        discard,
        "wh_load_run",
        lambda *_args, **_kwargs: {
            "run_id": "run-1",
            "space_id": "space-1",
            "status": "running",
        },
    )

    with pytest.raises(ValueError, match="still in progress.*RUNNING"):
        discard.discard_optimization(
            "run-1", MagicMock(), MagicMock(), _config(),
        )


def test_discard_rejects_newer_active_run_for_same_space(monkeypatch) -> None:
    monkeypatch.setattr(
        discard,
        "wh_load_run",
        lambda *_args, **_kwargs: {
            "run_id": "run-old",
            "space_id": "space-1",
            "status": "CONVERGED",
            "config_snapshot": {"serialized_space": {"version": 2}},
        },
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.user_can_edit_space",
        lambda *_args, **_kwargs: True,
    )
    guard = MagicMock(
        side_effect=ValueError(
            "Cannot discard while an optimization is active for this Genie Space."
        )
    )
    monkeypatch.setattr(discard, "_assert_no_active_space_runs", guard)

    with pytest.raises(ValueError, match="Cannot discard while"):
        discard.discard_optimization(
            "run-old", MagicMock(), MagicMock(), _config(),
        )

    guard.assert_called_once()
    assert guard.call_args.kwargs["space_id"] == "space-1"
    assert guard.call_args.kwargs["action"] == "discard"
