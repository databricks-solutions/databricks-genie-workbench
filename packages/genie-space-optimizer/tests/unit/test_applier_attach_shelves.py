"""The applier's attach-side contracts (MV-D118)."""

from __future__ import annotations

import copy
from unittest.mock import MagicMock

import pytest
from genie_space_optimizer.optimization import applier, mv_attach
from genie_space_optimizer.optimization.applier import _apply_action_to_config

_SENTINEL = "secret_literal"


def test_rollback_compensation_errors_carry_the_type_only(monkeypatch) -> None:
    calls = {"patch": 0}

    def patch_space_config(_w, _space_id, _config):
        calls["patch"] += 1
        if calls["patch"] > 1:
            raise RuntimeError(_SENTINEL)

    def update_space_description(*_a, **_k):
        raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(applier, "fetch_space_config", lambda *_a, **_k: {"description": "old"})
    monkeypatch.setattr(applier, "patch_space_config", patch_space_config)
    monkeypatch.setattr(applier, "update_space_description", update_space_description)

    result = applier.rollback(
        {"pre_snapshot": {"description": "restored", "data_sources": {}}},
        MagicMock(),
        "space-1",
    )

    assert result["status"] == "error"
    assert "serialized_space compensation failed: RuntimeError" in result["errors"]
    assert "description compensation failed: RuntimeError" in result["errors"]
    assert not [e for e in result["errors"] if _SENTINEL in e]


_VIEW = "main.sales.mv_revenue"


def _attach(identifier: str) -> dict:
    """The rendered action the phase's attach patch becomes (``command`` is a JSON string)."""
    return applier.render_patch(mv_attach._attach_patches([identifier])[0], "space-1", {})


@pytest.mark.parametrize("shelf", ["metric_views", "tables"])
@pytest.mark.parametrize("spelling", [_VIEW, _VIEW.upper()])
def test_an_attached_view_on_either_shelf_is_not_added_again(shelf, spelling) -> None:
    config = {"data_sources": {"tables": [], "metric_views": []}}
    config["data_sources"][shelf].append({"identifier": spelling})
    assert _apply_action_to_config(config, _attach(_VIEW)) is False
    listed = [
        e["identifier"].lower()
        for key in ("tables", "metric_views")
        for e in config["data_sources"][key]
    ]
    assert listed.count(_VIEW) == 1


def test_a_new_view_is_added_to_metric_views() -> None:
    config = {"data_sources": {"tables": [{"identifier": "main.sales.fact_orders"}], "metric_views": []}}
    assert _apply_action_to_config(config, _attach(_VIEW)) is True
    assert config["data_sources"]["metric_views"] == [{"identifier": _VIEW}]


def _remove(identifier: str) -> dict:
    return {"command": _attach(identifier)["rollback_command"]}


def test_remove_finds_the_view_in_any_case() -> None:
    config = {"data_sources": {"tables": [], "metric_views": [{"identifier": _VIEW.upper()}]}}
    assert _apply_action_to_config(config, _remove(_VIEW)) is True
    assert config["data_sources"]["metric_views"] == []


def test_remove_finds_a_view_genie_moved_to_tables() -> None:
    config = {
        "data_sources": {
            "tables": [{"identifier": "main.sales.fact_orders"}, {"identifier": _VIEW}],
            "metric_views": [],
        },
    }
    assert _apply_action_to_config(config, _remove(_VIEW)) is True
    assert config["data_sources"]["tables"] == [{"identifier": "main.sales.fact_orders"}]
    assert config["data_sources"]["metric_views"] == []


def test_remove_of_an_absent_view_changes_nothing() -> None:
    config = {
        "data_sources": {
            "tables": [{"identifier": "main.sales.fact_orders"}],
            "metric_views": [{"identifier": "main.sales.mv_other"}],
        },
    }
    before = copy.deepcopy(config)
    assert _apply_action_to_config(config, _remove(_VIEW)) is False
    assert config == before


def test_a_validation_failure_carries_an_empty_error_type(monkeypatch) -> None:
    """The validation-fail log has the PATCH-raised log's shape: it sent no PATCH."""
    from genie_space_optimizer.common import genie_schema

    monkeypatch.setattr(
        genie_schema, "validate_serialized_space", lambda *_a, **_k: (False, ["bad"]),
    )
    w = MagicMock()
    log = applier.apply_patch_set(
        w, "space-1", mv_attach._attach_patches([_VIEW]),
        {"data_sources": {"tables": [], "metric_views": []}},
        force_apply=True,
    )
    assert log["validation_errors"] == ["bad"]
    assert log["patch_deployed"] is False
    assert log["patch_error_type"] == ""
