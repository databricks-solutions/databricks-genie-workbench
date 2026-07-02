from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import harness


def _config(table_name: str | None, *, metric_cache: bool = True) -> dict:
    tables = (
        [{"identifier": f"cat.sch.{table_name}", "column_configs": []}]
        if table_name
        else []
    )
    cfg = {
        "_parsed_space": {
            "version": 2,
            "data_sources": {
                "tables": tables,
                "metric_views": [],
                "functions": [],
            },
            "instructions": {"text_instructions": []},
        },
    }
    if metric_cache:
        cfg["_metric_view_yaml"] = {"cached": {}}
    return cfg


def _stub_prepare_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(harness, "ENABLE_PROMPT_MATCHING_AUTO_APPLY", False)
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs",
        lambda config: [
            ("cat", "sch", t["identifier"].split(".")[-1])
            for t in (
                config.get("_parsed_space", {})
                .get("data_sources", {})
                .get("tables", [])
            )
        ],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_columns_for_tables_rest",
        lambda _w, refs: [{"table": ".".join(ref)} for ref in refs],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.asset_semantics.build_and_stamp_from_run",
        lambda *a, **k: {},
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.asset_semantics.format_semantics_block",
        lambda _semantics: [],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.iq_scan.collect_rls_audit",
        lambda *a, **k: {},
    )


def test_prepare_lever_loop_refetches_empty_cached_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_snapshot = _config(None)
    live_config = _config("orders")
    fetch = MagicMock(return_value=live_config)

    _stub_prepare_dependencies(monkeypatch)
    monkeypatch.setattr(
        harness,
        "load_run",
        lambda *a, **k: {"config_snapshot": empty_snapshot},
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        fetch,
    )

    config = harness._prepare_lever_loop(
        w=MagicMock(),
        spark=MagicMock(),
        run_id="run-empty-snapshot",
        space_id="space-1",
        catalog="cat",
        schema="sch",
    )

    fetch.assert_called_once()
    assert config is live_config
    assert (
        config["_parsed_space"]["data_sources"]["tables"][0]["identifier"]
        == "cat.sch.orders"
    )
    assert config["_uc_columns"] == [{"table": "cat.sch.orders"}]


def test_prepare_lever_loop_uses_non_empty_cached_snapshot_without_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cached_snapshot = _config("cached_orders")
    fetch = MagicMock(side_effect=AssertionError("should not refetch"))

    _stub_prepare_dependencies(monkeypatch)
    monkeypatch.setattr(
        harness,
        "load_run",
        lambda *a, **k: {"config_snapshot": cached_snapshot},
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        fetch,
    )

    config = harness._prepare_lever_loop(
        w=MagicMock(),
        spark=MagicMock(),
        run_id="run-cached-snapshot",
        space_id="space-1",
        catalog="cat",
        schema="sch",
    )

    fetch.assert_not_called()
    assert config is cached_snapshot
    assert (
        config["_parsed_space"]["data_sources"]["tables"][0]["identifier"]
        == "cat.sch.cached_orders"
    )
    assert config["_uc_columns"] == [{"table": "cat.sch.cached_orders"}]
