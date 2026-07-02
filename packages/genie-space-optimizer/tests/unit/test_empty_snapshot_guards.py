"""Regression guards for stale/empty Genie Space snapshots."""

from __future__ import annotations

from unittest.mock import MagicMock


def _snapshot(identifier: str | None) -> dict:
    tables = [{"identifier": identifier, "column_configs": []}] if identifier else []
    return {
        "_parsed_space": {
            "version": 2,
            "data_sources": {
                "tables": tables,
                "metric_views": [],
                "functions": [],
            },
            "config": {"sample_questions": []},
            "instructions": {"text_instructions": []},
        },
    }


def _patch_prepare_deps(monkeypatch, fetch_mock: MagicMock) -> None:
    from genie_space_optimizer.optimization import harness as _harness

    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        fetch_mock,
    )

    def extract_refs(config: dict) -> list[tuple[str, str, str]]:
        parsed = config.get("_parsed_space", config)
        data_sources = parsed.get("data_sources", {}) if isinstance(parsed, dict) else {}
        refs: list[tuple[str, str, str]] = []
        for table in data_sources.get("tables", []) or []:
            identifier = table.get("identifier", "")
            parts = identifier.split(".")
            if len(parts) == 3:
                refs.append((parts[0], parts[1], parts[2]))
        return refs

    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs",
        extract_refs,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_columns_for_tables_rest",
        lambda w, refs: [{"table_full_name": ".".join(refs[0]), "name": "id"}] if refs else [],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.metric_view_catalog."
        "detect_metric_views_via_catalog_with_outcomes",
        lambda *a, **k: (set(), {}, {}),
    )
    monkeypatch.setattr(_harness, "ENABLE_PROMPT_MATCHING_AUTO_APPLY", False)
    monkeypatch.setattr(
        "genie_space_optimizer.iq_scan.collect_rls_audit",
        lambda *a, **k: {},
    )


def test_prepare_lever_loop_refetches_empty_cached_snapshot(monkeypatch) -> None:
    from genie_space_optimizer.optimization import harness as _harness

    empty_cached = _snapshot(None)
    live_populated = _snapshot("cat.sch.live_table")
    fetch_mock = MagicMock(name="fetch_space_config", return_value=live_populated)

    monkeypatch.setattr(
        _harness,
        "load_run",
        lambda *a, **k: {"config_snapshot": empty_cached},
    )
    _patch_prepare_deps(monkeypatch, fetch_mock)

    config = _harness._prepare_lever_loop(
        w=MagicMock(),
        spark=MagicMock(),
        run_id="run-empty",
        space_id="space-1",
        catalog="cat",
        schema="sch",
    )

    fetch_mock.assert_called_once()
    tables = config["_parsed_space"]["data_sources"]["tables"]
    assert [t["identifier"] for t in tables] == ["cat.sch.live_table"]
    assert config["_uc_columns"], "UC columns should be populated from live refs"


def test_prepare_lever_loop_uses_non_empty_cached_snapshot_as_is(monkeypatch) -> None:
    from genie_space_optimizer.optimization import harness as _harness

    cached_populated = _snapshot("cat.sch.cached_table")
    fetch_mock = MagicMock(name="fetch_space_config", return_value=_snapshot("cat.sch.live_table"))

    monkeypatch.setattr(
        _harness,
        "load_run",
        lambda *a, **k: {"config_snapshot": cached_populated},
    )
    _patch_prepare_deps(monkeypatch, fetch_mock)

    config = _harness._prepare_lever_loop(
        w=MagicMock(),
        spark=MagicMock(),
        run_id="run-cached",
        space_id="space-1",
        catalog="cat",
        schema="sch",
    )

    fetch_mock.assert_not_called()
    tables = config["_parsed_space"]["data_sources"]["tables"]
    assert [t["identifier"] for t in tables] == ["cat.sch.cached_table"]
