"""Unit tests for ``scripts/migrate_expected_asset.py`` (C2)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def migrate_mod():
    script_path = (
        Path(__file__).resolve().parent.parent.parent
        / "scripts"
        / "migrate_expected_asset.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migrate_expected_asset", script_path
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def _force_scoring_v2_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)


def test_rewrites_stale_category_to_detected(migrate_mod) -> None:
    rows = [
        {
            "id": "q1",
            "expected_sql": "SELECT MEASURE(revenue) FROM mv_sales",
            "expected_asset": "TABLE",
        }
    ]
    new, diff = migrate_mod.migrate_expected_asset(rows)
    assert new[0]["expected_asset"] == "MV"
    assert diff[0]["action"] == "rewrite_expected_asset"


def test_no_change_when_detection_matches(migrate_mod) -> None:
    rows = [
        {
            "id": "q1",
            "expected_sql": "SELECT * FROM orders LIMIT 1",
            "expected_asset": "TABLE",
        }
    ]
    new, diff = migrate_mod.migrate_expected_asset(rows)
    assert diff == []
    assert new == rows


def test_table_name_goes_to_hint_field(migrate_mod) -> None:
    """Legacy rows that stored a *table name* in expected_asset should set
    the B2 ``expected_asset_hint`` instead of overwriting the name.
    """
    rows = [
        {
            "id": "q1",
            "expected_sql": "SELECT * FROM mv_customers LIMIT 10",
            # Author stored the name, not a category. Post-B1 this should
            # be classified as TABLE, and the hint should lock that in.
            "expected_asset": "mv_customers",
        }
    ]
    new, diff = migrate_mod.migrate_expected_asset(rows)
    assert new[0]["expected_asset"] == "mv_customers"  # name preserved
    assert new[0]["expected_asset_hint"] == "TABLE"
    assert diff[0]["action"] == "set_expected_asset_hint"


def test_missing_sql_is_noop(migrate_mod) -> None:
    rows = [{"id": "q1", "expected_asset": "TABLE"}]
    new, diff = migrate_mod.migrate_expected_asset(rows)
    assert diff == []
    assert new == rows


def test_mv_names_hint_from_space_config(migrate_mod) -> None:
    """Passing ``mv_names`` must be enough to reclassify mv_* tables as MV."""
    rows = [
        {
            "id": "q1",
            "expected_sql": "SELECT * FROM mv_customers",
            "expected_asset": "TABLE",
        }
    ]
    new, diff = migrate_mod.migrate_expected_asset(
        rows, mv_names=["mv_customers"]
    )
    assert new[0]["expected_asset"] == "MV"
    assert diff[0]["action"] == "rewrite_expected_asset"


def test_cli_dry_run_offline(tmp_path: Path, migrate_mod, capsys) -> None:
    src = tmp_path / "b.jsonl"
    src.write_text(
        json.dumps(
            {
                "id": "q1",
                "expected_sql": "SELECT MEASURE(rev) FROM mv_sales",
                "expected_asset": "TABLE",
            }
        ),
        encoding="utf-8",
    )

    rc = migrate_mod.main(["--jsonl", str(src)])
    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["rewrites"] == 1
    assert report["by_action"]["rewrite_expected_asset"] == 1

    # Dry-run must NOT mutate the source file.
    row = json.loads(src.read_text(encoding="utf-8"))
    assert row["expected_asset"] == "TABLE"


def test_cli_apply_offline_writes_changes(tmp_path: Path, migrate_mod) -> None:
    src = tmp_path / "b.jsonl"
    out = tmp_path / "diff.json"
    src.write_text(
        json.dumps(
            {
                "id": "q1",
                "expected_sql": "SELECT MEASURE(rev) FROM mv_sales",
                "expected_asset": "TABLE",
            }
        ),
        encoding="utf-8",
    )

    rc = migrate_mod.main(
        ["--jsonl", str(src), "--apply", "--output", str(out)]
    )
    assert rc == 0

    row = json.loads(src.read_text(encoding="utf-8"))
    assert row["expected_asset"] == "MV"

    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["rewrites"] == 1
