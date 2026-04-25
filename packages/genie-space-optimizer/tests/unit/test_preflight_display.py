"""Display-only tests for preflight summary blocks.

These tests lock the rendering of:
  * UC METADATA COLLECTION SUMMARY  — the tables/metric_views/functions split
  * DATA PROFILE                    — row-count formatting + coverage line

They are intentionally tight on substrings rather than whole-output so the
rest of the summary can evolve without breaking display contracts.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------

def _stub_patches():
    """Return the list of ``@patch`` decorators we always need.

    ``preflight_collect_uc_metadata`` orchestrates many side-effecting helpers
    (UC REST calls, stage writes, join overlap computation). These tests only
    care about stdout, so everything external gets stubbed.
    """
    return [
        patch(
            "genie_space_optimizer.optimization.preflight._compute_join_overlaps",
            return_value=[],
        ),
        patch(
            "genie_space_optimizer.optimization.preflight._validate_core_access",
        ),
        patch(
            "genie_space_optimizer.optimization.preflight.write_stage",
        ),
        patch(
            "genie_space_optimizer.optimization.preflight._update_run_status",
        ),
    ]


def _run_preflight(
    config: dict,
    *,
    genie_table_refs=None,
    collect_or_empty_rows=None,
    data_profile=None,
    catalog_mvs=None,
):
    """Invoke ``preflight_collect_uc_metadata`` with all externals stubbed.

    ``collect_or_empty_rows`` seeds the Spark-collect path with a canned
    ``(rows, error)`` tuple. ``data_profile`` seeds the profile returned by
    ``_collect_data_profile``. ``catalog_mvs`` seeds the catalog detector
    return value as ``(detected_set, yamls_dict)``. Each can be ``None``
    to use sensible empties.
    """
    from genie_space_optimizer.optimization import preflight

    rows_stub = collect_or_empty_rows or ([], None)
    catalog_stub = catalog_mvs if catalog_mvs is not None else (set(), {})

    with (
        _stub_patches()[0],
        _stub_patches()[1],
        _stub_patches()[2],
        _stub_patches()[3],
        patch.object(preflight, "_collect_or_empty", return_value=rows_stub),
        patch.object(
            preflight, "_collect_data_profile",
            return_value=data_profile or {},
        ),
        patch.object(
            preflight, "_detect_metric_views_via_catalog",
            return_value=catalog_stub,
        ),
        patch.object(
            preflight, "get_columns_for_tables_rest", return_value=[],
        ),
        patch.object(
            preflight, "get_tags_for_tables_rest", return_value=[],
        ),
        patch.object(
            preflight, "get_routines_for_schemas_rest", return_value=[],
        ),
        patch.object(
            preflight, "get_foreign_keys_for_tables_rest", return_value=[],
        ),
    ):
        preflight.preflight_collect_uc_metadata(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="run-test",
            catalog="cat",
            schema="sch",
            config=config,
            snapshot={},
            genie_table_refs=genie_table_refs or [],
        )


def _config_with_split(n_tables: int, n_mvs: int, n_funcs: int) -> dict:
    """Build a config dict with the given split of UC refs."""
    return {
        "_tables": [f"cat.sch.t{i}" for i in range(n_tables)],
        "_metric_views": [f"cat.sch.mv{i}" for i in range(n_mvs)],
        "_functions": [f"cat.sch.f{i}" for i in range(n_funcs)],
        "_parsed_space": {},
    }


def _uc_column_rows(table_names: list[str]) -> list[dict]:
    """Synthesize one column row per table so the summary has non-zero UC columns."""
    return [
        {
            "catalog_name": "cat",
            "schema_name": "sch",
            "table_name": name.split(".")[-1],
            "column_name": "c0",
            "data_type": "string",
        }
        for name in table_names
    ]


# ---------------------------------------------------------------------------
# Task 1: UC-refs split line
# ---------------------------------------------------------------------------

class TestUcRefsSplit:
    def test_summary_includes_refs_split_line(self, capsys):
        """The UC Metadata summary shows tables/metric_views/functions breakdown."""
        config = _config_with_split(n_tables=4, n_mvs=2, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(
                _uc_column_rows(config["_tables"] + config["_metric_views"]),
                None,
            ),
        )

        out = capsys.readouterr().out
        assert "UC Refs split" in out
        assert "tables=4" in out
        assert "metric_views=2" in out
        assert "functions=0" in out
        assert "total 6" in out

    def test_summary_handles_zero_refs_gracefully(self, capsys):
        """Empty configs still render a split line (all zeros, total 0)."""
        _run_preflight(_config_with_split(0, 0, 0))
        out = capsys.readouterr().out
        assert "UC Refs split" in out
        assert "tables=0" in out
        assert "metric_views=0" in out
        assert "functions=0" in out
        assert "total 0" in out

    def test_summary_with_functions_present(self, capsys):
        """Functions contribute to the split independent of tables/MVs."""
        _run_preflight(_config_with_split(n_tables=2, n_mvs=0, n_funcs=3))
        out = capsys.readouterr().out
        assert "tables=2" in out
        assert "metric_views=0" in out
        assert "functions=3" in out
        assert "total 5" in out


# ---------------------------------------------------------------------------
# Task 1 (continued): Data Profile coverage line
# ---------------------------------------------------------------------------

class TestDataProfileCoverage:
    def test_banner_reports_profiled_vs_refs_coverage(self, capsys):
        """The DATA PROFILE banner reconciles profiled count to total UC refs."""
        config = _config_with_split(n_tables=1, n_mvs=2, n_funcs=0)
        table_names = config["_tables"] + config["_metric_views"]
        _run_preflight(
            config,
            collect_or_empty_rows=(_uc_column_rows(table_names), None),
            data_profile={
                "cat.sch.t0": {"row_count": 28, "columns": {}},
            },
        )

        out = capsys.readouterr().out
        assert "PREFLIGHT — DATA PROFILE" in out
        assert "Coverage:" in out
        assert "Profiled 1 of 3 UC refs" in out
        assert "metric_views skipped: 2" in out
        assert "functions excluded: 0" in out

    def test_banner_omits_coverage_line_when_nothing_to_profile(self, capsys):
        """If table_names is empty, the DATA PROFILE block is skipped entirely."""
        config = _config_with_split(n_tables=0, n_mvs=0, n_funcs=1)
        _run_preflight(config)

        out = capsys.readouterr().out
        assert "PREFLIGHT — DATA PROFILE" not in out
        assert "Coverage:" not in out


# ---------------------------------------------------------------------------
# Task 2: Accurate row counts (no '~' prefix, guard row_count=-1)
# ---------------------------------------------------------------------------

class TestDataProfileRowCounts:
    def test_renders_exact_row_count_without_tilde(self, capsys):
        """row_count comes from an exact COUNT(*), not a sample — no '~' prefix."""
        config = _config_with_split(n_tables=1, n_mvs=0, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(_uc_column_rows(config["_tables"]), None),
            data_profile={
                "cat.sch.t0": {"row_count": 28, "columns": {}},
            },
        )

        out = capsys.readouterr().out
        assert "(28 rows)" in out
        assert "(~28 rows)" not in out

    def test_handles_row_count_failure_gracefully(self, capsys):
        """row_count=-1 is the failure sentinel; render 'row count unavailable'."""
        config = _config_with_split(n_tables=1, n_mvs=0, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(_uc_column_rows(config["_tables"]), None),
            data_profile={
                "cat.sch.t0": {"row_count": -1, "columns": {}},
            },
        )

        out = capsys.readouterr().out
        assert "(row count unavailable)" in out
        assert "~-1" not in out
        assert "(-1 rows)" not in out

    def test_handles_missing_row_count_key(self, capsys):
        """If a profile entry omits row_count, we render it as unavailable too."""
        config = _config_with_split(n_tables=1, n_mvs=0, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(_uc_column_rows(config["_tables"]), None),
            data_profile={
                "cat.sch.t0": {"columns": {}},
            },
        )

        out = capsys.readouterr().out
        assert "(row count unavailable)" in out
        assert "~?" not in out


# ---------------------------------------------------------------------------
# C3: Catalog-reclassification parenthetical
# ---------------------------------------------------------------------------

class TestCatalogReclassificationDisplay:
    def test_split_reflects_catalog_reclassification(self, capsys):
        """When the catalog detector finds an MV that Genie listed as a table,
        the UC Refs split shows the *effective* counts, not the raw config
        counts, and appends a reclassification parenthetical.
        """
        config = _config_with_split(n_tables=4, n_mvs=0, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(_uc_column_rows(config["_tables"]), None),
            catalog_mvs=({"cat.sch.t1"}, {"cat.sch.t1": {"source": "x"}}),
        )

        out = capsys.readouterr().out
        assert "UC Refs split" in out
        assert "tables=3" in out
        assert "metric_views=1" in out
        assert "functions=0" in out
        assert "total 4" in out
        assert "reclassified" in out.lower()

    def test_no_parenthetical_when_no_reclassification(self, capsys):
        """No catalog reclassifications -> no parenthetical in the split line."""
        config = _config_with_split(n_tables=4, n_mvs=2, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(
                _uc_column_rows(config["_tables"] + config["_metric_views"]),
                None,
            ),
            catalog_mvs=(set(), {}),
        )

        out = capsys.readouterr().out
        assert "UC Refs split" in out
        for line in out.splitlines():
            if "UC Refs split" in line:
                assert "reclassified" not in line.lower()
                break
        else:
            pytest.fail("UC Refs split line not found in output")

    def test_coverage_uses_effective_mv_count(self, capsys):
        """Coverage line counts catalog-detected MVs in the skipped tally."""
        config = _config_with_split(n_tables=1, n_mvs=0, n_funcs=0)
        _run_preflight(
            config,
            collect_or_empty_rows=(_uc_column_rows(config["_tables"]), None),
            catalog_mvs=({"cat.sch.t0"}, {"cat.sch.t0": {"source": "x"}}),
            data_profile={},
        )

        out = capsys.readouterr().out
        assert "Coverage:" in out
        assert "metric_views skipped: 1" in out
