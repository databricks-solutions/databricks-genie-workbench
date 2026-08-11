"""Hidden-column (`exclude: true` / legacy `visible: false`) honoring.

A Genie Agent hides a column from the model via ``exclude: true`` (modern
``serialized_space``) or ``visible: false`` (legacy). The optimizer must keep
hidden columns out of:

* the wide-schema selection plan's activatable working set, and
* every benchmark-generation LLM prompt allowlist and the deterministic
  metadata-validation allowlist.

These tests pin that contract across ``wide_schema.build_inventory``,
``wide_schema.build_selection_plan``, ``benchmarking._filter_uc_columns_to_space_assets``,
``benchmarking._build_schema_contexts``, and
``benchmarking._build_metadata_allowlist`` / ``_enforce_metadata_constraints``.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.benchmarking import (
    _build_metadata_allowlist,
    _build_schema_contexts,
    _enforce_metadata_constraints,
    _filter_uc_columns_to_space_assets,
)
from genie_space_optimizer.optimization.wide_schema import (
    active_column_keys,
    build_inventory,
    build_local_evidence,
    build_selection_plan,
    revise_plan_for_column,
)
from genie_space_optimizer.common.column_visibility import is_column_hidden


# ── shared fixtures ────────────────────────────────────────────────

_TABLE = ("cat", "sch0", "table0")
_TABLE_ID = "cat.sch0.table0"


def _config_with_columns(column_configs: list[dict]) -> dict:
    """Config with one table carrying the given column configs.

    Sets both ``_tables`` (the precomputed asset list the benchmark path's
    asset filter reads) and ``_parsed_space.data_sources.tables`` (where
    ``_hidden_column_keys`` reads column configs).
    """
    return {
        "_tables": [_TABLE_ID],
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": _TABLE_ID,
                        "column_configs": column_configs,
                    }
                ],
            },
        },
    }


def _uc_rows(columns: list[tuple[str, str, bool]]) -> list[dict]:
    """Build UC column rows. ``columns`` is (name, dtype, is_primary_key)."""
    rows = []
    for ordinal, (name, dtype, is_pk) in enumerate(columns):
        row = {
            "catalog_name": _TABLE[0],
            "schema_name": _TABLE[1],
            "table_name": _TABLE[2],
            "column_name": name,
            "data_type": dtype,
            "comment": f"description for {name}",
            "ordinal_position": ordinal + 1,
            "table_type": "MANAGED",
        }
        if is_pk:
            row["is_primary_key"] = True
        rows.append(row)
    return rows


# ── is_column_hidden (shared helper) ──────────────────────────────


class TestIsColumnHidden:
    def test_exclude_true_is_hidden(self):
        assert is_column_hidden({"column_name": "x", "exclude": True})

    def test_visible_false_is_hidden(self):
        assert is_column_hidden({"column_name": "x", "visible": False})

    def test_visible_column_is_not_hidden(self):
        assert not is_column_hidden({"column_name": "x", "exclude": False})
        assert not is_column_hidden({"column_name": "x"})
        assert not is_column_hidden({"column_name": "x", "visible": True})

    def test_none_config_is_not_hidden(self):
        assert not is_column_hidden(None)
        assert not is_column_hidden({})


# ── build_inventory ───────────────────────────────────────────────


class TestBuildInventoryExcludedFlag:
    def test_inventory_marks_excluded_and_visible_false_columns(self):
        config = _config_with_columns([
            {"column_name": "secret", "exclude": True},
            {"column_name": "legacy_hidden", "visible": False},
            {"column_name": "visible_col"},
        ])
        rows = _uc_rows([
            ("secret", "STRING", False),
            ("legacy_hidden", "STRING", False),
            ("visible_col", "STRING", False),
        ])
        inventory = build_inventory(rows, config, captured_at="2026-01-01T00:00:00Z")

        cols = {c["name"]: c for c in inventory["assets"][0]["columns"]}
        assert cols["secret"]["excluded"] is True
        assert cols["legacy_hidden"]["excluded"] is True
        assert cols["visible_col"]["excluded"] is False


# ── build_selection_plan ───────────────────────────────────────────


class TestBuildSelectionPlanHonorsHiddenColumns:
    def test_hidden_column_is_never_active_even_when_hard_required(self):
        # `secret` is hidden AND a primary key (JOIN_KEY = hard-required).
        # It must NOT consume an active slot; a visible peer must take it.
        col_configs = [{"column_name": "secret", "exclude": True}] + [
            {"column_name": f"col_{i:04d}"} for i in range(60)
        ]
        config = _config_with_columns(col_configs)
        rows = [("secret", "STRING", True)] + [
            (f"col_{i:04d}", "STRING", False) for i in range(60)
        ]
        inventory = build_inventory(_uc_rows(rows), config, captured_at="t")
        evidence = build_local_evidence(config, inventory)
        plan = build_selection_plan(inventory, evidence, run_id="run-hidden")

        plan_cols = {r["name"]: r for r in plan["assets"][0]["columns"]}

        # The hidden column is ranked but never activated, and is auditable.
        secret = plan_cols["secret"]
        assert secret["excluded"] is True
        assert secret["active"] is False
        assert secret["eviction_reason"] == "hidden_column"

        # It did not consume an active slot even though JOIN_KEY is
        # HARD_REQUIRED — the active set excludes it entirely.
        active_keys = active_column_keys(plan)
        assert ("cat", "sch0", "table0", "secret") not in active_keys

        # A visible peer still got activated (the slot was not wasted).
        assert plan["assets"][0]["active_count"] > 0
        assert any(
            r["active"] for r in plan["assets"][0]["columns"]
            if r["name"] != "secret"
        )

    def test_no_hidden_columns_behaves_unchanged(self):
        col_configs = [{"column_name": f"col_{i:04d}"} for i in range(60)]
        config = _config_with_columns(col_configs)
        rows = [(f"col_{i:04d}", "STRING", False) for i in range(60)]
        inventory = build_inventory(_uc_rows(rows), config, captured_at="t")
        evidence = build_local_evidence(config, inventory)
        plan = build_selection_plan(inventory, evidence, run_id="run-no-hidden")

        # Sanity: no eviction_reasons are "hidden_column" and the plan validates.
        assert all(
            r["eviction_reason"] is None
            for r in plan["assets"][0]["columns"]
        )
        assert plan["assets"][0]["active_count"] > 0


# ── _filter_uc_columns_to_space_assets ────────────────────────────


class TestFilterUcColumnsDropsHidden:
    def test_hidden_columns_are_dropped_visible_kept(self):
        config = _config_with_columns([
            {"column_name": "secret", "exclude": True},
            {"column_name": "legacy_hidden", "visible": False},
            {"column_name": "visible_col"},
        ])
        uc = _uc_rows([
            ("secret", "STRING", False),
            ("legacy_hidden", "STRING", False),
            ("visible_col", "STRING", False),
        ])
        scoped = _filter_uc_columns_to_space_assets(config, uc)

        names = {c["column_name"] for c in scoped}
        assert "visible_col" in names
        assert "secret" not in names
        assert "legacy_hidden" not in names

    def test_no_hidden_columns_keeps_all(self):
        config = _config_with_columns([{"column_name": "a"}, {"column_name": "b"}])
        uc = _uc_rows([("a", "STRING", False), ("b", "STRING", False)])
        scoped = _filter_uc_columns_to_space_assets(config, uc)
        assert {c["column_name"] for c in scoped} == {"a", "b"}

    def test_hidden_column_in_unrelated_table_is_kept(self):
        # A hidden column in table0 must not drop a same-named column in table1.
        config = _config_with_columns([{"column_name": "secret", "exclude": True}])
        config["_tables"] = [_TABLE_ID, "cat.sch0.table1"]
        uc = _uc_rows([("secret", "STRING", False)]) + [
            {
                "catalog_name": "cat",
                "schema_name": "sch0",
                "table_name": "table1",
                "column_name": "secret",
                "data_type": "STRING",
                "comment": "different table, same name",
                "ordinal_position": 1,
                "table_type": "MANAGED",
            }
        ]
        scoped = _filter_uc_columns_to_space_assets(config, uc)
        tables = {c["table_name"] for c in scoped}
        # table0.secret is dropped (hidden), table1.secret is kept.
        assert "table1" in tables
        assert "table0" not in tables


# ── _build_schema_contexts (prompt allowlist) ──────────────────────


class TestSchemaContextsColumnAllowlist:
    def test_column_allowlist_excludes_hidden_columns(self):
        config = _config_with_columns([
            {"column_name": "secret", "exclude": True},
            {"column_name": "visible_col"},
        ])
        uc = _uc_rows([
            ("secret", "STRING", False),
            ("visible_col", "STRING", False),
        ])
        ctx = _build_schema_contexts(config, uc, [])

        allowlist = ctx["column_allowlist"]
        assert "visible_col" in allowlist
        assert "secret" not in allowlist

        # tables_context also omits the hidden column.
        tables_ctx = ctx["tables_context"]
        assert "visible_col" in tables_ctx
        assert "secret" not in tables_ctx


# ── _build_metadata_allowlist + _enforce_metadata_constraints ──────


class TestMetadataAllowlistRejectsHiddenColumns:
    def _allowlist(self, config, uc):
        return _build_metadata_allowlist(config=config, uc_columns=uc, uc_routines=[])

    def test_hidden_column_absent_from_allowed_columns(self):
        config = _config_with_columns([
            {"column_name": "secret", "exclude": True},
            {"column_name": "visible_col"},
        ])
        uc = _uc_rows([
            ("secret", "STRING", False),
            ("visible_col", "STRING", False),
        ])
        allow = self._allowlist(config, uc)

        assert "visible_col" in allow["columns"]
        assert "secret" not in allow["columns"]

    def test_benchmark_required_hidden_column_is_rejected(self):
        config = _config_with_columns([
            {"column_name": "secret", "exclude": True},
            {"column_name": "visible_col"},
        ])
        uc = _uc_rows([
            ("secret", "STRING", False),
            ("visible_col", "STRING", False),
        ])
        allow = self._allowlist(config, uc)

        # A benchmark that declares the hidden column as required is rejected.
        ok, reason_code, _ = _enforce_metadata_constraints(
            benchmark={"required_columns": ["secret"]},
            sql=f"SELECT secret FROM {_TABLE_ID}",
            allowlist=allow,
            catalog="cat",
            schema="sch0",
        )
        assert ok is False
        assert reason_code == "unknown_column"

    def test_benchmark_referencing_visible_column_is_accepted(self):
        config = _config_with_columns([{"column_name": "visible_col"}])
        uc = _uc_rows([("visible_col", "STRING", False)])
        allow = self._allowlist(config, uc)

        ok, reason_code, _ = _enforce_metadata_constraints(
            benchmark={"required_columns": ["visible_col"]},
            sql=f"SELECT visible_col FROM {_TABLE_ID}",
            allowlist=allow,
            catalog="cat",
            schema="sch0",
        )
        assert ok is True
        assert reason_code == "ok"


# ── revise_plan_for_column (adaptive reactivation guard) ──────────


class TestRevisePlanForColumnRespectsHidden:
    """Adaptive revision must not reactivate a hidden column.

    ``unified_loop._reactivate_omitted`` and the benchmark QC job compute the
    set of columns referenced by candidate SQL (``sql_column_evidence``) and
    call :func:`revise_plan_for_column` for any that are not already active.
    Because the full inventory *retains* excluded columns, a hidden column
    referenced in SQL would otherwise be reactivated (and profiled) here. The
    guard raises ``ValueError`` — both callers catch and skip — so hidden
    columns stay out of the adaptive working set.
    """

    def test_reactivating_hidden_column_raises(self):
        col_configs = [
            {"column_name": "secret", "exclude": True},
        ] + [{"column_name": f"col_{i:04d}"} for i in range(60)]
        config = _config_with_columns(col_configs)
        rows = [("secret", "STRING", False)] + [
            (f"col_{i:04d}", "STRING", False) for i in range(60)
        ]
        inventory = build_inventory(_uc_rows(rows), config, captured_at="t")
        evidence = build_local_evidence(config, inventory)
        plan = build_selection_plan(inventory, evidence, run_id="run-guard")

        hidden_key = ("cat", "sch0", "table0", "secret")
        # Sanity: the hidden column is present in the inventory but inactive.
        assert hidden_key in {
            tuple(c["column_key"])
            for c in inventory["assets"][0]["columns"]
        }
        assert hidden_key not in active_column_keys(plan)

        with pytest.raises(ValueError, match="cannot activate hidden column"):
            revise_plan_for_column(plan, inventory, hidden_key)

    def test_reactivating_visible_omitted_column_still_works(self):
        # Ensure the guard does not over-fire on a legitimate omitted column.
        col_configs = [{"column_name": f"col_{i:04d}"} for i in range(60)]
        config = _config_with_columns(col_configs)
        rows = [(f"col_{i:04d}", "STRING", False) for i in range(60)]
        inventory = build_inventory(_uc_rows(rows), config, captured_at="t")
        evidence = build_local_evidence(config, inventory)
        plan = build_selection_plan(inventory, evidence, run_id="run-ok")

        omitted = next(
            tuple(r["column_key"])
            for r in plan["assets"][0]["columns"]
            if not r["active"]
        )
        revised = revise_plan_for_column(plan, inventory, omitted)
        target = next(
            r for r in revised["assets"][0]["columns"]
            if tuple(r["column_key"]) == omitted
        )
        assert target["active"] is True


# ── _build_schema_contexts (metric-view block) ────────────────────


class TestMetricViewContextDropsHidden:
    """The metric-view prompt block must not advertise hidden measures/dims."""

    def _config_with_metric_view(self, column_configs):
        return {
            "_tables": [],
            "_parsed_space": {
                "data_sources": {
                    "tables": [],
                    "metric_views": [
                        {
                            "identifier": "cat.sch0.mv_sales",
                            "column_configs": column_configs,
                        }
                    ],
                },
            },
        }

    def test_hidden_measure_and_dimension_omitted_from_mv_context(self):
        config = self._config_with_metric_view([
            {"column_name": "hidden_measure", "is_measure": True, "exclude": True},
            {"column_name": "total_sales", "is_measure": True},
            {"column_name": "hidden_dim", "exclude": True},
            {"column_name": "region"},
        ])
        ctx = _build_schema_contexts(config, [], [])
        mv = ctx["metric_views_context"]

        # Visible measure/dimension are advertised.
        assert "total_sales" in mv
        assert "region" in mv
        # Hidden measure/dimension are not.
        assert "hidden_measure" not in mv
        assert "hidden_dim" not in mv

    def test_all_mv_columns_hidden_shows_no_column_detail(self):
        config = self._config_with_metric_view([
            {"column_name": "hidden_measure", "is_measure": True, "exclude": True},
            {"column_name": "hidden_dim", "exclude": True},
        ])
        ctx = _build_schema_contexts(config, [], [])
        mv = ctx["metric_views_context"]
        assert "hidden_measure" not in mv
        assert "hidden_dim" not in mv
        assert "(no column detail available)" in mv


# ── _hidden_column_keys cross-schema precision (Finding 4) ──────────


class TestHiddenColumnKeysCrossSchemaPrecision:
    """Hiding a column in one schema must not drop a same-named visible column
    in another schema when both share the short table name.

    The earlier implementation built hidden keys from *short-name* identifier
    candidates (``orders`` and ``sch1.orders`` as well as ``cat.sch1.orders``),
    so a UC row for ``cat.sch2.orders.secret`` matched on the shared short
    candidate ``orders`` and was wrongly dropped. The fix uses only the
    precise 3-part key, so only the exact asset it was declared on is hidden.
    """

    def test_same_short_name_different_schema_is_not_shadowed(self):
        config = {
            "_tables": ["cat.sch1.orders", "cat.sch2.orders"],
            "_parsed_space": {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": "cat.sch1.orders",
                            "column_configs": [
                                {"column_name": "secret", "exclude": True},
                            ],
                        },
                        {
                            "identifier": "cat.sch2.orders",
                            "column_configs": [
                                {"column_name": "secret"},
                            ],
                        },
                    ],
                },
            },
        }
        uc = [
            {
                "catalog_name": "cat",
                "schema_name": "sch1",
                "table_name": "orders",
                "column_name": "secret",
                "data_type": "STRING",
                "comment": "hidden in sch1",
                "ordinal_position": 1,
                "table_type": "MANAGED",
            },
            {
                "catalog_name": "cat",
                "schema_name": "sch2",
                "table_name": "orders",
                "column_name": "secret",
                "data_type": "STRING",
                "comment": "visible in sch2",
                "ordinal_position": 1,
                "table_type": "MANAGED",
            },
        ]
        scoped = _filter_uc_columns_to_space_assets(config, uc)
        kept = {(c["schema_name"], c["table_name"]): c["column_name"] for c in scoped}
        # sch1.orders.secret is hidden → dropped; sch2.orders.secret is visible → kept.
        assert ("sch2", "orders") in kept
        assert ("sch1", "orders") not in kept
