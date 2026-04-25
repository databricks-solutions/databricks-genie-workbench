"""Reactive metric-view reclassification on profile failure.

When ``_collect_data_profile`` runs the standard table-shape aggregate
query against a ref that is actually a metric view, Spark's planner
rejects it with ``METRIC_VIEW_UNSUPPORTED_USAGE``. The harness must:
  * recognise the error as a metric-view rejection,
  * add the ref to a ``reclassified_mvs`` accumulator,
  * skip the table-shape fallback (which would also fail), and
  * not record any partial profile rows for the ref.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd


def _table_uc_columns(table_fqn: str) -> list[dict]:
    return [
        {
            "catalog_name": "cat",
            "schema_name": "sch",
            "table_name": table_fqn.split(".")[-1],
            "column_name": "id",
            "data_type": "string",
        },
    ]


def test_mv_error_triggers_reclassification():
    """Profile query raising an MV error → ref appears in reclassified set."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    table_fqn = "cat.sch.mv_in_disguise"

    def fake_exec_sql(sql, *args, **kwargs):
        if sql.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 100}])
        if "_card_" in sql:
            raise RuntimeError(
                "[METRIC_VIEW_UNSUPPORTED_USAGE] The metric view usage is not "
                "supported"
            )
        # Any other query (fallback) — should not be reached.
        raise AssertionError(f"Unexpected fallback SQL: {sql[:80]!r}")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        profile, reclassified = preflight._collect_data_profile(
            spark=MagicMock(),
            tables=[table_fqn],
            uc_columns=_table_uc_columns(table_fqn),
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert table_fqn in reclassified or table_fqn.lower() in reclassified
    # No profile entry was recorded for the reclassified MV — the
    # downstream MV-aware profile path will populate this in C5.
    assert all(table_fqn not in fqn for fqn in profile.keys())


def test_non_mv_error_does_not_trigger_reclassification():
    """Generic table failures still skip the ref but do NOT reclassify."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    table_fqn = "cat.sch.broken_table"

    def fake_exec_sql(sql, *args, **kwargs):
        if sql.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 100}])
        raise RuntimeError("Generic Spark error: TABLE_OR_VIEW_NOT_FOUND")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        profile, reclassified = preflight._collect_data_profile(
            spark=MagicMock(),
            tables=[table_fqn],
            uc_columns=_table_uc_columns(table_fqn),
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert reclassified == [] or list(reclassified) == []
    assert all(table_fqn not in fqn for fqn in profile.keys())


def test_mv_error_skips_fallback_query():
    """When the primary aggregate raises an MV error, the fallback (no
    TABLESAMPLE) is NOT attempted — it would also fail and just adds
    log noise."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    table_fqn = "cat.sch.mv_skip_fallback"
    call_log: list[str] = []

    def fake_exec_sql(sql, *args, **kwargs):
        call_log.append(sql)
        if sql.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 100}])
        if "_card_" in sql:
            raise RuntimeError(
                "[UNSUPPORTED_METRIC_VIEW_USAGE] cannot apply this aggregation"
            )
        raise AssertionError(f"Unexpected fallback SQL: {sql[:80]!r}")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        profile, reclassified = preflight._collect_data_profile(
            spark=MagicMock(),
            tables=[table_fqn],
            uc_columns=_table_uc_columns(table_fqn),
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    # We expect: COUNT(*) + the primary aggregate. NO fallback was called.
    aggregate_calls = [s for s in call_log if "_card_" in s]
    assert len(aggregate_calls) == 1, (
        f"Fallback fired despite MV error; calls: {call_log!r}"
    )
    assert table_fqn in reclassified or table_fqn.lower() in reclassified


def test_mixed_refs_only_mv_failures_reclassified():
    """A mix of table refs + one MV-rejecting ref → only the MV is reclassified;
    the regular table is profiled normally."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    good_fqn = "cat.sch.good_table"
    mv_fqn = "cat.sch.bad_mv"

    def _agg_row():
        # Synthetic single-row DataFrame matching the COUNT(DISTINCT id)
        # alias the profile query produces.
        return pd.DataFrame([{"_card_id": 5}])

    def fake_exec_sql(sql, *args, **kwargs):
        if sql.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 100}])
        if good_fqn.split(".")[-1] in sql and "_card_" in sql:
            return _agg_row()
        if mv_fqn.split(".")[-1] in sql and "_card_" in sql:
            raise RuntimeError(
                "[METRIC_VIEW_UNSUPPORTED_USAGE] not supported"
            )
        if "COLLECT_SET" in sql:
            # Distinct-values lookup for the regular table.
            return pd.DataFrame([{"vals": '["a","b"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:80]!r}")

    cols = _table_uc_columns(good_fqn) + _table_uc_columns(mv_fqn)
    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        profile, reclassified = preflight._collect_data_profile(
            spark=MagicMock(),
            tables=[good_fqn, mv_fqn],
            uc_columns=cols,
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    # Regular table profile is recorded.
    assert any(good_fqn in fqn for fqn in profile.keys())
    # MV ref reclassified, no profile rows for it.
    assert any(mv_fqn in r or mv_fqn.lower() in r for r in reclassified)
    assert all(mv_fqn not in fqn for fqn in profile.keys())


def test_caller_merges_reclassified_into_metric_view_yaml():
    """End-to-end: when ``_collect_data_profile`` returns reclassified
    refs, ``preflight_collect_uc_metadata`` merges them into
    ``config["_metric_view_yaml"]`` so downstream gates see them as MVs."""
    from genie_space_optimizer.optimization import preflight

    config: dict = {
        "_tables": ["cat.sch.mv_in_disguise"],
        "_metric_views": [],
        "_functions": [],
        "_parsed_space": {},
    }

    cols = _table_uc_columns("cat.sch.mv_in_disguise")

    with (
        patch.object(preflight, "_compute_join_overlaps", return_value=[]),
        patch.object(preflight, "_validate_core_access"),
        patch.object(preflight, "write_stage"),
        patch.object(preflight, "_update_run_status"),
        patch.object(preflight, "_collect_or_empty", return_value=(cols, None)),
        patch.object(
            preflight, "_collect_data_profile",
            return_value=({}, ["cat.sch.mv_in_disguise"]),
        ),
        patch.object(
            preflight, "_detect_metric_views_via_catalog",
            return_value=(set(), {}),
        ),
        patch.object(preflight, "get_columns_for_tables_rest", return_value=[]),
        patch.object(preflight, "get_tags_for_tables_rest", return_value=[]),
        patch.object(preflight, "get_routines_for_schemas_rest", return_value=[]),
        patch.object(preflight, "get_foreign_keys_for_tables_rest", return_value=[]),
    ):
        preflight.preflight_collect_uc_metadata(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="run-test",
            catalog="cat",
            schema="sch",
            config=config,
            snapshot={},
            genie_table_refs=[("cat", "sch", "mv_in_disguise")],
        )

    yaml_cache = config.get("_metric_view_yaml") or {}
    assert any(
        "mv_in_disguise" in str(k).lower() for k in yaml_cache
    ), (
        f"Expected reclassified MV to be merged into _metric_view_yaml; "
        f"got {list(yaml_cache.keys())!r}"
    )
