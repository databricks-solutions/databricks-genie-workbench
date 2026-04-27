"""PR 33 — Proactive join discovery must filter MV-touching specs before
patching the Genie space.

Locks the contract that
:func:`genie_space_optimizer.optimization.harness._mine_and_apply_proven_joins`
(and its sibling ``_run_proactive_join_discovery``) drop join specs
whose left or right identifier is a metric view per
``_asset_semantics`` *before* calling ``patch_space_config``. Direct
joins on metric views raise ``METRIC_VIEW_JOIN_NOT_SUPPORTED`` at
execute time; gating at discovery prevents the bad join from ever
reaching the Genie space.
"""

from __future__ import annotations

from unittest.mock import patch

import genie_space_optimizer.common.genie_client as _genie_client_mod
import genie_space_optimizer.optimization.harness as _harness_mod
from genie_space_optimizer.common.asset_semantics import (
    KIND_METRIC_VIEW,
    build_and_stamp_from_run,
)


def _seeded_metadata_snapshot() -> dict:
    """Build a metadata snapshot whose ``_asset_semantics`` flags
    ``cat.sch.mv1`` as a metric view and ``cat.sch.fact_sales`` /
    ``cat.sch.dim_store`` as plain tables.
    """
    snapshot: dict = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.fact_sales",
                    "column_configs": [
                        {"column_name": "store_id", "data_type": "STRING"},
                    ],
                },
                {
                    "identifier": "cat.sch.dim_store",
                    "column_configs": [
                        {"column_name": "store_id", "data_type": "STRING"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "_tables": ["cat.sch.fact_sales", "cat.sch.dim_store"],
        "_metric_views": ["cat.sch.mv1"],
        "_metric_view_yaml": {
            "cat.sch.mv1": {
                "version": 0.1,
                "source": "cat.sch.fact_sales",
                "measures": [{"name": "total_revenue", "expr": "SUM(revenue)"}],
                "dimensions": [{"name": "store_id"}],
            },
        },
        "instructions": {"join_specs": []},
    }
    build_and_stamp_from_run(
        snapshot,
        table_refs=[
            ("cat", "sch", "fact_sales"),
            ("cat", "sch", "dim_store"),
            ("cat", "sch", "mv1"),
        ],
        catalog_yamls={
            "cat.sch.mv1": snapshot["_metric_view_yaml"]["cat.sch.mv1"],
        },
        catalog_outcomes={},
        catalog_diagnostic_samples={},
        uc_columns=None,
    )
    sem = snapshot["_asset_semantics"]
    assert sem["cat.sch.mv1"]["kind"] == KIND_METRIC_VIEW
    return snapshot


def _make_specs() -> list[dict]:
    """Return one MV-MV spec (must be dropped) and one table-table spec
    (must be retained).
    """
    return [
        {
            "left": {"identifier": "cat.sch.mv1"},
            "right": {"identifier": "cat.sch.mv1"},
            "sql": ["mv1.x = mv1.y"],
            "_proactive_metadata": {"frequency": 2, "agreed": True},
        },
        {
            "left": {"identifier": "cat.sch.fact_sales"},
            "right": {"identifier": "cat.sch.dim_store"},
            "sql": ["fact_sales.store_id = dim_store.store_id"],
            "_proactive_metadata": {"frequency": 3, "agreed": True},
        },
    ]


def test_mine_and_apply_drops_mv_join_before_patch():
    """The iterative join miner must drop the MV-MV spec and PATCH only
    the table-table spec.
    """
    snapshot = _seeded_metadata_snapshot()

    exec_candidates = [
        {
            "left_table": "cat.sch.mv1",
            "right_table": "cat.sch.mv1",
            "frequency": 2,
            "agreed": True,
            "left_columns": ["x"],
            "right_columns": ["y"],
        },
        {
            "left_table": "cat.sch.fact_sales",
            "right_table": "cat.sch.dim_store",
            "frequency": 3,
            "agreed": True,
            "left_columns": ["store_id"],
            "right_columns": ["store_id"],
        },
    ]

    captured: dict = {}

    def _capture_patch(_w, _space_id, parsed):
        captured["parsed"] = parsed
        captured["join_specs"] = list(
            (parsed.get("instructions", {}) or {}).get("join_specs", [])
        )

    with patch(
        "genie_space_optimizer.optimization.optimizer._extract_proven_joins",
        return_value=(exec_candidates, {"total_rows": 2}),
    ), patch(
        "genie_space_optimizer.optimization.optimizer._corroborate_with_uc_metadata",
        side_effect=lambda cands, _meta: list(cands),
    ), patch(
        "genie_space_optimizer.optimization.optimizer._build_join_specs_from_proven",
        return_value=_make_specs(),
    ), patch.object(
        _harness_mod, "_explain_join_candidate", return_value=(True, None),
    ), patch.object(
        _harness_mod, "resolve_warehouse_id", return_value="dummy-warehouse",
    ), patch.object(
        _harness_mod, "write_patch", return_value=None,
    ), patch.object(
        _harness_mod, "write_stage", return_value=None,
    ), patch.object(
        _genie_client_mod, "patch_space_config", side_effect=_capture_patch,
    ):
        result = _harness_mod._mine_and_apply_proven_joins(
            w=None,
            spark=None,
            run_id="run-id",
            space_id="space-id",
            metadata_snapshot=snapshot,
            eval_rows=[{"placeholder": True}],
            catalog="cat",
            schema="sch",
            iteration=1,
        )

    assert result.get("joins_skipped_metric_view") == 1, result
    assert result.get("total_applied") == 1, result

    applied_specs = captured.get("join_specs") or []
    identifiers = {
        (
            (s.get("left") or {}).get("identifier"),
            (s.get("right") or {}).get("identifier"),
        )
        for s in applied_specs
    }
    assert ("cat.sch.fact_sales", "cat.sch.dim_store") in identifiers
    assert ("cat.sch.mv1", "cat.sch.mv1") not in identifiers
    assert all(
        ((s.get("left") or {}).get("identifier") != "cat.sch.mv1")
        and ((s.get("right") or {}).get("identifier") != "cat.sch.mv1")
        for s in applied_specs
    )


def test_mine_and_apply_no_patch_when_only_mv_specs():
    """When every candidate spec touches a metric view, the miner must
    skip the PATCH entirely and report the skip count.
    """
    snapshot = _seeded_metadata_snapshot()

    exec_candidates = [
        {
            "left_table": "cat.sch.mv1",
            "right_table": "cat.sch.mv1",
            "frequency": 1,
            "agreed": True,
            "left_columns": ["x"],
            "right_columns": ["y"],
        },
    ]
    only_mv_specs = [
        {
            "left": {"identifier": "cat.sch.mv1"},
            "right": {"identifier": "cat.sch.mv1"},
            "sql": ["mv1.x = mv1.y"],
            "_proactive_metadata": {"frequency": 1, "agreed": True},
        },
    ]

    patch_calls: list = []

    def _record_patch(*args, **kwargs):
        patch_calls.append((args, kwargs))

    with patch(
        "genie_space_optimizer.optimization.optimizer._extract_proven_joins",
        return_value=(exec_candidates, {"total_rows": 1}),
    ), patch(
        "genie_space_optimizer.optimization.optimizer._corroborate_with_uc_metadata",
        side_effect=lambda cands, _meta: list(cands),
    ), patch(
        "genie_space_optimizer.optimization.optimizer._build_join_specs_from_proven",
        return_value=only_mv_specs,
    ), patch.object(
        _harness_mod, "_explain_join_candidate", return_value=(True, None),
    ), patch.object(
        _harness_mod, "resolve_warehouse_id", return_value="dummy-warehouse",
    ), patch.object(
        _harness_mod, "write_patch", return_value=None,
    ), patch.object(
        _genie_client_mod, "patch_space_config", side_effect=_record_patch,
    ):
        result = _harness_mod._mine_and_apply_proven_joins(
            w=None,
            spark=None,
            run_id="run-id",
            space_id="space-id",
            metadata_snapshot=snapshot,
            eval_rows=[{"placeholder": True}],
            catalog="cat",
            schema="sch",
            iteration=1,
        )

    assert result.get("joins_skipped_metric_view") == 1, result
    assert result.get("total_applied", 0) == 0, result
    assert patch_calls == [], "patch_space_config must not be called when every spec is MV-MV"
