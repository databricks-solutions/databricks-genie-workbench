"""Trial 23 W5 — pre-generation asset grounding unit tests.

The d139/e943 postmortems showed the repair-diagnosis gate recording
``missing implicated_assets`` as observe-only: the LLM then emitted
ungrounded snippets that the applier could not land. W5 promotes the
signal to a *pre-generation grounding injection* — it resolves the
cluster's blame_set against the schema slice and injects the resolved
table+column references so the synthesis prompt anchors SQL-shape
repairs to assets that actually exist. It is NOT a hard block (that
stays behind ``trial23_asset_grounding_blocking_enabled``, default OFF,
until the W7-W9 repair paths exist).
"""
from __future__ import annotations

from genie_space_optimizer.optimization import asset_grounding as ag


# ---- schema-slice resolution (both production + legacy shapes) -------

def _slice_columns_shape() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.orders",
                    "columns": [{"name": "order_id"}, {"name": "amount"}],
                }
            ],
            "metric_views": [],
        }
    }


def _slice_column_configs_shape() -> dict:
    # Production serialized_space stores columns under ``column_configs``
    # with ``column_name`` (the silent data-path bug guard in
    # test_sql_qualification_and_miner.py).
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sales.fact_orders",
                    "column_configs": [
                        {"column_name": "order_id", "data_type": "BIGINT"},
                        {"column_name": "revenue", "data_type": "DECIMAL"},
                    ],
                }
            ],
            "metric_views": [
                {
                    "identifier": "cat.sales.mv_sales",
                    "measures": [{"name": "cy_sales"}],
                    "dimensions": [{"name": "region"}],
                }
            ],
        }
    }


def test_resolves_fqn_column_in_columns_shape():
    got = ag.resolve_assets_from_schema_slice(
        _slice_columns_shape(), ["cat.sch.orders.amount"]
    )
    assert got == ("cat.sch.orders.amount",)


def test_resolves_fqn_column_in_column_configs_shape():
    got = ag.resolve_assets_from_schema_slice(
        _slice_column_configs_shape(), ["cat.sales.fact_orders.revenue"]
    )
    assert got == ("cat.sales.fact_orders.revenue",)


def test_resolves_table_only_ref():
    got = ag.resolve_assets_from_schema_slice(
        _slice_columns_shape(), ["cat.sch.orders"]
    )
    assert got == ("cat.sch.orders",)


def test_resolves_two_part_table_column_fallback():
    # blame entry without catalog/schema — resolve by table last-segment.
    got = ag.resolve_assets_from_schema_slice(
        _slice_columns_shape(), ["orders.amount"]
    )
    assert got == ("cat.sch.orders.amount",)


def test_resolves_metric_view_measure():
    got = ag.resolve_assets_from_schema_slice(
        _slice_column_configs_shape(), ["cat.sales.mv_sales.cy_sales"]
    )
    assert got == ("cat.sales.mv_sales.cy_sales",)


def test_unresolved_blame_is_dropped():
    got = ag.resolve_assets_from_schema_slice(
        _slice_columns_shape(), ["cat.sch.orders.nonexistent", "ghost.table"]
    )
    assert got == ()


def test_resolution_is_deduplicated_and_order_preserving():
    got = ag.resolve_assets_from_schema_slice(
        _slice_columns_shape(),
        ["cat.sch.orders.amount", "orders.amount", "cat.sch.orders.order_id"],
    )
    assert got == ("cat.sch.orders.amount", "cat.sch.orders.order_id")


def test_empty_slice_resolves_nothing():
    assert ag.resolve_assets_from_schema_slice({}, ["cat.sch.orders"]) == ()
    assert ag.resolve_assets_from_schema_slice(
        _slice_columns_shape(), []
    ) == ()


# ---- needs_asset_grounding predicate --------------------------------

def test_needs_grounding_when_sql_shape_repair_lacks_assets():
    assert ag.needs_asset_grounding(
        implicated_assets=(),
        root_cause="extra_defensive_filter",
        sql_shape_delta="remove WHERE x IS NOT NULL",
    )


def test_no_grounding_when_assets_already_present():
    assert not ag.needs_asset_grounding(
        implicated_assets=("cat.sch.orders.amount",),
        root_cause="extra_defensive_filter",
        sql_shape_delta="remove filter",
    )


def test_no_grounding_when_no_shape_intent():
    # No root_cause and no sql_shape_delta — nothing shape-y to ground.
    assert not ag.needs_asset_grounding(
        implicated_assets=(),
        root_cause="",
        sql_shape_delta="",
    )


# ---- build_asset_grounding (the injected payload) --------------------

def test_build_returns_none_when_not_needed():
    block = ag.build_asset_grounding(
        schema_slice=_slice_columns_shape(),
        blame_set=("cat.sch.orders.amount",),
        implicated_assets=("cat.sch.orders.amount",),
        root_cause="extra_defensive_filter",
        sql_shape_delta="x",
    )
    assert block is None


def test_build_returns_none_when_nothing_resolves():
    # Needed, but blame_set does not resolve against the slice → no
    # grounding to inject (W6 real-slice / W7 repair handle the rest).
    block = ag.build_asset_grounding(
        schema_slice=_slice_columns_shape(),
        blame_set=("ghost.table.col",),
        implicated_assets=(),
        root_cause="extra_defensive_filter",
        sql_shape_delta="x",
    )
    assert block is None


def test_build_returns_grounding_block_when_resolvable():
    block = ag.build_asset_grounding(
        schema_slice=_slice_columns_shape(),
        blame_set=("cat.sch.orders.amount", "cat.sch.orders"),
        implicated_assets=(),
        root_cause="extra_defensive_filter",
        sql_shape_delta="remove the defensive filter",
    )
    assert block is not None
    assert block["resolved_assets"] == [
        "cat.sch.orders.amount",
        "cat.sch.orders",
    ]
    assert "anchor" in block["directive"].lower()


# ---- synthesis wiring ----------------------------------------------

def _synthesis_response():
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_2",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Add example",
                    "intent_description": "exemplar",
                    "repair_hypothesis": "show correct pattern",
                    "patch_type": "add_example_sql",
                    "rationale": "demonstrate",
                    "confidence": "high",
                    "patch_body": {
                        "example_question": "q?",
                        "example_sql": "SELECT 1",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=10,
        tokens_output=10,
        duration_ms=1,
        error=None,
    )


def _cluster(root_cause: str, blame: tuple[str, ...]):
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="theme",
        member_qids=("gs_009",),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=blame,
        confidence="high",
        root_cause=root_cause,
    )


def test_synthesis_emits_grounding_marker_when_assets_resolve(capsys):
    from unittest.mock import MagicMock, patch

    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response()
        )
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(
                "extra_defensive_filter", ("cat.sch.orders.amount",)
            ),
            schema_slice=_slice_columns_shape(),
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1" in out
    assert "cat.sch.orders.amount" in out


def test_synthesis_silent_when_no_assets_resolve(capsys):
    from unittest.mock import MagicMock, patch

    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response()
        )
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster("extra_defensive_filter", ("ghost.table.col",)),
            schema_slice=_slice_columns_shape(),
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1" not in out


def test_synthesis_silent_when_flag_off(capsys, monkeypatch):
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "0")
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response()
        )
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(
                "extra_defensive_filter", ("cat.sch.orders.amount",)
            ),
            schema_slice=_slice_columns_shape(),
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1" not in out


def test_marker_payload_shape():
    line = ag.asset_grounding_injected_marker(
        optimization_run_id="run_x",
        iteration=2,
        cluster_id="H001",
        root_cause="extra_defensive_filter",
        resolved_assets=("cat.sch.orders.amount",),
    )
    assert line.startswith("GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1 ")
    import json

    payload = json.loads(line.split(" ", 1)[1])
    assert payload["cluster_id"] == "H001"
    assert payload["resolved_assets"] == ["cat.sch.orders.amount"]
    assert payload["resolved_count"] == 1
    assert payload["root_cause"] == "extra_defensive_filter"
