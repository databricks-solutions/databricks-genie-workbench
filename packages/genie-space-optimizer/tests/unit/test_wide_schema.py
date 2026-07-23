from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from genie_space_optimizer.optimization import wide_schema_history
from genie_space_optimizer.optimization.wide_schema import (
    MAX_ACTIVE_COLUMNS_PER_ASSET,
    MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET,
    active_column_keys,
    build_inventory,
    build_local_evidence,
    build_selection_plan,
    collect_inventory,
    project_full_inventory,
    revise_plan_for_column,
    revise_plan_with_profile_outcomes,
    sql_column_evidence,
    validate_column_reference,
)
from genie_space_optimizer.optimization.wide_schema_history import (
    _rest_history_rows,
    _system_history_rows,
    normalize_history_rows,
)
from genie_space_optimizer.optimization.wide_schema_profile import (
    WorkResult,
    _execute,
    build_initial_work,
    build_profiling_budget,
    run_bounded_profile,
)
from genie_space_optimizer.optimization.wide_schema_prompt import (
    _compact_json_object,
    fit_messages,
    messages_size,
    pack_active_schema,
)


def _config(asset_names: list[tuple[str, str, str]]) -> dict:
    return {
        "_parsed_space": {
            "title": "Wide sales space",
            "data_sources": {
                "tables": [
                    {"identifier": ".".join(asset)} for asset in asset_names
                ]
            },
        }
    }


def _inventory(
    *,
    assets: int = 1,
    columns: int = 60,
    dtype: str = "STRING",
    table_type: str = "MANAGED",
):
    names = [("cat", f"sch{asset}", f"table{asset}") for asset in range(assets)]
    rows = []
    for catalog, schema, table in names:
        for ordinal in range(columns):
            rows.append({
                "catalog_name": catalog,
                "schema_name": schema,
                "table_name": table,
                "column_name": f"column_{ordinal:04d}",
                "data_type": dtype,
                "comment": "x" * 600,
                "ordinal_position": ordinal + 1,
                "table_type": table_type,
            })
    return build_inventory(rows, _config(names), captured_at="2026-01-01T00:00:00Z")


def test_five_thousand_column_inventory_is_complete_and_plan_is_bounded():
    inventory = _inventory(columns=5_000)
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-wide")

    assert len(inventory["assets"][0]["columns"]) == 5_000
    assert plan["assets"][0]["active_count"] == 45
    assert plan["assets"][0]["active_count"] <= MAX_ACTIVE_COLUMNS_PER_ASSET
    assert plan["assets"][0]["omitted_count"] == 4_955


def test_adaptive_revision_never_profiles_a_51st_distinct_column():
    inventory = _inventory(columns=51)
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-adaptive")
    columns = inventory["assets"][0]["columns"]

    for column in columns:
        key = tuple(column["column_key"])
        active = {
            tuple(row["column_key"])
            for row in plan["assets"][0]["columns"]
            if row["active"]
        }
        if len(active) >= 50:
            break
        if key not in active:
            plan = revise_plan_for_column(plan, inventory, key)

    active = {
        tuple(row["column_key"])
        for row in plan["assets"][0]["columns"]
        if row["active"]
    }
    plan = revise_plan_with_profile_outcomes(
        plan,
        inventory,
        {
            key: {
                "profile_status": "profiled",
                "submitted": True,
                "available_metrics": ["cardinality"],
            }
            for key in active
        },
    )
    omitted = next(
        tuple(row["column_key"])
        for row in plan["assets"][0]["columns"]
        if not row["active"]
    )
    revised = revise_plan_for_column(plan, inventory, omitted)
    target = next(
        row for row in revised["assets"][0]["columns"]
        if tuple(row["column_key"]) == omitted
    )

    assert revised["assets"][0]["active_count"] == 50
    assert revised["assets"][0]["cumulative_value_profiled_count"] == 50
    assert target["active"] is True
    assert target["profile_status"] == "metadata_only"
    assert target["cumulatively_value_profiled"] is False


def test_reactivated_profiled_column_is_not_profiled_twice():
    inventory = _inventory(columns=51)
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-reactivate")

    for row in plan["assets"][0]["columns"]:
        if not row["active"] and plan["assets"][0]["active_count"] < 50:
            plan = revise_plan_for_column(
                plan,
                inventory,
                tuple(row["column_key"]),
            )
    active = active_column_keys(plan)
    plan = revise_plan_with_profile_outcomes(
        plan,
        inventory,
        {
            key: {
                "profile_status": "profiled",
                "submitted": True,
                "available_metrics": ["cardinality"],
            }
            for key in active
        },
    )
    omitted = next(
        tuple(row["column_key"])
        for row in plan["assets"][0]["columns"]
        if not row["active"]
    )
    promoted = revise_plan_for_column(plan, inventory, omitted)
    evicted = next(
        tuple(after["column_key"])
        for before, after in zip(
            plan["assets"][0]["columns"],
            promoted["assets"][0]["columns"],
        )
        if before["active"] and not after["active"]
    )
    reactivated = revise_plan_for_column(promoted, inventory, evicted)
    target = next(
        row for row in reactivated["assets"][0]["columns"]
        if tuple(row["column_key"]) == evicted
    )

    assert target["active"] is True
    assert target["cumulatively_value_profiled"] is True
    assert target["profile_status"] == "profiled"
    assert all(
        item.kind == "row_count"
        for queue in build_initial_work(inventory, reactivated)[0].values()
        for item in queue
    )


def test_profile_work_has_explicit_projections_and_expression_caps():
    inventory = _inventory(columns=60, dtype="BIGINT")
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-profile")
    queues, _outcomes = build_initial_work(inventory, plan)
    work = list(next(iter(queues.values())))

    aggregate = [item for item in work if item.kind == "aggregate"]
    assert aggregate
    assert all(len(item.metrics) <= 10 for item in aggregate)
    assert all("SELECT *" not in item.sql.upper() for item in work)
    assert all("FROM (SELECT `" in item.sql for item in aggregate)


def test_pending_statement_is_cancelled_and_tagged():
    calls = {}

    class Execution:
        def execute_statement(self, **kwargs):
            calls["execute"] = kwargs
            return SimpleNamespace(
                status=SimpleNamespace(state="RUNNING", error=None),
                statement_id="statement-1",
                manifest=None,
                result=None,
            )

        def cancel_execution(self, **kwargs):
            calls["cancel"] = kwargs

    item = SimpleNamespace(
        sql="SELECT COUNT(*) FROM `cat`.`sch`.`table`",
        asset_key=("cat", "sch", "table"),
        asset_type="table",
        kind="row_count",
        column_keys=[],
    )
    result = _execute(
        SimpleNamespace(statement_execution=Execution()),
        "warehouse-1",
        item,
        run_id="run-1",
    )

    assert result.state == "timed_out"
    assert result.cancelled is True
    assert calls["cancel"] == {"statement_id": "statement-1"}
    tags = {tag.key: tag.value for tag in calls["execute"]["query_tags"]}
    assert tags == {
        "application": "genie_workbench",
        "component": "gso",
        "purpose": "profiling",
        "run_id": "run-1",
    }


def test_view_shape_fallback_splits_with_limit_projection(monkeypatch):
    inventory = _inventory(columns=3, dtype="BIGINT", table_type="VIEW")
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-view-fallback")
    calls = []

    def fake_execute(_w, _warehouse_id, item, *, run_id):
        assert run_id == "run-view-fallback"
        calls.append(item)
        if item.kind == "row_count":
            return WorkResult(item=item, state="succeeded", rows=[{"row_count": "10"}], submitted=True)
        if "TABLESAMPLE" in item.sql:
            return WorkResult(
                item=item,
                state="failed",
                error="TABLESAMPLE is unsupported for this view",
                submitted=True,
            )
        if item.split_depth == 0:
            return WorkResult(item=item, state="failed", error="aggregate analysis failed", submitted=True)
        return WorkResult(
            item=item,
            state="succeeded",
            rows=[{metric.alias: "1" for metric in item.metrics}],
            submitted=True,
        )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.wide_schema_profile._execute",
        fake_execute,
    )
    result = run_bounded_profile(
        SimpleNamespace(),
        "warehouse-1",
        inventory,
        plan,
        run_id="run-view-fallback",
    )

    split_calls = [item for item in calls if item.split_depth == 1]
    assert len(split_calls) == 2
    assert all(" LIMIT 100" in item.sql for item in split_calls)
    assert all("TABLESAMPLE" not in item.sql for item in split_calls)
    assert result["telemetry"]["view_shape_fallbacks"] == 1
    assert result["telemetry"]["split_retries"] == 2
    assert result["telemetry"]["submitted_statements"] == 5
    assert all(
        outcome["profile_status"] == "profiled"
        for outcome in result["outcomes"].values()
    )


def test_view_value_list_uses_the_same_bounded_shape_fallback(monkeypatch):
    inventory = _inventory(columns=1, dtype="STRING", table_type="VIEW")
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-value-fallback")
    calls = []

    def fake_execute(_w, _warehouse_id, item, *, run_id):
        calls.append(item)
        if item.kind == "row_count":
            return WorkResult(item=item, state="succeeded", rows=[{"row_count": "10"}], submitted=True)
        if "TABLESAMPLE" in item.sql:
            return WorkResult(
                item=item,
                state="failed",
                error="TABLESAMPLE is unsupported for this view",
                submitted=True,
            )
        if item.kind == "value_list":
            return WorkResult(item=item, state="succeeded", rows=[{"values": '["a", "b"]'}], submitted=True)
        return WorkResult(
            item=item,
            state="succeeded",
            rows=[{metric.alias: "2" for metric in item.metrics}],
            submitted=True,
        )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.wide_schema_profile._execute",
        fake_execute,
    )
    result = run_bounded_profile(
        SimpleNamespace(),
        "warehouse-1",
        inventory,
        plan,
        run_id="run-value-fallback",
    )

    assert result["telemetry"]["view_shape_fallbacks"] == 2
    assert result["telemetry"]["submitted_statements"] == 5
    assert any(
        item.kind == "value_list" and " LIMIT 100" in item.sql
        for item in calls
    )
    profile = result["data_profile"][inventory["assets"][0]["asset_id"]]
    assert profile["columns"]["column_0000"]["distinct_values"] == ["a", "b"]


def test_authorization_and_submission_failures_do_not_retry(monkeypatch):
    inventory = _inventory(columns=3, dtype="BIGINT", table_type="VIEW")
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-no-retry")

    for submitted, error, expected_status in (
        (True, "PERMISSION_DENIED: not authorized to query view", "partial"),
        (False, "RuntimeError: request was not submitted", "metadata_only"),
    ):
        calls = []

        def fake_execute(_w, _warehouse_id, item, *, run_id):
            calls.append(item)
            if item.kind == "row_count":
                return WorkResult(item=item, state="succeeded", rows=[{"row_count": "10"}], submitted=True)
            return WorkResult(item=item, state="failed", error=error, submitted=submitted)

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.wide_schema_profile._execute",
            fake_execute,
        )
        result = run_bounded_profile(
            SimpleNamespace(),
            "warehouse-1",
            inventory,
            plan,
            run_id="run-no-retry",
        )

        assert len(calls) == 2
        assert result["telemetry"].get("view_shape_fallbacks", 0) == 0
        assert result["telemetry"].get("split_retries", 0) == 0
        assert all(
            outcome["profile_status"] == expected_status
            and outcome["submitted"] is submitted
            for outcome in result["outcomes"].values()
        )


def test_persisted_run_and_asset_budgets_prevent_more_submissions(monkeypatch):
    inventory = _inventory(columns=3, dtype="BIGINT")
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-budget")
    asset_id = inventory["assets"][0]["asset_id"]

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("profiling work exceeded its persisted budget")

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.wide_schema_profile._execute",
        fail_if_called,
    )
    for telemetry in (
        {"submitted_statements": 600},
        {"submitted_statements": 30, "asset_statement_counts": {asset_id: 30}},
        {"elapsed_ms": 30 * 60 * 1000},
    ):
        budget = build_profiling_budget([telemetry])
        result = run_bounded_profile(
            SimpleNamespace(),
            "warehouse-1",
            inventory,
            plan,
            run_id="run-budget",
            budget=budget,
        )

        assert result["telemetry"]["submitted_statements"] == 0
        assert all(
            outcome == {
                "profile_status": "metadata_only",
                "submitted": False,
                "available_metrics": [],
            }
            for outcome in result["outcomes"].values()
        )


@pytest.mark.parametrize(
    "budget",
    [
        {"submitted_statements": 599},
        {"asset_statement_counts": {"`cat`.`sch0`.`table0`": 29}},
    ],
)
def test_concurrent_batch_reserves_last_persisted_statement_slot(
    monkeypatch,
    budget,
):
    inventory = _inventory(columns=3, dtype="BIGINT")
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-last-slot")
    calls = []

    def fake_execute(_w, _warehouse_id, item, *, run_id):
        calls.append(item)
        return WorkResult(
            item=item,
            state="succeeded",
            rows=[{"row_count": "3"}],
            submitted=True,
        )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.wide_schema_profile._execute",
        fake_execute,
    )
    result = run_bounded_profile(
        SimpleNamespace(),
        "warehouse-1",
        inventory,
        plan,
        run_id="run-last-slot",
        budget=build_profiling_budget([budget]),
    )

    assert len(calls) == 1
    assert result["telemetry"]["submitted_statements"] == 1


def test_prompt_packing_bounds_twenty_assets_without_losing_json_validity():
    inventory = _inventory(assets=20, columns=50)
    evidence = build_local_evidence(
        _config([("cat", f"sch{i}", f"table{i}") for i in range(20)]),
        inventory,
    )
    plan = build_selection_plan(inventory, evidence, run_id="run-prompt")
    projection, stats = pack_active_schema(inventory, plan, max_chars=40_000)
    messages, pack_stats = fit_messages([
        {"role": "system", "content": "Return valid JSON."},
        {"role": "user", "content": json.dumps({"schema": projection})},
    ])

    assert messages_size(messages) <= 60_000
    assert pack_stats["final_request_chars"] <= 60_000
    assert stats["omitted_counts"]["columns"] > 0
    json.loads(messages[1]["content"])


def test_prompt_packing_preserves_indivisible_system_message():
    system_prompt = "S" * 640
    user_payload = {
        "response_schema": {"patches": [{"type": "required"}]},
        "ordinary_context": [
            {"name": f"context_{index}", "detail": "x" * 1_000}
            for index in range(150)
        ],
    }

    messages, pack_stats = fit_messages([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload)},
    ])

    assert messages[0]["content"] == system_prompt
    assert messages_size(messages) <= 60_000
    assert pack_stats["final_request_chars"] <= 60_000
    packed_user = json.loads(messages[1]["content"])
    assert packed_user["response_schema"] == user_payload["response_schema"]
    assert len(packed_user["ordinary_context"]) < len(
        user_payload["ordinary_context"]
    )


def test_prompt_compaction_preserves_repair_targets_before_ordinary_context():
    repair_targets = [
        {"column": f"repair_{index}", "reason": "required"}
        for index in range(8)
    ]
    ordinary = [
        {"name": f"context_{index}", "detail": "x" * 200}
        for index in range(128)
    ]
    payload = {
        "repair_targets": repair_targets,
        "ordinary_context": ordinary,
    }
    target = len(json.dumps({
        "repair_targets": repair_targets,
        "ordinary_context": ordinary[:4],
    }))

    compact, _omitted = _compact_json_object(payload, target_chars=target)

    assert compact["repair_targets"] == repair_targets
    assert len(compact["ordinary_context"]) < len(ordinary)


def test_full_inventory_validation_is_independent_of_prompt_projection():
    inventory = _inventory(columns=60)
    evidence = build_local_evidence(_config([("cat", "sch0", "table0")]), inventory)
    plan = build_selection_plan(inventory, evidence, run_id="run-validation")
    omitted = next(
        tuple(row["column_key"])
        for row in plan["assets"][0]["columns"]
        if not row["active"]
    )

    assert validate_column_reference(inventory, omitted)
    assert any(
        row["column_name"] == omitted[-1]
        for row in project_full_inventory(inventory)
    )


def test_history_evidence_is_aggregate_only_and_select_star_is_not_column_use():
    inventory = _inventory(columns=3)
    now_ms = int(time.time() * 1000)
    evidence = normalize_history_rows(
        [
            {
                "statement_text": "SELECT * FROM cat.sch0.table0 WHERE column_0000 = 'secret'",
                "query_start_time_ms": now_ms,
                "executed_by": "alice@example.com",
            }
        ],
        inventory,
        source_mode="warehouse_api",
        source_scope=["warehouse-1"],
        max_statements=10,
    )
    serialized = json.dumps(evidence)

    assert [row["column_key"][-1] for row in evidence["columns"]] == ["column_0000"]
    assert "secret" not in serialized
    assert "alice@example.com" not in serialized
    assert "statement_text" not in serialized


def test_cte_alias_lineage_and_ambiguous_assets_are_resolved_safely():
    inventory = _inventory(assets=2, columns=3)
    diagnostics: dict[str, int] = {}
    evidence = sql_column_evidence(
        "WITH used AS ("
        "SELECT column_0000 AS customer_key FROM cat.sch0.table0"
        ") SELECT customer_key FROM used GROUP BY customer_key",
        inventory,
        diagnostics=diagnostics,
    )

    assert any(
        row["column_key"] == ["cat", "sch0", "table0", "column_0000"]
        and row["sql_role"] == "group"
        for row in evidence
    )

    ambiguous_inventory = build_inventory(
        [
            {
                "catalog_name": "cat",
                "schema_name": schema,
                "table_name": "shared",
                "column_name": "id",
                "data_type": "BIGINT",
                "ordinal_position": 1,
            }
            for schema in ("one", "two")
        ],
        _config([("cat", "one", "shared"), ("cat", "two", "shared")]),
        captured_at="2026-01-01T00:00:00Z",
    )
    ambiguous_diagnostics: dict[str, int] = {}
    assert sql_column_evidence(
        "SELECT id FROM shared",
        ambiguous_inventory,
        diagnostics=ambiguous_diagnostics,
    ) == []
    assert ambiguous_diagnostics["ambiguous_asset_references"] >= 1


def test_system_history_is_scoped_to_current_workspace():
    from databricks.sdk.service.sql import StatementState

    statements = []

    class Execution:
        def execute_statement(self, **kwargs):
            statements.append(kwargs["statement"])
            return SimpleNamespace(
                status=SimpleNamespace(state=StatementState.SUCCEEDED),
                manifest=None,
                result=None,
            )

    w = SimpleNamespace(
        get_workspace_id=lambda: 123456789,
        statement_execution=Execution(),
    )

    assert _system_history_rows(w, "warehouse-1", run_id="run-history") == []
    assert "workspace_id = 123456789" in statements[1]


def test_system_history_fails_closed_without_current_workspace_id():
    w = SimpleNamespace(get_workspace_id=lambda: None)

    with pytest.raises(RuntimeError, match="workspace ID"):
        _system_history_rows(w, "warehouse-1", run_id="run-history")


def test_rest_history_page_size_stays_below_api_limit():
    calls = []

    class QueryHistory:
        def list(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(res=[], has_next_page=False, next_page_token=None)

    _rest_history_rows(
        SimpleNamespace(query_history=QueryHistory()),
        ["warehouse-1"],
    )

    assert calls[0]["max_results"] == 999


def test_rest_history_visits_all_warehouses_before_global_cap(monkeypatch):
    monkeypatch.setattr(wide_schema_history, "MAX_REST_STATEMENTS", 2)
    calls = []

    class QueryHistory:
        def list(self, **kwargs):
            warehouse_id = kwargs["filter_by"].warehouse_ids[0]
            calls.append(warehouse_id)
            return SimpleNamespace(
                res=[{
                    "query_id": f"query-{warehouse_id}",
                    "query_text": "SELECT 1",
                    "query_start_time_ms": 1,
                }],
                has_next_page=False,
                next_page_token=None,
            )

    rows, inaccessible = _rest_history_rows(
        SimpleNamespace(query_history=QueryHistory()),
        ["warehouse-1", "warehouse-2"],
    )

    assert calls == ["warehouse-1", "warehouse-2"]
    assert len(rows) == 2
    assert inaccessible == []


def test_metric_view_listed_under_tables_uses_yaml_roles():
    config = _config([("cat", "sch", "sales_metrics")])
    config["_metric_view_yaml"] = {
        "cat.sch.sales_metrics": {
            "measures": [{"name": "total_revenue", "expr": "SUM(revenue)"}],
        },
    }
    inventory = build_inventory(
        [
            {
                "catalog_name": "cat",
                "schema_name": "sch",
                "table_name": "sales_metrics",
                "column_name": "region",
                "data_type": "STRING",
                "table_type": "METRIC_VIEW",
            },
            {
                "catalog_name": "cat",
                "schema_name": "sch",
                "table_name": "sales_metrics",
                "column_name": "total_revenue",
                "data_type": "DOUBLE",
                "table_type": "METRIC_VIEW",
            },
        ],
        config,
        captured_at="2026-01-01T00:00:00Z",
    )

    asset = inventory["assets"][0]
    roles = {column["name"]: column["metric_role"] for column in asset["columns"]}
    assert asset["asset_type"] == "metric_view"
    assert roles == {"region": "dimension", "total_revenue": "measure"}


def test_collect_inventory_falls_back_for_only_missing_rest_assets(monkeypatch):
    refs = [("cat", "sch", "orders"), ("cat", "sch", "customers")]
    config = _config(refs)
    rest_row = {
        "catalog_name": "cat",
        "schema_name": "sch",
        "table_name": "orders",
        "column_name": "order_id",
        "data_type": "BIGINT",
    }
    fallback_row = {
        "catalog_name": "cat",
        "schema_name": "sch",
        "table_name": "customers",
        "column_name": "customer_id",
        "data_type": "BIGINT",
    }
    fallback_calls = []

    class Row:
        def asDict(self, recursive=False):
            assert recursive is True
            return dict(fallback_row)

    def fallback_columns(_spark, missing_refs):
        fallback_calls.append(missing_refs)
        return SimpleNamespace(collect=lambda: [Row()])

    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_columns_for_tables_rest",
        lambda _w, _refs: [dict(rest_row)],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_columns_for_tables",
        fallback_columns,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_foreign_keys_for_tables_rest",
        lambda _w, _refs: [],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_foreign_keys_for_tables",
        lambda _spark, _refs: [],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.metric_view_catalog.detect_metric_views_via_catalog_with_outcomes",
        lambda *_args, **_kwargs: ([], {}, {}),
    )

    inventory, uc_columns, _foreign_keys = collect_inventory(
        SimpleNamespace(),
        SimpleNamespace(),
        config,
        refs,
        warehouse_id="warehouse-1",
    )

    assert fallback_calls == [[("cat", "sch", "customers")]]
    assert len(uc_columns) == 2
    assert {asset["asset_key"][-1] for asset in inventory["assets"]} == {
        "orders",
        "customers",
    }
