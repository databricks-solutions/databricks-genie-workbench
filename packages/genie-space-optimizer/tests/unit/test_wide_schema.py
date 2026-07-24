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
    merge_query_history_evidence,
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


def test_query_history_breaks_wide_metric_field_ties_without_overriding_hard_requirements():
    inventory = _inventory(columns=1_000, table_type="METRIC_VIEW")
    config = _config([("cat", "sch0", "table0")])
    config["_parsed_space"]["instructions"] = {
        "sql": "SELECT column_0000 FROM cat.sch0.table0",
    }
    local = build_local_evidence(config, inventory)
    history = {
        "contract_version": 1,
        "inventory_hash": inventory["inventory_hash"],
        "source_mode": "system_table",
        "source_scope": ["system.query.history"],
        "coverage": {"accepted_statements": 2},
        "degradation_counts": {},
        "source_attempts": {
            "system_table": "succeeded",
            "warehouse_api": "not_attempted",
        },
        "source_errors": {},
        "warnings": [],
        "columns": [
            {
                "column_key": ["cat", "sch0", "table0", "column_0999"],
                "column_id": "cat.sch0.table0.column_0999",
                "evidence_score": 20.0,
            },
            {
                "column_key": ["cat", "sch0", "table0", "column_0500"],
                "column_id": "cat.sch0.table0.column_0500",
                "evidence_score": 10.0,
            },
        ],
    }

    evidence = merge_query_history_evidence(local, history)
    plan = build_selection_plan(inventory, evidence, run_id="run-history-ranking")
    rows = {
        row["name"]: row
        for row in plan["assets"][0]["columns"]
    }

    assert rows["column_0000"]["stable_rank"] == 1
    assert rows["column_0000"]["priority"] == 2
    assert rows["column_0999"]["stable_rank"] == 2
    assert rows["column_0999"]["query_history_score"] == 20.0
    assert rows["column_0500"]["stable_rank"] == 3
    assert rows["column_0999"]["active"] is True
    assert plan["assets"][0]["active_count"] == 45
    assert plan["assets"][0]["required_overflow_count"] == 0
    assert plan["evidence_source_attempts"]["system_table"] == "succeeded"


def test_descriptions_and_synonyms_do_not_enable_column_behavior():
    inventory = _inventory(columns=2)
    config = _config([("cat", "sch0", "table0")])
    config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"] = [
        {
            "column_name": "column_0000",
            "description": "Important business metadata",
            "synonyms": ["business field"],
        },
        {
            "column_name": "column_0001",
            "enable_entity_matching": True,
        },
    ]

    evidence = build_local_evidence(config, inventory)
    rows = {
        row["column_key"][-1]: row
        for row in evidence["columns"]
    }

    assert "COLUMN_BEHAVIOR" not in rows["column_0000"]["reason_codes"]
    assert "COLUMN_BEHAVIOR" in rows["column_0001"]["reason_codes"]


def test_column_behavior_is_soft_and_query_history_ranks_ahead_of_it():
    inventory = _inventory(columns=100, table_type="METRIC_VIEW")
    config = _config([("cat", "sch0", "table0")])
    config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"] = [
        {
            "column_name": f"column_{index:04d}",
            "enable_entity_matching": True,
        }
        for index in range(100)
    ]
    local = build_local_evidence(config, inventory)
    history = {
        "contract_version": 1,
        "inventory_hash": inventory["inventory_hash"],
        "source_mode": "system_table",
        "source_scope": ["system.query.history"],
        "coverage": {"accepted_statements": 1},
        "degradation_counts": {},
        "columns": [{
            "column_key": ["cat", "sch0", "table0", "column_0099"],
            "column_id": "`cat`.`sch0`.`table0`.`column_0099`",
            "evidence_score": 10.0,
        }],
    }

    plan = build_selection_plan(
        inventory,
        merge_query_history_evidence(local, history),
        run_id="run-soft-behavior",
    )
    rows = {row["name"]: row for row in plan["assets"][0]["columns"]}

    assert rows["column_0099"]["stable_rank"] == 1
    assert rows["column_0099"]["query_history_score"] == 10.0
    assert rows["column_0099"]["column_behavior_score"] == 1.0
    assert plan["assets"][0]["required_overflow_count"] == 0


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


def test_history_frequency_space_weight_and_exclusion_telemetry():
    inventory = _inventory(columns=3)
    now_ms = int(time.time() * 1000)
    common = {
        "statement_type": "SELECT",
        "query_start_time_ms": now_ms,
        "executed_by": "alice@example.com",
        "query_source": {"genie_space_id": "space-1"},
    }
    evidence = normalize_history_rows(
        [
            {
                **common,
                "statement_text": (
                    "SELECT column_0000 FROM cat.sch0.table0 "
                    "WHERE column_0001 = 'first'"
                ),
            },
            {
                **common,
                "statement_text": (
                    "SELECT column_0000 FROM cat.sch0.table0 "
                    "WHERE column_0001 = 'second'"
                ),
            },
            {
                **common,
                "executed_by": "gso-app",
                "statement_text": "SELECT column_0002 FROM cat.sch0.table0",
            },
            {
                **common,
                "executed_as": "gso-app",
                "statement_text": "SELECT column_0002 FROM cat.sch0.table0",
            },
            {
                **common,
                "statement_type": "CREATE",
                "statement_text": "CREATE TABLE cat.sch0.other (id INT)",
            },
        ],
        inventory,
        source_mode="system_table",
        source_scope=["system.query.history"],
        service_principal_identities={"gso-app"},
        target_space_id="space-1",
        max_statements=10,
    )
    rows = {row["column_key"][-1]: row for row in evidence["columns"]}

    assert evidence["coverage"]["accepted_statements"] == 2
    assert evidence["coverage"]["distinct_query_shapes"] == 1
    assert evidence["coverage"]["target_space_statements"] == 2
    assert evidence["degradation_counts"]["duplicate_shape"] == 1
    assert evidence["degradation_counts"]["excluded_service_principal"] == 2
    assert evidence["degradation_counts"]["non_select"] == 1
    assert rows["column_0001"]["query_occurrence_count"] == 2
    assert rows["column_0001"]["target_space_query_shape_count"] == 1
    assert rows["column_0001"]["evidence_score"] > 10.0


def test_databricks_generated_profile_shapes_are_excluded_without_broad_false_positives():
    inventory = _inventory(columns=3)
    now_ms = int(time.time() * 1000)
    common = {
        "statement_type": "SELECT",
        "query_start_time_ms": now_ms,
        "executed_by": "alice@example.com",
    }
    evidence = normalize_history_rows(
        [
            {
                **common,
                "statement_text": (
                    "WITH SampledData AS (SELECT column_0000 FROM cat.sch0.table0) "
                    "SELECT COUNT(*) AS sample_size, "
                    "COUNT_IF(column_0000 IS NULL) AS _null_count, "
                    "COUNT(DISTINCT column_0000) AS _distinct_count FROM SampledData"
                ),
            },
            {
                **common,
                "statement_text": (
                    "SELECT item.item AS value FROM "
                    "(SELECT approx_top_k(column_0001, 10) AS items "
                    "FROM cat.sch0.table0) src "
                    "LATERAL VIEW explode(items) exploded AS item"
                ),
            },
            {
                **common,
                "statement_text": (
                    "WITH SampledData AS (SELECT column_0000 FROM cat.sch0.table0) "
                    "SELECT column_0000 FROM SampledData"
                ),
            },
            {
                **common,
                "statement_text": (
                    "SELECT approx_top_k(column_0002, 5) AS popular_values "
                    "FROM cat.sch0.table0"
                ),
            },
        ],
        inventory,
        source_mode="system_table",
        source_scope=["system.query.history"],
        max_statements=10,
    )

    assert evidence["coverage"]["accepted_statements"] == 2
    assert evidence["degradation_counts"]["excluded_databricks_sample_profile"] == 1
    assert evidence["degradation_counts"]["excluded_databricks_top_k_profile"] == 1
    assert evidence["degradation_counts"]["gso_excluded"] == 2
    assert {row["column_key"][-1] for row in evidence["columns"]} == {
        "column_0000",
        "column_0002",
    }


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

    rows, telemetry = _system_history_rows(
        w,
        "warehouse-1",
        run_id="run-history",
        inventory=_inventory(columns=3),
        target_space_id="space-1",
        gso_job_id="job-1",
        service_principal_identities={"gso-app"},
    )
    assert rows == []
    assert telemetry["workspace_scoped"] is True
    assert len(statements) == 1
    assert "workspace_id = 123456789" in statements[0]
    assert "statement_type = 'SELECT'" in statements[0]
    assert "query_source.genie_space_id = 'space-1'" in statements[0]
    assert "cat.sch0.table0" in statements[0]
    assert "job-1" in statements[0]
    assert "gso-app" in statements[0]
    assert "lower(coalesce(executed_as, '')) NOT IN" in statements[0]
    assert "with sampleddata" in statements[0]
    assert "_null_count" in statements[0]
    assert "_distinct_count" in statements[0]
    assert "approx_top_k " in statements[0]
    assert "item item as value" in statements[0]
    assert telemetry["server_side_generated_profile_filter"] is True
    assert statements[0].index("statement_type = 'SELECT'") < statements[0].index("ORDER BY")
    assert statements[0].index("gso-app") < statements[0].index("ORDER BY")
    assert (
        "CASE WHEN query_source.genie_space_id = 'space-1' THEN 0 ELSE 1 END"
        in statements[0]
    )
    assert "warehouse_id" not in statements[0]


def test_system_history_fails_closed_without_current_workspace_id():
    w = SimpleNamespace(get_workspace_id=lambda: None)

    with pytest.raises(RuntimeError, match="workspace ID"):
        _system_history_rows(
            w,
            "warehouse-1",
            run_id="run-history",
            inventory=_inventory(columns=3),
        )


def test_system_history_fetches_every_inline_result_chunk():
    from databricks.sdk.service.sql import StatementState

    names = [
        "statement_id", "statement_text", "statement_type", "start_time",
        "executed_by", "executed_as", "query_source", "query_tags",
    ]
    columns = [SimpleNamespace(name=name) for name in names]
    first = [
        "statement-1", "SELECT column_0000 FROM cat.sch0.table0", "SELECT",
        "2026-01-01T00:00:00Z", "alice", "alice", "{}", "{}",
    ]
    second = [
        "statement-2", "SELECT column_0001 FROM cat.sch0.table0", "SELECT",
        "2026-01-02T00:00:00Z", "alice", "alice", "{}", "{}",
    ]
    fetched = []

    class Execution:
        def execute_statement(self, **_kwargs):
            return SimpleNamespace(
                status=SimpleNamespace(state=StatementState.SUCCEEDED),
                statement_id="history-statement",
                manifest=SimpleNamespace(
                    schema=SimpleNamespace(columns=columns),
                    total_chunk_count=2,
                    total_row_count=2,
                    truncated=False,
                ),
                result=SimpleNamespace(
                    data_array=[first],
                    next_chunk_index=1,
                ),
            )

        def get_statement_result_chunk_n(self, **kwargs):
            fetched.append(kwargs)
            return SimpleNamespace(data_array=[second], next_chunk_index=None)

    rows, telemetry = _system_history_rows(
        SimpleNamespace(
            get_workspace_id=lambda: 123456789,
            statement_execution=Execution(),
        ),
        "warehouse-1",
        run_id="run-history",
        inventory=_inventory(columns=3),
    )

    assert [row["statement_id"] for row in rows] == ["statement-1", "statement-2"]
    assert fetched == [{"statement_id": "history-statement", "chunk_index": 1}]
    assert telemetry["chunks_fetched"] == 2
    assert telemetry["rows_returned"] == 2


def test_history_collection_persists_clear_degradation_diagnostics(monkeypatch):
    inventory = _inventory(columns=3)
    wide_schema_history._AGGREGATE_CACHE.clear()
    monkeypatch.setattr(
        wide_schema_history,
        "_system_history_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "[UNRESOLVED_COLUMN.WITH_SUGGESTION] warehouse_id is unavailable"
            )
        ),
    )

    history = wide_schema_history.collect_query_history_evidence(
        SimpleNamespace(),
        inventory,
        profiling_warehouse_id="warehouse-1",
        workload_warehouse_ids=[],
        run_id="run-history-unavailable",
    )
    evidence = merge_query_history_evidence(
        build_local_evidence(_config([("cat", "sch0", "table0")]), inventory),
        history,
    )
    plan = build_selection_plan(inventory, evidence, run_id="run-history-unavailable")

    assert history["source_mode"] == "none"
    assert history["source_attempts"] == {
        "system_table": "failed",
        "warehouse_api": "not_configured",
    }
    assert history["degradation_counts"]["history_unavailable"] == 1
    assert history["source_errors"] == {
        "system_table": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    }
    assert "warehouse_id" not in json.dumps(history)
    assert history["warnings"]
    assert plan["evidence_source_errors"] == history["source_errors"]
    assert plan["evidence_warnings"] == history["warnings"]


def test_history_collection_expands_to_ninety_days_when_signal_is_sparse(monkeypatch):
    inventory = _inventory(columns=3)
    wide_schema_history._AGGREGATE_CACHE.clear()
    lookbacks = []
    now_ms = int(time.time() * 1000)

    def fake_system_rows(*_args, lookback_days, **_kwargs):
        lookbacks.append(lookback_days)
        rows = []
        if lookback_days == wide_schema_history.EXTENDED_HISTORY_LOOKBACK_DAYS:
            rows = [{
                "statement_text": "SELECT column_0000 FROM cat.sch0.table0",
                "statement_type": "SELECT",
                "query_start_time_ms": now_ms,
                "query_source": {"genie_space_id": "space-1"},
                "executed_by": "alice@example.com",
            }]
        return rows, {
            "lookback_days": lookback_days,
            "chunks_fetched": 1,
            "rows_returned": len(rows),
        }

    monkeypatch.setattr(
        wide_schema_history,
        "_system_history_rows",
        fake_system_rows,
    )
    history = wide_schema_history.collect_query_history_evidence(
        SimpleNamespace(),
        inventory,
        profiling_warehouse_id="warehouse-1",
        workload_warehouse_ids=[],
        run_id="run-adaptive-history",
        target_space_id="space-1",
    )

    assert lookbacks == [30, 90]
    assert history["coverage"]["accepted_statements"] == 1
    assert history["coverage"]["target_space_statements"] == 1
    assert history["coverage"]["read_telemetry"]["lookback_days"] == 90
    assert "Expanded query-history lookback" in history["warnings"][0]


def test_history_collection_falls_back_when_system_rows_have_no_columns(monkeypatch):
    inventory = _inventory(columns=3)
    wide_schema_history._AGGREGATE_CACHE.clear()
    now_ms = int(time.time() * 1000)

    monkeypatch.setattr(
        wide_schema_history,
        "_system_history_rows",
        lambda *_args, lookback_days, **_kwargs: (
            [],
            {"lookback_days": lookback_days, "rows_returned": 0},
        ),
    )
    monkeypatch.setattr(
        wide_schema_history,
        "_rest_history_rows",
        lambda *_args, **_kwargs: ([{
            "statement_text": "SELECT column_0001 FROM cat.sch0.table0",
            "query_start_time_ms": now_ms,
            "executed_by": "alice@example.com",
        }], []),
    )

    history = wide_schema_history.collect_query_history_evidence(
        SimpleNamespace(),
        inventory,
        profiling_warehouse_id="warehouse-1",
        workload_warehouse_ids=["workload-1"],
        run_id="run-empty-system-fallback",
    )

    assert history["source_mode"] == "warehouse_api"
    assert history["coverage"]["accepted_statements"] == 1
    assert history["source_attempts"] == {
        "system_table": "succeeded_no_usable_rows",
        "warehouse_api": "succeeded",
    }
    assert history["degradation_counts"]["system_history_no_usable_statements"] == 1
    assert "no usable rows" in history["warnings"][-1]

    no_fallback = wide_schema_history.collect_query_history_evidence(
        SimpleNamespace(),
        inventory,
        profiling_warehouse_id="warehouse-1",
        workload_warehouse_ids=[],
        run_id="run-empty-system-no-fallback",
    )
    assert no_fallback["source_mode"] == "none"
    assert no_fallback["source_scope"] == []
    assert no_fallback["source_attempts"] == {
        "system_table": "succeeded_no_usable_rows",
        "warehouse_api": "not_configured",
    }


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
