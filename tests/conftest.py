"""Shared fixtures for the Genie Space Optimizer test suite."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, PropertyMock

import pandas as pd
import pytest

from genie_space_optimizer.common.config import MAX_ITERATIONS


@pytest.fixture
def mock_spark():
    """Mock SparkSession that returns empty DataFrames by default."""
    spark = MagicMock()
    spark.sql.return_value.toPandas.return_value = pd.DataFrame()
    spark.sql.return_value.collect.return_value = []
    return spark


@pytest.fixture
def mock_ws():
    """Mock Databricks WorkspaceClient with common stubs."""
    ws = MagicMock()
    ws.api_client.do.return_value = {
        "title": "Test Space",
        "description": "A test Genie Space",
        "create_time": "2025-01-01T00:00:00Z",
        "update_time": "2025-06-01T00:00:00Z",
        "serialized_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "catalog.schema.orders",
                        "description": ["Order data"],
                        "column_configs": [
                            {"column_name": "order_id", "description": ["Primary key"]},
                            {"column_name": "amount", "description": ["Order amount"]},
                        ],
                    }
                ]
            },
            "instructions": {
                "text_instructions": [
                    {"id": "instr_1", "content": ["Always filter by active orders."]}
                ],
                "example_question_sqls": [
                    {"question": "What is total revenue?"},
                    {"question": "Show top 10 orders"},
                ],
            },
        },
    }

    ws.jobs.list.return_value = [MagicMock(job_id=12345)]
    run_mock = MagicMock()
    run_mock.run_id = 67890
    ws.jobs.run_now.return_value = run_mock

    ws.serving_endpoints.query.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content='{"proposed_value": "test", "rationale": "test"}'))]
    )

    return ws


@pytest.fixture
def sample_run():
    """A representative optimization run dict as stored in Delta."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "run_id": "test-run-001",
        "space_id": "space-abc",
        "domain": "revenue",
        "catalog": "test_catalog",
        "uc_schema": "test_catalog.gold",
        "status": "CONVERGED",
        "started_at": now,
        "completed_at": now,
        "job_run_id": "67890",
        "max_iterations": MAX_ITERATIONS,
        "levers": [1, 2, 3, 4, 5],
        "apply_mode": "genie_config",
        "best_iteration": 3,
        "best_accuracy": 92.5,
        "best_repeatability": 95.0,
        "convergence_reason": "threshold_met",
        "experiment_name": "/Shared/genie-space-optimizer/test-space/revenue",
        "triggered_by": "test@example.com",
        "config_snapshot": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "catalog.schema.orders",
                        "description": ["Order data"],
                        "column_configs": [
                            {"column_name": "order_id", "description": ["Primary key"]},
                        ],
                    }
                ]
            },
            "instructions": {
                "text_instructions": [{"id": "instr_1", "content": ["Filter active."]}],
                "example_question_sqls": [],
            },
        },
    }


@pytest.fixture
def sample_metadata():
    """Metadata snapshot representing a Genie Space config."""
    return {
        "tables": [
            {
                "identifier": "catalog.schema.orders",
                "name": "orders",
                "description": ["Order data"],
                "column_configs": [
                    {"column_name": "order_id", "description": ["Primary key"]},
                    {"column_name": "amount", "description": ["Order amount"], "data_type": "DOUBLE"},
                    {"column_name": "status", "description": ["Active or inactive"], "data_type": "STRING"},
                ],
            },
            {
                "identifier": "catalog.schema.customers",
                "name": "customers",
                "description": ["Customer master data"],
                "column_configs": [
                    {"column_name": "customer_id", "description": ["Primary key"]},
                    {"column_name": "name", "description": ["Customer name"], "data_type": "STRING"},
                ],
            },
        ],
        "metric_views": [],
        "functions": [],
        "general_instructions": "Always use UTC timestamps.",
        "join_specs": [],
    }


@pytest.fixture
def sample_benchmarks():
    """A small set of benchmark questions for testing."""
    return [
        {
            "id": "rev_001",
            "question": "What is total revenue?",
            "expected_sql": "SELECT SUM(amount) FROM ${catalog}.${gold_schema}.orders",
            "expected_asset": "TABLE",
            "category": "aggregation",
            "required_tables": ["orders"],
            "required_columns": ["amount"],
            "priority": "P0",
            "split": "train",
        },
        {
            "id": "rev_002",
            "question": "Show top 10 customers by spend",
            "expected_sql": "SELECT customer_id, SUM(amount) AS spend FROM ${catalog}.${gold_schema}.orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            "expected_asset": "TABLE",
            "category": "ranking",
            "required_tables": ["orders"],
            "required_columns": ["customer_id", "amount"],
            "priority": "P0",
            "split": "train",
        },
        {
            "id": "rev_003",
            "question": "How many orders per month?",
            "expected_sql": "SELECT DATE_TRUNC('month', order_date), COUNT(*) FROM ${catalog}.${gold_schema}.orders GROUP BY 1",
            "expected_asset": "TABLE",
            "category": "time-series",
            "required_tables": ["orders"],
            "required_columns": ["order_date"],
            "priority": "P1",
            "split": "train",
        },
        {
            "id": "rev_004",
            "question": "List all active customers",
            "expected_sql": "SELECT * FROM ${catalog}.${gold_schema}.customers WHERE status = 'active'",
            "expected_asset": "TABLE",
            "category": "list",
            "required_tables": ["customers"],
            "required_columns": ["status"],
            "priority": "P1",
            "split": "held_out",
        },
    ]


@pytest.fixture
def sample_eval_results():
    """Mock evaluation results with per-judge feedback."""
    return {
        "eval_results": [
            {
                "inputs/question_id": "rev_001",
                "inputs/question": "What is total revenue?",
                "feedback/syntax_validity": "yes",
                "feedback/schema_accuracy": "yes",
                "feedback/result_correctness": "no",
                "rationale/result_correctness": "Wrong table used — should be orders, used invoices",
                "feedback/logical_accuracy": "yes",
                "feedback/completeness": "yes",
                "feedback/semantic_equivalence": "no",
                "rationale/semantic_equivalence": "Missing aggregation on amount column",
            },
            {
                "inputs/question_id": "rev_002",
                "inputs/question": "Show top 10 customers by spend",
                "feedback/syntax_validity": "yes",
                "feedback/schema_accuracy": "no",
                "rationale/schema_accuracy": "Wrong column name — used 'total' instead of 'amount'",
                "feedback/result_correctness": "no",
                "rationale/result_correctness": "Wrong table used — should be orders, used invoices",
                "feedback/logical_accuracy": "yes",
                "feedback/completeness": "yes",
                "feedback/semantic_equivalence": "yes",
            },
        ]
    }


@pytest.fixture
def sample_clusters():
    """Pre-built failure clusters for optimizer tests."""
    return [
        {
            "cluster_id": "C001",
            "root_cause": "wrong_table",
            "question_ids": ["rev_001", "rev_002"],
            "affected_judge": "result_correctness",
            "confidence": 0.7,
            "asi_failure_type": "wrong_table",
            "asi_blame_set": "orders",
            "asi_counterfactual_fixes": ["Use orders table instead of invoices"],
        },
        {
            "cluster_id": "C002",
            "root_cause": "wrong_aggregation",
            "question_ids": ["rev_001", "rev_003"],
            "affected_judge": "semantic_equivalence",
            "confidence": 0.6,
            "asi_failure_type": "wrong_aggregation",
            "asi_blame_set": None,
            "asi_counterfactual_fixes": [],
        },
    ]
