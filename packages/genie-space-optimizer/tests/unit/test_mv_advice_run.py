"""MV-D23 sentinel advice run: born-terminal writer + the pinned exclusion.

Guardrail (i) — advice runs are born terminal (never QUEUED/IN_PROGRESS), so
``wh_reconcile_active_runs`` can never adopt one. Guardrail (ii) — the run-kind
exclusion lives in exactly one constant/helper, not a per-caller convention, so
a run-listing query cannot forget the filter in one place.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.common import warehouse
from genie_space_optimizer.common.config import (
    MV_ADVICE_RUN_EXCLUSION,
    MV_ADVICE_RUN_STATUS,
    MV_RUN_KIND_ADVICE,
    MV_RUN_KIND_OPTIMIZATION,
    mv_advice_run_exclusion,
)


class _FakeWorkspaceClient:
    pass


@pytest.fixture
def executed(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: statements.append(sql),
    )
    return statements


# ── Guardrail (ii): the exclusion predicate is one pinned constant ─────────


def test_exclusion_predicate_excludes_advice_and_tolerates_legacy_null():
    # A legacy row (NULL run_kind) is an optimization run and must NOT be hidden.
    assert MV_ADVICE_RUN_EXCLUSION == (
        f"COALESCE(run_kind, '{MV_RUN_KIND_OPTIMIZATION}') <> '{MV_RUN_KIND_ADVICE}'"
    )


def test_exclusion_predicate_can_be_table_qualified():
    assert mv_advice_run_exclusion("r") == (
        f"COALESCE(r.run_kind, '{MV_RUN_KIND_OPTIMIZATION}') <> '{MV_RUN_KIND_ADVICE}'"
    )
    assert mv_advice_run_exclusion() == MV_ADVICE_RUN_EXCLUSION


def test_advice_status_is_not_an_active_status():
    # wh_reconcile_active_runs' active set is {QUEUED, IN_PROGRESS}; born-terminal
    # means the advice status is neither.
    assert MV_ADVICE_RUN_STATUS not in {"QUEUED", "IN_PROGRESS"}


# ── Guardrail (i): the advice run is born terminal ─────────────────────────


def _advice_sql(executed, **overrides):
    kwargs = {
        "run_id": "adv-1",
        "space_id": "space-1",
        "domain": "revenue",
        "catalog": "main",
        "schema": "gso",
    }
    kwargs.update(overrides)
    warehouse.wh_create_advice_run(_FakeWorkspaceClient(), "wh1", **kwargs)
    return executed[-1]


def test_advice_run_is_written_terminal_never_queued(executed):
    sql = _advice_sql(executed)
    assert f"'{MV_ADVICE_RUN_STATUS}'" in sql
    assert "'QUEUED'" not in sql
    assert "'IN_PROGRESS'" not in sql


def test_advice_run_carries_the_advice_kind_and_a_completion_timestamp(executed):
    sql = _advice_sql(executed)
    assert f"'{MV_RUN_KIND_ADVICE}'" in sql
    assert "completed_at" in sql


def test_advice_run_ran_no_eval(executed):
    sql = _advice_sql(executed)
    # zero iterations, empty lever list — it never ran a benchmark eval.
    assert "0, '[]'" in sql


# ── wh_create_run defaults are unchanged (optimization path) ───────────────


def _run_sql(executed, **overrides):
    kwargs = {
        "run_id": "run-1",
        "space_id": "space-1",
        "domain": "revenue",
        "catalog": "main",
        "schema": "gso",
    }
    kwargs.update(overrides)
    warehouse.wh_create_run(_FakeWorkspaceClient(), "wh1", **kwargs)
    return executed[-1]


def test_default_run_is_queued_optimization_without_completion(executed):
    sql = _run_sql(executed)
    assert "'QUEUED'" in sql
    assert f"'{MV_RUN_KIND_OPTIMIZATION}'" in sql
    assert "completed_at" not in sql
