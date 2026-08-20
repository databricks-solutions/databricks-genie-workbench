"""GSO Optimizer v2 — Phase 2 benchmark provenance ledger (§3.5).

The Delta DDL and the write path for ``genie_opt_benchmark_mutations``.
The backend endpoint + UI view that consume this ledger are Phase 6.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.common.config import TABLE_BENCHMARK_MUTATIONS
from genie_space_optimizer.optimization import state as state_mod
from genie_space_optimizer.optimization.ddl import _ALL_DDL


def test_ledger_table_registered_in_all_ddl():
    assert TABLE_BENCHMARK_MUTATIONS == "genie_opt_benchmark_mutations"
    assert TABLE_BENCHMARK_MUTATIONS in _ALL_DDL
    ddl = _ALL_DDL[TABLE_BENCHMARK_MUTATIONS]
    for col in ("run_id", "question_id", "op", "before", "after", "reason", "logged_at"):
        assert col in ddl, f"{col} missing from benchmark-mutations DDL"
    # Versioned history for the diff (CDF), consistent with sibling tables.
    assert "delta.enableChangeDataFeed" in ddl
    assert "PARTITIONED BY (run_id)" in ddl
    assert "excluded (run-local, non-mutating)" in ddl


def test_write_benchmark_mutations_serializes_and_filters():
    rows = [
        {
            "question_id": "q1",
            "op": "added",
            "before": None,
            "after": {"question": "new q", "sql": "SELECT 1"},
            "reason": "preflight_push",
        },
        {
            "question_id": "q2",
            "op": "excluded",
            "before": {"question": "bad q", "sql": "SELECT broken"},
            "after": {"question": "bad q", "sql": "SELECT broken"},
            "reason": "sql_compile_error",
        },
        # Bad op must be skipped, not written.
        {"question_id": "q3", "op": "noop"},
    ]

    captured: list[dict] = []

    def _capture(spark, catalog, schema, table, payload):
        captured.append(payload)

    with patch.object(state_mod, "insert_row", side_effect=_capture):
        written = state_mod.write_benchmark_mutations(
            MagicMock(), "run-xyz", rows, catalog="cat", schema="sch",
        )

    assert written == 2
    ops = {p["op"] for p in captured}
    assert ops == {"added", "excluded"}

    added = next(p for p in captured if p["op"] == "added")
    assert added["run_id"] == "run-xyz"
    assert added["before"] is None
    assert json.loads(added["after"]) == {"question": "new q", "sql": "SELECT 1"}
    assert added["reason"] == "preflight_push"

    excluded = next(p for p in captured if p["op"] == "excluded")
    expected = {"question": "bad q", "sql": "SELECT broken"}
    assert json.loads(excluded["before"]) == expected
    assert json.loads(excluded["after"]) == expected


def test_write_benchmark_mutations_keeps_legacy_removed_rows_compatible():
    rows = [{
        "question_id": "legacy-q",
        "op": "removed",
        "before": {"question": "legacy", "sql": "SELECT 1"},
        "after": None,
    }]
    with patch.object(state_mod, "insert_row") as insert:
        written = state_mod.write_benchmark_mutations(
            MagicMock(), "run-xyz", rows, catalog="cat", schema="sch",
        )
    assert written == 1
    assert insert.call_args.args[-1]["op"] == "removed"


def test_write_benchmark_mutations_empty_is_noop():
    with patch.object(state_mod, "insert_row") as ins:
        written = state_mod.write_benchmark_mutations(
            MagicMock(), "run-xyz", [], catalog="cat", schema="sch",
        )
    assert written == 0
    ins.assert_not_called()
