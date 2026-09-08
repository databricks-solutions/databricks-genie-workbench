"""Unit tests for the L and D signal producers (Prompt 6a).

Both producers depend only on an injected ``run_query`` seam, so every case here
runs against fixture warehouse rows — no workspace, no warehouse, no network. The
suite covers the three MV-D15 outcomes (COMPUTED, EMPTY, each UNAVAILABLE
reason), the MV-D10(b) firewall (no history literal reaches the payload), and the
MV-D19 grant pin (column-grain L requires the ``column_lineage`` SELECT).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from genie_space_optimizer.common.config import (
    MV_DEMAND_HISTORY_LOOKBACK_DAYS,
    MV_SIGNAL_COMPUTED,
    MV_SIGNAL_EMPTY,
    MV_SIGNAL_UNAVAILABLE,
)
from genie_space_optimizer.optimization.mv_fingerprint import corpus_scan
from genie_space_optimizer.optimization.mv_scoring import (
    REFERENCE_GOVERNED_MV,
    REFERENCE_LINEAGE_FOOTPRINT,
    DemandSignal,
    LineageOverlap,
    demand_score,
    lineage_overlap_score,
)
from genie_space_optimizer.optimization.mv_signals import (
    REASON_EMPTY_STATEMENT_TEXT,
    REASON_MISSING_GRANT,
    REASON_MISSING_TABLE,
    REASON_NO_REFERENCE,
    REASON_NO_SCOPE,
    REASON_READ_FAILED,
    SignalResult,
    demand_signal,
    lineage_signal,
    warehouse_reader,
)


# ── Fixtures ─────────────────────────────────────────────────────────────

REVENUE_SQL = (
    "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue "
    "FROM samples.tpch.lineitem"
)
REVENUE_SQL_ALIASED = (
    "SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) "
    "FROM samples.tpch.lineitem l"
)
LINEITEM = "samples.tpch.lineitem"


def _revenue_fingerprint() -> str:
    """The measure fingerprint a candidate for discounted revenue would carry.

    Derived from the canonicalizer rather than hard-coded, so the join the D
    producer performs is exercised against the real fingerprint, not a guess.
    """
    scan = corpus_scan([REVENUE_SQL])
    return scan.measures[0].fingerprint


class _Reader:
    """A capturing ``run_query`` fake: records the SQL, returns rows or raises."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, error: Exception | None = None):
        self.rows = rows or []
        self.error = error
        self.sql: str | None = None
        self.calls = 0

    def __call__(self, sql: str) -> list[dict[str, Any]]:
        self.calls += 1
        self.sql = sql
        if self.error is not None:
            raise self.error
        return self.rows


NOW = datetime(2026, 8, 24, 10, 0, 0, tzinfo=timezone.utc)


def _history_rows() -> list[dict[str, Any]]:
    return [
        {
            "statement_id": "s1",
            "executed_by": "analyst.a@example.com",
            "start_time": "2026-08-20T10:00:00Z",
            "total_duration_ms": 1000,
            "statement_text": REVENUE_SQL,
        },
        {
            "statement_id": "s2",
            "executed_by": "analyst.b@example.com",
            "start_time": "2026-08-22T10:00:00Z",
            "total_duration_ms": 2500,
            "statement_text": REVENUE_SQL_ALIASED,
        },
        {
            "statement_id": "s3",
            "executed_by": "analyst.a@example.com",
            "start_time": "2026-08-15T10:00:00Z",
            "total_duration_ms": 500,
            "statement_text": "SELECT COUNT(*) FROM samples.tpch.orders",
        },
    ]


# ── D: COMPUTED ──────────────────────────────────────────────────────────


def test_demand_computed_from_matching_traffic() -> None:
    reader = _Reader(rows=_history_rows())
    res = demand_signal(
        space_id="sp1",
        candidate_fingerprints={_revenue_fingerprint()},
        run_query=reader,
        now=NOW,
    )
    assert res.status == MV_SIGNAL_COMPUTED
    assert res.reason == ""
    demand = res.payload
    assert isinstance(demand, DemandSignal)
    # s1 + s2 re-derive the revenue measure; s3 (orders count) does not.
    assert demand.frequency == 2
    assert demand.distinct_users == 2
    assert demand.cost_ms == 3500.0
    # Age is measured from the most recent match (s2 @ 2026-08-22), not the mean.
    assert 1.9 < demand.age_days < 2.1
    assert demand_score(demand) >= 0.0


def test_demand_read_is_space_scoped_and_windowed() -> None:
    reader = _Reader(rows=_history_rows())
    demand_signal(
        space_id="sp1",
        candidate_fingerprints={_revenue_fingerprint()},
        run_query=reader,
        now=NOW,
    )
    assert reader.sql is not None
    assert "system.query.history" in reader.sql
    assert "query_source.genie_space_id = 'sp1'" in reader.sql
    assert f"- {MV_DEMAND_HISTORY_LOOKBACK_DAYS}" in reader.sql


# ── D: EMPTY ─────────────────────────────────────────────────────────────


def test_demand_empty_when_no_history_in_window() -> None:
    reader = _Reader(rows=[])
    res = demand_signal(
        space_id="sp1", candidate_fingerprints={"fp"}, run_query=reader, now=NOW
    )
    assert res.status == MV_SIGNAL_EMPTY
    assert res.payload == DemandSignal()


def test_demand_empty_when_traffic_never_re_derives_the_measure() -> None:
    rows = [r for r in _history_rows() if r["statement_id"] == "s3"]
    reader = _Reader(rows=rows)
    res = demand_signal(
        space_id="sp1",
        candidate_fingerprints={_revenue_fingerprint()},
        run_query=reader,
        now=NOW,
    )
    assert res.status == MV_SIGNAL_EMPTY
    assert "no matching measure" in res.reason


def test_demand_empty_when_candidate_has_no_fingerprints_reads_nothing() -> None:
    reader = _Reader(rows=_history_rows())
    res = demand_signal(
        space_id="sp1", candidate_fingerprints=[], run_query=reader, now=NOW
    )
    assert res.status == MV_SIGNAL_EMPTY
    assert reader.calls == 0  # nothing to match, so no read is issued


# ── D: UNAVAILABLE (every reason) ────────────────────────────────────────


def test_demand_unavailable_when_space_id_missing_reads_nothing() -> None:
    reader = _Reader(rows=_history_rows())
    res = demand_signal(
        space_id="", candidate_fingerprints={"fp"}, run_query=reader, now=NOW
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(REASON_NO_SCOPE)
    assert reader.calls == 0


def test_demand_unavailable_on_empty_statement_text_cmk_redaction() -> None:
    rows = [
        {
            "statement_id": "s1",
            "executed_by": "a",
            "start_time": "2026-08-20T10:00:00Z",
            "total_duration_ms": 10,
            "statement_text": "",
        },
        {
            "statement_id": "s2",
            "executed_by": "b",
            "start_time": "2026-08-21T10:00:00Z",
            "total_duration_ms": 10,
            "statement_text": "   ",
        },
    ]
    reader = _Reader(rows=rows)
    res = demand_signal(
        space_id="sp1", candidate_fingerprints={"fp"}, run_query=reader, now=NOW
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(REASON_EMPTY_STATEMENT_TEXT)


@pytest.mark.parametrize(
    "message, expected",
    [
        ("[PERMISSION_DENIED] principal does not have SELECT", REASON_MISSING_GRANT),
        ("TABLE_OR_VIEW_NOT_FOUND: system.query.history", REASON_MISSING_TABLE),
        ("connection reset by peer", REASON_READ_FAILED),
    ],
)
def test_demand_unavailable_classifies_read_failure(message: str, expected: str) -> None:
    reader = _Reader(error=RuntimeError(message))
    res = demand_signal(
        space_id="sp1", candidate_fingerprints={"fp"}, run_query=reader, now=NOW
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(expected)
    assert "system.query.history" in res.reason


# ── D: firewall (MV-D10(b)) ──────────────────────────────────────────────


def test_demand_never_carries_a_history_literal() -> None:
    """The producer fingerprints raw statement_text but stores none of it. A
    statement laden with PII still matches the measure, and nothing about the
    literal reaches the payload, the reason, or the serialized result."""
    rows = [
        {
            "statement_id": "s1",
            "executed_by": "analyst.a@example.com",
            "start_time": "2026-08-20T10:00:00Z",
            "total_duration_ms": 1000,
            "statement_text": REVENUE_SQL + " WHERE l_comment = 'jdoe@example.com'",
        }
    ]
    reader = _Reader(rows=rows)
    res = demand_signal(
        space_id="sp1",
        candidate_fingerprints={_revenue_fingerprint()},
        run_query=reader,
        now=NOW,
    )
    assert res.status == MV_SIGNAL_COMPUTED
    blob = json.dumps(res.to_dict())
    assert "jdoe@example.com" not in blob
    assert "l_comment" not in blob


# ── L: footprint COMPUTED / EMPTY (MV-D19 column grain) ──────────────────


def _footprint_rows() -> list[dict[str, Any]]:
    return [
        {"source_table_full_name": LINEITEM, "source_column_name": "l_extendedprice"},
        {"source_table_full_name": LINEITEM, "source_column_name": "l_discount"},
        {"source_table_full_name": LINEITEM, "source_column_name": "l_quantity"},
    ]


def test_lineage_footprint_computed_is_column_jaccard() -> None:
    reader = _Reader(rows=_footprint_rows())
    res = lineage_signal(
        candidate_columns=["l_extendedprice", "l_discount"],
        source_tables=[LINEITEM],
        space_id="sp1",
        run_query=reader,
    )
    assert res.status == MV_SIGNAL_COMPUTED
    overlap = res.payload
    assert isinstance(overlap, LineageOverlap)
    assert overlap.reference_kind == REFERENCE_LINEAGE_FOOTPRINT
    assert overlap.candidate_columns == frozenset({"l_extendedprice", "l_discount"})
    assert overlap.reference_columns == frozenset(
        {"l_extendedprice", "l_discount", "l_quantity"}
    )
    assert lineage_overlap_score(overlap) == pytest.approx(2 / 3)


def test_lineage_footprint_read_is_space_and_table_scoped() -> None:
    reader = _Reader(rows=_footprint_rows())
    lineage_signal(
        candidate_columns=["l_extendedprice"],
        source_tables=[LINEITEM],
        space_id="sp1",
        run_query=reader,
    )
    assert reader.sql is not None
    assert "system.access.column_lineage" in reader.sql
    assert "entity_metadata.genie_space_id = 'sp1'" in reader.sql
    assert f"source_table_full_name IN ('{LINEITEM}')" in reader.sql


def test_lineage_footprint_empty_when_no_rows_resolve() -> None:
    reader = _Reader(rows=[])
    res = lineage_signal(
        candidate_columns=["l_extendedprice"],
        source_tables=[LINEITEM],
        space_id="sp1",
        run_query=reader,
    )
    assert res.status == MV_SIGNAL_EMPTY
    assert lineage_overlap_score(res.payload) == 0.0


def test_lineage_footprint_empty_when_candidate_has_no_columns() -> None:
    reader = _Reader(rows=_footprint_rows())
    res = lineage_signal(
        candidate_columns=[],
        source_tables=[LINEITEM],
        space_id="sp1",
        run_query=reader,
    )
    assert res.status == MV_SIGNAL_EMPTY


# ── L: footprint UNAVAILABLE (every reason) ──────────────────────────────


@pytest.mark.parametrize(
    "message, expected",
    [
        ("PERMISSION_DENIED: no SELECT on column_lineage", REASON_MISSING_GRANT),
        ("[TABLE_OR_VIEW_NOT_FOUND] system.access.column_lineage", REASON_MISSING_TABLE),
        ("gateway timeout", REASON_READ_FAILED),
    ],
)
def test_lineage_footprint_unavailable_classifies_read_failure(
    message: str, expected: str
) -> None:
    reader = _Reader(error=RuntimeError(message))
    res = lineage_signal(
        candidate_columns=["l_extendedprice"],
        source_tables=[LINEITEM],
        space_id="sp1",
        run_query=reader,
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(expected)
    assert "system.access.column_lineage" in res.reason


def test_lineage_footprint_unavailable_without_space_scope() -> None:
    reader = _Reader(rows=_footprint_rows())
    res = lineage_signal(
        candidate_columns=["l_extendedprice"],
        source_tables=[LINEITEM],
        space_id="",
        run_query=reader,
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(REASON_NO_SCOPE)
    assert reader.calls == 0


def test_lineage_footprint_unavailable_without_reader() -> None:
    res = lineage_signal(
        candidate_columns=["l_extendedprice"],
        source_tables=[LINEITEM],
        space_id="sp1",
        run_query=None,
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(REASON_NO_SCOPE)


def test_lineage_footprint_unavailable_without_source_tables() -> None:
    reader = _Reader(rows=_footprint_rows())
    res = lineage_signal(
        candidate_columns=["l_extendedprice"],
        source_tables=[],
        space_id="sp1",
        run_query=reader,
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(REASON_NO_SCOPE)
    assert reader.calls == 0


# ── L: governed reference (no lineage read) ──────────────────────────────


def test_lineage_governed_computed_from_supplied_reference() -> None:
    res = lineage_signal(
        candidate_columns=["amount", "region"],
        source_tables=["sales.core.orders"],
        reference_kind=REFERENCE_GOVERNED_MV,
        reference_columns=["amount", "currency"],
    )
    assert res.status == MV_SIGNAL_COMPUTED
    assert res.payload.reference_kind == REFERENCE_GOVERNED_MV
    assert lineage_overlap_score(res.payload) == pytest.approx(1 / 3)


def test_lineage_governed_unavailable_without_reference() -> None:
    res = lineage_signal(
        candidate_columns=["amount"],
        source_tables=["sales.core.orders"],
        reference_kind=REFERENCE_GOVERNED_MV,
        reference_columns=None,
    )
    assert res.status == MV_SIGNAL_UNAVAILABLE
    assert res.reason.startswith(REASON_NO_REFERENCE)


def test_lineage_governed_empty_on_empty_reference() -> None:
    res = lineage_signal(
        candidate_columns=["amount"],
        source_tables=["sales.core.orders"],
        reference_kind=REFERENCE_GOVERNED_MV,
        reference_columns=[],
    )
    assert res.status == MV_SIGNAL_EMPTY


# ── warehouse adapter seam ───────────────────────────────────────────────


def test_warehouse_reader_returns_row_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    import pandas as pd

    def fake_query(ws: Any, warehouse_id: str, sql: str) -> pd.DataFrame:
        return pd.DataFrame([{"source_column_name": "l_discount"}])

    monkeypatch.setattr(
        "genie_space_optimizer.common.warehouse.sql_warehouse_query", fake_query
    )
    run = warehouse_reader(object(), "wh-123")
    assert run("SELECT 1") == [{"source_column_name": "l_discount"}]


def test_warehouse_reader_maps_empty_frame_to_empty_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pandas as pd

    def fake_query(ws: Any, warehouse_id: str, sql: str) -> pd.DataFrame:
        return pd.DataFrame([])

    monkeypatch.setattr(
        "genie_space_optimizer.common.warehouse.sql_warehouse_query", fake_query
    )
    run = warehouse_reader(object(), "wh-123")
    assert run("SELECT 1") == []


# ── MV-D19 pin: column grain requires the column_lineage grant ───────────


def test_mv_d19_column_lineage_grant_is_present() -> None:
    """MV-D19 landed on (b) column grain, so the SP must be granted SELECT on
    ``system.access.column_lineage``. This pins the grant row in the single
    source of truth both install paths read (``scripts/deploy_lib/uc.py``); a
    revert to table grain that removed the row would fail here rather than at
    deploy."""
    repo_root = Path(__file__).resolve().parents[4]
    scripts_dir = repo_root / "scripts"
    assert scripts_dir.is_dir(), f"expected scripts/ at {scripts_dir}"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    from deploy_lib.uc import WATCH_SYSTEM_GRANTS

    assert ("TABLE", "system.access.column_lineage", "SELECT") in WATCH_SYSTEM_GRANTS
    # The table-grain source stays granted too — column grain adds, not replaces.
    assert ("TABLE", "system.access.table_lineage", "SELECT") in WATCH_SYSTEM_GRANTS


def test_signal_result_to_dict_round_trips_payload() -> None:
    result = SignalResult(DemandSignal(frequency=3), MV_SIGNAL_COMPUTED)
    payload = result.to_dict()
    assert payload["status"] == MV_SIGNAL_COMPUTED
    assert payload["payload"]["frequency"] == 3
    assert payload["reason"] == ""
