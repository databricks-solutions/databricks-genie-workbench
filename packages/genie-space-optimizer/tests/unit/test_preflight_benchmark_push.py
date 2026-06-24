"""GSO Optimizer v2 — Phase 2 benchmark-lifecycle preflight wiring.

Covers the runner-independent preflight push:
* the 30–40 window recommendation (D8) — never a silent auto-delete;
* the prune-invalid-before-publish backstop (eval-validity);
* the merge-only push of the WHOLE validated set into the live space;
* the genie_opt_benchmark_mutations provenance ledger (§3.5) write path.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.common.genie_client import BenchmarkPushReport
from genie_space_optimizer.optimization.preflight import (
    compute_benchmark_window_recommendation,
    preflight_push_benchmarks_to_space,
)

_PUBLISH_PATH = (
    "genie_space_optimizer.common.genie_client."
    "publish_benchmarks_to_genie_space_with_report"
)


def _bench(qid: str, question: str, sql: str, **kw) -> dict:
    row = {
        "id": qid,
        "question": question,
        "expected_sql": sql,
        "validation_status": "valid",
    }
    row.update(kw)
    return row


# ── Window recommendation (30–40, D8) ──────────────────────────────────


def test_window_within_returns_within_window():
    bs = [_bench(f"q{i}", f"distinct question {i}", "SELECT 1") for i in range(3)]
    rec = compute_benchmark_window_recommendation(bs, window_min=2, window_max=5)
    assert rec["status"] == "within_window"
    assert rec["recommended_prune"] == []
    assert rec["recommended_topup"] == 0


def test_window_under_recommends_topup():
    bs = [_bench(f"q{i}", f"distinct question {i}", "SELECT 1") for i in range(4)]
    rec = compute_benchmark_window_recommendation(bs, window_min=10, window_max=20)
    assert rec["status"] == "under_window"
    assert rec["recommended_topup"] == 6
    assert rec["recommended_prune"] == []


def test_window_over_recommends_prune_down_to_max():
    bs = [_bench(f"q{i}", f"alpha bravo charlie {i}", f"SELECT {i}") for i in range(4)]
    rec = compute_benchmark_window_recommendation(bs, window_min=1, window_max=2)
    assert rec["status"] == "over_window"
    # over by 2 → recommend exactly 2 for removal; recommendation only.
    assert len(rec["recommended_prune"]) == 2


def test_window_over_prefers_near_duplicates_first():
    bs = [
        _bench("a", "what is total revenue by region", "SELECT 1"),
        _bench("b", "how many active customers are there", "SELECT 2"),
        _bench("a_dup", "what is total revenue by region", "SELECT 3"),
    ]
    rec = compute_benchmark_window_recommendation(bs, window_min=1, window_max=2)
    assert rec["status"] == "over_window"
    # Over by 1; the near-duplicate of 'a' is recommended first.
    assert rec["recommended_prune"] == ["a_dup"]


# ── Preflight push: prune-invalid, merge, ledger ────────────────────────


@pytest.fixture
def captured_publish():
    """Patch the space publisher; capture the benchmarks it receives."""
    captured: dict = {}

    def _fake(w, space_id, benchmarks, max_questions, *, run_id=None):
        captured["benchmarks"] = list(benchmarks)
        captured["space_id"] = space_id
        added = [
            {
                "id": b.get("id", ""),
                "question": b.get("question", ""),
                "sql": b.get("expected_sql", ""),
            }
            for b in benchmarks
        ]
        return BenchmarkPushReport(
            added_count=len(added),
            merged_total=len(added),
            added=added,
            patched=True,
        )

    with patch(_PUBLISH_PATH, side_effect=_fake):
        yield captured


def test_push_excludes_invalid_and_sqlless_rows_before_publish(captured_publish):
    benchmarks = [
        _bench("v1", "valid question one", "SELECT 1"),
        _bench("v2", "valid question two", "SELECT 2"),
        _bench("bad", "invalid question", "SELECT broken", validation_status="invalid"),
        _bench("nosql", "no sql question", ""),
    ]
    with patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        out = preflight_push_benchmarks_to_space(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
            benchmarks,
        )

    pushed_ids = {b["id"] for b in captured_publish["benchmarks"]}
    assert pushed_ids == {"v1", "v2"}, "only EXPLAIN-valid rows with SQL may publish"
    assert out["published_count"] == 2
    assert out["pruned_at_push"] == 2


def test_push_writes_added_removed_changed_ledger_rows(captured_publish):
    benchmarks = [
        _bench("v1", "valid question one", "SELECT 1"),
        _bench("nosql", "no sql question", ""),  # pruned at push → removed
    ]
    rejected = [
        {
            "id": "rej1",
            "question": "rejected question",
            "expected_sql": "SELECT broken",
            "validation_reason_code": "sql_compile_error",
        },
    ]
    changed = [
        {
            "id": "chg1",
            "question": "auto corrected question",
            "before_sql": "WHERE region = 'EU'",
            "after_sql": "WHERE region = 'Europe'",
            "reason": "predicate_value_autocorrect",
        },
    ]

    recorded: dict = {}

    def _capture(spark, run_id, rows, *, catalog, schema):
        recorded["rows"] = rows
        return len(rows)

    with patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        side_effect=_capture,
    ):
        out = preflight_push_benchmarks_to_space(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
            benchmarks,
            rejected_benchmarks=rejected,
            changed_benchmarks=changed,
        )

    rows = recorded["rows"]
    by_op: dict[str, list[dict]] = {}
    for r in rows:
        by_op.setdefault(r["op"], []).append(r)

    # added: the one valid published row
    assert {r["question_id"] for r in by_op["added"]} == {"v1"}
    assert by_op["added"][0]["reason"] == "preflight_push"
    assert by_op["added"][0]["before"] is None

    # removed: validation-rejected + pruned-at-push (no-sql)
    removed_ids = {r["question_id"] for r in by_op["removed"]}
    assert removed_ids == {"rej1", "nosql"}
    reasons = {r["question_id"]: r["reason"] for r in by_op["removed"]}
    assert reasons["rej1"] == "sql_compile_error"
    assert reasons["nosql"] == "prune_invalid_before_publish"

    # changed: predicate auto-correction with before/after SQL
    assert by_op["changed"][0]["question_id"] == "chg1"
    assert by_op["changed"][0]["before"]["sql"] == "WHERE region = 'EU'"
    assert by_op["changed"][0]["after"]["sql"] == "WHERE region = 'Europe'"

    assert out["ledger_rows"] == len(rows)


def test_push_skips_when_publishing_disabled(monkeypatch, captured_publish):
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.preflight.PUBLISH_BENCHMARKS_TO_SPACE",
        False,
    )
    with patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        out = preflight_push_benchmarks_to_space(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
            [_bench("v1", "valid question one", "SELECT 1")],
        )
    assert "benchmarks" not in captured_publish  # publisher never called
    assert out["published_count"] == 0


def test_push_is_nonfatal_when_publish_raises(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("genie API down")

    with patch(_PUBLISH_PATH, side_effect=_boom), patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        out = preflight_push_benchmarks_to_space(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
            [_bench("v1", "valid question one", "SELECT 1")],
        )
    # Push failure is swallowed; preflight continues.
    assert out["published_count"] == 0
