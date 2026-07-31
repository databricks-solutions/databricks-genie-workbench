"""GSO Optimizer v2 — Phase 2 benchmark-lifecycle preflight wiring.

Covers the runner-independent preflight push:
* the 30–40 window recommendation (D8) — computed over the POST-MERGE live
  set, never a silent auto-delete;
* the prune-invalid-before-publish backstop (eval-validity);
* the strictly additive/merge-only push of the WHOLE validated set into the
  live space (NEVER truncated; fail-closed only on the Genie API hard cap);
* the fail-closed behaviour when a required push fails (contract 1 — pushed
  BEFORE eval);
* the genie_opt_benchmark_mutations provenance ledger (§3.5) write path,
  including over-window prune RECOMMENDATIONS.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.common.genie_client import (
    BenchmarkPushReport,
    compute_benchmark_window_recommendation,
    publish_benchmarks_to_genie_space_with_report,
)
from genie_space_optimizer.optimization.preflight import (
    BenchmarkPushError,
    preflight_push_benchmarks_to_space,
)

_PUBLISH_PATH = (
    "genie_space_optimizer.common.genie_client."
    "publish_benchmarks_to_genie_space_with_report"
)
_FETCH_PATH = "genie_space_optimizer.common.genie_client.fetch_space_config"
_PATCH_PATH = "genie_space_optimizer.common.genie_client.patch_space_config"


def _bench(qid: str, question: str, sql: str, **kw) -> dict:
    row = {
        "id": qid,
        "question": question,
        "expected_sql": sql,
        "validation_status": "valid",
    }
    row.update(kw)
    return row


def _distinct_benchmarks(n: int, *, prefix: str) -> list[dict]:
    """N mutually-distinct benchmark rows.

    Each row is built from index-carrying tokens so window-pruning tests do not
    accidentally classify the generated rows as near-duplicates.
    """
    rows: list[dict] = []
    for i in range(n):
        words = " ".join(
            f"{prefix}{i}tok{j}{(i * 31 + j * 17) % 9973}" for j in range(5)
        )
        rows.append(_bench(f"{prefix}{i}", words, f"SELECT {prefix!r}, {i}"))
    return rows


def _live_question(qid: str, question: str, sql: str) -> dict:
    """A Genie-native benchmark question already present in the live space."""
    return {
        "id": qid,
        "question": [question],
        "answer": [{"format": "SQL", "content": [sql]}],
    }


def _live_from_bench(b: dict) -> dict:
    return _live_question(b["id"], b["question"], b["expected_sql"])


def _parsed_space(
    *,
    existing: list[dict] | None = None,
    example_question_sqls: list[dict] | None = None,
) -> dict:
    """Build a fake ``_parsed_space`` config for the publisher to read."""
    parsed: dict = {"benchmarks": {"questions": list(existing or [])}}
    if example_question_sqls is not None:
        parsed["example_question_sqls"] = example_question_sqls
    return parsed


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


# ── REAL publisher merge logic (no mock of the publisher) ──────────────


def test_real_publisher_preserves_existing_live_rows_and_adds_net_new():
    """(a) existing live rows preserved; net-new added additively."""
    existing = [
        _live_question("u1", "user authored one", "SELECT 11"),
        _live_question("u2", "user authored two", "SELECT 22"),
    ]
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    with patch(_FETCH_PATH, return_value={"_parsed_space": _parsed_space(existing=existing)}), \
         patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1",
            [_bench("n1", "net new alpha", "SELECT 1"),
             _bench("n2", "net new bravo", "SELECT 2")],
        )

    assert report.patched is True
    assert report.over_cap is False
    assert report.existing_count == 2
    assert report.added_count == 2
    assert report.merged_total == 4
    # The two user-authored rows survive verbatim in the patched config.
    patched_qs = patched_holder["cfg"]["benchmarks"]["questions"]
    patched_ids = [q.get("id") for q in patched_qs]
    assert "u1" in patched_ids and "u2" in patched_ids
    assert len(patched_qs) == 4


def test_real_publisher_repairs_sql_in_place_by_stable_id_without_rewording():
    existing_question = "How many support tickets were created per account segment?"
    existing = [_live_question("native-q1", existing_question, "SELECT broken")]
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    repaired = _bench(
        "native-q1",
        existing_question,
        "SELECT 1",
        source="genie_benchmark",
        space_question_id="native-q1",
    )
    with patch(
        _FETCH_PATH,
        return_value={"_parsed_space": _parsed_space(existing=existing)},
    ), patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", [repaired],
        )

    patched_questions = patched_holder["cfg"]["benchmarks"]["questions"]
    assert len(patched_questions) == 1
    assert patched_questions[0]["id"] == "native-q1"
    assert patched_questions[0]["question"] == [existing_question]
    assert patched_questions[0]["answer"][0]["content"] == ["SELECT 1"]
    assert report.added_count == 0
    assert report.updated_count == 1
    assert report.updated[0]["before_sql"] == "SELECT broken"
    assert report.updated[0]["after_sql"] == "SELECT 1"


def test_real_publisher_refuses_stable_id_update_when_question_text_changed():
    existing_question = "How many tickets were created?"
    existing = [_live_question("native-q1", existing_question, "SELECT old")]
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    rewritten = _bench(
        "native-q1",
        "How many tickets were created? Join tickets to accounts.",
        "SELECT new",
        source="genie_benchmark",
        space_question_id="native-q1",
    )
    with patch(
        _FETCH_PATH,
        return_value={"_parsed_space": _parsed_space(existing=existing)},
    ), patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", [rewritten],
        )

    patched_questions = patched_holder["cfg"]["benchmarks"]["questions"]
    assert patched_questions == existing
    assert report.added_count == 0
    assert report.updated_count == 0
    assert report.dedup_skipped == 1


def test_real_publisher_adds_near_duplicate_when_text_is_not_exact():
    """Similarity is advisory only; it must not erase a distinct eval row."""
    existing_question = "How many support tickets were created per account segment?"
    distinct_question = "How many support tickets were created per account segments?"
    existing = [_live_question("native-q1", existing_question, "SELECT 1")]
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    with patch(
        _FETCH_PATH,
        return_value={"_parsed_space": _parsed_space(existing=existing)},
    ), patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(),
            "space-1",
            [_bench("internal-q2", distinct_question, "SELECT 2")],
        )

    questions = patched_holder["cfg"]["benchmarks"]["questions"]
    assert [q["question"][0] for q in questions] == [
        existing_question,
        distinct_question,
    ]
    assert report.added_count == 1
    assert report.dedup_skipped == 0


def test_real_publisher_pushes_valid_31_to_40_set_without_truncation():
    """(b) a valid 31–40 set is pushed whole — never truncated to 30."""
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    benchmarks = _distinct_benchmarks(35, prefix="b")
    with patch(_FETCH_PATH, return_value={"_parsed_space": _parsed_space(existing=[])}), \
         patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", benchmarks,
        )

    assert report.patched is True
    assert report.over_cap is False
    assert report.added_count == 35
    assert report.merged_total == 35
    # All 35 written — NOT truncated to 30 (the old MAX_BENCHMARK_COUNT bug).
    assert len(patched_holder["cfg"]["benchmarks"]["questions"]) == 35
    assert report.window is not None
    # 35 is inside the 30–40 working window.
    assert report.window["status"] == "within_window"


def test_real_publisher_over_window_is_recommend_only_no_truncation():
    """(c) >40 rows → patched additively, recommend-only prune, NO truncation."""
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    benchmarks = _distinct_benchmarks(45, prefix="c")
    with patch(_FETCH_PATH, return_value={"_parsed_space": _parsed_space(existing=[])}), \
         patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", benchmarks,
        )

    assert report.patched is True
    assert report.over_cap is False
    # All 45 written — the over-window prune is a recommendation, not applied.
    assert report.merged_total == 45
    assert len(patched_holder["cfg"]["benchmarks"]["questions"]) == 45
    assert report.window is not None
    assert report.window["status"] == "over_window"
    # over by 5 → recommend exactly 5 for removal; never auto-deleted.
    assert len(report.window["recommended_prune"]) == 5


def test_real_publisher_post_merge_overflow_no_deletion_of_existing():
    """(d) existing + new post-merge overflow → recommend-only, no deletion.

    25 existing + 30 net-new = 55 live; the window must report over_window on
    the POST-MERGE count of 55 (Blocking 2), and NO existing row is deleted.
    """
    existing_rows = _distinct_benchmarks(25, prefix="ex")
    existing = [_live_from_bench(b) for b in existing_rows]
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    benchmarks = _distinct_benchmarks(30, prefix="nw")
    with patch(_FETCH_PATH, return_value={"_parsed_space": _parsed_space(existing=existing)}), \
         patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", benchmarks,
        )

    assert report.patched is True
    assert report.over_cap is False
    assert report.existing_count == 25
    assert report.added_count == 30
    assert report.merged_total == 55
    # Window status reflects the POST-MERGE set (55), not the 30 net-new.
    assert report.window is not None
    assert report.window["status"] == "over_window"
    # Every one of the 25 existing rows is still present — none deleted.
    patched_ids = {q.get("id") for q in patched_holder["cfg"]["benchmarks"]["questions"]}
    for b in existing_rows:
        assert b["id"] in patched_ids
    assert len(patched_holder["cfg"]["benchmarks"]["questions"]) == 55


def test_real_publisher_skips_questions_already_mirrored_in_example_sqls():
    """(e) mirror-skip — a benchmark already in example_question_sqls is dropped."""
    eqs = [{"question": ["show revenue by region"], "sql": ["SELECT 1"]}]
    patched_holder: dict = {}

    def _capture_patch(w, space_id, cfg, **kw):
        patched_holder["cfg"] = cfg
        return {}

    benchmarks = [
        _bench("m1", "show revenue by region", "SELECT 1"),     # mirrors an example SQL
        _bench("n1", "a genuinely different question", "SELECT 2"),
    ]
    with patch(
        _FETCH_PATH,
        return_value={"_parsed_space": _parsed_space(existing=[], example_question_sqls=eqs)},
    ), patch(_PATCH_PATH, side_effect=_capture_patch):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", benchmarks,
        )

    assert report.mirror_skipped == 1
    assert report.added_count == 1
    assert report.merged_total == 1
    questions = [q["question"][0] for q in patched_holder["cfg"]["benchmarks"]["questions"]]
    assert "a genuinely different question" in questions
    assert "show revenue by region" not in questions


def test_real_publisher_fails_closed_over_hard_cap_without_patching():
    """Hard Genie API cap exceeded → over_cap report, NO patch performed."""
    benchmarks = _distinct_benchmarks(6, prefix="hc")
    patch_mock = MagicMock()
    with patch(_FETCH_PATH, return_value={"_parsed_space": _parsed_space(existing=[])}), \
         patch(_PATCH_PATH, patch_mock):
        report = publish_benchmarks_to_genie_space_with_report(
            MagicMock(), "space-1", benchmarks, max_questions=5,
        )

    assert report.over_cap is True
    assert report.patched is False
    assert report.added_count == 0
    assert report.merged_total == 6
    # Fail-closed: nothing was written to the live space.
    patch_mock.assert_not_called()


# ── Preflight push: prune-invalid, merge, ledger ────────────────────────


@pytest.fixture
def captured_publish():
    """Patch the space publisher; capture the benchmarks it receives and
    return a realistic post-merge report (window computed over the merged
    set, so the preflight's post-merge window path is exercised)."""
    captured: dict = {}

    def _fake(w, space_id, benchmarks, max_questions=None, *, run_id=None):
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
        window = compute_benchmark_window_recommendation(added)
        return BenchmarkPushReport(
            added_count=len(added),
            merged_total=len(added),
            existing_count=0,
            added=added,
            merged=added,
            window=window,
            over_cap=False,
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
    assert out["resolved_question_ids"] == 2
    assert out["pruned_at_push"] == 2
    assert out["push_ok"] is True


def test_push_attaches_exact_live_question_id_for_delta_handoff():
    native_id = "a" * 32
    benchmark = _bench("internal-q1", "How many tickets were created?", "SELECT 1")
    report = BenchmarkPushReport(
        added_count=1,
        merged_total=1,
        added=[{
            "id": native_id,
            "question": benchmark["question"],
            "sql": benchmark["expected_sql"],
        }],
        merged=[{
            "id": native_id,
            "question": benchmark["question"],
            "sql": benchmark["expected_sql"],
        }],
        window=compute_benchmark_window_recommendation([benchmark]),
        patched=True,
    )

    with patch(_PUBLISH_PATH, return_value=report), patch(
        "genie_space_optimizer.optimization.preflight.write_stage",
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        out = preflight_push_benchmarks_to_space(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
            [benchmark],
        )

    assert benchmark["space_question_id"] == native_id
    assert out["resolved_question_ids"] == 1
    assert out["push_ok"] is True


def test_push_fails_when_publisher_does_not_return_exact_question_mapping():
    benchmark = _bench("internal-q1", "How many tickets were created?", "SELECT 1")
    report = BenchmarkPushReport(
        added_count=1,
        merged_total=1,
        added=[{"id": "a" * 32, "question": "Different wording", "sql": "SELECT 1"}],
        merged=[{"id": "a" * 32, "question": "Different wording", "sql": "SELECT 1"}],
        window=compute_benchmark_window_recommendation([benchmark]),
        patched=True,
    )

    with patch(_PUBLISH_PATH, return_value=report), patch(
        "genie_space_optimizer.optimization.preflight.write_stage",
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        with pytest.raises(BenchmarkPushError, match="unresolved_after_publish"):
            preflight_push_benchmarks_to_space(
                MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
                [benchmark],
            )

    assert "space_question_id" not in benchmark


def test_push_fails_when_exact_question_mapping_is_ambiguous():
    benchmark = _bench("internal-q1", "How many tickets were created?", "SELECT 1")
    merged = [
        {"id": "a" * 32, "question": benchmark["question"], "sql": "SELECT 1"},
        {"id": "b" * 32, "question": benchmark["question"], "sql": "SELECT 1"},
    ]
    report = BenchmarkPushReport(
        merged_total=2,
        merged=merged,
        window=compute_benchmark_window_recommendation(merged),
        patched=True,
    )

    with patch(_PUBLISH_PATH, return_value=report), patch(
        "genie_space_optimizer.optimization.preflight.write_stage",
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        with pytest.raises(BenchmarkPushError, match="unresolved_after_publish"):
            preflight_push_benchmarks_to_space(
                MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
                [benchmark],
            )

    assert "space_question_id" not in benchmark


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


def test_push_ledgers_native_sql_repair_as_changed() -> None:
    updated = [{
        "id": "native-q1",
        "question": "How many tickets were created?",
        "before_sql": "SELECT broken",
        "after_sql": "SELECT 1",
    }]
    report = BenchmarkPushReport(
        updated_count=1,
        merged_total=1,
        existing_count=1,
        updated=updated,
        merged=[{
            "id": "native-q1",
            "question": "How many tickets were created?",
            "sql": "SELECT 1",
        }],
        window=compute_benchmark_window_recommendation(updated),
        patched=True,
    )
    recorded: dict = {}

    def _capture(spark, run_id, rows, *, catalog, schema):
        recorded["rows"] = rows
        return len(rows)

    with patch(_PUBLISH_PATH, return_value=report), patch(
        "genie_space_optimizer.optimization.preflight.write_stage",
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        side_effect=_capture,
    ):
        preflight_push_benchmarks_to_space(
            MagicMock(),
            MagicMock(),
            "run-1",
            "space-1",
            "cat",
            "sch",
            [_bench("native-q1", "How many tickets were created?", "SELECT 1")],
        )

    changed = [row for row in recorded["rows"] if row["op"] == "changed"]
    assert len(changed) == 1
    assert changed[0]["question_id"] == "native-q1"
    assert changed[0]["before"]["sql"] == "SELECT broken"
    assert changed[0]["after"]["sql"] == "SELECT 1"
    assert changed[0]["reason"] == "curated_sql_repair"


def test_push_records_over_window_prune_recommendations_in_ledger():
    """Over-window prune RECOMMENDATIONS are recorded as non-mutating
    advisory ledger rows (op=prune_recommended), and the net-new push set is
    recorded as added rows — contract 5 stays satisfied with no truncation."""
    benchmarks = _distinct_benchmarks(45, prefix="lg")

    recorded: dict = {}

    def _capture(spark, run_id, rows, *, catalog, schema):
        recorded["rows"] = rows
        return len(rows)

    # Use the REAL publisher with mocked Genie I/O so the post-merge window
    # is genuinely over_window.
    def _capture_patch(w, space_id, cfg, **kw):
        return {}

    with patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        side_effect=_capture,
    ), patch(
        _FETCH_PATH, return_value={"_parsed_space": _parsed_space(existing=[])},
    ), patch(_PATCH_PATH, side_effect=_capture_patch):
        out = preflight_push_benchmarks_to_space(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
            benchmarks,
        )

    by_op: dict[str, list[dict]] = {}
    for r in recorded["rows"]:
        by_op.setdefault(r["op"], []).append(r)

    assert out["window"]["status"] == "over_window"
    assert len(by_op.get("added", [])) == 45            # net-new push set recorded
    assert len(by_op.get("prune_recommended", [])) == 5  # full recommendation recorded
    assert all(r["reason"] == "over_window_recommendation"
               for r in by_op["prune_recommended"])


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
    assert out["push_ok"] is True  # disabled push is not a failure


def test_push_failure_is_fatal_when_publishing_enabled():
    """Blocking 4: a required push that raises must FAIL the preflight job so
    baseline eval never runs against the stale live benchmark set."""
    def _boom(*a, **k):
        raise RuntimeError("genie API down")

    with patch(_PUBLISH_PATH, side_effect=_boom), patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        with pytest.raises(BenchmarkPushError):
            preflight_push_benchmarks_to_space(
                MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
                [_bench("v1", "valid question one", "SELECT 1")],
            )


def test_push_over_cap_is_fatal_when_publishing_enabled():
    """A required push that the publisher refuses (over hard cap, no mutation)
    is also fatal — eval must not run against the stale set."""
    over_cap_report = BenchmarkPushReport(
        added_count=0, merged_total=999, existing_count=0,
        added=[], merged=[], window=None, over_cap=True, patched=False,
    )
    with patch(_PUBLISH_PATH, return_value=over_cap_report), patch(
        "genie_space_optimizer.optimization.preflight.write_stage"
    ), patch(
        "genie_space_optimizer.optimization.preflight.write_benchmark_mutations",
        return_value=0,
    ):
        with pytest.raises(BenchmarkPushError):
            preflight_push_benchmarks_to_space(
                MagicMock(), MagicMock(), "run-1", "space-1", "cat", "sch",
                [_bench("v1", "valid question one", "SELECT 1")],
            )
