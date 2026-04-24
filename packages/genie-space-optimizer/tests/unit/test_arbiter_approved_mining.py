"""Arbiter-approved benchmark mining + structural-SQL firewall drop + headroom gate.

Covers the behaviour introduced by the "GSO: Arbiter-approved benchmark
mining" PR:

* :func:`_extract_arbiter_approved_benchmarks` filters to rows whose
  baseline arbiter verdict is ``both_correct`` ONLY. ``genie_correct``
  (wrong expected_sql), ``ground_truth_correct``, ``neither_correct``,
  ``skipped`` all excluded. Empty baseline -> empty subset.

* Bug #4 firewall is a no-op for structural SQL patch types
  (``add_sql_snippet_*`` and ``add_join_spec`` / ``update_join_spec``) —
  covered by the expanded parametrization in ``test_leakage_firewall.py``.
  This file asserts the Lever 6 call-site-level behaviour (firewall
  still called for forward-compat; no-op dispatch for the snippet
  patch types).

* Headroom gate math: seeding skips when
  ``existing + LEVER_RESERVE >= MAX_SQL_SNIPPETS``. Cap otherwise.

* :func:`_explain_join_candidate` rejects joins whose EXPLAIN fails on
  the warehouse (or spark), letting prose-derived joins inherit exec-
  validation parity with the sql_snippet path.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


# ─────────────────────────────────────────────────────────────────────
# 1. _extract_arbiter_approved_benchmarks
# ─────────────────────────────────────────────────────────────────────


def _fake_baseline_iteration(rows: list[dict]) -> dict:
    """Shape the Delta row returned by load_latest_full_iteration."""
    return {"rows_json": json.dumps(rows)}


def _mk_row(qid: str, verdict: str) -> dict:
    """Minimal eval row carrying an arbiter verdict + question id."""
    return {
        "arbiter/value": verdict,
        "request": json.dumps({"question": f"q-text-{qid}"}),
        "inputs/question": f"q-text-{qid}",
        "inputs/question_id": qid,
    }


def _stub_load_latest_full_iteration(monkeypatch, baseline_iter: dict | None):
    """Redirect ``load_latest_full_iteration`` so the helper under test
    reads from an in-memory stub instead of Delta."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness.load_latest_full_iteration",
        lambda *a, **k: baseline_iter,
    )


class TestArbiterApprovedFilter:
    """Pre-mining verdict gate for proactive SQL-expression mining."""

    _BENCHMARKS = [
        {"id": "q1", "expected_sql": "SELECT 1"},
        {"id": "q2", "expected_sql": "SELECT 2"},
        {"id": "q3", "expected_sql": "SELECT 3"},
        {"id": "q4", "expected_sql": "SELECT 4"},
        {"id": "q5", "expected_sql": "SELECT 5"},
    ]

    def test_both_correct_only_qualifies(self, monkeypatch):
        """Only rows with ``both_correct`` verdict contribute. Every other
        verdict is excluded — critical for expected_sql mining safety."""
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )

        baseline = _fake_baseline_iteration([
            _mk_row("q1", "both_correct"),
            _mk_row("q2", "genie_correct"),        # EXCLUDED — GT is wrong
            _mk_row("q3", "ground_truth_correct"),  # EXCLUDED — per user stance
            _mk_row("q4", "neither_correct"),       # EXCLUDED
            _mk_row("q5", "skipped"),               # EXCLUDED
        ])
        _stub_load_latest_full_iteration(monkeypatch, baseline)

        approved, counts = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", self._BENCHMARKS,
        )
        assert [b["id"] for b in approved] == ["q1"], (
            "Only both_correct row should qualify; got "
            f"{[b['id'] for b in approved]}"
        )
        assert counts == {
            "both_correct": 1,
            "genie_correct": 1,
            "ground_truth_correct": 1,
            "neither_correct": 1,
            "skipped": 1,
        }

    def test_genie_correct_explicitly_excluded(self, monkeypatch):
        """Critical review finding #1 regression guard: ``genie_correct``
        means the benchmark's ``expected_sql`` is WRONG (Genie is right,
        GT is wrong). Mining from it would ingest bad SQL patterns."""
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )

        baseline = _fake_baseline_iteration([
            _mk_row("q2", "genie_correct"),
        ])
        _stub_load_latest_full_iteration(monkeypatch, baseline)

        approved, counts = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", self._BENCHMARKS,
        )
        assert approved == [], (
            "genie_correct MUST NOT pass the filter — its expected_sql "
            "is unreliable by definition"
        )
        assert counts == {"genie_correct": 1}

    def test_ground_truth_correct_excluded(self, monkeypatch):
        """User-directed strict stance: only ``both_correct`` qualifies.
        ``ground_truth_correct`` is also excluded (conservative)."""
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )

        baseline = _fake_baseline_iteration([
            _mk_row("q3", "ground_truth_correct"),
        ])
        _stub_load_latest_full_iteration(monkeypatch, baseline)

        approved, _ = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", self._BENCHMARKS,
        )
        assert approved == []

    def test_empty_baseline_returns_empty(self, monkeypatch):
        """First-run guard: no baseline iteration persisted yet → empty
        subset. Caller falls back to schema-discovery mining only."""
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )

        _stub_load_latest_full_iteration(monkeypatch, None)
        approved, counts = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", self._BENCHMARKS,
        )
        assert approved == []
        assert counts == {}

    def test_malformed_rows_json_returns_empty(self, monkeypatch):
        """Belt-and-suspenders: bad JSON in Delta → empty subset, no raise."""
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )

        _stub_load_latest_full_iteration(
            monkeypatch, {"rows_json": "not-valid-json{"},
        )
        approved, _ = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", self._BENCHMARKS,
        )
        assert approved == []

    def test_empty_benchmarks_short_circuits(self, monkeypatch):
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )
        # Even when baseline has approved verdicts, an empty benchmarks
        # list returns empty without reading Delta.
        _stub_load_latest_full_iteration(monkeypatch, None)  # never called
        approved, counts = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", [],
        )
        assert approved == []
        assert counts == {}

    def test_benchmark_without_id_skipped(self, monkeypatch):
        """Rows with neither ``id`` nor ``question_id`` cannot be matched
        against baseline and are silently dropped."""
        from genie_space_optimizer.optimization.harness import (
            _extract_arbiter_approved_benchmarks,
        )

        benchmarks_no_id = [
            {"expected_sql": "SELECT 1"},  # no id
            {"id": "q1", "expected_sql": "SELECT 1"},
        ]
        baseline = _fake_baseline_iteration([_mk_row("q1", "both_correct")])
        _stub_load_latest_full_iteration(monkeypatch, baseline)

        approved, _ = _extract_arbiter_approved_benchmarks(
            MagicMock(), "run-1", "cat", "sch", benchmarks_no_id,
        )
        assert [b["id"] for b in approved] == ["q1"]


# ─────────────────────────────────────────────────────────────────────
# 2. _explain_join_candidate
# ─────────────────────────────────────────────────────────────────────


class TestExplainJoinCandidate:
    """EXPLAIN-based validation for instruction-derived and proven joins."""

    def test_passes_when_spark_accepts(self):
        """EXPLAIN that returns successfully -> (True, '')."""
        from genie_space_optimizer.optimization.harness import (
            _explain_join_candidate,
        )
        spark = MagicMock()
        # spark.sql is called multiple times (USE CATALOG / USE SCHEMA
        # / EXPLAIN). All succeed by default on the MagicMock.
        ok, err = _explain_join_candidate(
            w=None, spark=spark,
            left_identifier="cat.sch.orders",
            right_identifier="cat.sch.customers",
            join_sql_cond="`orders`.`customer_id` = `customers`.`id`",
            catalog="cat", gold_schema="sch",
        )
        assert ok
        assert err == ""
        # EXPLAIN was attempted.
        explain_calls = [
            args[0] for args, _ in spark.sql.call_args_list
            if args and args[0].upper().startswith("EXPLAIN ")
        ]
        assert len(explain_calls) == 1

    def test_rejects_when_spark_raises(self):
        """EXPLAIN that raises -> (False, 'EXPLAIN failed: ...')."""
        from genie_space_optimizer.optimization.harness import (
            _explain_join_candidate,
        )
        spark = MagicMock()

        def _explode(stmt):
            if stmt.upper().startswith("EXPLAIN "):
                raise RuntimeError("ORG_AMBIGUOUS_COLUMN: id")
            return MagicMock()  # USE CATALOG / USE SCHEMA succeed

        spark.sql.side_effect = _explode
        ok, err = _explain_join_candidate(
            w=None, spark=spark,
            left_identifier="cat.sch.orders",
            right_identifier="cat.sch.customers",
            join_sql_cond="`orders`.`id` = `customers`.`id`",  # ambiguous
            catalog="cat", gold_schema="sch",
        )
        assert not ok
        assert "EXPLAIN failed" in err
        assert "AMBIGUOUS" in err

    def test_rejects_missing_inputs(self):
        """Guard rail for malformed inputs."""
        from genie_space_optimizer.optimization.harness import (
            _explain_join_candidate,
        )
        ok, err = _explain_join_candidate(
            w=None, spark=MagicMock(),
            left_identifier="",  # empty
            right_identifier="cat.sch.customers",
            join_sql_cond="x = y",
            catalog="cat", gold_schema="sch",
        )
        assert not ok
        assert "missing" in err.lower()

    def test_no_backend_optimistic_pass(self):
        """Unit-test convenience: no spark + no warehouse -> optimistic
        pass. Production always has one, and the Genie API PATCH rejects
        malformed specs at persist time as a safety net."""
        from genie_space_optimizer.optimization.harness import (
            _explain_join_candidate,
        )
        ok, err = _explain_join_candidate(
            w=None, spark=None,
            left_identifier="cat.sch.orders",
            right_identifier="cat.sch.customers",
            join_sql_cond="`orders`.`customer_id` = `customers`.`id`",
            catalog="cat", gold_schema="sch",
        )
        assert ok
        assert err == ""


# ─────────────────────────────────────────────────────────────────────
# 3. _apply_instruction_join_specs with EXPLAIN gate
# ─────────────────────────────────────────────────────────────────────


class TestApplyInstructionJoinSpecsExplain:
    """EXPLAIN-failing join candidates are rejected before persistence."""

    def _stub_patch_and_write_stage(self, monkeypatch) -> list[dict]:
        patched: list[dict] = []
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            lambda w, space_id, cfg, **kw: patched.append(cfg),
        )
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.write_stage",
            lambda *a, **k: None,
        )
        return patched

    def test_explain_failure_rejects_candidate(self, monkeypatch):
        from genie_space_optimizer.optimization.harness import (
            _apply_instruction_join_specs,
        )

        patched = self._stub_patch_and_write_stage(monkeypatch)
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("DATATYPE_MISMATCH")

        meta = {"instructions": {"join_specs": []}}
        candidates = [{
            "left": {"identifier": "cat.sch.orders", "alias": "orders"},
            "right": {"identifier": "cat.sch.customers", "alias": "customers"},
            "sql": ["`orders`.`customer_id` = `customers`.`name`"],
        }]

        applied = _apply_instruction_join_specs(
            w=None, spark=spark, run_id="r1", space_id="s1",
            candidates=candidates, metadata_snapshot=meta,
            catalog="cat", schema="sch",
        )
        assert applied == 0
        assert meta["instructions"]["join_specs"] == []
        assert patched == []  # no PATCH emitted

    def test_explain_pass_persists_candidate(self, monkeypatch):
        from genie_space_optimizer.optimization.harness import (
            _apply_instruction_join_specs,
        )

        patched = self._stub_patch_and_write_stage(monkeypatch)
        spark = MagicMock()  # default returns a MagicMock — no raise

        meta = {"instructions": {"join_specs": []}}
        candidates = [{
            "left": {"identifier": "cat.sch.orders", "alias": "orders"},
            "right": {"identifier": "cat.sch.customers", "alias": "customers"},
            "sql": ["`orders`.`customer_id` = `customers`.`id`"],
        }]

        applied = _apply_instruction_join_specs(
            w=None, spark=spark, run_id="r1", space_id="s1",
            candidates=candidates, metadata_snapshot=meta,
            catalog="cat", schema="sch",
        )
        assert applied == 1
        assert len(meta["instructions"]["join_specs"]) == 1
        assert len(patched) == 1


# ─────────────────────────────────────────────────────────────────────
# 4. Headroom gate math
# ─────────────────────────────────────────────────────────────────────


class TestHeadroomGate:
    """``headroom = MAX_SQL_SNIPPETS - existing - LEVER_RESERVE`` arithmetic."""

    def _headroom(self, existing: int) -> int:
        from genie_space_optimizer.common.config import (
            SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
        )
        from genie_space_optimizer.common.genie_schema import MAX_SQL_SNIPPETS
        return max(
            0,
            MAX_SQL_SNIPPETS - existing - SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
        )

    def test_empty_space_allows_full_budget(self):
        """Fresh space: headroom = 200 - 0 - 50 = 150."""
        assert self._headroom(0) == 150

    def test_partially_populated_space(self):
        """100 existing + 50 reserve = 50 remaining."""
        assert self._headroom(100) == 50

    def test_at_cap_minus_reserve_is_zero(self):
        """existing hits the reserve threshold -> no headroom."""
        assert self._headroom(150) == 0

    def test_over_cap_clamped_to_zero(self):
        """Defensive: existing > cap returns 0, never negative."""
        assert self._headroom(250) == 0

    def test_mine_cap_is_minimum(self):
        """``mine_cap = min(MAX_CANDIDATES, headroom)``."""
        from genie_space_optimizer.common.config import (
            SQL_EXPRESSION_SEEDING_MAX_CANDIDATES,
        )
        headroom = self._headroom(100)  # 50
        mine_cap = min(SQL_EXPRESSION_SEEDING_MAX_CANDIDATES, headroom)
        assert mine_cap == 50
        assert SQL_EXPRESSION_SEEDING_MAX_CANDIDATES == 60

    def test_lever_reserve_default(self):
        """Sanity: the reserve is 50 as specified by the plan."""
        from genie_space_optimizer.common.config import (
            SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
        )
        assert SQL_EXPRESSION_SEEDING_LEVER_RESERVE == 50


# ─────────────────────────────────────────────────────────────────────
# 5. Lever 6 firewall no-op for structural SQL patch types
# ─────────────────────────────────────────────────────────────────────


class TestLever6FirewallNoOp:
    """After the plan's ``_PATCH_TEXT_FIELDS`` scoping, the Lever 6
    call site's ``is_benchmark_leak`` invocation becomes a no-op for
    sql_snippet patch types. The call is retained for future-compat
    with any patch type that IS still firewalled."""

    def test_sql_snippet_no_longer_fingerprint_matched(self):
        """A Lever 6 measure proposal whose SQL fingerprints to a
        benchmark is now ACCEPTED (no firewall rejection)."""
        from genie_space_optimizer.optimization.leakage import (
            BenchmarkCorpus, is_benchmark_leak,
        )
        corpus = BenchmarkCorpus.from_benchmarks([{
            "id": "qx",
            "question": "total revenue",
            "expected_sql": "SELECT SUM(revenue) FROM sales",
        }])
        # Lever 6 proposes a measure whose SQL matches the benchmark.
        proposal = {
            "patch_type": "add_sql_snippet_measure",
            "sql": "SUM(revenue)",
            "display_name": "total_revenue",
        }
        is_leak, reason = is_benchmark_leak(
            proposal, proposal["patch_type"], corpus,
        )
        assert not is_leak, (
            f"Structural SQL firewall dropped; got leak={is_leak} reason={reason!r}"
        )

    def test_example_sql_still_firewalled(self):
        """Example_sql (answer-shape) remains strictly gated — this is
        the one path the firewall still covers after the scoping change."""
        from genie_space_optimizer.optimization.leakage import (
            BenchmarkCorpus, is_benchmark_leak,
        )
        corpus = BenchmarkCorpus.from_benchmarks([{
            "id": "qx",
            "question": "total revenue",
            "expected_sql": "SELECT SUM(revenue) FROM sales",
        }])
        proposal = {
            "patch_type": "add_example_sql",
            "example_question": "total revenue",
            "example_sql": "SELECT SUM(revenue) FROM sales",
        }
        is_leak, _ = is_benchmark_leak(
            proposal, proposal["patch_type"], corpus,
        )
        assert is_leak, "example_sql firewall must still catch fingerprint matches"


# ─────────────────────────────────────────────────────────────────────
# 6. _run_enrichment signature accepts held_out_benchmarks
# ─────────────────────────────────────────────────────────────────────


class TestRunEnrichmentSignature:
    """The widened ``_run_enrichment`` signature accepts ``held_out_benchmarks``
    as a keyword-only parameter; callers that don't pass it still work."""

    def test_signature_has_held_out_benchmarks_kwonly(self):
        import inspect

        from genie_space_optimizer.optimization.harness import _run_enrichment

        sig = inspect.signature(_run_enrichment)
        params = sig.parameters
        assert "held_out_benchmarks" in params
        ho = params["held_out_benchmarks"]
        assert ho.kind == inspect.Parameter.KEYWORD_ONLY
        assert ho.default is None
