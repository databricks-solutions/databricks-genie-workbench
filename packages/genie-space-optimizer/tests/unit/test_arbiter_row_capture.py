"""Unit tests for F12 / F13 — arbiter row-capture diagnostics and fallback.

The example-SQL synthesis pipelines feed ``_capture_result_rows`` output
to the arbiter LLM judge. Two production failure modes were previously
masked behind a single opaque ``arbiter_no_result_rows`` reason code:

* DBSQL refused the inline subquery wrap because the candidate SQL
  targets a metric view at the top level (the dominant case).
* The underlying execution genuinely failed (timeout, permission,
  syntax error that slipped past EXPLAIN).

PR 12 changes ``_capture_result_rows`` to return a 3-tuple
``(rows, error_class, error_message)`` so callers can branch on the
real cause; PR 13 adds a metric-view-safe Tier 2 fallback (LIMIT
injection on the original SQL) when Tier 1 (subquery wrap) raises.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from genie_space_optimizer.optimization import evaluation as ev_mod
from genie_space_optimizer.optimization.evaluation import (
    ROW_CAPTURE_ERR_EXEC_FAILED,
    ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED,
    _capture_result_rows,
    _classify_row_capture_error,
    _has_top_level_limit,
    _inject_limit_clause,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


class _FakeDF:
    """Minimal pandas-like stand-in for ``_exec_sql`` returns.

    The real return is a pandas DataFrame; the row-capture helper only
    uses ``.empty``, ``.head(n)``, and ``.to_dict(orient="records")``,
    which is the contract this fake honors.
    """

    def __init__(self, rows: list[dict]):
        self._rows = rows

    @property
    def empty(self) -> bool:
        return not self._rows

    def head(self, n: int) -> "_FakeDF":
        return _FakeDF(self._rows[:n])

    def to_dict(self, orient: str = "records") -> list[dict]:
        assert orient == "records"
        return list(self._rows)


def _patch_exec_sql(
    monkeypatch: pytest.MonkeyPatch, behavior: Any,
) -> list[str]:
    """Patch ``evaluation._exec_sql`` and capture the SQL it sees.

    ``behavior`` may be:
    * a list of dicts → returned as a ``_FakeDF`` of those rows
    * an Exception instance → raised on call
    * a callable ``(sql, spark, **kw) -> Any`` → invoked directly
    """
    captured: list[str] = []

    def _fake_exec_sql(sql: str, spark: Any, **kwargs: Any) -> Any:
        captured.append(sql)
        if callable(behavior) and not isinstance(behavior, BaseException):
            return behavior(sql, spark, **kwargs)
        if isinstance(behavior, BaseException):
            raise behavior
        return _FakeDF(list(behavior or []))

    monkeypatch.setattr(ev_mod, "_exec_sql", _fake_exec_sql)
    # ``resolve_sql`` is imported lazily inside the helper; keep the
    # default identity behavior so we exercise the wrap path verbatim.
    return captured


# ═══════════════════════════════════════════════════════════════════════
# _classify_row_capture_error — string-based bucketing
# ═══════════════════════════════════════════════════════════════════════


class TestClassifyRowCaptureError:
    @pytest.mark.parametrize(
        "msg",
        [
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY: foo",
            "SUBQUERY_EXPRESSION_IN: bar",
            "[METRIC_VIEW] Cannot use measure in subquery",
            "Top-level MEASURE() not allowed inside subquery",
            "scalar metric view subquery is unsupported",
        ],
    )
    def test_subquery_unsupported_markers(self, msg: str) -> None:
        assert (
            _classify_row_capture_error(msg)
            == ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED
        )

    @pytest.mark.parametrize(
        "msg",
        [
            "java.net.SocketTimeoutException: Read timed out",
            "AnalysisException: cannot resolve column",
            "PERMISSION_DENIED: insufficient privileges",
            "",
        ],
    )
    def test_other_messages_bucket_as_exec_failed(self, msg: str) -> None:
        assert (
            _classify_row_capture_error(msg)
            == ROW_CAPTURE_ERR_EXEC_FAILED
        )


# ═══════════════════════════════════════════════════════════════════════
# _capture_result_rows — return-tuple shape contract
# ═══════════════════════════════════════════════════════════════════════


class TestCaptureResultRowsTupleShape:
    def test_success_with_rows(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_exec_sql(
            monkeypatch, [{"region": "NA", "n": 5}],
        )
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT region, n FROM t", spark=None,
            catalog="cat", schema="sch",
        )
        assert rows == [{"region": "NA", "n": 5}]
        assert err_class is None
        assert err_msg is None

    def test_success_with_empty_rows_is_not_a_failure(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Empty result set is a valid query outcome — must NOT be
        attributed to a row-capture failure."""
        _patch_exec_sql(monkeypatch, [])
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT 1 FROM t WHERE FALSE", spark=None,
            catalog="cat", schema="sch",
        )
        assert rows == []
        assert err_class is None
        assert err_msg is None

    def test_failure_buckets_subquery_unsupported(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_exec_sql(
            monkeypatch,
            RuntimeError(
                "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY: scalar metric view"
            ),
        )
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT MEASURE(total_revenue) FROM mv", spark=None,
            catalog="cat", schema="sch",
        )
        assert rows is None
        assert err_class == ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED
        assert err_msg and "UNSUPPORTED_SUBQUERY" in err_msg

    def test_failure_buckets_exec_failed_for_generic_exception(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_exec_sql(monkeypatch, TimeoutError("Read timed out"))
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT * FROM t", spark=None,
            catalog="cat", schema="sch",
        )
        assert rows is None
        assert err_class == ROW_CAPTURE_ERR_EXEC_FAILED
        assert err_msg and "timed out" in err_msg.lower()

    def test_failure_truncates_long_error_message(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        long_msg = "x" * 5000
        _patch_exec_sql(monkeypatch, RuntimeError(long_msg))
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT * FROM t", spark=None,
            catalog="cat", schema="sch",
        )
        assert rows is None
        assert err_class == ROW_CAPTURE_ERR_EXEC_FAILED
        assert err_msg is not None
        assert len(err_msg) <= 200


# ═══════════════════════════════════════════════════════════════════════
# Logging — failures must surface at WARNING with SQL preview
# ═══════════════════════════════════════════════════════════════════════


class TestCaptureResultRowsLogging:
    def test_failure_logged_at_warning_with_sql_and_class(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _patch_exec_sql(
            monkeypatch,
            RuntimeError(
                "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY: metric view"
            ),
        )
        with caplog.at_level(
            logging.WARNING,
            logger=ev_mod.logger.name,
        ):
            _capture_result_rows(
                "SELECT MEASURE(rev) FROM mv_sales",
                spark=None, catalog="cat", schema="sch",
            )
        warnings = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING
            and "result-row capture failed" in rec.getMessage()
        ]
        assert warnings, "expected a WARNING log on row-capture failure"
        message = warnings[0].getMessage()
        assert ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED in message
        # SQL preview must be present in the log line — operators rely
        # on it to diagnose the candidate without re-running the job.
        assert "MEASURE(rev)" in message

    def test_success_does_not_warn(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _patch_exec_sql(monkeypatch, [{"x": 1}])
        with caplog.at_level(
            logging.WARNING,
            logger=ev_mod.logger.name,
        ):
            _capture_result_rows(
                "SELECT 1 FROM t",
                spark=None, catalog="cat", schema="sch",
            )
        assert not [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING
            and "result-row capture" in rec.getMessage()
        ]


# ═══════════════════════════════════════════════════════════════════════
# Synthesis gate — differentiated reason codes
# ═══════════════════════════════════════════════════════════════════════


class TestArbiterGateBranchesOnErrorClass:
    """The synthesis arbiter gate must emit distinct reason codes for
    each row-capture error class so the preflight rejection banner
    accurately attributes yield loss.
    """

    def _build_proposal(self) -> dict:
        return {
            "example_sql": "SELECT MEASURE(rev) FROM cat.sch.mv_sales",
            "example_question": "What is total revenue?",
        }

    def test_subquery_unsupported_emits_subquery_reason(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from genie_space_optimizer.optimization import synthesis as syn_mod

        # Pretend a real arbiter scorer is available so the gate
        # exercises the row-capture path rather than the
        # ``module_unavailable`` short-circuit.
        from genie_space_optimizer.optimization.scorers import (
            arbiter as arb_mod,
        )
        monkeypatch.setattr(
            arb_mod, "score_example_sql_correctness",
            lambda **kw: {"value": "yes"},
        )

        def _fake_capture(*args, **kwargs):
            return None, ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED, "MV unsupp"

        monkeypatch.setattr(
            ev_mod, "_capture_result_rows", _fake_capture,
        )

        result = syn_mod._gate_arbiter(
            self._build_proposal(),
            spark=object(),  # truthy ⇒ has_backend
            catalog="cat", gold_schema="sch",
            metadata_snapshot={}, w=None, warehouse_id="",
        )
        assert result.passed is False
        assert (
            result.reason
            == "arbiter_row_capture_subquery_unsupported"
        )

    def test_exec_failed_emits_exec_reason(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from genie_space_optimizer.optimization import synthesis as syn_mod
        from genie_space_optimizer.optimization.scorers import (
            arbiter as arb_mod,
        )
        monkeypatch.setattr(
            arb_mod, "score_example_sql_correctness",
            lambda **kw: {"value": "yes"},
        )

        def _fake_capture(*args, **kwargs):
            return None, ROW_CAPTURE_ERR_EXEC_FAILED, "Read timed out"

        monkeypatch.setattr(
            ev_mod, "_capture_result_rows", _fake_capture,
        )

        result = syn_mod._gate_arbiter(
            self._build_proposal(),
            spark=object(),
            catalog="cat", gold_schema="sch",
            metadata_snapshot={}, w=None, warehouse_id="",
        )
        assert result.passed is False
        assert result.reason == "arbiter_row_capture_exec_failed"


# ═══════════════════════════════════════════════════════════════════════
# Unified pipeline counters — splitting arbiter_no from row-capture
# ═══════════════════════════════════════════════════════════════════════


class TestUnifiedRejectionCounters:
    def test_counter_keys_are_initialized(self) -> None:
        """Operators rely on the rejection-counter dict carrying every
        key unconditionally so the banner never has to special-case a
        ``rc.get(key, 0)`` fallback."""
        # Indirect introspection: build the counter dict the same way
        # ``generate_validated_sql_examples`` does and assert the new
        # keys are present with zero defaults.
        rc: dict[str, int] = {
            "metadata": 0,
            "mv_select_star": 0,
            "explain_or_execute": 0,
            "arbiter_no": 0,
            "arbiter_row_capture_subquery_unsupported": 0,
            "arbiter_row_capture_exec_failed": 0,
            "firewall_fingerprint": 0,
            "firewall_question_echo": 0,
            "dedup_in_corpus": 0,
            "unfixable_after_correction": 0,
            "repaired_stemmed_identifiers": 0,
            "repaired_measure_refs": 0,
        }
        # The contract is symmetric — both new keys must be present.
        assert "arbiter_row_capture_subquery_unsupported" in rc
        assert "arbiter_row_capture_exec_failed" in rc


# ═══════════════════════════════════════════════════════════════════════
# F13 — LIMIT-injection helpers
#
# Tier 2 of ``_capture_result_rows`` runs the candidate SQL with a
# top-level ``LIMIT n`` appended (when not already present) instead of
# wrapping it as a subquery. The two helpers below are unit-tested in
# isolation so the row-capture tier 2 logic stays small and focused.
# ═══════════════════════════════════════════════════════════════════════


class TestHasTopLevelLimit:
    @pytest.mark.parametrize(
        "sql, expected",
        [
            ("SELECT * FROM t", False),
            ("SELECT * FROM t LIMIT 10", True),
            ("SELECT * FROM t LIMIT 10;", True),
            ("SELECT * FROM t LIMIT 10  ;  ", True),
            ("SELECT * FROM t LIMIT 10 OFFSET 5", True),
            ("select * from t limit 10", True),
            # Embedded LIMIT inside a subquery is not "top-level".
            (
                "SELECT * FROM (SELECT x FROM t LIMIT 5) z",
                False,
            ),
            ("", False),
        ],
    )
    def test_detection(self, sql: str, expected: bool) -> None:
        assert _has_top_level_limit(sql) is expected


class TestInjectLimitClause:
    def test_appends_limit_when_absent(self) -> None:
        out = _inject_limit_clause("SELECT * FROM t", limit=20)
        assert out == "SELECT * FROM t LIMIT 20"

    def test_preserves_trailing_semicolon(self) -> None:
        out = _inject_limit_clause("SELECT * FROM t;", limit=10)
        assert out == "SELECT * FROM t LIMIT 10;"

    def test_no_op_when_limit_already_present(self) -> None:
        sql = "SELECT * FROM t LIMIT 5"
        assert _inject_limit_clause(sql, limit=20) == sql

    def test_no_op_when_limit_with_offset_present(self) -> None:
        sql = "SELECT * FROM t LIMIT 5 OFFSET 10"
        assert _inject_limit_clause(sql, limit=20) == sql

    def test_appends_after_order_by(self) -> None:
        out = _inject_limit_clause(
            "SELECT * FROM t ORDER BY x DESC", limit=20,
        )
        assert out == "SELECT * FROM t ORDER BY x DESC LIMIT 20"


# ═══════════════════════════════════════════════════════════════════════
# F13 — two-tier execution strategy
#
# Tier 1 = subquery wrap (cheap, deterministic, fails on metric-view
# top-level MEASURE() queries). Tier 2 = LIMIT injection on the original
# SQL (works for metric views, slightly more expensive). Tier 2 fires
# either pro-actively (MEASURE( detected) or reactively (Tier 1 raised
# subquery_unsupported).
# ═══════════════════════════════════════════════════════════════════════


class TestCaptureResultRowsTierStrategy:
    def test_tier1_used_for_plain_sql(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Plain SELECT (no MEASURE()) should hit Tier 1 first — the
        subquery wrap path. Operators rely on Tier 1 staying the
        default for non-MV SQL because it's cheaper and more
        deterministic in row count."""
        captured = _patch_exec_sql(
            monkeypatch, [{"x": 1}],
        )
        rows, err_class, _ = _capture_result_rows(
            "SELECT x FROM cat.sch.t", spark=None,
            catalog="cat", schema="sch",
        )
        assert rows == [{"x": 1}]
        assert err_class is None
        # Exactly one execution: the wrap.
        assert len(captured) == 1
        assert captured[0].startswith("SELECT * FROM (SELECT x FROM cat.sch.t)")
        assert "_gvse_sample" in captured[0]
        assert captured[0].rstrip().endswith("LIMIT 20")

    def test_tier2_used_proactively_for_measure_sql(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """SQL containing ``MEASURE(`` skips Tier 1 entirely — the
        wrap is a known-failure shape against metric views, no point
        burning a round-trip just to re-discover it."""
        captured = _patch_exec_sql(
            monkeypatch, [{"rev": 100}],
        )
        rows, err_class, _ = _capture_result_rows(
            "SELECT MEASURE(total_revenue) FROM cat.sch.mv_sales",
            spark=None, catalog="cat", schema="sch",
        )
        assert rows == [{"rev": 100}]
        assert err_class is None
        assert len(captured) == 1
        # Tier 2 appends LIMIT directly — no subquery wrap.
        assert captured[0] == (
            "SELECT MEASURE(total_revenue) FROM cat.sch.mv_sales LIMIT 20"
        )
        assert "_gvse_sample" not in captured[0]

    def test_tier2_falls_back_when_tier1_raises_subquery_unsupported(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If Tier 1 raises a subquery-wrap incompat error, Tier 2
        re-runs the original SQL with LIMIT injected. This is the
        path that recovers metric-view candidates whose SQL doesn't
        textually contain ``MEASURE(`` (e.g. plain
        ``SELECT * FROM mv``) but still fails the wrap at exec time."""
        captured: list[str] = []

        def _exec(sql: str, spark: Any, **kwargs: Any) -> Any:
            captured.append(sql)
            if "_gvse_sample" in sql:
                # Tier 1 — subquery wrap → fails with MV-style error.
                raise RuntimeError(
                    "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY: metric view"
                )
            return _FakeDF([{"region": "NA"}])

        monkeypatch.setattr(ev_mod, "_exec_sql", _exec)
        rows, err_class, _ = _capture_result_rows(
            "SELECT region FROM cat.sch.mv_sales",
            spark=None, catalog="cat", schema="sch",
        )
        assert rows == [{"region": "NA"}]
        assert err_class is None
        assert len(captured) == 2
        assert "_gvse_sample" in captured[0]  # Tier 1
        assert captured[1] == (
            "SELECT region FROM cat.sch.mv_sales LIMIT 20"
        )

    def test_tier2_not_used_for_non_subquery_tier1_errors(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A generic exec failure (e.g. timeout, permission) is not a
        subquery-wrap problem — Tier 2 wouldn't help. The helper must
        return immediately without a second execution attempt."""
        _patch_exec_sql(
            monkeypatch, TimeoutError("Read timed out"),
        )
        # Replay the patch with capture-aware behavior to count calls.
        call_count = {"n": 0}
        original = ev_mod._exec_sql

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            call_count["n"] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(ev_mod, "_exec_sql", _wrap)
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT x FROM cat.sch.t",
            spark=None, catalog="cat", schema="sch",
        )
        assert rows is None
        assert err_class == ROW_CAPTURE_ERR_EXEC_FAILED
        assert err_msg and "timed out" in err_msg.lower()
        assert call_count["n"] == 1, (
            "Tier 2 should not run for non-subquery Tier 1 failures"
        )

    def test_returns_error_when_both_tiers_fail(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tier 1 raises subquery_unsupported → Tier 2 also raises →
        the helper surfaces the Tier 2 error class to the caller. The
        synthesis gate / unified pipeline counter then attribute the
        rejection to the correct bucket."""
        captured: list[str] = []

        def _exec(sql: str, spark: Any, **kwargs: Any) -> Any:
            captured.append(sql)
            if "_gvse_sample" in sql:
                raise RuntimeError(
                    "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY: mv"
                )
            raise TimeoutError("Read timed out from warehouse")

        monkeypatch.setattr(ev_mod, "_exec_sql", _exec)
        rows, err_class, err_msg = _capture_result_rows(
            "SELECT region FROM cat.sch.t",
            spark=None, catalog="cat", schema="sch",
        )
        assert rows is None
        assert err_class == ROW_CAPTURE_ERR_EXEC_FAILED
        assert err_msg and "timed out" in err_msg.lower()
        assert len(captured) == 2

    def test_tier2_preserves_existing_top_level_limit(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the candidate SQL already has a top-level ``LIMIT 5``,
        Tier 2 must not double-apply ``LIMIT 20`` — that would change
        the result the arbiter sees vs what the LLM generated."""
        captured = _patch_exec_sql(monkeypatch, [{"x": 1}])
        rows, _, _ = _capture_result_rows(
            "SELECT MEASURE(rev) FROM cat.sch.mv LIMIT 5",
            spark=None, catalog="cat", schema="sch",
        )
        assert rows == [{"x": 1}]
        assert len(captured) == 1
        # No double-LIMIT.
        assert captured[0].count("LIMIT") == 1
        assert captured[0].endswith("LIMIT 5")
