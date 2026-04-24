"""Tests for intelligent entity-matching slot allocation + RLS audit.

Covers the Phase 3 work in ``optimization/applier.py`` and
``iq_scan/rls_audit.py``:

- ``_entity_matching_score`` hard disqualifiers and bonuses.
- ``auto_apply_prompt_matching`` filter-not-sort semantics (closes the
  silent-PII leak on <120-col spaces).
- ``auto_apply_prompt_matching`` idempotent diff allocator: empty diff
  on re-run, deterministic tie-break, displacement of low-score slots,
  disable of PII / RLS-tainted slots, dry-run mode.
- ``collect_rls_audit`` via mocked ``information_schema`` queries:
  direct row_filter, direct column_mask, inherited RLS via views,
  dynamic-view detection, probe failure → verdict=unknown.
- OFF-mode regression via ``@pytest.mark.parametrize("smarter_scoring", [True, False])``
  so the legacy 0/1/2 scorer + no-filter + enable-only path continues
  to reproduce today's exact behaviour (including the PII leak) under
  ``GSO_SMARTER_SCORING=false``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from genie_space_optimizer.common import config as cfg
from genie_space_optimizer.iq_scan.rls_audit import (
    _canonical_fqn,
    _extract_space_tables,
    _regex_base_tables,
    collect_rls_audit,
)
from genie_space_optimizer.optimization.applier import (
    _entity_matching_score,
    _entity_matching_score_legacy,
    _extract_benchmark_col_refs,
    auto_apply_prompt_matching,
)


# ───────────────────────────────────────────────────────────────────────
# _entity_matching_score — hard disqualifiers (12 cases)
# ───────────────────────────────────────────────────────────────────────


class TestScorerHardDisqualifiers:
    def test_free_text_name_description_rejects(self):
        score, reason = _entity_matching_score("description")
        assert score == 0.0
        assert reason == "free_text_name"

    def test_free_text_name_notes_rejects(self):
        score, reason = _entity_matching_score("customer_notes")
        assert score == 0.0
        assert reason == "free_text_name"

    def test_pii_name_email_takes_priority_over_free_text(self):
        """email is in BOTH lists; PII check runs first so reason is more specific."""
        score, reason = _entity_matching_score("customer_email")
        assert score == 0.0
        assert reason == "pii_name"

    def test_pii_name_ssn_rejects(self):
        score, reason = _entity_matching_score("customer_ssn")
        assert score == 0.0
        assert reason == "pii_name"

    def test_pii_name_phone_rejects(self):
        score, reason = _entity_matching_score("customer_phone")
        assert score == 0.0
        assert reason == "pii_name"

    def test_boolean_flag_name_rejects(self):
        score, reason = _entity_matching_score("is_active")
        assert score == 0.0
        assert reason == "boolean_flag"

    def test_boolean_flag_yn_rejects(self):
        score, reason = _entity_matching_score("subscribed_yn")
        assert score == 0.0
        assert reason == "boolean_flag"

    def test_pii_description_rejects(self):
        score, reason = _entity_matching_score(
            "x_col", description="This column contains PII data",
        )
        assert score == 0.0
        assert reason == "pii_description"

    def test_cardinality_too_low_rejects(self):
        score, reason = _entity_matching_score(
            "status", profile={"cardinality": 1},
        )
        assert score == 0.0
        assert reason == "cardinality_too_low"

    def test_cardinality_too_high_rejects(self):
        score, reason = _entity_matching_score(
            "status", profile={"cardinality": 2048},
        )
        assert score == 0.0
        assert reason == "cardinality_too_high"

    def test_id_like_distinct_ratio_rejects(self):
        # 95 distinct values out of 100 rows → obviously ID-like
        score, reason = _entity_matching_score(
            "transaction_guid",
            profile={"cardinality": 95},
            row_count=100,
        )
        assert score == 0.0
        assert reason == "id_like_distinct_ratio"

    def test_rls_tainted_rejects(self):
        score, reason = _entity_matching_score(
            "region_name", rls_verdict="tainted",
        )
        assert score == 0.0
        assert reason == "rls_tainted"


# ───────────────────────────────────────────────────────────────────────
# RLS unknown semantics — default tolerant, strict mode rejects
# ───────────────────────────────────────────────────────────────────────


class TestScorerRLSUnknown:
    def test_unknown_default_scores_normally(self):
        score, reason = _entity_matching_score(
            "region_name", rls_verdict="unknown", strict_rls=False,
        )
        assert score > 0.0
        assert reason == "ok"

    def test_unknown_strict_mode_rejects(self):
        score, reason = _entity_matching_score(
            "region_name", rls_verdict="unknown", strict_rls=True,
        )
        assert score == 0.0
        assert reason == "rls_unknown_strict"


# ───────────────────────────────────────────────────────────────────────
# _entity_matching_score — score bands
# ───────────────────────────────────────────────────────────────────────


class TestScorerBands:
    def test_categorical_name_base(self):
        score, reason = _entity_matching_score("region_code")
        assert score == 3.0
        assert reason == "ok"

    def test_non_categorical_base(self):
        score, reason = _entity_matching_score("login_ip")
        assert score == 1.0
        assert reason == "ok"

    def test_sweet_spot_cardinality_bonus(self):
        score, _ = _entity_matching_score(
            "region_code", profile={"cardinality": 50},
        )
        assert score == 5.0  # 3 categorical + 2 sweet spot

    def test_medium_cardinality_bonus(self):
        score, _ = _entity_matching_score(
            "region_code", profile={"cardinality": 500},
        )
        assert score == 4.0  # 3 + 1

    def test_thin_cardinality_bonus(self):
        score, _ = _entity_matching_score(
            "region_code", profile={"cardinality": 3},
        )
        assert score == pytest.approx(3.3)

    def test_benchmark_ref_bonus(self):
        score, _ = _entity_matching_score(
            "amount", benchmark_col_refs=frozenset({"amount"}),
        )
        assert score == 4.0  # 1 generic + 3 benchmark

    def test_description_positive_hint(self):
        score, _ = _entity_matching_score(
            "xyz", description="Enum: one of ACTIVE/INACTIVE/SUSPENDED",
        )
        assert score == 2.0  # 1 generic + 1 positive

    def test_description_negative_hint(self):
        score, _ = _entity_matching_score(
            "region_code", description="Internal ETL audit field",
        )
        # 3 + (-2) = 1 but both "internal" and "etl" + "audit" match, and
        # the negative penalty is capped at -2 per pattern (first match
        # subtracts once). Verify it's lower than bare categorical.
        assert score <= 1.0
        assert score >= 0.0

    def test_multi_bonus_capped_at_10(self):
        score, _ = _entity_matching_score(
            "region_code",
            profile={"cardinality": 50},           # +2
            benchmark_col_refs=frozenset({"region_code"}),  # +3
            description="valid values: one of ...",  # +1 positive
        )
        # 3 + 2 + 3 + 1 = 9, still < 10 cap
        assert 8.5 <= score <= 10.0


# ───────────────────────────────────────────────────────────────────────
# has_profile guard — unprofiled columns fall through to name-based
# ───────────────────────────────────────────────────────────────────────


class TestScorerUnprofiledFallthrough:
    def test_no_profile_scores_by_name(self):
        score, reason = _entity_matching_score("region_code", profile=None)
        assert score == 3.0
        assert reason == "ok"

    def test_empty_profile_scores_by_name(self):
        score, _ = _entity_matching_score("region_code", profile={})
        assert score == 3.0

    def test_zero_cardinality_treated_as_unprofiled(self):
        """A profile with cardinality=0 is treated as 'not profiled'
        (not hard-rejected) — covers MVs that get empty profile entries
        from ``_collect_data_profile`` on unprofiled tables."""
        score, reason = _entity_matching_score(
            "region_code", profile={"cardinality": 0},
        )
        assert score == 3.0
        assert reason == "ok"


# ───────────────────────────────────────────────────────────────────────
# _extract_benchmark_col_refs
# ───────────────────────────────────────────────────────────────────────


class TestBenchmarkColRefs:
    def test_empty(self):
        assert _extract_benchmark_col_refs(None) == frozenset()
        assert _extract_benchmark_col_refs([]) == frozenset()

    def test_tokens_from_expected_sql(self):
        refs = _extract_benchmark_col_refs([
            {"expected_sql": "SELECT amount, region FROM orders"},
        ])
        assert "amount" in refs
        assert "region" in refs
        assert "orders" in refs

    def test_strips_string_literals(self):
        """String literal contents should NOT become col refs."""
        refs = _extract_benchmark_col_refs([
            {"expected_sql": "SELECT x FROM t WHERE status = 'active_region'"},
        ])
        assert "active_region" not in refs

    def test_strips_comments(self):
        refs = _extract_benchmark_col_refs([
            {"expected_sql": "-- a comment mentioning token\nSELECT x FROM t"},
        ])
        assert "token" not in refs


# ───────────────────────────────────────────────────────────────────────
# collect_rls_audit — mocked information_schema
# ───────────────────────────────────────────────────────────────────────


def _make_exec_sql_mock(query_responses: dict[str, pd.DataFrame]):
    """Build a mock exec_sql that matches queries by substring."""
    def _mock(sql: str):
        # Longest-match first so 'view_table_usage' matches before 'view'
        for pattern, df in sorted(
            query_responses.items(), key=lambda kv: -len(kv[0])
        ):
            if pattern in sql.upper():
                return df
        return pd.DataFrame()
    return _mock


class TestRLSAudit:
    def test_empty_input(self):
        assert collect_rls_audit([]) == {}

    def test_probe_failure_marks_unknown(self):
        def bad_exec(sql):
            raise RuntimeError("information_schema does not exist")

        tables = [{"identifier": "cat.sch.t1"}, {"identifier": "cat.sch.t2"}]
        result = collect_rls_audit(tables, exec_sql=bad_exec)
        assert result["cat.sch.t1"]["verdict"] == "unknown"
        assert result["cat.sch.t2"]["verdict"] == "unknown"
        assert "probe failed" in result["cat.sch.t1"]["reason"]

    def test_direct_row_filter_marks_tainted(self):
        tables = [{"identifier": "cat.sch.orders"}]
        rf_df = pd.DataFrame([{
            "table_schema": "sch", "table_name": "orders",
            "table_catalog": "cat", "filter_name": "pii_filter",
            "target_columns": "",
        }])
        exec_sql = _make_exec_sql_mock({
            "ROW_FILTERS": rf_df,
            "COLUMN_MASKS": pd.DataFrame(),
            "VIEWS": pd.DataFrame(),
            "VIEW_TABLE_USAGE": pd.DataFrame(),
        })
        result = collect_rls_audit(tables, exec_sql=exec_sql)
        assert result["cat.sch.orders"]["verdict"] == "tainted"
        assert result["cat.sch.orders"]["has_direct_row_filter"] is True
        assert "row_filter" in result["cat.sch.orders"]["reason"]

    def test_direct_column_mask_marks_tainted(self):
        tables = [{"identifier": "cat.sch.customers"}]
        cm_df = pd.DataFrame([{
            "table_schema": "sch", "table_name": "customers",
            "table_catalog": "cat",
            "column_name": "email", "mask_name": "email_redact",
        }])
        exec_sql = _make_exec_sql_mock({
            "ROW_FILTERS": pd.DataFrame(),
            "COLUMN_MASKS": cm_df,
        })
        result = collect_rls_audit(tables, exec_sql=exec_sql)
        assert result["cat.sch.customers"]["verdict"] == "tainted"
        assert result["cat.sch.customers"]["has_direct_column_mask"] is True

    def test_inherited_rls_via_view(self):
        """A view references a row-filtered base table → verdict=tainted."""
        tables = [
            {"identifier": "cat.sch.mv_sales", "table_type": "VIEW"},
            {"identifier": "cat.sch.orders"},
        ]
        rf_df = pd.DataFrame([{
            "table_schema": "sch", "table_name": "orders",
            "table_catalog": "cat", "filter_name": "pii_filter",
            "target_columns": "",
        }])
        vtu_df = pd.DataFrame([{
            "view_schema": "sch", "view_name": "mv_sales",
            "table_catalog": "cat", "table_schema": "sch", "table_name": "orders",
        }])
        views_df = pd.DataFrame([{
            "table_schema": "sch", "table_name": "mv_sales",
            "view_definition": "SELECT * FROM cat.sch.orders WHERE status = 'active'",
        }])
        exec_sql = _make_exec_sql_mock({
            "ROW_FILTERS": rf_df,
            "COLUMN_MASKS": pd.DataFrame(),
            "VIEW_TABLE_USAGE": vtu_df,
            "INFORMATION_SCHEMA.VIEWS": views_df,
        })
        result = collect_rls_audit(tables, exec_sql=exec_sql)
        assert result["cat.sch.mv_sales"]["verdict"] == "tainted"
        assert "cat.sch.orders" in result["cat.sch.mv_sales"]["inherits_rls_via"]

    def test_dynamic_view_detection(self):
        """A view using current_user() is flagged as tainted."""
        tables = [{"identifier": "cat.sch.mv_me", "table_type": "VIEW"}]
        views_df = pd.DataFrame([{
            "table_schema": "sch", "table_name": "mv_me",
            "view_definition":
                "SELECT * FROM cat.sch.orders WHERE owner = current_user()",
        }])
        vtu_df = pd.DataFrame([{
            "view_schema": "sch", "view_name": "mv_me",
            "table_catalog": "cat", "table_schema": "sch", "table_name": "orders",
        }])
        exec_sql = _make_exec_sql_mock({
            "ROW_FILTERS": pd.DataFrame(),
            "COLUMN_MASKS": pd.DataFrame(),
            "VIEW_TABLE_USAGE": vtu_df,
            "INFORMATION_SCHEMA.VIEWS": views_df,
        })
        result = collect_rls_audit(tables, exec_sql=exec_sql)
        assert result["cat.sch.mv_me"]["verdict"] == "tainted"
        assert result["cat.sch.mv_me"]["has_dynamic_view_function"] is True
        assert "identity function" in result["cat.sch.mv_me"]["reason"]

    def test_clean_verdict(self):
        """All probes succeed with empty results → verdict=clean."""
        tables = [{"identifier": "cat.sch.t1"}]
        exec_sql = _make_exec_sql_mock({})  # every query returns empty
        result = collect_rls_audit(tables, exec_sql=exec_sql)
        assert result["cat.sch.t1"]["verdict"] == "clean"

    def test_row_filters_sql_uses_real_infoschema_columns(self):
        """Regression: ``information_schema.row_filters`` exposes
        ``table_schema``/``table_name`` — NOT ``schema_name``. Selecting
        or filtering on ``schema_name`` raises ``UNRESOLVED_COLUMN`` at
        runtime, and the caller falls back to an empty set (clean), which
        silently disables the direct row-filter check. This test pins
        the SQL shape to the real info_schema field names.
        """
        captured_sql: list[str] = []

        def capturing_exec(sql: str):
            captured_sql.append(sql)
            # Probe SELECT 1: return empty df. The real executors return
            # a pandas DataFrame; `not df.empty` guards the caller.
            return pd.DataFrame()

        tables = [{"identifier": "cat.sch.orders"}]
        collect_rls_audit(tables, exec_sql=capturing_exec)

        rf_sql = next(
            (
                s for s in captured_sql
                if "row_filters" in s.lower()
                and "select 1" not in s.lower()  # skip probe
            ),
            None,
        )
        assert rf_sql is not None, (
            f"row_filters query not issued; captured: {captured_sql}"
        )
        # The view's real column is ``table_schema``. Any appearance of
        # ``schema_name`` as a projected/filtered identifier means we've
        # reintroduced the unresolved-column regression.
        assert "table_schema" in rf_sql
        # Guard against naive ``schema_name`` usage (allow the word only
        # inside string literals, but our SQL has no literal text here).
        assert "schema_name" not in rf_sql, (
            "row_filters SQL must NOT reference `schema_name`; "
            f"got: {rf_sql}"
        )

    def test_column_masks_sql_uses_real_infoschema_columns(self):
        """Regression twin for ``information_schema.column_masks``.

        Same contract as ``row_filters``: the view exposes
        ``table_schema`` / ``table_name`` and the query must not
        reference ``schema_name``.
        """
        captured_sql: list[str] = []

        def capturing_exec(sql: str):
            captured_sql.append(sql)
            return pd.DataFrame()

        tables = [{"identifier": "cat.sch.customers"}]
        collect_rls_audit(tables, exec_sql=capturing_exec)

        cm_sql = next(
            (s for s in captured_sql if "column_masks" in s.lower()),
            None,
        )
        assert cm_sql is not None, (
            f"column_masks query not issued; captured: {captured_sql}"
        )
        assert "table_schema" in cm_sql
        assert "schema_name" not in cm_sql, (
            "column_masks SQL must NOT reference `schema_name`; "
            f"got: {cm_sql}"
        )

    def test_row_filters_unresolved_column_is_fail_open(self):
        """If ``information_schema.row_filters`` raises at query time
        (simulating the ``UNRESOLVED_COLUMN`` regression we just fixed),
        the audit must stay fail-open: probe succeeds, the downstream
        query returns an empty set via logged warning, and the table
        stays ``clean`` rather than exploding. This pins the contract
        in the module docstring.
        """

        def exec_sql(sql: str):
            upper = sql.upper()
            if "SELECT 1" in upper and "ROW_FILTERS" in upper:
                # Probe succeeds.
                return pd.DataFrame()
            if "ROW_FILTERS" in upper:
                raise RuntimeError(
                    "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, "
                    "variable, or function parameter with name "
                    "`schema_name` cannot be resolved."
                )
            return pd.DataFrame()

        tables = [{"identifier": "cat.sch.orders"}]
        result = collect_rls_audit(tables, exec_sql=exec_sql)
        assert result["cat.sch.orders"]["verdict"] == "clean"
        assert result["cat.sch.orders"]["has_direct_row_filter"] is False


# ───────────────────────────────────────────────────────────────────────
# rls_audit helpers
# ───────────────────────────────────────────────────────────────────────


class TestRLSAuditHelpers:
    def test_extract_space_tables_fq_only(self):
        parsed = _extract_space_tables([
            {"identifier": "cat.sch.tbl"},
            {"identifier": "short_name"},           # skipped — not FQ
            {"identifier": "`cat`.`sch`.`t`"},       # backticks stripped
            {"identifier": "cat.sch.mv", "table_type": "VIEW"},
        ])
        idents = [(c, s, t) for c, s, t, _ in parsed]
        assert ("cat", "sch", "tbl") in idents
        assert ("cat", "sch", "t") in idents
        assert not any(t == "short_name" for _, _, t in idents)

    def test_regex_base_tables_extracts_fq_refs(self):
        ddl = (
            "SELECT a, b FROM cat.sch.orders o "
            "JOIN cat.sch.customers c ON o.cust_id = c.id"
        )
        refs = _regex_base_tables(ddl)
        assert ("cat", "sch", "orders") in refs
        assert ("cat", "sch", "customers") in refs

    def test_regex_base_tables_skips_bare_refs(self):
        """Bare ``FROM t`` (without catalog.schema.) is not resolvable
        without context — we skip rather than guess."""
        refs = _regex_base_tables("SELECT * FROM orders")
        assert refs == []

    def test_canonical_fqn_normalises(self):
        assert _canonical_fqn("CAT", "SCH", "`Tbl`") == "cat.sch.tbl"


# ───────────────────────────────────────────────────────────────────────
# auto_apply_prompt_matching — filter-not-sort (silent PII leak fix)
# ───────────────────────────────────────────────────────────────────────


def _make_config(
    columns_by_table: dict[str, list[tuple[str, str]]],
    *,
    enabled_em: dict[str, set[str]] | None = None,
    rls_audit: dict[str, dict] | None = None,
) -> dict:
    """Build a minimal config dict for auto_apply_prompt_matching.

    ``enabled_em`` pre-sets ``enable_entity_matching=True`` on specific
    (table, column) pairs so tests can simulate a prior-run state.
    ``rls_audit`` populates ``_rls_audit`` so tests can simulate tainted
    tables without monkeypatching ``collect_rls_audit``.
    """
    enabled_em = enabled_em or {}

    def _cc(tbl: str, col: str) -> dict:
        entry = {"column_name": col}
        if col in enabled_em.get(tbl, set()):
            entry["enable_entity_matching"] = True
            entry["enable_format_assistance"] = True
        return entry

    return {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": tbl,
                        "column_configs": [_cc(tbl, col) for col, _ in cols],
                    }
                    for tbl, cols in columns_by_table.items()
                ],
                "metric_views": [],
            },
        },
        "_uc_columns": [
            {
                "table_name": tbl.split(".")[-1],
                "column_name": col,
                "data_type": dtype,
            }
            for tbl, cols in columns_by_table.items()
            for col, dtype in cols
        ],
        "_rls_audit": rls_audit or {},
    }


def _run_apply(config: dict) -> dict:
    """Invoke auto_apply_prompt_matching with patch_space_config stubbed."""
    w = MagicMock()
    with patch(
        "genie_space_optimizer.optimization.applier.patch_space_config",
    ):
        return auto_apply_prompt_matching(w, "sp-1", config)


def _pin_smarter_scoring(monkeypatch, enabled: bool) -> None:
    """Override ENABLE_SMARTER_SCORING at both the module and the
    applier-local binding so the branch at the top of
    ``auto_apply_prompt_matching`` takes the desired path."""
    monkeypatch.setattr(cfg, "ENABLE_SMARTER_SCORING", enabled)
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.applier.ENABLE_SMARTER_SCORING",
        enabled,
    )


_NEUTRAL_COLUMNS: list[tuple[str, str]] = [
    ("region_code", "STRING"),
    ("country_code", "STRING"),
    ("status", "STRING"),
    ("segment", "STRING"),
    ("channel", "STRING"),
    ("brand", "STRING"),
    ("tier", "STRING"),
    ("category", "STRING"),
    ("department", "STRING"),
    ("zone_code", "STRING"),
]


class TestIdempotency:
    """Same inputs → same top-120 → empty diff on re-run."""

    def test_second_run_produces_empty_diff(self, monkeypatch):
        _pin_smarter_scoring(monkeypatch, True)
        config = _make_config({"cat.sch.orders": list(_NEUTRAL_COLUMNS)})

        # First run: every column passes the scorer, so they all get enabled.
        first = _run_apply(config)
        enabled_first = [
            c for c in first["applied"] if c["type"] == "enable_value_dictionary"
        ]
        assert enabled_first, "first run should enable at least one slot"
        assert first["entity_matching_disabled_count"] == 0

        # Second run starts from the refreshed config (caller simulates
        # the post-PATCH state by flipping the flags on the same dict).
        for tbl in config["_parsed_space"]["data_sources"]["tables"]:
            for cc in tbl["column_configs"]:
                if any(c["column"] == cc["column_name"] for c in enabled_first):
                    cc["enable_entity_matching"] = True
                    cc["enable_format_assistance"] = True

        second = _run_apply(config)
        em_changes = [
            c
            for c in second["applied"]
            if c["type"]
            in ("enable_value_dictionary", "disable_value_dictionary")
        ]
        assert em_changes == [], f"second run should be a no-op for EM, got {em_changes}"

    def test_deterministic_tie_breaking(self, monkeypatch):
        """Two candidates with identical scores + descriptions — stable
        sort on ``(-score, table, col)`` must pick the same winner across
        invocations regardless of column_configs insertion order."""
        _pin_smarter_scoring(monkeypatch, True)
        # Two columns at score=3.0 (categorical bare name, no profile).
        # Forward-ordered on first run, reversed on second.
        cols_a = [("region_code", "STRING"), ("zone_code", "STRING")]
        cols_b = list(reversed(cols_a))
        cfg_a = _make_config({"cat.sch.t": cols_a})
        cfg_b = _make_config({"cat.sch.t": cols_b})

        a = _run_apply(cfg_a)
        b = _run_apply(cfg_b)

        enables_a = sorted(
            c["column"] for c in a["applied"]
            if c["type"] == "enable_value_dictionary"
        )
        enables_b = sorted(
            c["column"] for c in b["applied"]
            if c["type"] == "enable_value_dictionary"
        )
        assert enables_a == enables_b, (
            "tie-break should be insertion-order independent"
        )


class TestDiffApplication:
    """Diff semantics: displacement, PII disable, RLS disable."""

    def test_new_high_score_column_displaces_low_score(self, monkeypatch):
        """When the space is at the 120-slot cap with low-score fillers,
        a new high-score candidate should displace exactly one low slot."""
        _pin_smarter_scoring(monkeypatch, True)
        monkeypatch.setattr(cfg, "MAX_VALUE_DICTIONARY_COLUMNS", 2)
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.applier.MAX_VALUE_DICTIONARY_COLUMNS",
            2,
        )
        # Two existing low-score slots (generic, score=1.0 each) plus one
        # new high-score candidate (categorical, score=3.0).
        cols = [
            ("login_ip", "STRING"),   # score ~1.0
            ("session_host", "STRING"),  # score ~1.0
            ("region_code", "STRING"),  # score 3.0 (categorical)
        ]
        config = _make_config(
            {"cat.sch.t": cols},
            enabled_em={"cat.sch.t": {"login_ip", "session_host"}},
        )
        result = _run_apply(config)
        enables = [
            c["column"] for c in result["applied"]
            if c["type"] == "enable_value_dictionary"
        ]
        disables = [
            c["column"] for c in result["applied"]
            if c["type"] == "disable_value_dictionary"
        ]
        assert "region_code" in enables
        # Exactly one of the two low-score slots got displaced.
        assert len(disables) == 1
        assert disables[0] in {"login_ip", "session_host"}

    def test_pii_column_enabled_previously_gets_disabled(self, monkeypatch):
        """Prior-run space had customer_email enabled (legacy scorer). New
        scorer hard-rejects PII → column falls out of top-120 → disable."""
        _pin_smarter_scoring(monkeypatch, True)
        cols = [
            ("customer_email", "STRING"),  # score 0.0 under new scorer
            ("region_code", "STRING"),
        ]
        config = _make_config(
            {"cat.sch.orders": cols},
            enabled_em={"cat.sch.orders": {"customer_email"}},
        )
        result = _run_apply(config)
        disables = [
            c["column"] for c in result["applied"]
            if c["type"] == "disable_value_dictionary"
        ]
        assert "customer_email" in disables
        # pii rejection reflected in aggregate tally too.
        assert result["rejected_by_reason"].get("pii_name", 0) >= 1

    def test_rls_tainted_slot_gets_disabled(self, monkeypatch):
        """Existing EM slot on a table whose RLS audit verdict is
        'tainted' — scorer rejects → column falls out → disable."""
        _pin_smarter_scoring(monkeypatch, True)
        cols = [("region_code", "STRING")]
        rls_audit = {
            "cat.sch.secret": {
                "verdict": "tainted",
                "reason": "row_filter = secure.pii_filter",
            },
        }
        config = _make_config(
            {"cat.sch.secret": cols},
            enabled_em={"cat.sch.secret": {"region_code"}},
            rls_audit=rls_audit,
        )
        result = _run_apply(config)
        disables = [
            c["column"] for c in result["applied"]
            if c["type"] == "disable_value_dictionary"
        ]
        assert "region_code" in disables
        assert result["rejected_by_reason"].get("rls_tainted", 0) >= 1

    def test_schema_unchanged_has_no_slots_disabled(self, monkeypatch):
        """Steady state: scores unchanged, slots under cap → zero diff
        after the first-time enablement lands."""
        _pin_smarter_scoring(monkeypatch, True)
        cols = [
            ("region_code", "STRING"),
            ("country_code", "STRING"),
        ]
        # Both already enabled (simulates post-first-run state).
        config = _make_config(
            {"cat.sch.t": cols},
            enabled_em={"cat.sch.t": {"region_code", "country_code"}},
        )
        result = _run_apply(config)
        em_changes = [
            c for c in result["applied"]
            if c["type"] in ("enable_value_dictionary", "disable_value_dictionary")
        ]
        assert em_changes == []

    @pytest.mark.parametrize("smarter_scoring", [True, False])
    def test_small_space_with_pii_column(self, smarter_scoring, monkeypatch):
        """Regression pin: under smarter_scoring=True the new scorer filters
        PII out of the pool; under smarter_scoring=False the legacy shim
        preserves today's silent-leak behaviour — so tests detect any
        accidental change to ``_legacy_apply_em``."""
        _pin_smarter_scoring(monkeypatch, smarter_scoring)
        cols = [
            ("customer_email", "STRING"),
            ("region_code", "STRING"),
            ("country_code", "STRING"),
            ("status", "STRING"),
            ("segment", "STRING"),
            ("channel", "STRING"),
            ("brand", "STRING"),
            ("tier", "STRING"),
            ("category", "STRING"),
            ("department", "STRING"),
        ]
        config = _make_config({"cat.sch.orders": cols})
        result = _run_apply(config)
        enabled_em = {
            c["column"] for c in result["applied"]
            if c["type"] == "enable_value_dictionary"
        }
        if smarter_scoring:
            assert "customer_email" not in enabled_em
            assert "region_code" in enabled_em
            assert result["rejected_by_reason"].get("pii_name", 0) >= 1
        else:
            # Legacy behaviour: PII still gets slotted because the pool
            # is smaller than the 120-cap and the legacy path doesn't
            # filter score-0 candidates.
            assert "customer_email" in enabled_em


class TestDryRun:
    """DRY_RUN_ENTITY_MATCHING: logs the diff but mutates nothing."""

    def test_dry_run_logs_but_no_changes(self, monkeypatch):
        _pin_smarter_scoring(monkeypatch, True)
        monkeypatch.setattr(cfg, "DRY_RUN_ENTITY_MATCHING", True)
        monkeypatch.setattr(
            "genie_space_optimizer.common.config.DRY_RUN_ENTITY_MATCHING",
            True,
        )
        cols = [
            ("customer_email", "STRING"),
            ("region_code", "STRING"),
        ]
        config = _make_config(
            {"cat.sch.orders": cols},
            enabled_em={"cat.sch.orders": {"customer_email"}},
        )
        result = _run_apply(config)
        em_changes = [
            c for c in result["applied"]
            if c["type"] in ("enable_value_dictionary", "disable_value_dictionary")
        ]
        assert em_changes == [], (
            "dry-run must NOT append any EM-mutating change records"
        )
        # Parsed config should be untouched — customer_email stays enabled.
        tbl = config["_parsed_space"]["data_sources"]["tables"][0]
        email_cc = next(
            cc for cc in tbl["column_configs"]
            if cc["column_name"] == "customer_email"
        )
        assert email_cc.get("enable_entity_matching") is True


# ───────────────────────────────────────────────────────────────────────
# Legacy scorer — pinned for OFF-mode regression
# ───────────────────────────────────────────────────────────────────────


class TestLegacyScorer:
    def test_free_text_zero(self):
        assert _entity_matching_score_legacy("description") == 0
        assert _entity_matching_score_legacy("customer_email") == 0

    def test_categorical_two(self):
        assert _entity_matching_score_legacy("country_code") == 2
        assert _entity_matching_score_legacy("region_name") == 2

    def test_generic_one(self):
        assert _entity_matching_score_legacy("login_ip") == 1
