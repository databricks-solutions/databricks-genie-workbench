"""Tests for the SQL-qualification and prose-rule-miner changes.

Covers Task D from the implementation plan:

- **Bug 1 — FQ prefixing** (``_auto_prefix_bare_columns`` + ``normalize_sql_snippet``)
  - bare column round-trips to full ``catalog.schema.table.col``
  - ambiguous column (present on 2+ in-scope tables) is skipped with a warning
  - MV measures / dimensions get discovered and prefixed
  - CTE alias is not rewritten as a column
  - placeholder ``${catalog}`` / ``${gold_schema}`` resolves

- **Bug 2 — JSON extraction hardening** (``_extract_json`` + ``_extract_json_array``)
  - JSON array wrapped in prose parses
  - JSON object wrapped in prose parses (existing behaviour)
  - fenced code block anywhere in the string parses
  - extraction of an array raises when the payload is an object

- **Bug 2 — canonical schema validator** (``validate_instruction_text``)
  - accepts the 5-section canonical form
  - rejects ``## Purpose`` capitalisation on the verbatim-required header #5
  - rejects ``###`` subheaders
  - rejects SQL in prose (scanner check #4)
  - rejects > ``MAX_TEXT_INSTRUCTIONS_CHARS``

- **Miner semantics** (``_validate_miner_candidate``, ``rewrite_instructions_from_miner_output``)
  - keep_in_prose with non-canonical section → rejected
  - keep_in_prose whose span contains SQL → rejected (scanner-parity)
  - low-confidence candidate → rejected
  - rewrite removes promoted spans idempotently
  - rewrite declines when output would contain SQL
  - rewrite declines when output grows length
  - rewrite skips when nothing changed

- **Idempotency** — running the rewrite a second time on already-normalised prose
  returns ``SKIP_NO_CHANGE`` with no writes.

These tests are pure-Python: no Databricks / Spark / LLM dependencies. Integration
coverage against a live failing space belongs in the end-to-end harness and is
not asserted here.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.common.config import (
    CANONICAL_SECTION_HEADERS,
    MAX_TEXT_INSTRUCTIONS_CHARS,
    PROMOTE_MIN_CONFIDENCE,
    SQL_IN_TEXT_RE,
    VERBATIM_REQUIRED_HEADERS,
)
from genie_space_optimizer.optimization.applier import (
    RewriteResult,
    parse_canonical_sections,
    render_canonical_sections,
    rewrite_instructions_from_miner_output,
    validate_instruction_text,
)
from genie_space_optimizer.optimization.benchmarks import (
    _auto_prefix_bare_columns,
    normalize_sql_snippet,
)
from genie_space_optimizer.optimization.evaluation import (
    _extract_json,
    _extract_json_array,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _make_snapshot(tables: list[dict], mvs: list[dict] | None = None) -> dict:
    return {
        "data_sources": {
            "tables": tables,
            "metric_views": mvs or [],
        },
    }


# ─────────────────────────────────────────────────────────────────────
# Pillar A — SQL qualification
# ─────────────────────────────────────────────────────────────────────


class TestAutoPrefixBareColumns:
    def _single_table_snapshot(self) -> dict:
        return _make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "columns": [
                    {"name": "order_id"},
                    {"name": "amount"},
                    {"name": "status"},
                ],
            },
        ])

    def test_prefixes_bare_column_with_full_identifier(self):
        """Bare column names get the full ``catalog.schema.table`` prefix, not short name."""
        sql = "SUM(amount)"
        prefixed, warnings = _auto_prefix_bare_columns(
            sql, "cat.sch.orders", self._single_table_snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert "cat.sch.orders.amount" in prefixed
        # Short-form prefix must NOT be produced — the scanner rejects it.
        assert "orders.amount" not in prefixed.replace("cat.sch.orders.amount", "")
        assert warnings == []

    def test_does_not_double_prefix(self):
        """Already-qualified references are left alone."""
        sql = "SUM(cat.sch.orders.amount)"
        prefixed, warnings = _auto_prefix_bare_columns(
            sql, "cat.sch.orders", self._single_table_snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert prefixed.count("cat.sch.orders.amount") == 1
        assert warnings == []

    def test_resolves_placeholder_identifiers(self):
        sql = "SUM(amount)"
        prefixed, _ = _auto_prefix_bare_columns(
            sql, "${catalog}.${gold_schema}.orders",
            self._single_table_snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert "cat.sch.orders.amount" in prefixed

    def test_resolves_short_name_identifier(self):
        sql = "SUM(amount)"
        prefixed, _ = _auto_prefix_bare_columns(
            sql, "orders", self._single_table_snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert "cat.sch.orders.amount" in prefixed

    def test_ambiguity_guard_skips_column_present_on_multiple_tables(self):
        """``login_ip`` on two tables that the SQL references → skipped + warning."""
        snapshot = _make_snapshot([
            {
                "identifier": "cat.sch.sessions",
                "columns": [{"name": "login_ip"}, {"name": "user_id"}],
            },
            {
                "identifier": "cat.sch.requests",
                "columns": [{"name": "login_ip"}, {"name": "request_id"}],
            },
        ])
        sql = "COUNT(DISTINCT login_ip) FROM cat.sch.sessions JOIN cat.sch.requests ON sessions.user_id = requests.user_id"
        prefixed, warnings = _auto_prefix_bare_columns(
            sql, "cat.sch.sessions", snapshot,
            catalog="cat", gold_schema="sch",
        )
        # Ambiguous column is not prefixed.
        assert "cat.sch.sessions.login_ip" not in prefixed
        assert any("ambiguous" in w.lower() for w in warnings)

    def test_mv_measures_and_dimensions_are_discovered(self):
        """MV ``measures[].name`` is recognised alongside regular columns."""
        snapshot = _make_snapshot(
            tables=[],
            mvs=[{
                "identifier": "cat.sch.mv_sales",
                "measures": [{"name": "cy_sales"}, {"name": "py_sales"}],
                "dimensions": [{"name": "region"}],
            }],
        )
        sql = "SUM(cy_sales) - SUM(py_sales)"
        prefixed, warnings = _auto_prefix_bare_columns(
            sql, "cat.sch.mv_sales", snapshot,
            catalog="cat", gold_schema="sch",
        )
        assert "cat.sch.mv_sales.cy_sales" in prefixed
        assert "cat.sch.mv_sales.py_sales" in prefixed
        assert warnings == []

    def test_cte_alias_is_not_rewritten_as_column(self):
        """``WITH x AS (…)`` — the alias ``x`` is not a column of the primary table."""
        snapshot = _make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "columns": [{"name": "amount"}],
            },
        ])
        # Use a contrived example where an alias shares a name with no
        # column, so the prefixer doesn't touch it.
        sql = "WITH totals AS (SELECT SUM(amount) FROM cat.sch.orders) SELECT * FROM totals"
        prefixed, warnings = _auto_prefix_bare_columns(
            sql, "cat.sch.orders", snapshot,
            catalog="cat", gold_schema="sch",
        )
        # ``totals`` is an alias, not a column; must not become prefixed.
        assert "cat.sch.orders.totals" not in prefixed
        assert warnings == []

    def test_returns_warning_when_primary_table_unknown(self):
        prefixed, warnings = _auto_prefix_bare_columns(
            "COUNT(x)", "cat.sch.unknown", self._single_table_snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert any("no columns known" in w.lower() for w in warnings)
        assert prefixed == "COUNT(x)"

    def test_empty_inputs(self):
        prefixed, warnings = _auto_prefix_bare_columns(
            "", "cat.sch.orders", self._single_table_snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert prefixed == ""
        assert warnings == []


class TestNormalizeSqlSnippet:
    def _snapshot(self) -> dict:
        return _make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "columns": [
                    {"name": "order_id"},
                    {"name": "amount"},
                    {"name": "status"},
                ],
            },
        ])

    def test_trims_semicolons_and_prefixes(self):
        sql = "SUM(amount);"
        out, warnings = normalize_sql_snippet(
            sql, "measure", self._snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert out.endswith("cat.sch.orders.amount)")
        assert ";" not in out

    def test_strips_filter_where_prefix(self):
        sql = "WHERE status = 'active'"
        out, warnings = normalize_sql_snippet(
            sql, "filter", self._snapshot(),
            catalog="cat", gold_schema="sch",
        )
        assert not out.lstrip().upper().startswith("WHERE ")
        assert "cat.sch.orders.status" in out

    def test_no_backend_means_no_explain(self):
        """Without ``spark`` or ``w+warehouse_id``, no EXPLAIN runs — no warnings."""
        sql = "SUM(amount)"
        out, warnings = normalize_sql_snippet(
            sql, "measure", self._snapshot(),
            catalog="cat", gold_schema="sch",
            spark=None, w=None, warehouse_id="",
        )
        assert "EXPLAIN failed" not in " ".join(warnings)


# ─────────────────────────────────────────────────────────────────────
# Pillar B — JSON hardening + canonical schema validator
# ─────────────────────────────────────────────────────────────────────


class TestExtractJson:
    def test_pure_json_array_parses(self):
        assert _extract_json('[1, 2, 3]') == [1, 2, 3]

    def test_pure_json_object_parses(self):
        assert _extract_json('{"a": 1}') == {"a": 1}

    def test_json_array_wrapped_in_prose_parses(self):
        """The bug that silenced the instruction-to-SQL path."""
        payload = 'Here is the array: [{"target": "sql_snippet"}, {"target": "join_spec"}]'
        out = _extract_json(payload)
        assert isinstance(out, list)
        assert len(out) == 2

    def test_json_object_wrapped_in_prose_parses(self):
        payload = 'Response: {"key": "value"}. Done.'
        assert _extract_json(payload) == {"key": "value"}

    def test_fenced_block_anywhere_in_string(self):
        payload = 'Preamble text\n```json\n[{"a": 1}]\n```\nTrailer.'
        out = _extract_json(payload)
        assert out == [{"a": 1}]

    def test_extract_json_array_raises_when_object(self):
        with pytest.raises(ValueError, match="Expected JSON array"):
            _extract_json_array('{"not": "an array"}')

    def test_extract_json_array_returns_list(self):
        assert _extract_json_array('[1, 2]') == [1, 2]

    def test_totally_invalid_raises(self):
        with pytest.raises(Exception):
            _extract_json('this is not json at all')


class TestCanonicalSchemaValidator:
    def test_accepts_valid_5_section_prose(self):
        text = (
            "## PURPOSE\n- Analytics space.\n\n"
            "## DISAMBIGUATION\n- Q1 means calendar Q1.\n\n"
            "## DATA QUALITY NOTES\n- status column has mixed casing.\n\n"
            "## CONSTRAINTS\n- never return ssn.\n\n"
            "## Instructions you must follow when providing summaries\n"
            "- Round to two decimals.\n"
        )
        ok, errors = validate_instruction_text(text, strict=True)
        assert ok, f"should accept valid prose, got errors: {errors}"

    def test_rejects_wrong_case_on_verbatim_header(self):
        text = (
            "## PURPOSE\n- Valid.\n\n"
            "## Instructions You Must Follow When Providing Summaries\n- Bad case.\n"
        )
        ok, errors = validate_instruction_text(text, strict=True)
        assert not ok
        assert any("non-canonical header" in e.lower() or "verbatim" in e.lower() for e in errors)

    def test_accepts_case_insensitive_header_1_to_4(self):
        text = "## purpose\n- lowercase ok for #1-#4 under the PR-#178 case policy.\n"
        ok, errors = validate_instruction_text(text, strict=True)
        assert ok, f"case-insensitive #1-#4 should pass: {errors}"

    def test_rejects_h3_subheader(self):
        text = (
            "## PURPOSE\n- scope\n\n"
            "### SubHeader\n- forbidden.\n"
        )
        ok, errors = validate_instruction_text(text, strict=True)
        assert not ok
        assert any("h3 header" in e.lower() for e in errors)

    def test_rejects_sql_in_prose(self):
        text = (
            "## CONSTRAINTS\n- SELECT * FROM users WHERE is_admin = true\n"
        )
        ok, errors = validate_instruction_text(text, strict=True)
        assert not ok
        assert any("sql detected" in e.lower() for e in errors)

    def test_rejects_over_length_cap(self):
        text = "## PURPOSE\n" + ("- filler line\n" * (MAX_TEXT_INSTRUCTIONS_CHARS // 8))
        ok, errors = validate_instruction_text(text, strict=True)
        assert not ok
        assert any("length" in e.lower() for e in errors)

    def test_compat_mode_tolerates_legacy_sections(self):
        """In ``strict=False`` the legacy vocabulary is allowed (lever-loop path)."""
        text = "PURPOSE:\n- legacy ALL-CAPS.\nASSET ROUTING:\n- some routing.\n"
        ok, errors = validate_instruction_text(text, strict=False)
        assert ok, f"compat mode should accept legacy: {errors}"

    def test_parse_canonical_sections_extracts_all_five(self):
        text = (
            "## PURPOSE\n- scope\n\n"
            "## DISAMBIGUATION\n- disambig\n\n"
            "## DATA QUALITY NOTES\n- dq\n\n"
            "## CONSTRAINTS\n- constraints\n\n"
            "## Instructions you must follow when providing summaries\n- summary\n"
        )
        canonical, legacy, preamble = parse_canonical_sections(text)
        for header in CANONICAL_SECTION_HEADERS:
            assert header in canonical, f"missing header {header}"
        assert legacy == {}
        assert preamble == []

    def test_parse_canonical_sections_reads_legacy_all_caps(self):
        text = "PURPOSE:\n- foo\nBUSINESS DEFINITIONS:\n- revenue = SUM(x)\n"
        _, legacy, _ = parse_canonical_sections(text)
        assert "PURPOSE" in legacy
        assert "BUSINESS DEFINITIONS" in legacy

    def test_render_canonical_sections_is_ordered_list_of_lines(self):
        secs = {
            "## DATA QUALITY NOTES": "- first line\n- second line",
            "## PURPOSE": "- top of space\n",
        }
        rendered = render_canonical_sections(secs)
        assert isinstance(rendered, list)
        assert all(isinstance(ln, str) for ln in rendered)
        # Purpose must come before Data Quality in the rendered order.
        joined = "".join(rendered)
        assert joined.index("## PURPOSE") < joined.index("## DATA QUALITY NOTES")


# ─────────────────────────────────────────────────────────────────────
# Pillar C — Miner candidate validator + rewrite
# ─────────────────────────────────────────────────────────────────────


def _miner_validator(candidate, instructions_text: str = ""):
    """Thin wrapper so tests don't depend on importing the private helper."""
    from genie_space_optimizer.optimization.optimizer import (
        _validate_miner_candidate,
    )
    return _validate_miner_candidate(candidate, instructions_text)


class TestMinerCandidateValidator:
    def _valid_keep_in_prose(self, **overrides):
        base = {
            "target": "keep_in_prose",
            "source_span": "- always round revenue to 2 decimals",
            "confidence": 0.95,
            "payload": {
                "section": "## Instructions you must follow when providing summaries",
            },
        }
        base.update(overrides)
        return base

    def test_rejects_low_confidence(self):
        cand = self._valid_keep_in_prose(confidence=0.3)
        ok, reason = _miner_validator(cand)
        assert not ok
        assert "low_confidence" in reason

    def test_rejects_missing_source_span(self):
        cand = self._valid_keep_in_prose()
        cand["source_span"] = ""
        ok, reason = _miner_validator(cand)
        assert not ok and "source_span" in reason

    def test_rejects_bad_target(self):
        cand = self._valid_keep_in_prose()
        cand["target"] = "ufo_sighting"
        ok, reason = _miner_validator(cand)
        assert not ok and "bad_target" in reason

    def test_rejects_keep_in_prose_with_non_canonical_section(self):
        cand = self._valid_keep_in_prose()
        cand["payload"]["section"] = "## Random Thoughts"
        ok, reason = _miner_validator(cand)
        assert not ok and "keep_in_prose_bad_section" in reason

    def test_rejects_keep_in_prose_containing_sql(self):
        """Scanner-parity: SQL-in-prose must be promoted, not kept."""
        cand = self._valid_keep_in_prose()
        cand["source_span"] = "Always SELECT revenue FROM orders GROUP BY region"
        ok, reason = _miner_validator(cand)
        assert not ok and reason == "keep_in_prose_contains_sql"

    def test_accepts_valid_candidate(self):
        ok, reason = _miner_validator(self._valid_keep_in_prose())
        assert ok and reason == "ok"

    def test_promote_min_confidence_is_boundary(self):
        """A candidate at exactly ``PROMOTE_MIN_CONFIDENCE`` is accepted."""
        cand = self._valid_keep_in_prose(confidence=PROMOTE_MIN_CONFIDENCE)
        ok, _ = _miner_validator(cand)
        assert ok


class TestRewriteInstructions:
    def test_removes_promoted_span_and_writes(self):
        original = (
            "## PURPOSE\n- Sales analytics.\n\n"
            "## CONSTRAINTS\n- filter status = 'active' always.\n- never return ssn.\n"
        )
        applied = ["- filter status = 'active' always.\n"]
        outcome, new_text, errors = rewrite_instructions_from_miner_output(
            original, applied, keep_in_prose_spans=[],
        )
        assert outcome == RewriteResult.WRITE
        assert "filter status = 'active'" not in new_text
        assert "never return ssn" in new_text

    def test_declines_when_output_contains_sql(self):
        """keep_in_prose with SQL inside source_span produces malformed output."""
        original = "## PURPOSE\n- Sales.\n"
        keep = [{
            "section": "## CONSTRAINTS",
            "source_span": "- Never SELECT raw pii from users",
        }]
        outcome, new_text, errors = rewrite_instructions_from_miner_output(
            original, applied_spans=[], keep_in_prose_spans=keep,
        )
        assert outcome == RewriteResult.DECLINE_MALFORMED
        assert new_text == original  # untouched

    def test_declines_when_output_grows_length(self):
        original = "## PURPOSE\n- Sales.\n"
        keep = [{
            "section": "## CONSTRAINTS",
            "source_span": "- " + ("new constraint " * 50),
        }]
        outcome, new_text, _ = rewrite_instructions_from_miner_output(
            original, applied_spans=[], keep_in_prose_spans=keep,
        )
        assert outcome == RewriteResult.DECLINE_MALFORMED
        assert new_text == original

    def test_skip_no_change_when_idempotent(self):
        """Second pass on already-normalised prose: no-op."""
        original = "## PURPOSE\n- Sales analytics for H1 revenue.\n"
        outcome, new_text, _ = rewrite_instructions_from_miner_output(
            original, applied_spans=[], keep_in_prose_spans=[],
        )
        assert outcome == RewriteResult.SKIP_NO_CHANGE
        assert new_text == original

    def test_span_not_found_is_soft_warning_not_fatal(self):
        original = "## PURPOSE\n- Sales.\n"
        applied = ["- this span is not actually in the prose\n"]
        outcome, new_text, errors = rewrite_instructions_from_miner_output(
            original, applied, keep_in_prose_spans=[],
        )
        # Missing span → rewrite continues; likely SKIP_NO_CHANGE because
        # nothing else changed. The error list carries the diagnostic.
        assert any("applied_span not found" in e for e in errors)
        assert outcome in (RewriteResult.SKIP_NO_CHANGE, RewriteResult.WRITE)

    def test_regroups_keep_in_prose_under_tagged_section(self):
        """A keep_in_prose entry tagged with ``## CONSTRAINTS`` lands there."""
        original = (
            "## PURPOSE\n- Sales.\n\n"
            "BUSINESS DEFINITIONS:\n- never return internal ids\n"
        )
        keep = [{
            "section": "## CONSTRAINTS",
            "source_span": "- never return internal ids",
        }]
        outcome, new_text, _ = rewrite_instructions_from_miner_output(
            original, applied_spans=[], keep_in_prose_spans=keep,
        )
        assert outcome == RewriteResult.WRITE
        # Content appears under CONSTRAINTS; legacy ALL-CAPS section is gone.
        assert "BUSINESS DEFINITIONS" not in new_text
        assert "## CONSTRAINTS" in new_text
        assert "never return internal ids" in new_text

    def test_preserves_existing_canonical_content(self):
        """Content already under ``## PURPOSE`` must survive the rewrite."""
        original = (
            "## PURPOSE\n- user-curated purpose that was not tagged by the miner\n\n"
            "BUSINESS DEFINITIONS:\n- rule that will be promoted elsewhere\n"
        )
        applied = ["- rule that will be promoted elsewhere"]
        outcome, new_text, _ = rewrite_instructions_from_miner_output(
            original, applied, keep_in_prose_spans=[],
        )
        assert outcome == RewriteResult.WRITE
        assert "user-curated purpose that was not tagged by the miner" in new_text


# ─────────────────────────────────────────────────────────────────────
# Pillar B — Miner happy-path integration (LLM call mocked)
# ─────────────────────────────────────────────────────────────────────


class TestMinerMultiTargetDispatcher:
    """End-to-end integration for the multi-target miner.

    The LLM call is mocked; validators and the return-shape assertions do the
    actual work. Downstream appliers are not exercised here.
    """

    def _mock_llm(self, monkeypatch, content: str):
        def _fake(_w, _system, _prompt, *, span_name: str = "", **kwargs):
            return content, None
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

    def _metadata_with_orders(self) -> dict:
        return {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "columns": [
                        {"name": "amount"},
                        {"name": "status"},
                    ],
                }],
                "metric_views": [],
            },
            "instructions": {
                "text_instructions": [{
                    "id": "i1",
                    "content": ["## PURPOSE\n- some prose that will be mined for SQL rules.\n"],
                }],
            },
        }

    def test_returns_empty_buckets_on_thin_instructions(self):
        from genie_space_optimizer.optimization.optimizer import (
            _convert_instructions_to_sql_expressions, _MINER_TARGETS,
        )
        result = _convert_instructions_to_sql_expressions({
            "data_sources": {},
            "instructions": {"text_instructions": [{"content": ["hi"]}]},
        })
        assert sorted(k for k in result if k != "stats") == sorted(_MINER_TARGETS)
        assert all(result[t] == [] for t in _MINER_TARGETS)

    def test_dispatcher_buckets_keep_in_prose(self, monkeypatch):
        """LLM returns a single keep_in_prose candidate → buckets it correctly."""
        from genie_space_optimizer.optimization.optimizer import (
            _convert_instructions_to_sql_expressions,
        )
        content = (
            '[{"target": "keep_in_prose", '
            '"source_span": "some prose that will be mined", '
            '"confidence": 0.95, '
            '"payload": {"section": "## DATA QUALITY NOTES"}}]'
        )
        self._mock_llm(monkeypatch, content)
        result = _convert_instructions_to_sql_expressions(
            self._metadata_with_orders(),
            w=MagicMock(),
        )
        assert len(result["keep_in_prose"]) == 1
        assert result["keep_in_prose"][0]["section"] == "## DATA QUALITY NOTES"
        assert result["sql_snippet"] == []
        assert result["stats"]["candidates_total"] == 1

    def test_dispatcher_drops_low_confidence(self, monkeypatch):
        from genie_space_optimizer.optimization.optimizer import (
            _convert_instructions_to_sql_expressions,
        )
        content = (
            '[{"target": "keep_in_prose", '
            '"source_span": "x", "confidence": 0.2, '
            '"payload": {"section": "## PURPOSE"}}]'
        )
        self._mock_llm(monkeypatch, content)
        result = _convert_instructions_to_sql_expressions(
            self._metadata_with_orders(), w=MagicMock(),
        )
        assert result["keep_in_prose"] == []
        assert "low_confidence:0.20" in result["stats"]["rejected_by_reason"]

    def test_dispatcher_retries_on_bad_json_then_succeeds(self, monkeypatch):
        """Retry loop per B.2: first attempt invalid JSON, second attempt ok."""
        from genie_space_optimizer.optimization.optimizer import (
            _convert_instructions_to_sql_expressions,
        )
        attempts = {"n": 0}

        def _fake(_w, _system, _prompt, *, span_name: str = "", **kwargs):
            attempts["n"] += 1
            if attempts["n"] == 1:
                return "not json at all and no array here", None
            return (
                '[{"target": "keep_in_prose", '
                '"source_span": "mined prose", "confidence": 0.9, '
                '"payload": {"section": "## PURPOSE"}}]'
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )
        result = _convert_instructions_to_sql_expressions(
            self._metadata_with_orders(), w=MagicMock(),
        )
        assert attempts["n"] == 2
        assert len(result["keep_in_prose"]) == 1
        assert result["stats"]["retries"] == 1


# ─────────────────────────────────────────────────────────────────────
# Pillar B extensions — expand budget math (A4 / reviewer finding #1)
# ─────────────────────────────────────────────────────────────────────


class TestExpandBudgetMath:
    """_expand_instructions uses floor-free budget math.

    Previous formula had ``max(..., 200)`` / ``max(..., 100)`` floors that
    could inflate the claimed remaining budget past the actual 2000-char
    cap. Reviewer flagged this as High severity. New formula:

        remaining = max(cap - existing, 0)  # strict
        if remaining < MIN_EXPAND_BUDGET: skip
        per_section = remaining // missing_count  # strict upper bound
    """

    def _fake_llm_zero_sections(self, monkeypatch):
        def _fake(_w, _system, _prompt, *, span_name="", **kwargs):
            return '{"sections": {}}', None
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

    def _empty_metadata(self) -> dict:
        return {"data_sources": {"tables": [], "metric_views": []}}

    def test_skips_when_remaining_below_min_budget(self, monkeypatch):
        """1950-char prose + 2 missing = 50 chars room = skip (< 100 floor)."""
        from genie_space_optimizer.optimization.optimizer import _expand_instructions

        llm_called = {"n": 0}

        def _fake(*a, **k):
            llm_called["n"] += 1
            return '{"sections": {}}', None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        existing = "x" * 1950
        missing = ["## CONSTRAINTS", "## Instructions you must follow when providing summaries"]
        out = _expand_instructions(self._empty_metadata(), existing, missing, w=MagicMock())
        assert out == {"__skip_reason__": "no_budget"}
        assert llm_called["n"] == 0, "LLM must not be called when budget is too small"

    def test_skips_when_existing_equals_cap(self, monkeypatch):
        """Existing prose already AT cap → remaining=0 → skip."""
        from genie_space_optimizer.optimization.optimizer import _expand_instructions
        from genie_space_optimizer.common.config import MAX_TEXT_INSTRUCTIONS_CHARS

        llm_called = {"n": 0}

        def _fake(*a, **k):
            llm_called["n"] += 1
            return '{"sections": {}}', None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        existing = "x" * MAX_TEXT_INSTRUCTIONS_CHARS
        out = _expand_instructions(self._empty_metadata(), existing, ["## PURPOSE"], w=MagicMock())
        assert out == {"__skip_reason__": "no_budget"}
        assert llm_called["n"] == 0

    def test_calls_llm_when_budget_sufficient(self, monkeypatch):
        from genie_space_optimizer.optimization.optimizer import _expand_instructions
        self._fake_llm_zero_sections(monkeypatch)
        # 500 chars used → 1500 remaining → plenty of room.
        existing = "x" * 500
        missing = ["## CONSTRAINTS"]
        out = _expand_instructions(self._empty_metadata(), existing, missing, w=MagicMock())
        # LLM returned no content → empty dict (not the skip sentinel).
        assert out == {}

    def test_budget_template_vars_render_correctly(self):
        """Prompt must render the dynamic budget variables in the right place."""
        from genie_space_optimizer.common.config import (
            EXPAND_INSTRUCTION_PROMPT, format_mlflow_template,
        )

        rendered = format_mlflow_template(
            EXPAND_INSTRUCTION_PROMPT,
            existing_instructions="existing",
            tables_context="tables",
            metric_views_context="mvs",
            join_specs_context="joins",
            missing_sections="- ## PURPOSE",
            existing_length="500",
            remaining_budget="1500",
            missing_count="1",
            per_section_budget="1500",
        )
        assert "existing prose is 500 chars" in rendered
        assert "1500 chars remaining" in rendered
        assert "1 section(s)" in rendered
        # The strictly-forbidden "invent rules" language is present.
        assert "Do NOT invent business rules" in rendered
        # The SQL-in-prose rule is present (A3 — added to expand).
        assert "Do NOT emit any SQL keyword or clause" in rendered
        # The softened Source: hints — no standalone "Source:" heading.
        assert "Source: columns whose names" not in rendered
        assert "If such content is evident" in rendered


class TestProactiveInstructionPromptStrengthened:
    """A3 — proactive prompt's existing SQL-in-prose rule is strengthened.

    The rule now covers 'SQL keyword or clause' (not just 'SQL snippet')
    and includes the English-verb-replacement hints.
    """

    def test_proactive_prompt_has_strengthened_sql_rule(self):
        from genie_space_optimizer.common.config import PROACTIVE_INSTRUCTION_PROMPT
        assert "SQL keyword or clause" in PROACTIVE_INSTRUCTION_PROMPT
        # Verb replacement hints are present.
        assert "combine" in PROACTIVE_INSTRUCTION_PROMPT.lower()
        # Old weaker wording is gone.
        # (Old rule used "SQL snippet" — ensure it's been upgraded.)
        assert "any SQL keyword or clause" in PROACTIVE_INSTRUCTION_PROMPT


# ─────────────────────────────────────────────────────────────────────
# Commit 3 — Structural resilience: trim helpers + repair loop + UX
# ─────────────────────────────────────────────────────────────────────


class TestTrimBulletsToBudget:
    """_trim_bullets_to_budget — Layer 1 of the two-level budget enforcement.

    Drops trailing bullets until the body fits, truncates the first bullet
    at a word boundary if it alone exceeds budget. Empty / over-cap edges.
    """

    def test_empty_body_returns_empty(self):
        from genie_space_optimizer.optimization.applier import _trim_bullets_to_budget
        assert _trim_bullets_to_budget("", 100) == ""
        assert _trim_bullets_to_budget("   ", 100) == ""

    def test_body_under_budget_unchanged(self):
        from genie_space_optimizer.optimization.applier import _trim_bullets_to_budget
        body = "- one\n- two\n- three"
        assert _trim_bullets_to_budget(body, 100) == body

    def test_drops_trailing_bullets(self):
        from genie_space_optimizer.optimization.applier import _trim_bullets_to_budget
        body = "- first\n- second\n- third\n- fourth"
        # Budget fits only first + second.
        out = _trim_bullets_to_budget(body, 20)
        assert "first" in out
        assert "fourth" not in out
        assert len(out) <= 20

    def test_truncates_first_bullet_at_word_boundary(self):
        from genie_space_optimizer.optimization.applier import _trim_bullets_to_budget
        body = "- this is a single very long bullet that exceeds the budget by quite a lot"
        out = _trim_bullets_to_budget(body, 30)
        assert len(out) <= 30
        # Doesn't end mid-word.
        assert not out.endswith(("t", "s", "a", "e"))  # heuristic
        # Actually: should end on a whitespace-ish boundary.
        assert " " in out

    def test_zero_or_negative_budget_returns_empty(self):
        from genie_space_optimizer.optimization.applier import _trim_bullets_to_budget
        assert _trim_bullets_to_budget("- foo\n- bar", 0) == ""
        assert _trim_bullets_to_budget("- foo\n- bar", -5) == ""


class TestTrimRenderedToCap:
    """_trim_rendered_to_cap — Layer 2 post-render priority trim.

    Trims trailing bullets in priority order: DATA QUALITY NOTES first,
    PURPOSE last. Drops headers when their section becomes empty.
    """

    def _make_rendered(self) -> list[str]:
        # Simulate render_canonical_sections output: each element ends in \n.
        return [
            "## PURPOSE\n",
            "- Analytics for the sales team.\n",
            "\n",
            "## DATA QUALITY NOTES\n",
            "- first dq note that can be trimmed\n",
            "- second dq note also trimmable\n",
            "- third dq note\n",
        ]

    def test_under_cap_unchanged(self):
        from genie_space_optimizer.optimization.applier import _trim_rendered_to_cap
        rendered = self._make_rendered()
        total = sum(len(p) for p in rendered)
        assert _trim_rendered_to_cap(rendered, total + 100) == rendered

    def test_drops_data_quality_bullets_first(self):
        from genie_space_optimizer.optimization.applier import _trim_rendered_to_cap
        rendered = self._make_rendered()
        total = sum(len(p) for p in rendered)
        # Force trim: cap just under current total so one DQ bullet drops.
        out = _trim_rendered_to_cap(rendered, total - 20)
        out_text = "".join(out)
        assert "Analytics for the sales team" in out_text  # PURPOSE preserved
        # At least one DQ note dropped.
        assert out_text.count("dq note") < 3

    def test_drops_section_header_when_body_empty(self):
        from genie_space_optimizer.optimization.applier import _trim_rendered_to_cap
        rendered = [
            "## PURPOSE\n",
            "- Sales analytics for the team covering H1 revenue.\n",
            "\n",
            "## DATA QUALITY NOTES\n",
            "- The status column has mixed casing.\n",
        ]
        total = sum(len(p) for p in rendered)
        # Cap after full DQ section is dropped: we want PURPOSE to still fit.
        # Setting the cap large enough to hold PURPOSE + blank + trailing
        # but not DQ header + its body.
        purpose_len = sum(len(p) for p in rendered[:3])
        out = _trim_rendered_to_cap(rendered, purpose_len + 5)
        out_text = "".join(out)
        # Purpose stays, DQ entirely gone.
        assert "Sales analytics" in out_text
        assert "status column" not in out_text
        assert "## DATA QUALITY NOTES" not in out_text

    def test_preserves_purpose_last(self):
        """Even under aggressive trim, PURPOSE is the last section to lose content."""
        from genie_space_optimizer.optimization.applier import _trim_rendered_to_cap
        rendered = self._make_rendered()
        # Tight cap — aggressive trim.
        out = _trim_rendered_to_cap(rendered, 50)
        out_text = "".join(out)
        # PURPOSE header always survives until the whole section empties.
        assert "## PURPOSE" in out_text

    def test_empty_input_unchanged(self):
        from genie_space_optimizer.optimization.applier import _trim_rendered_to_cap
        assert _trim_rendered_to_cap([], 100) == []


class TestExpandRepairLoop:
    """C1 — expand_instructions retries once on per-section validation failure.

    First LLM call returns content with SQL-in-prose; repair call returns clean.
    Asserts two attempts + final output is valid.
    """

    def _empty_metadata(self) -> dict:
        return {"data_sources": {"tables": [], "metric_views": []}}

    def test_sql_in_prose_triggers_repair(self, monkeypatch):
        from genie_space_optimizer.optimization.optimizer import _expand_instructions

        attempts = {"n": 0, "span_names": []}

        def _fake(_w, _system, prompt, *, span_name="", **kwargs):
            attempts["n"] += 1
            attempts["span_names"].append(span_name)
            if attempts["n"] == 1:
                # First call: emit a section containing SQL-in-prose.
                return (
                    '{"sections": {"## CONSTRAINTS": '
                    '"- Do not WHERE col = 1 then SELECT * FROM orders\\n"}}'
                ), None
            # Second call: clean content.
            return (
                '{"sections": {"## CONSTRAINTS": '
                '"- Never return customer PII.\\n"}}'
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        out = _expand_instructions(
            self._empty_metadata(),
            "x" * 200,  # existing prose has enough room to call LLM
            ["## CONSTRAINTS"],
            w=MagicMock(),
        )
        assert attempts["n"] == 2
        assert "expand_instructions_repair" in attempts["span_names"]
        # Clean content retained.
        assert out.get("## CONSTRAINTS", "").startswith("- Never return")

    def test_clean_output_no_repair(self, monkeypatch):
        from genie_space_optimizer.optimization.optimizer import _expand_instructions

        attempts = {"n": 0}

        def _fake(*a, **k):
            attempts["n"] += 1
            return (
                '{"sections": {"## CONSTRAINTS": "- Never return PII.\\n"}}'
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        out = _expand_instructions(
            self._empty_metadata(),
            "x" * 200,
            ["## CONSTRAINTS"],
            w=MagicMock(),
        )
        # Only one attempt when content is clean.
        assert attempts["n"] == 1
        assert "## CONSTRAINTS" in out


class TestSeedRepairLoop:
    """C1 — _generate_proactive_instructions retries once on validation failure."""

    def _metadata_with_orders(self) -> dict:
        return {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "columns": [{"name": "amount"}],
                }],
                "metric_views": [],
            },
            "instructions": {
                "text_instructions": [],
            },
        }

    def test_validation_failure_triggers_repair(self, monkeypatch):
        from genie_space_optimizer.optimization.optimizer import (
            _generate_proactive_instructions,
        )

        attempts = {"n": 0, "span_names": []}

        def _fake(_w, _system, prompt, *, span_name="", **kwargs):
            attempts["n"] += 1
            attempts["span_names"].append(span_name)
            if attempts["n"] == 1:
                # First call: include SQL-in-prose that will fail strict
                # validation (scanner v2 catches "SELECT ... FROM").
                return (
                    "## PURPOSE\n"
                    "- Sales analytics for the team.\n"
                    "\n"
                    "## CONSTRAINTS\n"
                    "- Use SELECT amount FROM orders WHERE status = 'active'\n"
                ), None
            # Second call: clean.
            return (
                "## PURPOSE\n"
                "- Sales analytics for the team covering H1 revenue reports.\n"
                "\n"
                "## CONSTRAINTS\n"
                "- Never return customer PII or internal identifiers.\n"
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        result = _generate_proactive_instructions(
            self._metadata_with_orders(), w=MagicMock(),
        )
        assert attempts["n"] == 2
        assert "generate_proactive_instructions_repair" in attempts["span_names"]
        assert "Never return customer PII" in result
        assert "SELECT amount FROM" not in result

    def test_repair_also_fails_returns_empty(self, monkeypatch):
        from genie_space_optimizer.optimization.optimizer import (
            _generate_proactive_instructions,
        )

        attempts = {"n": 0}

        def _fake(*a, **k):
            attempts["n"] += 1
            # Every call has SQL-in-prose → both attempts fail.
            return (
                "## PURPOSE\n"
                "- Sales data pipeline for the analytics team.\n"
                "## CONSTRAINTS\n"
                "- Use WHERE col = 'x' pattern for filtering active records\n"
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        result = _generate_proactive_instructions(
            self._metadata_with_orders(), w=MagicMock(),
        )
        # Both attempts failed → empty string.
        assert result == ""
        assert attempts["n"] == 2  # one normal + one repair


# ─────────────────────────────────────────────────────────────────────
# Commit 4 — Minimal miner routing for negative-join constraints
# ─────────────────────────────────────────────────────────────────────


class TestNegativeJoinRouting:
    """Minimal D — 'Do not / Never join X to Y' routes to keep_in_prose
    under ## CONSTRAINTS and survives the scanner v2 validator.

    With Commit 1 (scanner v2), the prose-imperative short-circuit means
    "Do not join" no longer triggers the SQL-in-text finding. Commit 4
    just adds an explicit routing rule to the mining prompt so the LLM
    doesn't misroute this to ``join_spec`` (which would be a positive
    join the model CAN use — semantically wrong for a negative constraint).
    """

    def test_prompt_has_negative_join_routing_rule(self):
        from genie_space_optimizer.common.config import PROSE_RULE_MINING_PROMPT

        # Target-routing table row.
        assert "Do not / Never join X to Y" in PROSE_RULE_MINING_PROMPT
        # Per-target rule mention.
        assert "NEGATIVE JOIN CONSTRAINTS" in PROSE_RULE_MINING_PROMPT
        # Explicit section assignment.
        assert "section=\"## CONSTRAINTS\"" in PROSE_RULE_MINING_PROMPT

    def test_miner_accepts_negative_join_keep_in_prose(self, monkeypatch):
        """When the LLM routes 'Do not join A to B' as keep_in_prose under
        ## CONSTRAINTS, the validator accepts it (scanner v2 doesn't flag).
        """
        from genie_space_optimizer.optimization.optimizer import (
            _convert_instructions_to_sql_expressions,
        )

        def _fake(_w, _system, _prompt, *, span_name="", **kwargs):
            return (
                '[{"target": "keep_in_prose", '
                '"source_span": "- Do not join `mv_esr_fact_sales` directly '
                'to `mv_7now_fact_sales`; they represent disjoint channels", '
                '"confidence": 0.95, '
                '"payload": {"section": "## CONSTRAINTS"}}]'
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake,
        )

        metadata = {
            "data_sources": {
                "tables": [{"identifier": "cat.sch.t", "columns": []}],
                "metric_views": [],
            },
            "instructions": {
                "text_instructions": [{
                    "id": "i1",
                    "content": [
                        "- Do not join `mv_esr_fact_sales` directly "
                        "to `mv_7now_fact_sales`; they represent "
                        "disjoint channels\n"
                    ],
                }],
            },
        }

        result = _convert_instructions_to_sql_expressions(metadata, w=MagicMock())

        # Candidate accepted under ## CONSTRAINTS.
        assert len(result["keep_in_prose"]) == 1
        candidate = result["keep_in_prose"][0]
        assert candidate["section"] == "## CONSTRAINTS"
        assert "Do not join" in candidate["source_span"]

        # Validator didn't reject it as SQL-in-prose (scanner v2 pass).
        assert result["stats"]["rejected_by_reason"] == {}

    def test_rewrite_preserves_negative_join_under_constraints(self):
        """End-to-end rewrite: existing prose + negative-join keep_in_prose
        produces ## CONSTRAINTS containing the rule and passes strict
        validation.

        Realistic scenario: the miner promotes a SQL snippet AWAY (spans
        removed) and tags the negative-join bullet as keep_in_prose. Net
        bytes DECREASE — rewrite returns WRITE.
        """
        from genie_space_optimizer.optimization.applier import (
            RewriteResult, rewrite_instructions_from_miner_output,
        )

        # Net-smaller scenario: original has several bulky SQL snippets
        # that get promoted away, a couple of free-floating bullets that
        # get dropped, and the negative-join constraint that stays in
        # prose under ## CONSTRAINTS.
        original = (
            "## PURPOSE\n"
            "- Sales analytics space covering H1 revenue for executives.\n"
            "- Net revenue formula: SUM(mv_7now_fact_sales.cy_sales) - "
            "SUM(mv_7now_fact_sales.discounts) - promoted to sql_snippet\n"
            "- Active filter: status = 'active' AND region <> 'BLOCKED' "
            "-- promoted to sql_snippet (filter)\n"
            "- Day-over-day growth: (cy - py) / NULLIF(py, 0) * 100 "
            "-- promoted to sql_snippet (expression)\n"
            "\n"
            "- Do not join `mv_esr_fact_sales` directly "
            "to `mv_7now_fact_sales`; they represent disjoint channels\n"
        )
        # Three SQL snippets get promoted (bulky; ~150 chars each).
        applied_spans = [
            "- Net revenue formula: SUM(mv_7now_fact_sales.cy_sales) - "
            "SUM(mv_7now_fact_sales.discounts) - promoted to sql_snippet",
            "- Active filter: status = 'active' AND region <> 'BLOCKED' "
            "-- promoted to sql_snippet (filter)",
            "- Day-over-day growth: (cy - py) / NULLIF(py, 0) * 100 "
            "-- promoted to sql_snippet (expression)",
        ]
        # The 'Do not join' bullet stays in prose under ## CONSTRAINTS.
        keep_spans = [{
            "section": "## CONSTRAINTS",
            "source_span": (
                "- Do not join `mv_esr_fact_sales` directly "
                "to `mv_7now_fact_sales`; they represent disjoint channels"
            ),
        }]

        outcome, new_text, errors = rewrite_instructions_from_miner_output(
            original, applied_spans=applied_spans, keep_in_prose_spans=keep_spans,
        )

        assert outcome == RewriteResult.WRITE, (
            f"Expected WRITE, got {outcome}; errors={errors}"
        )
        # Constraint lives under ## CONSTRAINTS header.
        assert "## CONSTRAINTS" in new_text
        # Original negative-join text preserved.
        assert "Do not join" in new_text
        assert "mv_esr_fact_sales" in new_text
        # The promoted SQL snippet is gone from the prose.
        assert "SUM(" not in new_text


# ─────────────────────────────────────────────────────────────────────
# Integration regression — replay the failing-run prose
# ─────────────────────────────────────────────────────────────────────


class TestFailingRunRegression:
    """End-to-end replay of the prose from the failing optimization run.

    Asserts the full pipeline (scanner v2 + expand budget + pre/post-trim
    + decline-log UX) now produces a valid outcome on prose that previously
    failed with:
      - "length 2872 exceeds MAX_TEXT_INSTRUCTIONS_CHARS (2000)"
      - "SQL detected in prose line: '- Do not join `mv_esr_fact_sales`...'"
    """

    def test_do_not_join_bullet_passes_strict_validation(self):
        """Scanner v2 doesn't flag 'Do not join' as SQL-in-prose."""
        from genie_space_optimizer.optimization.applier import validate_instruction_text

        prose = (
            "## PURPOSE\n"
            "- Sales analytics for the executive team.\n"
            "\n"
            "## CONSTRAINTS\n"
            "- Do not join `mv_esr_fact_sales` directly "
            "to `mv_7now_fact_sales`; they represent disjoint channels\n"
            "- Never expose customer PII.\n"
        )
        ok, errs = validate_instruction_text(prose, strict=True)
        assert ok, f"Failing-run prose should pass strict validation now. errors={errs}"

    def test_two_level_enforcement_keeps_output_under_cap(self):
        """Pathological LLM output never exceeds MAX_TEXT_INSTRUCTIONS_CHARS
        after the harness applies Layer 1 (pre-render trim) + Layer 2
        (post-render priority trim).

        Simulates the failing-run scenario: existing 1879-char seed +
        expand LLM emits 500+ chars per missing section. Layer 1 clips
        each section to per_section_budget BEFORE merge, then Layer 2
        handles any remaining overflow from rendering overhead.
        """
        from genie_space_optimizer.common.config import (
            CANONICAL_SECTION_HEADERS, MAX_TEXT_INSTRUCTIONS_CHARS,
        )
        from genie_space_optimizer.optimization.applier import (
            _trim_bullets_to_budget, _trim_rendered_to_cap,
            render_canonical_sections,
        )

        # Simulate seeded prose: 3 sections totalling ~1879 chars.
        seeded_len = 1879
        existing_sections = {
            "## PURPOSE": [f"- {'p' * 600}"],
            "## DISAMBIGUATION": [f"- {'d' * 600}"],
            "## DATA QUALITY NOTES": [f"- {'q' * 580}"],
        }

        # Missing sections; expand LLM returns 500 chars each (pathological).
        missing = [
            "## CONSTRAINTS",
            "## Instructions you must follow when providing summaries",
        ]
        expand_output = {
            "## CONSTRAINTS": "- " + ("x" * 500),
            "## Instructions you must follow when providing summaries":
                "- " + ("y" * 500),
        }

        existing_length = seeded_len
        remaining = max(MAX_TEXT_INSTRUCTIONS_CHARS - existing_length, 0)
        per_section_budget = remaining // len(missing)

        # Layer 1: pre-render clip each expand section to its budget.
        merged = dict(existing_sections)
        for header, body in expand_output.items():
            clipped = _trim_bullets_to_budget(body, per_section_budget)
            if clipped.strip():
                merged[header] = [ln for ln in clipped.splitlines() if ln.strip()]

        rendered = render_canonical_sections(merged)
        # Layer 2: post-render clip for rendering overhead.
        rendered = _trim_rendered_to_cap(rendered, MAX_TEXT_INSTRUCTIONS_CHARS)
        new_text = "".join(rendered)

        assert len(new_text) <= MAX_TEXT_INSTRUCTIONS_CHARS, (
            f"Final text is {len(new_text)} chars, must be <= 2000 "
            "after two-level enforcement."
        )
