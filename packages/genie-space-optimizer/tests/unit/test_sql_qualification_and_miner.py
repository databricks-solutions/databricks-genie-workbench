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
