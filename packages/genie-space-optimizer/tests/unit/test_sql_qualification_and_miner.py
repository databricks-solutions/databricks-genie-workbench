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

    def test_totally_invalid_raises_under_strict(self):
        # Task 8 — default behaviour returns ``None``; callers that
        # require a hard failure must pass ``strict=True``.
        assert _extract_json('this is not json at all') is None
        with pytest.raises(Exception):
            _extract_json('this is not json at all', strict=True)


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

    # ─────────────────────────────────────────────────────────────
    # Fix 2 — deterministic trim before strict validation.
    #
    # Without the two-layer trim, legacy spaces whose seeded prose is
    # already over ``MAX_TEXT_INSTRUCTIONS_CHARS`` (the failing run had
    # 2315-char originals) deterministically returned
    # ``DECLINE_MALFORMED`` even when span removal + trim could fit the
    # output under the cap. The trim mirrors ``_run_enrichment``'s expand
    # path (see harness.py) so the WRITE outcome is reachable without a
    # separate compaction LLM call.
    # ─────────────────────────────────────────────────────────────

    def test_over_cap_but_trimmable_writes_under_cap(self, caplog):
        """Original prose is >2000 chars; span removal + trim fits cap.

        Constructs a realistic over-cap original (existing canonical
        ``## PURPOSE`` + ``## CONSTRAINTS`` totalling ~2400 chars) and a
        miner that promotes one bullet (~80 chars). After span removal
        the text would still be ~2300 chars; the deterministic two-layer
        trim must clip the rendered output to fit under the cap and emit
        ``WRITE`` (not ``DECLINE_MALFORMED``).
        """
        # ~70-char bullets × 35 ≈ 2450 chars body + headers > 2000 cap.
        bullets_purpose = [
            f"- purpose detail {i:02d}: " + ("p" * 50)
            for i in range(20)
        ]
        bullets_constraints = [
            f"- constraint rule {i:02d}: " + ("c" * 50)
            for i in range(15)
        ]
        original = (
            "## PURPOSE\n"
            + "\n".join(bullets_purpose)
            + "\n\n## CONSTRAINTS\n"
            + "\n".join(bullets_constraints)
            + "\n"
        )
        assert len(original) > MAX_TEXT_INSTRUCTIONS_CHARS, (
            "test setup: original prose must exceed the cap to exercise "
            f"the trim path (len={len(original)}, cap="
            f"{MAX_TEXT_INSTRUCTIONS_CHARS})"
        )

        # Miner promotes one bullet; without trim the rest of the prose
        # would still keep the rendered output over cap.
        applied = [bullets_purpose[0]]

        with caplog.at_level("INFO"):
            outcome, new_text, errors = rewrite_instructions_from_miner_output(
                original, applied, keep_in_prose_spans=[],
            )

        assert outcome == RewriteResult.WRITE, (
            f"expected WRITE after trim, got {outcome}; errors={errors}"
        )
        assert len(new_text) <= MAX_TEXT_INSTRUCTIONS_CHARS, (
            f"trimmed text still exceeds cap: {len(new_text)} > "
            f"{MAX_TEXT_INSTRUCTIONS_CHARS}"
        )
        # Promoted bullet should be gone.
        assert bullets_purpose[0].lstrip("- ") not in new_text
        # Trim emitted the structured telemetry log line.
        assert any(
            "miner.rewrite.trimmed" in rec.getMessage()
            for rec in caplog.records
        ), "expected miner.rewrite.trimmed log line on trim"

    def test_under_cap_skips_trim_path(self, caplog):
        """Inputs already under cap must not engage the trim layers.

        Negative coverage so we don't accidentally clip well-formed
        prose. The ``miner.rewrite.trimmed`` log line is the canary.
        """
        original = (
            "## PURPOSE\n- Sales analytics for H1 revenue.\n\n"
            "## CONSTRAINTS\n- never expose ssn.\n"
        )
        assert len(original) < MAX_TEXT_INSTRUCTIONS_CHARS

        with caplog.at_level("INFO"):
            outcome, _new_text, _errors = rewrite_instructions_from_miner_output(
                original, applied_spans=[], keep_in_prose_spans=[],
            )
        assert outcome == RewriteResult.SKIP_NO_CHANGE
        assert not any(
            "miner.rewrite.trimmed" in rec.getMessage()
            for rec in caplog.records
        )

    def test_over_cap_with_sql_still_declines(self):
        """The trim must not bypass SQL-in-prose validation.

        Over-cap original with a keep_in_prose entry that injects raw
        SQL into ``## CONSTRAINTS``. After trim the length is fine, but
        scanner v2 still rejects SQL → ``DECLINE_MALFORMED``. Confirms
        the existing DECLINE path is preserved end-to-end (the plan's
        ``header-only-overhead DECLINE path`` invariant generalised:
        when validation fails for any reason after trim, we decline).
        """
        bullets = [
            f"- ordinary bullet {i:02d}: " + ("x" * 60)
            for i in range(35)
        ]
        original = "## PURPOSE\n" + "\n".join(bullets) + "\n"
        assert len(original) > MAX_TEXT_INSTRUCTIONS_CHARS
        keep = [{
            "section": "## CONSTRAINTS",
            "source_span": "- SELECT raw_pii FROM users WHERE id = 1",
        }]
        outcome, new_text, errors = rewrite_instructions_from_miner_output(
            original, applied_spans=[], keep_in_prose_spans=keep,
        )
        assert outcome == RewriteResult.DECLINE_MALFORMED, (
            f"expected DECLINE on SQL-in-prose, got {outcome}; errors={errors}"
        )
        # ``new_text`` is the untouched original on decline.
        assert new_text == original


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


# ─────────────────────────────────────────────────────────────────────
# NameError regression — _run_enrichment summary block (Bug #5 in audit)
# ─────────────────────────────────────────────────────────────────────
#
# A production run with this build fell back to baseline with:
#
#     NameError: name '_instr_sql_applied' is not defined
#         at harness.py:3256 inside _run_enrichment's summary block.
#
# Cause: Task C.5 refactored the inline miner block into the helper
# ``_run_instruction_prose_mining``, which returns a dict (``_miner_out``)
# instead of the old single-target local ``_instr_sql_applied``. Two
# downstream references in the summary block were missed in the refactor.
#
# The fix switched the summary block to ``_miner_out.get("sql_applied", 0)``
# and added four more keys for the other miner targets. These tests guard
# against the entire class of bug by:
#
#   - Pinning the return-dict contract of ``_run_instruction_prose_mining``
#     so the summary block can read keys without KeyError.
#   - Running ``_run_enrichment`` end-to-end with every downstream
#     dependency mocked, asserting no NameError / KeyError / AttributeError
#     in the summary block.
#
# These tests deliberately do NOT verify enrichment behaviour — that is
# covered by the per-subsystem tests elsewhere in this file. The goal is
# narrowly to catch a future refactor that either renames a summary-block
# local or drops a key from the miner's return dict.


class TestMinerReturnContract:
    """The summary block in _run_enrichment reads five per-target keys
    (``sql_applied``, ``join_applied``, ``example_applied``, ``desc_applied``,
    ``synonym_applied``) plus two gate keys (``total_applied``,
    ``keep_in_prose_count``) from the dict returned by
    ``_run_instruction_prose_mining``. The helper MUST always return all
    seven so downstream code — even with defensive ``.get(default=0)``
    guards — never faces an undefined-field scenario.
    """

    # Keys the summary block in ``_run_enrichment`` reads.
    # If this set changes, the summary block needs updating too — keep
    # them in lock-step.
    _REQUIRED_KEYS: frozenset[str] = frozenset({
        "sql_applied", "join_applied", "example_applied",
        "desc_applied", "synonym_applied",
        "total_applied", "keep_in_prose_count",
    })

    def _metadata_with_orders(self) -> dict:
        return {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "columns": [{"name": "amount"}, {"name": "status"}],
                }],
                "metric_views": [],
            },
            "instructions": {
                "text_instructions": [{
                    "id": "i1",
                    "content": ["## PURPOSE\n- sales analytics.\n"],
                }],
            },
        }

    def test_return_contains_all_summary_block_keys(self, monkeypatch):
        """Miner returns a dict with every key the summary block reads.

        Uses an LLM stub that produces a valid keep_in_prose candidate so
        the helper takes the ``happy path`` and populates every key.
        """
        from genie_space_optimizer.optimization.harness import (
            _run_instruction_prose_mining,
        )

        # LLM returns one keep_in_prose entry — confidence above the promote
        # gate so the rewrite path is exercised.
        def _fake_llm(_w, _system, _prompt, *, span_name: str = "", **kwargs):
            return (
                '[{"target": "keep_in_prose", '
                '"source_span": "sales analytics.", '
                '"confidence": 0.95, '
                '"payload": {"section": "## PURPOSE"}}]'
            ), None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake_llm,
        )
        # patch_space_config is hit by the rewrite path when it emits a
        # set_text_instructions op. Swallow without touching the network.
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            lambda *a, **k: None,
        )
        # write_stage writes to Delta — stub for unit-test isolation.
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.write_stage",
            lambda *a, **k: None,
        )

        metadata = self._metadata_with_orders()
        result = _run_instruction_prose_mining(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="test-run-1",
            space_id="space-test",
            config={"_parsed_space": metadata},
            metadata_snapshot=metadata,
            catalog="cat", schema="sch",
        )

        missing = self._REQUIRED_KEYS - set(result.keys())
        assert not missing, (
            f"_run_instruction_prose_mining must return every key the "
            f"_run_enrichment summary block reads. Missing: {sorted(missing)}"
        )

    def test_return_values_are_integer_sum_safe(self, monkeypatch):
        """Every per-target count is an ``int`` (not None / not MagicMock).

        The summary block does ``sum(_miner_out.get(k, 0) for k in ...)``
        — any non-int value would raise TypeError during arithmetic, which
        is exactly the failure mode we're guarding against (it's a sibling
        of the original NameError).
        """
        from genie_space_optimizer.optimization.harness import (
            _run_instruction_prose_mining,
        )

        def _fake_llm(*a, **k):
            # No candidates — exercises the early-return path.
            return "[]", None

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer._traced_llm_call",
            _fake_llm,
        )
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.write_stage",
            lambda *a, **k: None,
        )

        metadata = self._metadata_with_orders()
        result = _run_instruction_prose_mining(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="test-run-2",
            space_id="space-test",
            config={"_parsed_space": metadata},
            metadata_snapshot=metadata,
            catalog="cat", schema="sch",
        )

        for key in self._REQUIRED_KEYS:
            value = result.get(key, 0)
            assert isinstance(value, int), (
                f"Miner return key {key!r} must be int for summary arithmetic; "
                f"got {type(value).__name__}={value!r}"
            )


class TestRunEnrichmentSummaryBlock:
    """End-to-end regression test for the NameError crash in the
    ``_run_enrichment`` summary block.

    Every downstream dependency is stubbed; the test harness does NOT exercise
    the real enrichment logic. The single assertion is:

        ``_run_enrichment`` completes without raising NameError / KeyError /
        AttributeError inside the summary block.

    Any future refactor that drops a summary-block local, renames a
    miner-return-dict key, or changes the summary-block arithmetic in a
    way that breaks the contract will trip this test.
    """

    def _install_stubs(self, monkeypatch, miner_result: dict) -> list[str]:
        """Monkeypatch every downstream function ``_run_enrichment`` calls.

        Returns the list of stub names that were installed (for debugging
        a future test failure — if the bug is "_run_enrichment now calls
        a new function that isn't stubbed here", the stack trace plus this
        list usually pinpoints it).
        """
        installed: list[str] = []

        def _stub(target: str, fn):
            monkeypatch.setattr(target, fn)
            installed.append(target.rsplit(".", 1)[-1])

        # ── Stage 1: prepare_lever_loop ──────────────────────────────
        _stub(
            "genie_space_optimizer.optimization.harness._prepare_lever_loop",
            lambda *a, **k: {
                "_parsed_space": {
                    "data_sources": {"tables": [], "metric_views": []},
                    "instructions": {"text_instructions": []},
                },
                "_uc_columns": [],
                "description": "",
            },
        )

        # ── enrich_metadata_with_uc_types is called unconditionally;
        # it mutates in place.  Stub to no-op so the test snapshot shape
        # stays simple.
        _stub(
            "genie_space_optimizer.optimization.harness.enrich_metadata_with_uc_types",
            lambda *a, **k: None,
        )

        # ── Stage 2 sub-steps: every proactive enrichment returns a
        # plausible empty result so the truthiness-gated refetches in
        # _run_enrichment fall through without reaching the network.
        _stub(
            "genie_space_optimizer.optimization.harness._run_description_enrichment",
            lambda *a, **k: {"total_enriched": 0, "tables_enriched": 0},
        )
        _stub(
            "genie_space_optimizer.optimization.harness._run_proactive_join_discovery",
            lambda *a, **k: {"total_applied": 0},
        )
        _stub(
            "genie_space_optimizer.optimization.harness._run_space_metadata_enrichment",
            lambda *a, **k: {
                "description_generated": False,
                "questions_generated": False,
            },
        )
        _stub(
            "genie_space_optimizer.optimization.harness._run_instruction_prose_mining",
            lambda *a, **k: miner_result,
        )
        _stub(
            "genie_space_optimizer.optimization.harness._run_proactive_instruction_seeding",
            lambda *a, **k: {
                "instructions_seeded": False,
                "instructions_expanded": False,
                "instruction_chars": 0,
            },
        )
        _stub(
            "genie_space_optimizer.optimization.harness._run_sql_expression_seeding",
            lambda *a, **k: {
                "total_candidates": 0, "total_seeded": 0,
                "repair": {"rewritten": 0},
            },
        )

        # ── Persistence: Delta writes and MLflow model versioning ────
        _stub(
            "genie_space_optimizer.optimization.harness.write_stage",
            lambda *a, **k: None,
        )
        _stub(
            "genie_space_optimizer.optimization.harness.create_genie_model_version",
            lambda *a, **k: "stub-model-id",
        )
        # fetch_space_config is imported inside the function body; stub
        # the source module so the ``from X import Y`` inside picks it up.
        _stub(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            lambda *a, **k: {
                "_parsed_space": {
                    "data_sources": {"tables": [], "metric_views": []},
                    "instructions": {"text_instructions": []},
                },
                "description": "",
            },
        )

        # ── MLflow: start_run is a context manager; log_metrics a no-op.
        # Replace the module the function imports at runtime.
        import mlflow as _real_mlflow  # noqa: PLC0415 - shadow into test scope

        class _NullRun:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        monkeypatch.setattr(_real_mlflow, "start_run", lambda *a, **k: _NullRun())
        monkeypatch.setattr(_real_mlflow, "set_tags", lambda *a, **k: None)
        monkeypatch.setattr(_real_mlflow, "log_metrics", lambda *a, **k: None)
        installed.extend(["mlflow.start_run", "mlflow.set_tags", "mlflow.log_metrics"])

        return installed

    def _run_enrichment_with_stubs(self, monkeypatch, miner_result: dict) -> dict:
        self._install_stubs(monkeypatch, miner_result)

        from genie_space_optimizer.optimization.harness import _run_enrichment

        return _run_enrichment(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="test-run-enrichment",
            space_id="space-test",
            domain="sales",
            benchmarks=[],
            exp_name="/Shared/test/sales",
            catalog="cat",
            schema="sch",
        )

    def test_full_miner_result_completes_without_error(self, monkeypatch):
        """Happy path: miner returns the full spec'd dict. Summary block
        reads every key without issue.
        """
        full_miner = {
            "sql_applied": 3,
            "join_applied": 1,
            "example_applied": 2,
            "desc_applied": 1,
            "synonym_applied": 4,
            "total_applied": 11,
            "keep_in_prose_count": 2,
            "rewrite_outcome": "write",
            "stats": {"candidates_total": 13, "promoted_by_target": {},
                      "by_target": {}, "rejected_by_reason": {}, "retries": 0},
        }
        result = self._run_enrichment_with_stubs(monkeypatch, full_miner)

        # Precisely this arithmetic was broken before the fix:
        #   total_enrichments = ... + _miner_out.get("sql_applied", 0) + ...
        # If it raises NameError inside _run_enrichment, we never reach here.
        summary = result["summary"]
        assert summary["total_enrichments"] >= (
            full_miner["sql_applied"] + full_miner["join_applied"]
            + full_miner["example_applied"] + full_miner["desc_applied"]
            + full_miner["synonym_applied"]
        ), (
            "Summary total_enrichments must include every per-target miner count"
        )

    def test_miner_result_missing_optional_keys_completes_without_error(self, monkeypatch):
        """Defensive guard regression: if a future refactor of the miner
        helper drops an optional per-target key, the summary block's
        ``.get(default=0)`` guards should absorb it — NOT raise.

        This is a sibling of the original NameError: same class of bug
        (a summary-block field going undefined at runtime), same blast
        radius (enrichment falls back to baseline on every optimization
        run). Catch it in CI instead.
        """
        # Only the two gate keys the rewrite/refetch block reads via raw
        # subscript — all per-target keys intentionally omitted.
        minimal_miner = {
            "total_applied": 0,
            "keep_in_prose_count": 0,
        }
        result = self._run_enrichment_with_stubs(monkeypatch, minimal_miner)

        # If we got here, the summary block's ``.get(default=0)`` guards
        # absorbed every missing per-target key. No NameError / KeyError.
        assert result["summary"]["total_enrichments"] == 0, (
            "With every per-target miner count absent, total_enrichments "
            "must be 0 — the guards must default to 0 rather than explode"
        )

    def test_name_error_regression_exact_bug_site(self, monkeypatch):
        """Reproduction of the exact failure mode from the production run.

        Before the fix, this test would crash with::

            NameError: name '_instr_sql_applied' is not defined

        inside _run_enrichment's summary block. With the fix, it completes
        cleanly. Future refactors that re-introduce an undefined local in
        the summary block will fail here first.
        """
        miner_result = {
            "sql_applied": 0,
            "join_applied": 0,
            "example_applied": 0,
            "desc_applied": 0,
            "synonym_applied": 0,
            "total_applied": 0,
            "keep_in_prose_count": 0,
            "rewrite_outcome": "skip_no_change",
            "stats": {"candidates_total": 0, "promoted_by_target": {},
                      "by_target": {}, "rejected_by_reason": {}, "retries": 0},
        }

        # The fact that this call returns (rather than raising) is the
        # regression test. Any future NameError/KeyError/AttributeError in
        # the summary block will propagate out of _run_enrichment because
        # its try/except re-raises.
        result = self._run_enrichment_with_stubs(monkeypatch, miner_result)

        assert isinstance(result, dict)
        assert "summary" in result
        assert "config" in result


# ─────────────────────────────────────────────────────────────────────
# Pillar E.5 — SQL expression seeding rejection capture
# ─────────────────────────────────────────────────────────────────────
#
# Regression guard for the Seeding-stage observability gap:
# ``Rejected: Validation (EXPLAIN): N`` used to surface a counter with
# no reasons. After the fix, ``result["rejected_examples"]`` carries a
# bounded list of ``{sql_prefix, snippet_type, gate, reason}`` entries.


class TestSqlSeedingRejectionCapture:
    def _build_stub_candidate(self) -> dict:
        return {
            "snippet_type": "measure",
            "sql": "SUM(cat.sch.fact_sales.amount)",
            "display_name": "Total Amount",
            "alias": "total_amount",
            "source_count": 0,
        }

    def test_validation_rejection_reason_is_captured(self, monkeypatch):
        """Happy path for the observability contract: a candidate that
        fails ``validate_sql_snippet`` lands in
        ``result["rejected_examples"]`` with the EXPLAIN error."""
        from genie_space_optimizer.optimization import harness as _harness_mod

        stub_candidate = self._build_stub_candidate()

        # Mine returns nothing; schema-discovery returns our stub so we
        # have exactly one candidate to exercise the validator path.
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer."
            "_mine_sql_expression_candidates",
            lambda *_a, **_kw: [],
        )
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer."
            "_discover_schema_sql_expressions",
            lambda *_a, **_kw: [stub_candidate],
        )
        # Bypass the LLM enrichment step — return candidates as-is.
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.optimizer."
            "_enrich_candidates_with_llm",
            lambda candidates, *_a, **_kw: candidates,
        )
        # Drive the validation-rejection branch.
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
            lambda *_a, **_kw: (
                False,
                "UNRESOLVED_COLUMN `cat.sch.fact_sales.amount`",
                stub_candidate["sql"],
            ),
        )
        # No-op for persistence side effects.
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.write_stage",
            lambda *_a, **_kw: None,
        )
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            lambda *_a, **_kw: None,
        )

        config = {"_parsed_space": {"instructions": {"sql_snippets": {}}}}
        metadata_snapshot = {"data_sources": {"tables": [], "metric_views": []}}

        result = _harness_mod._seed_new_sql_snippets(
            w=MagicMock(), spark=MagicMock(),
            run_id="r", space_id="s",
            config=config, metadata_snapshot=metadata_snapshot,
            benchmarks=[], catalog="c", schema="sch",
        )

        assert result["validation_rejected"] == 1, (
            f"expected one validation reject; got result={result}"
        )
        examples = result.get("rejected_examples") or []
        assert len(examples) == 1, (
            f"expected one rejection example; got {examples}"
        )
        rejected = examples[0]
        assert rejected["gate"] == "validation"
        assert rejected["snippet_type"] == "measure"
        assert "UNRESOLVED_COLUMN" in rejected["reason"]
        assert stub_candidate["sql"].split("(")[0] in rejected["sql_prefix"]


# ─────────────────────────────────────────────────────────────────────
# Pillar F — Schema-discovery SQL expression mining
# ─────────────────────────────────────────────────────────────────────
#
# Regression guard for the silent data-path bug in
# ``_discover_schema_sql_expressions``: it used to read
# ``table.get("columns", [])`` but the Genie ``serialized_space`` shape
# stores columns under ``column_configs``. In production every table
# iterated zero columns, so schema discovery silently returned zero
# SQL-expression candidates regardless of how rich the schema was. The
# seeding pool collapsed to benchmark-mined candidates only.
#
# The fix reads ``column_configs`` first with a legacy fallback to
# ``columns``, and visits metric_views as well as tables.


class TestDiscoverSchemaSqlExpressions:
    def _production_snapshot(self) -> dict:
        """Serialized_space shape that the harness actually passes."""
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sales.fact_orders",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "BIGINT"},
                            {"column_name": "revenue_amount", "data_type": "DECIMAL"},
                            {"column_name": "order_date", "data_type": "DATE"},
                            {"column_name": "region_code", "data_type": "STRING"},
                        ],
                    },
                ],
                "metric_views": [
                    {
                        "identifier": "cat.sales.mv_sales",
                        "column_configs": [
                            {"column_name": "total_cost", "data_type": "DOUBLE"},
                            {"column_name": "created_at", "data_type": "TIMESTAMP"},
                        ],
                    },
                ],
            },
        }

    def test_discovers_numeric_measures_from_column_configs(self):
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        candidates = _discover_schema_sql_expressions(self._production_snapshot())

        sqls = {c["sql"] for c in candidates}
        # Numeric "revenue_amount" on fact_orders → SUM measure.
        assert "SUM(cat.sales.fact_orders.revenue_amount)" in sqls, (
            f"expected SUM(...) measure; got {sqls}"
        )

    def test_discovers_date_expressions_from_column_configs(self):
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        candidates = _discover_schema_sql_expressions(self._production_snapshot())
        sqls = {c["sql"] for c in candidates}

        # DATE "order_date" → MONTH + QUARTER expressions.
        assert "MONTH(cat.sales.fact_orders.order_date)" in sqls
        assert "QUARTER(cat.sales.fact_orders.order_date)" in sqls

    def test_metric_view_measures_do_not_emit_table_style_sum(self):
        """Effective MV measures must not produce SUM(mv.measure).

        Databricks metric views require MEASURE(measure_name) in full SQL
        queries, and Genie SQL snippets cannot safely store a table-style
        aggregate over an MV measure. The schema-discovery source should
        skip MV measure aggregates instead of creating known-invalid
        candidates that validation later rejects.
        """
        from genie_space_optimizer.common.asset_semantics import (
            KIND_METRIC_VIEW,
            stamp_asset_semantics,
        )
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        snapshot = self._production_snapshot()
        stamp_asset_semantics(snapshot, {
            "cat.sales.mv_sales": {
                "identifier": "cat.sales.mv_sales",
                "short_name": "mv_sales",
                "kind": KIND_METRIC_VIEW,
                "measures": ["total_cost"],
                "dimensions": ["created_at"],
            },
        })

        candidates = _discover_schema_sql_expressions(snapshot)
        sqls = {c["sql"] for c in candidates}

        assert "SUM(cat.sales.mv_sales.total_cost)" not in sqls
        assert "MONTH(cat.sales.mv_sales.created_at)" in sqls

    def test_table_shelf_semantic_metric_view_skips_measure_sum(self):
        from genie_space_optimizer.common.asset_semantics import (
            KIND_METRIC_VIEW,
            stamp_asset_semantics,
        )
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        snapshot = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sales.mv_sales",
                    "column_configs": [
                        {"column_name": "total_sales", "data_type": "DOUBLE"},
                        {"column_name": "business_date", "data_type": "DATE"},
                    ],
                }],
                "metric_views": [],
            },
        }
        stamp_asset_semantics(snapshot, {
            "cat.sales.mv_sales": {
                "identifier": "cat.sales.mv_sales",
                "short_name": "mv_sales",
                "kind": KIND_METRIC_VIEW,
                "measures": ["total_sales"],
                "dimensions": ["business_date"],
            },
        })

        sqls = {
            c["sql"]
            for c in _discover_schema_sql_expressions(snapshot)
        }
        assert "SUM(cat.sales.mv_sales.total_sales)" not in sqls
        assert "MONTH(cat.sales.mv_sales.business_date)" in sqls

    def test_legacy_columns_shape_still_works(self):
        """Backcompat: tables with ``columns`` (older / test shape)
        must continue to produce candidates via the fallback branch."""
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "columns": [
                            {"name": "revenue", "type_text": "double"},
                            {"name": "sale_date", "type_text": "date"},
                        ],
                    },
                ],
            },
        }
        candidates = _discover_schema_sql_expressions(snapshot)
        sqls = {c["sql"] for c in candidates}
        assert "SUM(cat.sch.t.revenue)" in sqls
        assert "MONTH(cat.sch.t.sale_date)" in sqls

    def test_empty_data_sources_returns_empty(self):
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        assert _discover_schema_sql_expressions({}) == []
        assert _discover_schema_sql_expressions({"data_sources": {}}) == []

    def test_skips_non_matching_columns(self):
        """Bland STRING identifiers and non-matching numeric columns do
        not produce candidates — the heuristic is intentionally
        conservative."""
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "column_configs": [
                            # No MEASURE_PATTERNS hit.
                            {"column_name": "customer_id", "data_type": "BIGINT"},
                            # No DATE_PATTERNS hit.
                            {"column_name": "notes", "data_type": "STRING"},
                        ],
                    },
                ],
            },
        }
        assert _discover_schema_sql_expressions(snapshot) == []

    def test_hidden_columns_are_skipped(self):
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )

        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "column_configs": [
                            {
                                "column_name": "revenue",
                                "data_type": "double",
                                "is_hidden": True,
                            },
                        ],
                    },
                ],
            },
        }
        assert _discover_schema_sql_expressions(snapshot) == []


# ─────────────────────────────────────────────────────────────────────
# SQL Expression naming disambiguation — deterministic qualifier policy
# ─────────────────────────────────────────────────────────────────────


class TestDomainQualifierExtraction:
    """``_domain_qualifier_from_identifier`` extracts a compact source
    qualifier (e.g. ``7NOW``, ``ESR``) from a fully-qualified or short
    table identifier so SQL Expression names can be disambiguated when
    multiple fact tables / metric views could plausibly share a generic
    concept like a "Month-to-Date Filter".
    """

    def test_recognizes_mv_seven_now_prefix(self):
        from genie_space_optimizer.optimization.optimizer import (
            _domain_qualifier_from_identifier,
        )
        assert (
            _domain_qualifier_from_identifier(
                "catalog.schema.mv_7now_fact_sales"
            )
            == "7NOW"
        )

    def test_recognizes_mv_esr_prefix(self):
        from genie_space_optimizer.optimization.optimizer import (
            _domain_qualifier_from_identifier,
        )
        assert (
            _domain_qualifier_from_identifier(
                "catalog.schema.mv_esr_dim_date"
            )
            == "ESR"
        )

    def test_short_identifier_without_catalog_still_works(self):
        from genie_space_optimizer.optimization.optimizer import (
            _domain_qualifier_from_identifier,
        )
        assert _domain_qualifier_from_identifier("mv_7now_fact_sales") == "7NOW"

    def test_returns_empty_for_generic_table_name(self):
        from genie_space_optimizer.optimization.optimizer import (
            _domain_qualifier_from_identifier,
        )
        assert _domain_qualifier_from_identifier("cat.sch.fact_orders") == ""

    def test_returns_empty_for_blank(self):
        from genie_space_optimizer.optimization.optimizer import (
            _domain_qualifier_from_identifier,
        )
        assert _domain_qualifier_from_identifier("") == ""
        assert _domain_qualifier_from_identifier("   ") == ""


class TestExtractPrimaryTableIdentifier:
    """``_extract_primary_table_identifier`` finds a table-like
    identifier inside a SQL fragment so the qualifier helper can run
    even when the candidate has no ``target_table`` set (the current
    benchmark miner doesn't attach one)."""

    def test_extracts_from_aggregation_call(self):
        from genie_space_optimizer.optimization.optimizer import (
            _extract_primary_table_identifier,
        )
        sql = "SUM(catalog.schema.mv_7now_fact_sales.amount)"
        assert (
            _extract_primary_table_identifier(sql)
            == "catalog.schema.mv_7now_fact_sales"
        )

    def test_extracts_from_filter_predicate(self):
        from genie_space_optimizer.optimization.optimizer import (
            _extract_primary_table_identifier,
        )
        sql = (
            "catalog.schema.mv_7now_fact_sales.sales_date "
            ">= DATE_TRUNC('MONTH', CURRENT_DATE())"
        )
        assert (
            _extract_primary_table_identifier(sql)
            == "catalog.schema.mv_7now_fact_sales"
        )

    def test_extracts_from_date_function(self):
        from genie_space_optimizer.optimization.optimizer import (
            _extract_primary_table_identifier,
        )
        sql = "MONTH(catalog.schema.mv_esr_dim_date.order_date)"
        assert (
            _extract_primary_table_identifier(sql)
            == "catalog.schema.mv_esr_dim_date"
        )

    def test_returns_empty_when_no_qualified_identifier(self):
        from genie_space_optimizer.optimization.optimizer import (
            _extract_primary_table_identifier,
        )
        assert _extract_primary_table_identifier("amount > 0") == ""
        assert _extract_primary_table_identifier("") == ""


class TestQualifySqlSnippetMetadata:
    """``_qualify_sql_snippet_metadata`` is the single deterministic
    enforcement layer. It runs after every LLM enrichment / heuristic
    naming step in all three SQL Expression population paths
    (proactive seeding, reactive Lever 6, prose mining). It must:

    - Add a domain qualifier to ``display_name`` when the SQL or
      ``target_table`` references a domain-specific table such as
      ``mv_7now_fact_sales``.
    - Not double-prefix names that already carry the qualifier.
    - Leave generic / unqualified candidates alone so we don't add
      noisy artificial prefixes like ``CATSCH `` or ``T `` to a
      ``Total Revenue`` measure on a plain ``cat.sch.t`` table.
    - Backfill an empty / generic ``instruction`` with text that
      mentions the source domain or table.
    """

    def _candidate(self, **kwargs) -> dict:
        base = {
            "snippet_type": "filter",
            "sql": (
                "catalog.schema.mv_7now_fact_sales.sales_date "
                ">= DATE_TRUNC('MONTH', CURRENT_DATE())"
            ),
            "display_name": "Month-to-Date Filter",
            "alias": "",
            "synonyms": [],
            "instruction": "",
        }
        base.update(kwargs)
        return base

    def test_prefixes_display_name_with_domain_from_sql(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        out = _qualify_sql_snippet_metadata(self._candidate())
        assert out["display_name"] == "7NOW Month-to-Date Filter"

    def test_prefixes_display_name_using_target_table_argument(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        cand = self._candidate(
            sql="amount > 0",
            display_name="High-Value Filter",
        )
        out = _qualify_sql_snippet_metadata(
            cand,
            target_table="catalog.schema.mv_esr_fact_sales",
        )
        assert out["display_name"] == "ESR High-Value Filter"

    def test_does_not_double_prefix_already_qualified_name(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        cand = self._candidate(display_name="7NOW Month-to-Date Filter")
        out = _qualify_sql_snippet_metadata(cand)
        assert out["display_name"] == "7NOW Month-to-Date Filter"

    def test_leaves_generic_table_alone(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        cand = self._candidate(
            sql="SUM(cat.sch.t.revenue_amount)",
            display_name="Total Revenue Amount",
            snippet_type="measure",
        )
        out = _qualify_sql_snippet_metadata(cand)
        assert out["display_name"] == "Total Revenue Amount"

    def test_backfills_empty_instruction_with_source_table_hint(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        out = _qualify_sql_snippet_metadata(self._candidate())
        instr = out.get("instruction", "")
        assert isinstance(instr, str)
        assert instr, "expected a non-empty instruction backfill"
        assert "7NOW" in instr or "mv_7now_fact_sales" in instr

    def test_preserves_existing_instruction(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        cand = self._candidate(
            instruction=(
                "Use this when answering questions about the current "
                "month sales for 7NOW."
            ),
        )
        out = _qualify_sql_snippet_metadata(cand)
        assert out["instruction"].startswith(
            "Use this when answering questions"
        )

    def test_returns_a_distinct_dict_from_input(self):
        from genie_space_optimizer.optimization.optimizer import (
            _qualify_sql_snippet_metadata,
        )
        cand = self._candidate()
        out = _qualify_sql_snippet_metadata(cand)
        assert out is not cand


class TestSchemaDiscoveryQualification:
    """``_discover_schema_sql_expressions`` should produce qualified
    ``display_name``s for domain-specific tables/MVs."""

    def _snapshot(self) -> dict:
        return {
            "data_sources": {
                "tables": [],
                "metric_views": [
                    {
                        "identifier": "catalog.schema.mv_7now_fact_sales",
                        "column_configs": [
                            {
                                "column_name": "revenue_amount",
                                "data_type": "double",
                            },
                            {
                                "column_name": "sales_date",
                                "data_type": "date",
                            },
                        ],
                    },
                ],
            },
        }

    def test_seven_now_measure_is_domain_qualified(self):
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )
        cands = _discover_schema_sql_expressions(self._snapshot())
        measures = [c for c in cands if c["snippet_type"] == "measure"]
        assert measures, f"expected a measure candidate; got {cands}"
        assert any(
            c["display_name"].startswith("7NOW ")
            for c in measures
        ), (
            f"expected 7NOW-qualified measure name; got "
            f"{[c['display_name'] for c in measures]}"
        )

    def test_seven_now_expression_is_domain_qualified(self):
        from genie_space_optimizer.optimization.optimizer import (
            _discover_schema_sql_expressions,
        )
        cands = _discover_schema_sql_expressions(self._snapshot())
        exprs = [c for c in cands if c["snippet_type"] == "expression"]
        assert exprs, f"expected an expression candidate; got {cands}"
        assert any(
            c["display_name"].startswith("7NOW ")
            for c in exprs
        ), (
            f"expected 7NOW-qualified expression name; got "
            f"{[c['display_name'] for c in exprs]}"
        )


class TestLever6ProposalQualification:
    """The reactive Lever 6 path runs an LLM and then constructs a
    ``proposal`` dict. After the deterministic qualifier hook runs,
    a generic LLM-supplied ``display_name`` for a 7NOW SQL must come
    out qualified."""

    def _cluster(self) -> dict:
        return {
            "cluster_id": "c1",
            "root_cause": "missing_filter",
            "question_traces": [{"q": "x"}],
        }

    def _snapshot(self) -> dict:
        return {
            "data_sources": {
                "tables": [],
                "metric_views": [
                    {
                        "identifier": "catalog.schema.mv_7now_fact_sales",
                        "column_configs": [
                            {
                                "column_name": "sales_date",
                                "data_type": "date",
                            },
                        ],
                    },
                ],
            },
            "sql_snippets": {},
        }

    def test_lever6_qualifies_generic_display_name(self, monkeypatch):
        from genie_space_optimizer.optimization import optimizer as opt

        llm_payload = (
            '{"snippet_type": "filter", '
            '"display_name": "Month-to-Date Filter", '
            '"alias": "mtd_filter", '
            '"sql": "catalog.schema.mv_7now_fact_sales.sales_date '
            ">= DATE_TRUNC(\\\"MONTH\\\", CURRENT_DATE())\", "
            '"synonyms": ["MTD"], '
            '"instruction": "", '
            '"target_table": "catalog.schema.mv_7now_fact_sales", '
            '"rationale": "x", '
            '"affected_questions": []}'
        )

        def _fake_llm(_w, _system, _prompt, *, span_name="", **kwargs):
            return llm_payload, None

        monkeypatch.setattr(opt, "_traced_llm_call", _fake_llm)
        monkeypatch.setattr(
            opt, "_validate_sql_identifiers",
            lambda *_a, **_k: (True, []),
        )

        proposal = opt._generate_lever6_proposal(
            self._cluster(),
            self._snapshot(),
            w=MagicMock(),
            spark=None,
            warehouse_id="",
        )
        assert proposal is not None
        assert proposal["display_name"] == "7NOW Month-to-Date Filter"


class TestProseMiningInstructionPersistence:
    """Prose-mined SQL snippets carry a ``description`` from the LLM —
    the applier must persist it as ``instruction`` so Genie keeps the
    "when to use this" hint."""

    def test_apply_persists_description_as_instruction(self, monkeypatch):
        from genie_space_optimizer.optimization import harness

        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            lambda *_a, **_k: None,
        )
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_schema.generate_genie_id",
            lambda: "test-id",
        )
        monkeypatch.setattr(
            harness, "write_stage", lambda *_a, **_k: None,
        )

        candidates = [
            {
                "snippet_type": "filter",
                "sql": (
                    "catalog.schema.mv_7now_fact_sales.sales_date "
                    ">= DATE_TRUNC('MONTH', CURRENT_DATE())"
                ),
                "display_name": "7NOW Month-to-Date Filter",
                "description": (
                    "Use this filter for 7NOW current month sales."
                ),
                "synonyms": ["MTD"],
                "alias": "",
            },
        ]
        metadata_snapshot: dict = {"instructions": {}}

        applied = harness._apply_instruction_sql_expressions(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="r1",
            space_id="s1",
            candidates=candidates,
            metadata_snapshot=metadata_snapshot,
            catalog="cat",
            schema="sch",
        )
        assert applied == 1
        snippets = (
            metadata_snapshot["instructions"]["sql_snippets"]["filters"]
        )
        assert len(snippets) == 1
        instr = snippets[0].get("instruction")
        assert instr, f"expected instruction to be set; got entry={snippets[0]}"
        if isinstance(instr, list):
            assert any("7NOW current month sales" in s for s in instr)
        else:
            assert "7NOW current month sales" in instr


class TestLever6StructuralCandidateBridge:
    def test_structural_candidate_becomes_sql_snippet_proposal(self, monkeypatch):
        from genie_space_optimizer.optimization import optimizer as opt

        monkeypatch.setattr(
            opt,
            "_validate_sql_identifiers",
            lambda *_args, **_kwargs: (True, []),
        )
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
            lambda sql, snippet_type, *_args, **_kwargs: (True, "", sql),
        )

        ag = {
            "id": "AG_FN",
            "root_cause_summary": "missing function expression",
            "affected_questions": ["q_fn"],
            "source_cluster_ids": ["H001"],
            "lever_directives": {},
            "_lever6_structural_candidates": [
                {
                    "snippet_type": "expression",
                    "sql": (
                        "prashanth_subrahmanyam_catalog.sales_reports."
                        "fn_mtd_or_mtday(MEASURE(`_7now_py_sales_mtd`))"
                    ),
                    "display_name": "Expression: fn_mtd_or_mtday",
                    "alias": "fn_mtd_or_mtday",
                    "instruction": "Use this expression for prior-year MTD sales.",
                    "source_question_id": "q_fn",
                    "source": "rca_failed_question_sql",
                    "evidence": "expected SQL used function absent from generated SQL",
                    "confidence": 0.85,
                }
            ],
        }

        proposals = opt.generate_proposals_from_strategy(
            strategy={},
            action_group=ag,
            metadata_snapshot={"sql_snippets": {}, "data_sources": {"tables": [], "metric_views": []}},
            target_lever=6,
            apply_mode="genie_config",
            spark=object(),
            catalog="cat",
            gold_schema="sch",
            warehouse_id="wh",
            benchmarks=[],
        )

        assert proposals
        assert proposals[0]["patch_type"] == "add_sql_snippet_expression"
        assert proposals[0]["source"] == "rca_failed_question_sql"
        assert proposals[0]["validation_passed"] is True
        assert proposals[0]["target_qids"] == ["q_fn"]

    def test_structural_candidate_bridge_rejects_non_sql_snippet_patch_type(self):
        from genie_space_optimizer.optimization.optimizer import (
            _proposal_from_structural_sql_candidate,
        )

        candidate = {
            "snippet_type": "example_sql",
            "sql": "SELECT 1",
            "display_name": "bad",
            "source": "rca_failed_question_sql",
        }

        assert _proposal_from_structural_sql_candidate(
            candidate,
            metadata_snapshot={"sql_snippets": {}},
            cluster_id="H001",
            target_qids=("q1",),
            spark=None,
            catalog="cat",
            gold_schema="sch",
            w=None,
            warehouse_id="",
            benchmarks=[],
        ) is None


def test_prose_rule_mining_prompt_includes_unified_rca_contract_tag() -> None:
    """Structural guard. Phrasing assertions live on the contract constant."""
    from genie_space_optimizer.common.config import PROSE_RULE_MINING_PROMPT

    assert "<unified_rca_engine_contract>" in PROSE_RULE_MINING_PROMPT
    assert "</unified_rca_engine_contract>" in PROSE_RULE_MINING_PROMPT
    assert "{{ instructions_text }}" in PROSE_RULE_MINING_PROMPT
