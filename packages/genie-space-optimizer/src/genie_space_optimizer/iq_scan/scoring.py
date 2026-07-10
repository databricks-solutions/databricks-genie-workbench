"""IQ scoring engine for Genie Space configurations.

Pure-function scoring logic extracted from ``backend/services/scanner.py`` so
that both the backend scanner service and the GSO optimizer preflight can
share a single source of truth without creating a cross-package dependency.

3-tier maturity: Not Ready → Ready to Optimize → Trusted.
12 checks, 1 point each.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime


# First 10 checks are config checks; the last 2 are optimization checks.
CONFIG_CHECK_COUNT = 10


def get_maturity_label(checks: list[dict]) -> str:
    """Return maturity label based on which checks pass.

    - All checks pass → Trusted
    - First CONFIG_CHECK_COUNT (config) checks pass → Ready to Optimize
    - Otherwise → Not Ready
    """
    if all(c["passed"] for c in checks):
        return "Trusted"
    if all(c["passed"] for c in checks[:CONFIG_CHECK_COUNT]):
        return "Ready to Optimize"
    return "Not Ready"


def _check(checks: list, label: str, passed: bool,
           detail: str | None = None, severity: str | None = None) -> None:
    """Record a check result (1 point each).

    severity: "pass" | "warning" | "fail" (auto-set from passed if None).
    """
    if severity is None:
        severity = "pass" if passed else "fail"
    checks.append({
        "label": label,
        "passed": passed,
        "detail": detail,
        "severity": severity,
    })


_PLACEHOLDER_TEXT = {
    "",
    "n/a",
    "na",
    "none",
    "null",
    "todo",
    "tbd",
    "unknown",
    "placeholder",
    "description",
}

_VAGUE_DESCRIPTION_TEXT = {
    "id",
    "identifier",
    "key",
    "date",
    "string",
    "number",
    "integer",
    "field",
    "column",
    "timestamp",
    "a string",
    "an id",
    "id field",
    "date field",
    "string field",
    "number field",
}

_NOISE_COLUMN_RE = re.compile(
    r"^(?:"
    r"id|uuid|guid|hash|.*_hash|.*_key|"
    r"etl_.*|ingest_.*|load_.*|raw_.*|.*_raw|.*_json|"
    r"debug_.*|audit_.*|col_\d+|field_\d+|fld_.*|attr_.*"
    r")$|^(?:created|updated|deleted)_at$",
    re.IGNORECASE,
)


def _join_text(value) -> str:
    if isinstance(value, list):
        return " ".join(str(v) for v in value if v is not None)
    return str(value or "")


def _meaningful_text(value, *, min_chars: int = 8, min_words: int = 2) -> bool:
    """Return True for non-placeholder, minimally descriptive text."""
    text = _join_text(value).strip()
    normalized = re.sub(r"\s+", " ", text).lower()
    if normalized in _PLACEHOLDER_TEXT or normalized in _VAGUE_DESCRIPTION_TEXT:
        return False
    words = re.findall(r"[A-Za-z0-9]+", text)
    return len(text) >= min_chars and len(words) >= min_words


def _column_name(col: dict) -> str:
    return str(col.get("column_name") or col.get("name") or "").strip()


def _visible_columns_for_table(table: dict) -> list[dict]:
    """Return visible columns/column_configs, de-duplicated by column name."""
    by_name: dict[str, dict] = {}
    unnamed: list[dict] = []
    for col in table.get("columns", []) + table.get("column_configs", []):
        if col.get("exclude") is True:
            continue
        name = _column_name(col)
        if not name:
            unnamed.append(col)
            continue
        key = name.lower()
        existing = by_name.get(key)
        if existing is None:
            by_name[key] = col
            continue
        # Preserve the richer duplicate entry when columns + column_configs
        # both mention the same physical column.
        if (not (existing.get("description") or existing.get("comment"))) and (
            col.get("description") or col.get("comment")
        ):
            by_name[key] = col
    return list(by_name.values()) + unnamed


def _all_visible_columns(tables: list[dict]) -> list[tuple[dict, dict]]:
    return [
        (table, col)
        for table in tables
        for col in _visible_columns_for_table(table)
    ]


def _sql_guidance_counts(instructions: dict) -> dict[str, int]:
    sql_snippets = instructions.get("sql_snippets", {}) or {}
    return {
        "examples": len(instructions.get("example_question_sqls", []) or []),
        "functions": len(instructions.get("sql_functions", []) or []),
        "expressions": len(sql_snippets.get("expressions", []) or []),
        "measures": len(sql_snippets.get("measures", []) or []),
        "filters": len(sql_snippets.get("filters", []) or []),
    }


def _has_sql_guidance_artifact(counts: dict[str, int]) -> bool:
    return any(counts.get(key, 0) > 0 for key in counts)


def _looks_like_noise_column(name: str) -> bool:
    return bool(_NOISE_COLUMN_RE.search(name.strip()))


_SQL_IN_TEXT_RE = re.compile(
    r"\b(SELECT|WHERE|JOIN|GROUP\s+BY|ORDER\s+BY|HAVING)\b", re.IGNORECASE
)

# ── Smart SQL-in-prose detector (scanner v2) ─────────────────────────
#
# The naïve ``_SQL_IN_TEXT_RE`` above flags any occurrence of a SQL keyword,
# including everyday English uses like "do not join", "where applicable",
# or "having determined". That was an unacceptably high false-positive rate
# once the GSO optimizer started validating rewrites against the same rule.
#
# ``looks_like_sql_in_prose`` returns True only when a line carries SQL
# *structure*, not just a lone keyword. Two gates:
#
#  1. **Anchor patterns** — keyword + structural neighbour (e.g. ``SELECT …
#     FROM``, ``WHERE ident op``, ``JOIN ident ON``). If any anchor matches,
#     the line is flagged regardless of other signals.
#
#  2. **Density fallback** — line with 2+ distinct SQL keywords AND 1+
#     structural signal (dotted identifier, SQL comparator, aggregate call,
#     ``IS NULL``), but NOT starting with a prose imperative.
#
# Natural-language prose like "Do not join X to Y" passes the detector:
# the prose-imperative short-circuit blocks the density fallback, and no
# anchor pattern matches because there is no ON-clause / FROM-clause / etc.

# Tier 1 — clause-shape anchors. Any one of these is sufficient to flag.
_SQL_ANCHOR_PATTERNS: tuple[re.Pattern[str], ...] = (
    # SELECT ... FROM — full statement shape.
    re.compile(r"\bSELECT\b[^.]*?\bFROM\b", re.IGNORECASE),
    # FROM ident + clause — table reference followed by a SQL clause keyword.
    re.compile(
        r"\bFROM\s+[`\"\w.]+\s+(?:AS\s+\w+|WHERE|JOIN|GROUP|ORDER|HAVING|LIMIT)\b",
        re.IGNORECASE,
    ),
    # JOIN ident ON — join with explicit ON clause.
    re.compile(r"\bJOIN\s+[`\"\w.]+\s+ON\b", re.IGNORECASE),
    # WHERE <ident> <comparator> — filter with comparison.
    re.compile(
        r"\bWHERE\s+[`\"\w.]+\s*(?:=|<>|!=|<=|>=|<|>|\bLIKE\b|\bIN\s*\()",
        re.IGNORECASE,
    ),
    # GROUP BY col (+ more cols) + a clause keyword afterwards. Bare
    # "GROUP BY region" at end-of-line is NOT anchor-flagged — the trailing
    # clause keyword is what disambiguates SQL from English "group by
    # urgency". Density fallback can still catch bare forms if the rest
    # of the line has another keyword + structural signal.
    re.compile(
        r"\bGROUP\s+BY\s+[`\"\w.]+(?:\s*,\s*[`\"\w.]+)*\s+"
        r"(?:\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b)",
        re.IGNORECASE,
    ),
    # ORDER BY col + explicit direction OR + LIMIT. "Order by priority,
    # not date" doesn't match because "not" isn't a SQL direction keyword.
    re.compile(
        r"\bORDER\s+BY\s+[`\"\w.]+(?:\s*,\s*[`\"\w.]+)*\s+"
        r"(?:\bASC\b|\bDESC\b|\bLIMIT\b)",
        re.IGNORECASE,
    ),
    # HAVING <aggregate> — clause with aggregate function call.
    re.compile(
        r"\bHAVING\s+(?:SUM|COUNT|AVG|MAX|MIN|STDDEV|VARIANCE)\s*\(",
        re.IGNORECASE,
    ),
)

# Prose-imperative prefix — if a line starts with one of these (optionally
# preceded by a bullet marker), the density fallback is skipped. Anchors
# still fire, because a line like "- Use: SELECT col FROM t" IS SQL.
_PROSE_IMPERATIVE_RE: re.Pattern[str] = re.compile(
    r"^\s*[-*\u2022]?\s*(?:Do\s+not|Do\s+NOT|Don['\u2019]t|Never|Always|When|If|Prefer|Avoid|Combine|Link|Pair|Associate|Consider|Note)\b",
    re.IGNORECASE,
)

# Tier 2 — structural signals. Density fallback requires at least one.
_SQL_STRUCTURAL_SIGNALS: tuple[re.Pattern[str], ...] = (
    # Dotted identifier (optionally backtick-quoted). Matches t.col, a.b.c,
    # `schema`.`table`, etc. Note: "do not join" prose without dotted
    # identifiers doesn't trigger; prose WITH dotted identifiers still
    # requires 2+ keywords, which it won't have.
    re.compile(r"[`\"]?\w+[`\"]?\.[`\"]?\w+[`\"]?", re.IGNORECASE),
    # SQL comparators. Parenthesis required for IN to avoid catching English
    # "in the morning". BETWEEN requires a word-boundary neighbour.
    re.compile(
        r"(?:=|<>|!=|<=|>=|\bLIKE\b|\bIN\s*\(|\bBETWEEN\s+\w+)",
        re.IGNORECASE,
    ),
    # Aggregate function call at word boundary.
    re.compile(
        r"\b(?:SUM|COUNT|AVG|MAX|MIN|STDDEV|VARIANCE)\s*\(",
        re.IGNORECASE,
    ),
    # NULL predicate.
    re.compile(r"\bIS\s+(?:NOT\s+)?NULL\b", re.IGNORECASE),
)


def looks_like_sql_in_prose(line: str) -> bool:
    """Return True iff ``line`` contains a SQL fragment (not just a keyword).

    Single-line detector. For multi-line text use :func:`sql_in_text_findings`.

    A line is flagged when either:

    1. An anchor pattern matches (keyword + structural neighbour — e.g.
       ``SELECT ... FROM``, ``WHERE ident op``, ``JOIN ident ON``), OR
    2. The line does NOT start with a prose imperative (``Do not`` / ``Never`` /
       ``Always`` / ``When`` / ``If`` / ``Prefer`` / ``Avoid`` / …) AND has
       2+ distinct SQL keywords AND 1+ structural signal (dotted identifier,
       comparator, aggregate call, ``IS NULL``).

    Designed to eliminate the false positives the naïve regex produced on
    natural-language prose like "Do not join X to Y".
    """
    if not line or not line.strip():
        return False
    for pat in _SQL_ANCHOR_PATTERNS:
        if pat.search(line):
            return True
    if _PROSE_IMPERATIVE_RE.match(line):
        return False
    distinct_kws = {m.group(1).upper() for m in _SQL_IN_TEXT_RE.finditer(line)}
    if len(distinct_kws) >= 2 and any(
        sig.search(line) for sig in _SQL_STRUCTURAL_SIGNALS
    ):
        return True
    return False


def sql_in_text_findings(text: str) -> list[str]:
    """Return the offending lines for multi-line text.

    Thin line-by-line wrapper over :func:`looks_like_sql_in_prose`. Use this
    when validating a ``source_span`` or a whole ``text_instructions`` blob —
    the span may be multi-line (a compound bullet + its sub-bullets), and
    the per-line function alone would under-detect SQL appearing on lines
    past the first.
    """
    if not text:
        return []
    return [line for line in text.splitlines() if looks_like_sql_in_prose(line)]


def calculate_score(space_data: dict, optimization_run: dict | None = None) -> dict:
    """Calculate IQ score for a Genie Space configuration.

    Returns a dict with:
        - score: int (0-12)
        - total: 12
        - maturity: str
        - checks: flat list of {label, passed, detail, severity}
        - findings / next_steps: from failed checks (fix agent input)
        - warnings / warning_next_steps: advisory guidance from warning-severity checks
    """
    checks: list[dict] = []
    findings = []
    next_steps = []
    warnings = []
    warning_next_steps = []

    data_sources = space_data.get("data_sources", {})
    instructions = space_data.get("instructions", {})
    tables = data_sources.get("tables", [])
    metric_views = data_sources.get("metric_views", [])
    total_sources = len(tables) + len(metric_views)

    # --- Config checks (1-10) ---

    # 1. Space description
    description = space_data.get("description")
    has_description = _meaningful_text(description, min_chars=30, min_words=5)
    desc_text = _join_text(description).strip()
    detail = (
        f"{len(desc_text)} chars"
        if desc_text
        else "No space description configured"
    )
    severity = "fail"
    if has_description:
        severity = "warning" if len(desc_text) < 100 else "pass"
        if severity == "warning":
            detail += " — add domain, audience, and scope details"
    _check(checks, "Space description", has_description, detail=detail, severity=severity)
    if not has_description:
        findings.append("Missing or placeholder space description")
        next_steps.append("Add a space description that defines the domain, audience, and scope")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Expand the space description with domain, audience, and guardrails")

    # 2. Table descriptions — require ≥80% coverage (Gap 2)
    #    Auto-pass when no tables (metric-view-only spaces manage descriptions in UC).
    if tables:
        described_tables = sum(
            1
            for t in tables
            if _meaningful_text(t.get("description") or t.get("comment"))
        )
        total_tables = len(tables)
        pct = described_tables / total_tables
        passed = pct >= 0.80
        detail = f"{described_tables}/{total_tables} tables have descriptions ({pct:.0%})"
        if not passed:
            detail += " — 80%+ required"
        severity = "fail" if not passed else ("warning" if pct < 1.0 else "pass")
        if severity == "warning":
            detail += " — aim for 100%"
        _check(checks, "Table descriptions", passed, detail=detail, severity=severity)
        if not passed:
            findings.append(detail)
            next_steps.append("Add descriptions to your tables to help Genie understand context")
        elif severity == "warning":
            warnings.append(detail)
            warning_next_steps.append("Add descriptions to remaining tables for better Intent Agent routing")
    elif metric_views:
        _check(checks, "Table descriptions", True, detail="Metric-view-only space — descriptions managed in Unity Catalog", severity="pass")
    else:
        _check(checks, "Table descriptions", False, detail="No data sources configured", severity="fail")

    # 3. Column descriptions — require ≥50% coverage (Gap 1)
    #    Auto-pass when no tables (metric-view-only spaces manage columns in UC).
    if tables:
        visible_columns = _all_visible_columns(tables)
        total_cols = len(visible_columns)
        described_cols = sum(
            1
            for _, col in visible_columns
            if _meaningful_text(col.get("description") or col.get("comment"))
        )
        has_synonyms = False
        for _, col in visible_columns:
            if col.get("synonyms"):
                has_synonyms = True
        pct = described_cols / total_cols if total_cols > 0 else 0
        passed = pct >= 0.50
        detail = f"{described_cols}/{total_cols} visible columns have descriptions ({pct:.0%})"
        if not passed:
            detail += " — 50%+ required"
        severity = "fail" if not passed else ("warning" if pct < 0.80 else "pass")
        if severity == "warning":
            detail += " — aim for 80%+"
        _check(checks, "Column descriptions", passed, detail=detail, severity=severity)
        if not passed:
            findings.append(detail)
            next_steps.append("Add column descriptions to improve query accuracy")
        elif severity == "warning":
            warnings.append(detail)
            warning_next_steps.append("Higher coverage improves SQL generation accuracy")
        # Advisory: column synonyms (Gap 7)
        if passed and not has_synonyms:
            warnings.append("No column synonyms defined")
            warning_next_steps.append("Add synonyms for columns with abbreviated or technical names")
    elif metric_views:
        _check(checks, "Column descriptions", True, detail="Metric-view-only space — columns managed in Unity Catalog", severity="pass")
    else:
        _check(checks, "Column descriptions", False, detail="No data sources configured", severity="fail")

    # 4. Text instructions > 50 chars (Gap 6: length + SQL-in-text warnings)
    text_instructions = instructions.get("text_instructions", [])
    total_chars = 0
    all_text = ""
    for t in text_instructions:
        content = t.get("content", "")
        text = "".join(content) if isinstance(content, list) else content
        total_chars += len(text)
        all_text += text
    passed = bool(text_instructions) and total_chars > 50
    detail = f"{len(text_instructions)} instruction(s), {total_chars:,} chars total" if text_instructions else "No text instructions configured"
    severity = "pass" if passed else "fail"
    # Check for warnings on passing check
    ti_warnings = []
    if passed:
        if total_chars > 2000:
            ti_warnings.append(f"Instructions total {total_chars:,} chars — keep under 2,000 to avoid pushing out higher-value SQL context")
        # Smart SQL-in-prose detection (scanner v2). ``sql_in_text_findings``
        # runs the structure-aware ``looks_like_sql_in_prose`` on each line
        # rather than the naïve keyword regex — eliminates false positives
        # on prose like "Do not join X to Y", "Where applicable", etc.
        sql_offenders = sql_in_text_findings(all_text)
        if sql_offenders:
            sample = sql_offenders[0].strip()
            if len(sample) > 100:
                sample = sample[:97] + "..."
            ti_warnings.append(
                f"SQL patterns found in text instructions — move to Example SQLs or "
                f"SQL Expressions. First offender: {sample!r}"
            )
        if ti_warnings:
            severity = "warning"
            detail += f" — {len(ti_warnings)} warning(s)"
    _check(checks, "Text instructions (>50 chars)", passed, detail=detail, severity=severity)
    if not passed:
        findings.append("No text instructions configured" if not text_instructions else "Text instructions are too brief")
        next_steps.append("Add text instructions to explain business context and terminology")
    for w in ti_warnings:
        warnings.append(w)
        warning_next_steps.append("Restructure text instructions for optimal LLM context usage")

    # 5. Join specifications
    join_specs = instructions.get("join_specs", [])
    join_count = len(join_specs)
    passed = total_sources == 1 or (total_sources > 1 and join_count > 0)
    detail = f"{join_count} join spec(s) for {total_sources} data source(s)"
    severity = "pass" if passed else "fail"
    if passed and total_sources > 1 and join_count < max(total_sources - 1, 1):
        severity = "warning"
        detail += " — relationship coverage may be incomplete"
    _check(checks, "Join specifications", passed, detail=detail, severity=severity)
    if not passed and total_sources > 1:
        findings.append("No join specifications for multi-source space")
        next_steps.append("Add join specifications to help Genie correctly join your data sources")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Add join specs for the remaining key relationships between sources")

    # 6. Data source count 1-12 (Gap 10: adjusted from 1-10)
    passed = 1 <= total_sources <= 12
    detail = f"{total_sources} data source(s)"
    severity = "pass" if passed else "fail"
    if passed and total_sources >= 9:
        detail += " — consider focused spaces for broad domains"
        severity = "warning"
    if not passed and total_sources > 12:
        detail += " — consider multi-room architecture"
    _check(checks, "Data source count 1-12", passed, detail=detail, severity=severity)
    if not passed and total_sources == 0:
        findings.append("No tables or metric views configured")
        next_steps.append("Add at least one table or metric view to your Genie Space")
    elif not passed and (tables or metric_views):
        if total_sources > 12:
            findings.append(f"{total_sources} data sources — more than 12 reduces Genie accuracy")
            next_steps.append("Consider multi-room architecture or reducing to the most relevant 5-12 data sources")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Consider splitting broad domains into smaller focused Genie Spaces")

    # 7. SQL guidance artifacts (SQL snippets/functions or example SQLs)
    example_sqls = instructions.get("example_question_sqls", [])
    guidance_counts = _sql_guidance_counts(instructions)
    n_examples = guidance_counts["examples"]
    passed = _has_sql_guidance_artifact(guidance_counts)
    detail = (
        f"{guidance_counts['functions']} functions, "
        f"{guidance_counts['measures']} measures, "
        f"{guidance_counts['filters']} filters, "
        f"{guidance_counts['expressions']} expressions, "
        f"{n_examples} example SQLs"
    )
    severity = "pass" if passed else "fail"
    guidance_warnings: list[str] = []
    if not passed:
        detail += " — at least one required"
    else:
        if (
            guidance_counts["functions"]
            or guidance_counts["expressions"]
            or guidance_counts["measures"]
            or guidance_counts["filters"]
        ) and (not guidance_counts["filters"] or not guidance_counts["measures"]):
            missing = []
            if not guidance_counts["filters"]:
                missing.append("filters")
            if not guidance_counts["measures"]:
                missing.append("measures")
            guidance_warnings.append(f"Add {' and '.join(missing)} for better SQL snippet coverage")
        missing_guidance = sum(1 for e in example_sqls if not e.get("usage_guidance"))
        if missing_guidance > len(example_sqls) / 2:
            guidance_warnings.append(f"{missing_guidance}/{n_examples} example SQLs lack usage_guidance")
    if guidance_warnings:
        severity = "warning"
        detail += f" — {len(guidance_warnings)} warning(s)"
    _check(checks, "SQL guidance artifacts", passed, detail=detail, severity=severity)
    if not passed:
        findings.append("No SQL guidance artifacts configured")
        next_steps.append("Add at least one SQL snippet, SQL function, or example SQL")
    for w in guidance_warnings:
        warnings.append(w)
        warning_next_steps.append("Improve SQL guidance artifacts with reusable snippets and usage guidance")

    # 8. Entity/format matching (Gap 5: count + RLS detection)
    entity_count = 0
    format_count = 0
    rls_tables = []
    for t in tables + metric_views:
        table_has_rls = bool(t.get("row_filter") or t.get("column_mask"))
        for col in t.get("column_configs", []) + t.get("columns", []):
            if col.get("enable_entity_matching"):
                entity_count += 1
            if col.get("enable_format_assistance") or col.get("format_assistance_enabled"):
                format_count += 1
            if col.get("row_filter") or col.get("column_mask"):
                table_has_rls = True
        if table_has_rls:
            table_name = t.get("name") or t.get("table_name") or "unknown"
            rls_tables.append(table_name)
    entity_or_format = entity_count > 0 or format_count > 0
    detail = f"{entity_count} columns with entity matching, {format_count} with format assistance"
    severity = "pass" if entity_or_format else "fail"
    if entity_or_format:
        if entity_count > 120:
            detail += f" — exceeds 120/space limit, excess will be ignored"
            severity = "warning"
        elif entity_count > 100:
            detail += f" — approaching 120/space limit"
            severity = "warning"
    _check(checks, "Entity/format matching", entity_or_format, detail=detail, severity=severity)
    if not entity_or_format and (tables or metric_views):
        findings.append("No columns have entity matching or format assistance enabled")
        next_steps.append("Enable entity matching on categorical columns and format assistance on date/number columns")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Reduce entity matching columns to stay within the 120/space limit")
    # Advisory: RLS disables entity matching silently
    if rls_tables and entity_or_format:
        rls_msg = f"Tables with row-level security ({', '.join(rls_tables[:3])}) — entity matching is silently disabled for these"
        warnings.append(rls_msg)
        warning_next_steps.append("Entity matching won't work on tables with row filters or column masks")

    # 9. 10+ benchmark questions
    benchmarks = space_data.get("benchmarks", {}).get("questions", [])
    n_benchmarks = len(benchmarks)
    passed = n_benchmarks >= 10
    detail = f"{n_benchmarks} benchmark question(s)"
    severity = "pass" if passed else "fail"
    if passed and n_benchmarks < 20:
        detail += " — add more for broader coverage"
        severity = "warning"
    _check(checks, "10+ benchmark questions", passed, detail=detail, severity=severity)
    if not passed:
        if benchmarks:
            findings.append(f"Only {n_benchmarks} benchmark question(s) — add at least 10")
        else:
            findings.append("No benchmark questions configured")
        next_steps.append("Add at least 10 benchmark questions to measure and track Genie accuracy")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Add more benchmark questions across distinct query shapes")

    # 10. Column visibility / noise control
    visible_columns = _all_visible_columns(tables)
    visible_col_count = len(visible_columns)
    noisy_columns = [
        _column_name(col)
        for _, col in visible_columns
        if _looks_like_noise_column(_column_name(col))
    ]
    noisy_count = len(noisy_columns)
    noisy_ratio = noisy_count / visible_col_count if visible_col_count else 0
    max_table_visible = max(
        (len(_visible_columns_for_table(t)) for t in tables),
        default=0,
    )
    passed = total_sources > 0 and not (visible_col_count >= 20 and noisy_ratio >= 0.30)
    detail = f"{noisy_count}/{visible_col_count} visible columns look internal/noisy ({noisy_ratio:.0%})"
    severity = "pass" if passed else "fail"
    visibility_warnings: list[str] = []
    if passed and visible_col_count >= 20 and noisy_ratio >= 0.15:
        visibility_warnings.append("review noisy internal columns")
    if passed and max_table_visible > 75:
        visibility_warnings.append(f"a table exposes {max_table_visible} columns")
    if visibility_warnings:
        severity = "warning"
        detail += " — " + "; ".join(visibility_warnings)
    _check(checks, "Column visibility / noise control", passed, detail=detail, severity=severity)
    if not passed:
        sample = ", ".join(noisy_columns[:5])
        findings.append(
            f"{noisy_count}/{visible_col_count} visible columns look internal/noisy"
            + (f" ({sample})" if sample else "")
        )
        next_steps.append("Hide noisy internal, audit, raw, ingestion, and opaque technical columns")
    elif visibility_warnings:
        warnings.append(detail)
        warning_next_steps.append("Review visible columns and hide fields that do not help users ask questions")

    # --- Optimization checks (11-12) ---

    # 11. Optimization run recorded
    has_run = bool(optimization_run)
    _check(checks, "Optimization workflow completed", has_run)
    if not has_run:
        findings.append("Space has not been through the optimization workflow")
        next_steps.append("Benchmark and improve Genie's accuracy with Optimization")

    # 12. Accuracy ≥ 85%
    accuracy = optimization_run.get("accuracy", 0) if optimization_run else 0
    passed = has_run and accuracy >= 0.85
    detail = f"Accuracy: {accuracy:.0%}" if has_run else None
    _check(checks, "Optimization accuracy ≥ 85%", passed, detail=detail)
    if has_run and not passed:
        findings.append(f"Optimization accuracy is {accuracy:.0%} — target ≥ 85%")
        next_steps.append("Re-run the optimization workflow to improve benchmark accuracy to 85%+")

    score = sum(1 for c in checks if c["passed"])
    maturity = get_maturity_label(checks)

    return {
        "score": score,
        "total": 12,
        "maturity": maturity,
        "checks": checks,
        "optimization_accuracy": accuracy if optimization_run else None,
        "findings": findings[:8],
        "next_steps": next_steps[:8],
        "warnings": warnings[:8],
        "warning_next_steps": warning_next_steps[:8],
        "scanned_at": datetime.now(UTC).isoformat(),
    }
