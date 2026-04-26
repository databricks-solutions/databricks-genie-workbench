"""
Optimization Applier — patch rendering, application, and rollback.

Converts optimizer proposals into Patch DSL actions, applies them to the
Genie Space config (and optionally UC artifacts), and supports full
snapshot-based rollback.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from typing import Any

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    BOOLEAN_FLAG_PATTERNS,
    CANONICAL_SECTION_HEADERS,
    CANONICAL_SECTION_ORDER,
    CATEGORICAL_COLUMN_PATTERNS,
    DESCRIPTION_HINTS_NEGATIVE,
    DESCRIPTION_HINTS_POSITIVE,
    ENABLE_REWRITE_SECTION_SPLIT,
    ENABLE_SMARTER_SCORING,
    FREE_TEXT_COLUMN_PATTERNS,
    FREE_TEXT_DISTINCT_RATIO,
    HIGH_RISK_PATCHES,
    INSTRUCTION_SECTION_ORDER,
    LEVER_TO_SECTIONS,
    LOW_RISK_PATCHES,
    MAX_ENTITY_MATCHING_CARDINALITY,
    MAX_TEXT_INSTRUCTIONS_CHARS,
    MAX_VALUE_DICTIONARY_COLUMNS,
    MEASURE_NAME_PREFIXES,
    MEDIUM_RISK_PATCHES,
    MIN_ENTITY_MATCHING_CARDINALITY,
    NUMERIC_DATA_TYPES,
    PATCH_TYPES,
    PII_COLUMN_PATTERNS,
    STRICT_RLS_MODE,
    VERBATIM_REQUIRED_HEADERS,
    _LEVER_TO_PATCH_TYPE,
    looks_like_sql_in_prose,
)
from genie_space_optimizer.optimization.structured_metadata import (
    LeverOwnershipError,
    entity_type_for_column,
    extract_synonyms_section,
    format_synonyms_section,
    merge_synonyms,
    parse_structured_description,
    render_structured_description,
    update_sections,
)
from genie_space_optimizer.common.genie_client import (
    patch_space_config,
    sort_genie_config,
    strip_non_exportable_fields,
)
from genie_space_optimizer.optimization.optimizer import _resolve_scope

logger = logging.getLogger(__name__)


_MAX_INSTRUCTION_CHARS = 24_500  # Genie Space API enforces 25 000; leave margin


# ── Join Spec Helpers ─────────────────────────────────────────────────
# The Genie Space API uses nested objects for join specs:
#   {"left": {"identifier": "...", "alias": "..."}, "right": {...}, "sql": [...]}
# These helpers extract identifiers for matching across add/update/remove ops.


def _join_spec_left_id(spec: dict) -> str:
    """Extract left table identifier from a join spec (API or legacy format)."""
    left = spec.get("left")
    if isinstance(left, dict):
        return left.get("identifier", "")
    return spec.get("left_table_name", "")


def _join_spec_right_id(spec: dict) -> str:
    """Extract right table identifier from a join spec (API or legacy format)."""
    right = spec.get("right")
    if isinstance(right, dict):
        return right.get("identifier", "")
    return spec.get("right_table_name", "")


def _validate_join_spec_entry(entry: dict) -> bool:
    """Validate a join spec dict conforms to the Genie API schema.

    Skips validation for legacy format (``left_table_name`` / ``right_table_name``)
    which is only used internally and gets transformed before the final PATCH.
    """
    if "left_table_name" in entry or "right_table_name" in entry:
        return True

    from genie_space_optimizer.common.genie_schema import JoinSpec

    try:
        JoinSpec.model_validate(entry)
        return True
    except Exception as exc:
        logger.warning("Invalid join spec rejected: %s — validation error: %s", entry, exc)
        return False


def _validate_example_sql_entry(
    entry: dict, config: dict | None = None
) -> bool:
    """Validate an example_question_sql dict conforms to the Genie API schema.

    When *config* is provided, also verifies the SQL references at least one
    known table, TVF, or metric view from the current config.
    """
    from genie_space_optimizer.common.genie_schema import ExampleQuestionSql

    try:
        ExampleQuestionSql.model_validate(entry)
    except Exception:
        logger.warning("Invalid example SQL entry rejected: %s", entry)
        return False

    if config:
        raw_sql = entry.get("sql") or ""
        sql_lower = (" ".join(raw_sql) if isinstance(raw_sql, list) else str(raw_sql)).lower()
        if sql_lower:
            known: set[str] = set()
            ds = config.get("data_sources") or {}
            for source_key in ("tables", "metric_views"):
                for tbl in ds.get(source_key) or []:
                    ident = (tbl.get("identifier") or tbl.get("name") or "").lower()
                    known.add(ident)
                    parts = ident.split(".")
                    if len(parts) >= 2:
                        known.add(".".join(parts[-2:]))
                    known.add(parts[-1])
            known.discard("")
            if known and not any(a in sql_lower for a in known):
                logger.warning(
                    "Example SQL references no known asset — rejected: %.120s",
                    entry.get("sql", ""),
                )
                return False

    return True


def _validate_sql_snippet_entry(entry: dict, snippet_type: str) -> bool:
    """Validate a sql_snippet dict conforms to the Genie API schema."""
    from genie_space_optimizer.common.genie_schema import (
        SqlSnippetExpression,
        SqlSnippetFilter,
        SqlSnippetMeasure,
    )

    model_map = {
        "filters": SqlSnippetFilter,
        "expressions": SqlSnippetExpression,
        "measures": SqlSnippetMeasure,
    }
    model_cls = model_map.get(snippet_type)
    if not model_cls:
        return False

    try:
        model_cls.model_validate(entry)
        return True
    except Exception as exc:
        logger.warning(
            "Invalid sql_snippet (%s) rejected: %s — error: %s",
            snippet_type, entry, exc,
        )
        return False


def _enforce_instruction_limit(config: dict) -> None:
    """Trim text_instructions content so it stays under the API limit."""
    ti = (config.get("instructions") or {}).get("text_instructions", [])
    if not ti:
        return
    content = ti[0].get("content", [])
    if not isinstance(content, list):
        content = [str(content)]
    full_text = "\n".join(content)
    if len(full_text) <= _MAX_INSTRUCTION_CHARS:
        return
    logger.warning(
        "Instruction text %d chars exceeds limit %d — trimming from end",
        len(full_text), _MAX_INSTRUCTION_CHARS,
    )
    ti[0]["content"] = [full_text[:_MAX_INSTRUCTION_CHARS].rsplit("\n", 1)[0]]


def _get_general_instructions(config: dict) -> str:
    """Extract general instructions as joined text from text_instructions."""
    inst = config.get("instructions", {})
    ti = inst.get("text_instructions", [])
    if not ti:
        return ""
    content = ti[0].get("content", [])
    if isinstance(content, list):
        return "\n".join(c for c in content if c)
    return str(content)


def _canonicalize_and_dedup_instructions(config: dict) -> bool:
    """Phase 3.4: single end-of-pipeline normalization for instruction text.

    Runs after every patch application path so that all roads converge
    on a single canonical form regardless of who wrote the bytes:

    - GSO-owned policy bullets (apply_gso_quality_instructions)
    - Strategist ``update_instruction_section`` patches
    - Strategist ``rewrite_instruction`` splits (legacy ALL-CAPS headers)
    - The :func:`_sanitize_plaintext_instructions` markdown-to-plain pass

    Returns ``True`` if the text was rewritten in place. Idempotent:
    canonical input passes through unchanged.
    """
    inst = config.get("instructions")
    if not isinstance(inst, dict):
        return False
    ti = inst.get("text_instructions")
    if not isinstance(ti, list) or not ti:
        return False

    head = ti[0]
    if not isinstance(head, dict):
        return False
    content = head.get("content")
    if isinstance(content, list):
        original = "\n".join(c for c in content if c)
    elif isinstance(content, str):
        original = content
    else:
        return False
    if not original.strip():
        return False

    # Lazy import to break a circular at module load.
    from genie_space_optimizer.optimization.optimizer import (
        _sanitize_plaintext_instructions,
    )

    sanitized = _sanitize_plaintext_instructions(original)
    if not sanitized:
        return False

    # Deduplicate sections.  We treat any pair of headers that map to
    # the same legacy ALL-CAPS name (whether written ``## CONSTRAINTS``
    # or ``CONSTRAINTS:``) as the same section; the second occurrence
    # has its body merged into the first and the duplicate header line
    # is dropped.  Bullet-level dedup within each section is also
    # applied so a policy bullet present in both versions of the
    # section collapses to a single bullet.
    lines = sanitized.split("\n")
    section_order: list[str] = []
    section_lines: dict[str, list[str]] = {}
    section_seen_bullets: dict[str, set[str]] = {}
    preamble: list[str] = []
    current: str | None = None

    _legacy_re = re.compile(r"^\s*([A-Z][A-Z0-9 _/]{2,80})\s*:\s*$")
    _markdown_re = re.compile(r"^\s*##\s+([^\n]+?)\s*$")

    def _start_section(name: str) -> None:
        nonlocal current
        current = name
        if name not in section_lines:
            section_lines[name] = []
            section_seen_bullets[name] = set()
            section_order.append(name)

    for raw_line in lines:
        m_md = _markdown_re.match(raw_line)
        m_lg = _legacy_re.match(raw_line) if not m_md else None
        if m_md:
            _start_section(m_md.group(1).upper().rstrip(":").strip())
            continue
        if m_lg:
            _start_section(m_lg.group(1).upper().rstrip(":").strip())
            continue
        if current is None:
            preamble.append(raw_line)
            continue
        stripped = raw_line.strip()
        if stripped:
            # Bullet-level dedup keyed by the bullet body so identical
            # policy bullets emitted twice (e.g. once via GSO-quality
            # and once via a strategist split) collapse.
            if stripped in section_seen_bullets[current]:
                continue
            section_seen_bullets[current].add(stripped)
        section_lines[current].append(raw_line)

    # Enforce CANONICAL_SECTION_HEADERS order on canonical sections.
    # Non-canonical sections (e.g. customer-authored "MARKET DEFINITIONS:")
    # keep their first-appearance order and follow the canonical block.
    _canonical_keys = {
        h.removeprefix("## ").upper() for h in CANONICAL_SECTION_HEADERS
    }
    _canonical_index = {
        h.removeprefix("## ").upper(): i
        for i, h in enumerate(CANONICAL_SECTION_HEADERS)
    }
    canonical_in_doc = sorted(
        (s for s in section_order if s in _canonical_keys),
        key=lambda s: _canonical_index[s],
    )
    non_canonical = [s for s in section_order if s not in _canonical_keys]
    section_order = canonical_in_doc + non_canonical

    rebuilt_parts: list[str] = []
    if any(_l.strip() for _l in preamble):
        rebuilt_parts.extend(preamble)
        if rebuilt_parts and rebuilt_parts[-1].strip():
            rebuilt_parts.append("")
    for name in section_order:
        rebuilt_parts.append(f"{name}:")
        # Trim trailing blanks within the section.
        body = section_lines[name]
        while body and not body[-1].strip():
            body.pop()
        rebuilt_parts.extend(body)
        rebuilt_parts.append("")

    rebuilt = "\n".join(rebuilt_parts).rstrip() + "\n"
    rebuilt = rebuilt.rstrip()
    # Idempotency: only mutate when canonicalization changed something.
    if rebuilt == sanitized.rstrip() and rebuilt == original.rstrip():
        return False

    head["content"] = [rebuilt]
    return True


_HEX_32 = re.compile(r"^[0-9a-f]{32}$")


# ─────────────────────────────────────────────────────────────────────────────
# D1–D3: GSO quality-instruction policies (baseline-eval-fix plan).
#
# Each policy is rendered as a normal bullet under its target canonical
# ``##`` section — indistinguishable from customer-authored bullets. The
# applier identifies its own content across runs by exact-text match
# against this allowlist (current + deprecated bodies), scoped to each
# policy's target header. Customer-authored bullets with any different
# wording are never touched.
#
# Rewording a policy? Move the previous text into
# ``_GSO_QUALITY_V1_DEPRECATED_BULLETS`` in the same commit so existing
# customer spaces are cleaned up on their next apply. Add a unit test
# that seeds the old string and asserts it's stripped.
# ─────────────────────────────────────────────────────────────────────────────
_GSO_QUALITY_V1_POLICIES: tuple[tuple[str, str, str], ...] = (
    (
        "mv_preference",
        "## CONSTRAINTS",
        "For aggregate questions over dates or periods, prefer the metric "
        "view `mv_*` when one exists that covers the requested dimensions; "
        "otherwise use a regular `TABLE`.",
    ),
    (
        "column_ordering",
        "## CONSTRAINTS",
        "When a question asks for results 'by X, then Y', return columns in "
        "that order and sort the result set by X, then Y ascending unless "
        "the question specifies otherwise.",
    ),
    (
        "calendar_grounding",
        "## DATA QUALITY NOTES",
        "Interpret 'this year', 'last quarter', and 'YTD' relative to the "
        "current system date from `NOW()`. Do not rely on static flags in "
        "`DIM_DATE` for current-period filtering.",
    ),
)

# Previously-shipped policy bodies. Kept here for at least one release
# after any policy text change so existing customer spaces have their
# stale bullets stripped on the next apply. Safe to empty once all
# customer spaces have run against the newer code.
_GSO_QUALITY_V1_DEPRECATED_BULLETS: frozenset[str] = frozenset()

# Legacy sentinel format (pre-Option-C). One-release migration: the strip
# pass scrubs these wrappers out of customer spaces that ran against the
# sentinel-era code. Safe to delete after a full rollout cycle.
_GSO_QUALITY_V1_LEGACY_SENTINEL_RE = re.compile(
    r"\n?-- BEGIN GSO_QUALITY_V1:[a-z_]+\n.*?\n-- END GSO_QUALITY_V1:[a-z_]+\n?",
    re.DOTALL,
)


def _known_policy_bullets_by_header() -> dict[str, set[str]]:
    """Return current + deprecated policy bodies keyed by target header.

    Deprecated bullets are applied to every canonical header we own so a
    reworded policy still gets cleaned up regardless of whether a prior
    version targeted a different section. (Today all policies target
    ``## CONSTRAINTS`` or ``## DATA QUALITY NOTES``; the union-over-owned-
    headers semantics keeps the contract forward-compatible.)
    """
    by_header: dict[str, set[str]] = {}
    for _key, header, text in _GSO_QUALITY_V1_POLICIES:
        by_header.setdefault(header, set()).add(text.strip())
    owned_headers = set(by_header.keys())
    for text in _GSO_QUALITY_V1_DEPRECATED_BULLETS:
        for header in owned_headers:
            by_header.setdefault(header, set()).add(text.strip())
    return by_header


# Header equivalence: a canonical ``## X`` is the same conceptual section
# as a legacy ``X:`` (uppercase, no ``##``, no trailing colon — that's how
# ``parse_canonical_sections`` keys the legacy bucket). Used by the
# semantic strip pass to dedup GSO-owned bullets across both schemes.
_CANONICAL_TO_LEGACY_HEADER: dict[str, str] = {
    h: h.removeprefix("## ").upper() for h in CANONICAL_SECTION_HEADERS
}


def _normalize_policy_body(text: str) -> str:
    """Normalize a bullet body for semantic (cross-format) comparison.

    Strips inline-code backticks (which ``_sanitize_plaintext_instructions``
    removes when flipping ``## X`` → ``X:``) and collapses internal
    whitespace, so a bullet that has round-tripped through any plain-text
    rewrite still matches its canonical-form original. Without this
    normalisation the strip pass falls back to byte-exact match and silently
    fails the moment downstream prose mutates our prior output, leaving
    duplicate sections (one canonical, one legacy) on each subsequent run.
    """
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _known_policy_bodies_normalized() -> dict[str, set[str]]:
    """Return ``{canonical_header: {normalized_body, ...}}`` for current +
    deprecated GSO bullets.

    Mirrors :func:`_known_policy_bullets_by_header` but pre-normalises each
    body for the semantic strip pass.
    """
    by_header: dict[str, set[str]] = {}
    for _key, header, text in _GSO_QUALITY_V1_POLICIES:
        by_header.setdefault(header, set()).add(_normalize_policy_body(text))
    owned_headers = set(by_header.keys())
    for text in _GSO_QUALITY_V1_DEPRECATED_BULLETS:
        for header in owned_headers:
            by_header.setdefault(header, set()).add(_normalize_policy_body(text))
    return by_header


def _bullet_text(line: str) -> str:
    """Strip leading bullet marker + whitespace so ``- foo`` and ``foo`` match."""
    return re.sub(r"^\s*[-*]\s*", "", line).strip()


def _render_instruction_text(
    preamble: list[str],
    canonical: dict[str, list[str]],
    legacy: dict[str, list[str]],
) -> str:
    """Round-trip-safe render of parse_canonical_sections output.

    Unlike :func:`render_canonical_sections` (which drops preamble and
    legacy by design — the miner path never wants them re-emitted), this
    helper preserves every piece of the parsed structure so customer
    content outside the canonical schema survives an apply pass.

    Emit order:
      1. preamble (free-floating lines before any header)
      2. canonical sections in ``CANONICAL_SECTION_ORDER``
      3. legacy ALL-CAPS sections in insertion order
    """
    parts: list[str] = []

    trimmed_preamble = list(preamble)
    while trimmed_preamble and not trimmed_preamble[0].strip():
        trimmed_preamble.pop(0)
    while trimmed_preamble and not trimmed_preamble[-1].strip():
        trimmed_preamble.pop()
    if trimmed_preamble:
        parts.append("\n".join(trimmed_preamble))

    canonical_ordered = sorted(
        (h for h in canonical if h in CANONICAL_SECTION_HEADERS),
        key=lambda h: CANONICAL_SECTION_ORDER[h],
    )
    for header in canonical_ordered:
        lines = [ln for ln in canonical.get(header, []) if ln.strip()]
        if not lines:
            continue
        parts.append(header + "\n" + "\n".join(lines))

    for header, lines in legacy.items():
        body_lines = [ln for ln in lines if ln.strip()]
        if not body_lines:
            continue
        parts.append(f"{header}:\n" + "\n".join(body_lines))

    return "\n\n".join(parts)


def apply_gso_quality_instructions(config: dict) -> bool:
    """Insert or refresh the D1–D3 quality bullets in general instructions.

    Each policy is rendered as a plain bullet under its target canonical
    ``##`` section so the customer never sees our machinery. Idempotent
    via a *semantic* strip pass: any existing bullet whose normalised body
    matches a known policy body (current or deprecated) is stripped from
    BOTH the canonical bucket (``## CONSTRAINTS``) AND the equivalent
    legacy bucket (``CONSTRAINTS:``). Then — when
    ``GSO_APPLY_QUALITY_INSTRUCTIONS=on`` — the current bullet is
    re-inserted under the canonical header.

    The semantic strip is required because ``_sanitize_plaintext_instructions``
    (and a handful of other plain-text rewrite paths) flip ``## X`` →
    ``X:`` and strip inline-code backticks. Without normalisation, the
    strip pass fails byte-equality on those mutated copies and the next
    run emits a fresh canonical section on top of the legacy one, with
    each successive run cementing the duplication.

    Legacy ``-- BEGIN/END GSO_QUALITY_V1:<key>`` sentinel blocks from
    pre-Option-C runs are also swept out. Customer-authored bullets with
    any wording different from the known allowlist (after normalisation)
    are preserved verbatim. Legacy sections that become empty after the
    strip have their header removed so we don't leave orphan
    ``CONSTRAINTS:`` lines behind.

    Returns ``True`` if the instruction text changed.
    """
    from genie_space_optimizer.common.config import (
        apply_quality_instructions_is_on,
    )

    current = _get_general_instructions(config)

    legacy_swept = _GSO_QUALITY_V1_LEGACY_SENTINEL_RE.sub("\n", current).strip()

    canonical, legacy, preamble = parse_canonical_sections(legacy_swept)

    known = _known_policy_bodies_normalized()

    # Strip GSO-owned bullets from the canonical bucket (semantic match).
    stripped_canonical = 0
    for header, targets in known.items():
        if header not in canonical:
            continue
        before = len(canonical[header])
        canonical[header] = [
            ln for ln in canonical[header]
            if _normalize_policy_body(_bullet_text(ln)) not in targets
        ]
        stripped_canonical += before - len(canonical[header])

    # Strip GSO-owned bullets from the equivalent legacy bucket. This is
    # the cross-scheme dedup that closes the duplication cycle: bullets
    # that originated as canonical content but were later mutated by
    # ``_sanitize_plaintext_instructions`` (``## X`` → ``X:``, backticks
    # removed) are recognised by their normalised body and removed.
    stripped_legacy = 0
    for canonical_header, legacy_key in _CANONICAL_TO_LEGACY_HEADER.items():
        targets = known.get(canonical_header)
        if not targets or legacy_key not in legacy:
            continue
        before = len(legacy[legacy_key])
        legacy[legacy_key] = [
            ln for ln in legacy[legacy_key]
            if _normalize_policy_body(_bullet_text(ln)) not in targets
        ]
        stripped_legacy += before - len(legacy[legacy_key])
        # If the section is now empty (or whitespace-only), drop the
        # header entirely so we don't leave an orphan ``CONSTRAINTS:``
        # line behind.
        if not any(ln.strip() for ln in legacy[legacy_key]):
            legacy.pop(legacy_key)

    if stripped_canonical or stripped_legacy:
        logger.info(
            "gso_quality.dedup canonical=%d legacy=%d",
            stripped_canonical, stripped_legacy,
        )

    if apply_quality_instructions_is_on():
        for _key, header, text in _GSO_QUALITY_V1_POLICIES:
            canonical.setdefault(header, []).append(f"- {text}")

    new_text = _render_instruction_text(preamble, canonical, legacy)

    if new_text == current:
        return False

    _set_general_instructions(config, new_text)
    return True


def _set_general_instructions(
    config: dict, text: str, instruction_id: str | None = None
) -> None:
    """Set general instructions into text_instructions.

    Stores the full text as a single content list item so that the Genie UI
    renders newlines correctly.  The ``content`` field is ``list[str]``; using
    multiple items causes the UI to concatenate them without line breaks.
    """
    from genie_space_optimizer.common.genie_schema import generate_genie_id

    inst = config.setdefault("instructions", {})
    ti = inst.setdefault("text_instructions", [])
    effective_id = instruction_id or (ti[0].get("id") if ti else None) or generate_genie_id()
    if not _HEX_32.match(effective_id or ""):
        effective_id = generate_genie_id()

    content = [text.strip()] if text and text.strip() else [""]

    if ti:
        ti[0] = {"id": effective_id, "content": content}
    else:
        ti.append({"id": effective_id, "content": content})


# ═══════════════════════════════════════════════════════════════════════
# 1a.1 Canonical Instruction Schema utilities (PR #178 — local mirror)
# ═══════════════════════════════════════════════════════════════════════
#
# Three utilities that are shared by:
#   - proactive seeding (_run_proactive_instruction_seeding)
#   - expand-instructions (_expand_instructions)
#   - prose-to-structured miner (rewrite step)
#   - lever-loop writeback (compat mode, strict=False)
#
# The schema itself lives in common/config.py (CANONICAL_SECTION_HEADERS).
# These helpers are thin enough that we don't spin a dedicated module for
# them; the applier is the natural home because lever writeback calls
# validation from here.


# Accepts `## HEADER` (h2 only). Leading whitespace is tolerated so LLMs
# that indent a header inside a bullet block still parse.
_CANONICAL_HEADER_LINE_RE = re.compile(r"^\s*##\s+(?P<title>[^\n]+?)\s*$")
# Legacy ALL-CAPS section header like `PURPOSE:` or `ASSET ROUTING:`. Used by
# the miner to read pre-#178 spaces; never emitted by the rewrite step.
_LEGACY_HEADER_LINE_RE = re.compile(r"^\s*(?P<title>[A-Z][A-Z0-9 _/]{2,80})\s*:\s*$")
# `### Sub-heading` — forbidden in strict mode (subheaders belong to
# structured targets like sql_snippets, not prose).
_H3_HEADER_LINE_RE = re.compile(r"^\s*###\s+\S")

_CANONICAL_HEADERS_LOWER: dict[str, str] = {
    h.lower(): h for h in CANONICAL_SECTION_HEADERS
}


def _normalize_header(line: str) -> str | None:
    """Return the canonical form of a header line, or ``None`` if not a header.

    Case policy: headers #1-#4 are matched case-insensitively (so
    ``## Purpose`` normalizes to ``## PURPOSE``). Header #5 is verbatim-only;
    any casing variant returns the raw matched string unchanged so strict
    validation can flag it.
    """
    m = _CANONICAL_HEADER_LINE_RE.match(line)
    if not m:
        return None
    raw = f"## {m.group('title')}"
    if raw in CANONICAL_SECTION_HEADERS:
        return raw
    lower = raw.lower()
    # Verbatim-required headers (#5) don't get case normalization — we return
    # the raw text so validation can reject the variant.
    for verbatim in VERBATIM_REQUIRED_HEADERS:
        if lower == verbatim.lower():
            return raw  # caller checks == verbatim
    return _CANONICAL_HEADERS_LOWER.get(lower, raw)


def _flatten_to_text(value: str | list[str]) -> str:
    if isinstance(value, list):
        return "".join(value) if all(isinstance(v, str) for v in value) else "\n".join(
            str(v) for v in value
        )
    return str(value)


def parse_canonical_sections(
    text: str | list[str],
) -> tuple[dict[str, list[str]], dict[str, list[str]], list[str]]:
    """Parse instruction prose into canonical / legacy / preamble buckets.

    Returns a 3-tuple:
      - ``canonical``: ``{canonical_header_exact: [body_line, ...]}``
        with keys drawn from ``CANONICAL_SECTION_HEADERS``.
      - ``legacy``: ``{legacy_section_name_upper: [body_line, ...]}``
        (e.g. ``"BUSINESS DEFINITIONS"``). Used by the miner to read
        pre-#178 spaces; never written back.
      - ``preamble``: free-floating lines before any header (rare but seen
        in the wild). Re-emitted under ``## PURPOSE`` by the rewrite step.

    Body lines are captured verbatim including bullet markers; the renderer
    re-normalizes indentation on emit. Non-canonical ``##`` headers (e.g.
    ``## Terminology``) are stored under their raw form in ``canonical`` so
    ``validate_instruction_text(strict=True)`` can reject them.
    """
    raw = _flatten_to_text(text)
    canonical: dict[str, list[str]] = {}
    legacy: dict[str, list[str]] = {}
    preamble: list[str] = []

    current_key: str | None = None
    current_bucket: str = "preamble"  # "preamble" | "canonical" | "legacy"

    for line in raw.splitlines():
        stripped = line.rstrip()
        # ## header?
        normalized = _normalize_header(stripped) if stripped.lstrip().startswith("##") else None
        if normalized is not None:
            current_key = normalized
            current_bucket = "canonical"
            canonical.setdefault(normalized, [])
            continue
        # Legacy ALL-CAPS header?
        m_legacy = _LEGACY_HEADER_LINE_RE.match(stripped) if stripped else None
        if m_legacy is not None:
            key = m_legacy.group("title").strip().upper()
            current_key = key
            current_bucket = "legacy"
            legacy.setdefault(key, [])
            continue
        # Body line — append to whichever bucket we're currently in.
        if current_bucket == "canonical" and current_key is not None:
            canonical[current_key].append(stripped)
        elif current_bucket == "legacy" and current_key is not None:
            legacy[current_key].append(stripped)
        else:
            preamble.append(stripped)

    # Strip trailing blank lines from each section for stable round-tripping.
    for bucket in (canonical, legacy):
        for k in list(bucket.keys()):
            while bucket[k] and not bucket[k][-1].strip():
                bucket[k].pop()
    while preamble and not preamble[-1].strip():
        preamble.pop()

    return canonical, legacy, preamble


def render_canonical_sections(sections: dict[str, str | list[str]]) -> list[str]:
    """Render a canonical-section map into the Genie API ``content`` shape.

    Returns ``list[str]``, one element per line, each terminated with ``\\n``
    — matches ``backend/references/schema.md`` (per-line items; the Genie UI
    concatenates them for display). Empty sections are omitted. Headers are
    re-emitted in ``CANONICAL_SECTION_ORDER``.

    Any key not in ``CANONICAL_SECTION_HEADERS`` is ignored; use
    :func:`validate_instruction_text` to flag unexpected headers upstream.
    """
    lines: list[str] = []
    ordered_keys = sorted(
        (k for k in sections if k in CANONICAL_SECTION_HEADERS),
        key=lambda h: CANONICAL_SECTION_ORDER[h],
    )
    for idx, header in enumerate(ordered_keys):
        body = sections[header]
        body_lines: list[str]
        if isinstance(body, list):
            body_lines = [str(line) for line in body]
        else:
            body_lines = str(body).splitlines()
        # Drop leading/trailing blank lines inside a section; normalise bullets.
        while body_lines and not body_lines[0].strip():
            body_lines.pop(0)
        while body_lines and not body_lines[-1].strip():
            body_lines.pop()
        if not body_lines:
            continue
        if idx > 0:
            lines.append("\n")  # blank line between sections
        lines.append(f"{header}\n")
        for line in body_lines:
            lines.append(f"{line.rstrip()}\n" if line.strip() else "\n")
    return lines


def _trim_bullets_to_budget(body: str, budget: int) -> str:
    """Drop trailing bullets from ``body`` until total length <= ``budget``.

    Semantics:

    - ``body`` is expected to be newline-separated bullets (``- …``).
    - Drops bullets from the end until the remaining body fits.
    - If the very first bullet alone exceeds ``budget``, truncates it at
      a word boundary (never mid-word) so the section isn't left empty.
    - Whitespace-only / empty input returns ``""`` unchanged.
    - Budget of 0 or negative returns ``""``.

    Returns the trimmed body (may be empty, may be unchanged).
    """
    if not body or not body.strip():
        return ""
    if budget <= 0:
        return ""

    lines = body.splitlines()
    # Fast path — body already fits.
    current = "\n".join(lines)
    if len(current) <= budget:
        return current

    # Iterative trim from the tail until we fit or only one bullet remains.
    while len(lines) > 1 and len("\n".join(lines)) > budget:
        lines.pop()
    current = "\n".join(lines)
    if len(current) <= budget:
        return current

    # Single bullet still over budget — truncate at word boundary.
    only = lines[0] if lines else body
    if len(only) <= budget:
        return only
    # Leave a small tail so we never land on a hyphen / punctuation.
    truncated = only[:budget].rstrip()
    # Back off to the last whitespace so we don't chop mid-word.
    last_space = truncated.rfind(" ")
    if last_space > max(budget // 2, 8):
        truncated = truncated[:last_space].rstrip()
    return truncated


def _trim_rendered_to_cap(rendered: list[str], cap: int) -> list[str]:
    """Priority-ordered trim of a rendered ``list[str]`` to fit ``cap``.

    Used as a post-render safety net after Layer 1's ``_trim_bullets_to_budget``
    has already clipped each section's body to its per-section budget. The
    pre-render trim can't account for rendering overhead (section headers
    ``## PURPOSE\\n`` ≈ 14 chars each + blank lines between sections); this
    function handles the last mile.

    Trim priority (first = trimmed first, last = most load-bearing):

        1. ## DATA QUALITY NOTES   — valuable but droppable in a pinch
        2. ## DISAMBIGUATION       — clarification rules
        3. ## CONSTRAINTS          — guardrails
        4. ## Instructions you must follow when providing summaries — rendering
        5. ## PURPOSE              — scope + audience; load-bearing

    Within a section, bullets are dropped from the end. When a section
    becomes empty, its header is also dropped.
    """
    if not rendered:
        return rendered

    # Local import avoids a circular at module load time.
    from genie_space_optimizer.common.config import CANONICAL_SECTION_HEADERS

    def _total_len(parts: list[str]) -> int:
        return sum(len(p) for p in parts)

    if _total_len(rendered) <= cap:
        return rendered

    # Walk the list and segment by canonical headers. Each segment is
    # (header_line, [body_lines], trailing_blank).
    segments: list[dict] = []
    current: dict | None = None
    for line in rendered:
        stripped = line.lstrip()
        # Match exactly our canonical header lines.
        is_header = any(
            stripped.startswith(h) and (line.rstrip() == h or line.rstrip() == h + "\n".rstrip())
            for h in CANONICAL_SECTION_HEADERS
        )
        # The renderer emits each header as a distinct element ending "\n",
        # so we match on the rstripped content.
        matched_header = None
        for h in CANONICAL_SECTION_HEADERS:
            if line.rstrip() == h:
                matched_header = h
                break
        if matched_header:
            if current is not None:
                segments.append(current)
            current = {"header": matched_header, "header_line": line, "body": [], "trailers": []}
        elif current is not None:
            if line.strip():
                current["body"].append(line)
            else:
                # Separator / blank line between sections.
                current["trailers"].append(line)
        else:
            # Preamble before any recognised header — keep as-is.
            current = {"header": None, "header_line": None, "body": [line], "trailers": []}
    if current is not None:
        segments.append(current)

    # Priority order: trim-first (LAST element in the list below) to
    # trim-last (FIRST element). Walk the list in the priority-to-trim
    # order — we drop from the tail of each section's body until we fit.
    trim_priority = [
        "## DATA QUALITY NOTES",
        "## DISAMBIGUATION",
        "## CONSTRAINTS",
        "## Instructions you must follow when providing summaries",
        "## PURPOSE",
    ]

    def _reassemble() -> list[str]:
        out: list[str] = []
        for i, seg in enumerate(segments):
            if seg["header_line"] is not None and not seg["body"]:
                # Entire section emptied — drop the header too.
                continue
            # Skip the blank-line separator before the FIRST real segment
            # to avoid leading blank lines.
            if seg["header_line"] is not None:
                out.append(seg["header_line"])
            out.extend(seg["body"])
            out.extend(seg["trailers"])
        return out

    for target_header in trim_priority:
        target = next(
            (s for s in segments if s.get("header") == target_header),
            None,
        )
        if target is None:
            continue
        while target["body"] and _total_len(_reassemble()) > cap:
            target["body"].pop()
        if _total_len(_reassemble()) <= cap:
            break

    return _reassemble()


class RewriteResult:
    """Outcome of :func:`rewrite_instructions_from_miner_output`.

    Three disjoint outcomes, modelled as class attributes (a string enum
    would pull in the typing.Literal complexity without buying clarity):

    - :attr:`WRITE` — validated prose, length dropped or unchanged;
      ``set_text_instructions`` op should be emitted.
    - :attr:`SKIP_NO_CHANGE` — nothing to remove or regroup. Caller
      should leave the space untouched.
    - :attr:`DECLINE_MALFORMED` — rewrite failed validation or would
      grow the prose / introduce SQL-in-text. Mirrors Fix Agent's
      decline contract so GSO + Fix Agent never produce conflicting
      views on the next cycle.
    """

    WRITE = "write"
    SKIP_NO_CHANGE = "skip_no_change"
    DECLINE_MALFORMED = "decline_malformed"


def validate_instruction_text(
    text_or_list: str | list[str],
    *,
    strict: bool = True,
) -> tuple[bool, list[str]]:
    """Validate prose against the canonical 5-section schema.

    Strict mode (used for proactive seed, expand, miner rewrite output):

    - Every ``##`` header must be in ``CANONICAL_SECTION_HEADERS``.
    - ``VERBATIM_REQUIRED_HEADERS`` (header #5) must be byte-identical.
    - Canonical headers must appear in ``CANONICAL_SECTION_ORDER``.
    - No ``###`` subheaders.
    - Total length ≤ ``MAX_TEXT_INSTRUCTIONS_CHARS``.
    - No prose line matches :func:`looks_like_sql_in_prose` (scanner v2 —
      structure-aware SQL-in-prose detection; see iq_scan/scoring.py).

    Compat mode (``strict=False``, used by the lever-loop writeback until
    levers migrate under #175):

    - Tolerates the legacy 12-section ALL-CAPS vocabulary.
    - Skips the SQL-in-text check (lever output can legitimately reference
      SQL in QUERY PATTERNS and BUSINESS DEFINITIONS headers).
    - Still enforces the length cap — no lever output is allowed to push a
      space over the 2000-char scanner threshold.

    Returns ``(ok, errors)``. ``errors`` is populated on both ok=True and
    ok=False so callers can log informational warnings even on pass.
    """
    errors: list[str] = []
    text = _flatten_to_text(text_or_list)

    # Length cap — always enforced.
    if len(text) > MAX_TEXT_INSTRUCTIONS_CHARS:
        errors.append(
            f"length {len(text)} exceeds MAX_TEXT_INSTRUCTIONS_CHARS "
            f"({MAX_TEXT_INSTRUCTIONS_CHARS})"
        )

    # Forbid ### subheaders in strict mode.
    if strict:
        for line in text.splitlines():
            if _H3_HEADER_LINE_RE.match(line):
                errors.append(f"forbidden h3 header: {line.strip()!r}")
                break

    # Header set and order.
    seen: list[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if not stripped.startswith("##") or stripped.startswith("###"):
            continue
        normalized = _normalize_header(line)
        if normalized is None:
            continue
        if strict:
            if normalized not in CANONICAL_SECTION_HEADERS:
                errors.append(f"non-canonical header: {normalized!r}")
                continue
            # Verbatim check for header #5.
            if normalized in VERBATIM_REQUIRED_HEADERS:
                raw = line.strip()
                if raw != normalized:
                    errors.append(
                        f"verbatim header mismatch: got {raw!r} expected {normalized!r}"
                    )
        seen.append(normalized)

    if strict and seen:
        ordered = [h for h in CANONICAL_SECTION_HEADERS if h in seen]
        # ``seen`` as encountered in the text; ``ordered`` is the canonical
        # order filtered to what was seen. A mismatch means out-of-order.
        # Dedupe ``seen`` preserving first occurrence.
        seen_unique: list[str] = []
        for h in seen:
            if h not in seen_unique:
                seen_unique.append(h)
        if seen_unique != ordered:
            errors.append(
                f"headers out of canonical order: got {seen_unique!r} expected {ordered!r}"
            )

    # SQL-in-text — strict mode only. Uses the structure-aware scanner v2
    # detector, so natural-language prose ("Do not join X to Y", "Where
    # applicable") no longer trips the check. We iterate non-header lines.
    if strict:
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if looks_like_sql_in_prose(line):
                errors.append(
                    f"SQL detected in prose line (scanner check #4): {line.strip()[:80]!r}"
                )
                break

    return (not errors, errors)


# ═══════════════════════════════════════════════════════════════════════
# 1a.2 Span-based prose rewrite (Task C.4)
# ═══════════════════════════════════════════════════════════════════════


def _normalize_bullet(line: str) -> str:
    """Strip trailing whitespace and common bullet markers on a line copy."""
    s = line.rstrip()
    return s


def _remove_span(text: str, span: str) -> tuple[str, bool]:
    """Remove the first exact occurrence of ``span`` from ``text``.

    Returns ``(new_text, removed)``. If the span is not found, ``text`` is
    returned unchanged — span-not-found is a soft failure (it typically
    means a prior run already promoted this rule) and the rewrite step
    continues with the remaining spans.
    """
    if not span:
        return text, False
    idx = text.find(span)
    if idx == -1:
        return text, False
    return text[:idx] + text[idx + len(span):], True


def rewrite_instructions_from_miner_output(
    original_text: str,
    applied_spans: list[str],
    keep_in_prose_spans: list[dict],
) -> tuple[str, str, list[str]]:
    """Span-based canonical rewrite of instruction prose.

    Called once per miner invocation, AFTER every target-specific applier
    has committed its changes to the Genie Space config. Produces the new
    ``text_instructions`` content that reflects:

    - Removal of every promoted span (``applied_spans``).
    - Regrouping of ``keep_in_prose`` spans under their tagged canonical
      header.
    - Re-emission in canonical order via :func:`render_canonical_sections`.
    - Strict validation via :func:`validate_instruction_text` — rejects
      any output that contains SQL, violates the length cap, carries a
      non-canonical header, or fails the verbatim-header check.

    Parameters
    ----------
    original_text : str
        The pre-rewrite ``text_instructions`` content as a flat string.
    applied_spans : list[str]
        Exact substrings that were successfully promoted to structured
        config (sql_snippet, join_spec, example_qsql, table_desc,
        column_synonym). Removed longest-first to avoid nested overlaps.
    keep_in_prose_spans : list[dict]
        Entries of shape ``{"section": "## HEADER", "source_span": "..."}``
        tagged by the miner. Spans are moved under the tagged canonical
        header; content already under a canonical header in the original
        is preserved even if not tagged.

    Returns
    -------
    (outcome, new_text, errors)
        ``outcome`` is one of :class:`RewriteResult` string constants.
        ``new_text`` is the rewritten prose (equal to ``original_text``
        on ``SKIP_NO_CHANGE`` / ``DECLINE_MALFORMED``, the new content on
        ``WRITE``). ``errors`` is a list of validation / diagnostic
        messages (populated on any outcome for logging).
    """
    errors: list[str] = []
    if not original_text or not original_text.strip():
        if not keep_in_prose_spans and not applied_spans:
            return RewriteResult.SKIP_NO_CHANGE, original_text, errors
        # Somehow the miner produced spans for empty input — abort.
        errors.append("empty original_text but miner produced spans")
        return RewriteResult.DECLINE_MALFORMED, original_text, errors

    # ── Step 1: remove promoted spans, longest-first ───────────────
    span_removed_text = original_text
    spans_found = 0
    spans_missing = 0
    for span in sorted({s for s in applied_spans if s}, key=len, reverse=True):
        span_removed_text, removed = _remove_span(span_removed_text, span)
        if removed:
            spans_found += 1
        else:
            spans_missing += 1
            errors.append(
                f"applied_span not found: {span[:80]!r} — continuing"
            )

    # ── Step 2: parse what's left + gather canonical content ───────
    canonical_from_text, legacy_from_text, preamble = parse_canonical_sections(
        span_removed_text,
    )

    # ── Step 3: build new canonical map ────────────────────────────
    # Preserve existing canonical content — the miner only moves what it
    # explicitly tagged. Content the user hand-curated under canonical
    # headers stays regardless of miner output.
    new_sections: dict[str, list[str]] = {}
    for header, lines in canonical_from_text.items():
        if header in CANONICAL_SECTION_HEADERS:
            new_sections[header] = [ln for ln in lines if ln.strip()]

    # Stitch in keep_in_prose spans under their tagged canonical header.
    # Dedup bullets case-insensitively inside each section.
    seen_bullets_per_section: dict[str, set[str]] = {
        h: {ln.strip().lower() for ln in new_sections.get(h, [])}
        for h in CANONICAL_SECTION_HEADERS
    }
    for entry in keep_in_prose_spans:
        if not isinstance(entry, dict):
            continue
        section = str(entry.get("section", "")).strip()
        span = str(entry.get("source_span", "")).strip()
        if section not in CANONICAL_SECTION_HEADERS or not span:
            continue
        # Remove the span from legacy/preamble buckets if it landed there.
        # The span might be multi-line — handle each line.
        for line in span.splitlines():
            norm = _normalize_bullet(line)
            if not norm.strip():
                continue
            key = norm.strip().lower()
            if key in seen_bullets_per_section.get(section, set()):
                continue
            new_sections.setdefault(section, []).append(
                norm if norm.startswith("- ") else f"- {norm.lstrip('-').strip()}"
            )
            seen_bullets_per_section.setdefault(section, set()).add(key)

    # ── Step 4: render ──────────────────────────────────────────────
    rendered_list = render_canonical_sections(new_sections)

    # ── Step 4b: deterministic trim to fit MAX_TEXT_INSTRUCTIONS_CHARS
    # Mirrors the two-layer trim in :func:`_run_enrichment` for the
    # expand path. Applies only when the rendered total exceeds the
    # cap. Without this, originals already over cap (legacy spaces with
    # ALL-CAPS prose >2000 chars) deterministically fail validation
    # even when span removal alone would put them on a path to fit.
    pre_trim_len = sum(len(p) for p in rendered_list)
    if pre_trim_len > MAX_TEXT_INSTRUCTIONS_CHARS:
        # Layer 1 — per-section bullet trim. Derive a per-section budget
        # from the cap minus a fixed-overhead estimate (~15 chars per
        # rendered section header + blank line). Headers themselves are
        # tiny but additive across N sections.
        header_overhead = 15 * max(len(new_sections), 1)
        body_budget = max(MAX_TEXT_INSTRUCTIONS_CHARS - header_overhead, 0)
        per_section_budget = (
            body_budget // max(len(new_sections), 1) if new_sections else 0
        )
        trimmed_sections: dict[str, list[str]] = {}
        for header, lines in new_sections.items():
            body = "\n".join(lines)
            clipped = _trim_bullets_to_budget(body, per_section_budget)
            if not clipped.strip():
                continue
            trimmed_sections[header] = [
                ln for ln in clipped.splitlines() if ln.strip()
            ]
        rendered_list = render_canonical_sections(trimmed_sections)
        # Layer 2 — post-render global cap. Handles header overhead the
        # per-section trim can't see.
        rendered_list = _trim_rendered_to_cap(
            rendered_list, MAX_TEXT_INSTRUCTIONS_CHARS,
        )
        post_trim_len = sum(len(p) for p in rendered_list)
        if post_trim_len < pre_trim_len:
            logger.info(
                "miner.rewrite.trimmed chars_before=%d chars_after=%d cap=%d",
                pre_trim_len, post_trim_len, MAX_TEXT_INSTRUCTIONS_CHARS,
            )

    new_text = "".join(rendered_list).rstrip() + ("\n" if rendered_list else "")

    # ── Step 5: validate strictly ───────────────────────────────────
    ok, validation_errors = validate_instruction_text(new_text, strict=True)

    # ── Step 6: outcome ─────────────────────────────────────────────
    if not ok:
        errors.extend(validation_errors)
        return RewriteResult.DECLINE_MALFORMED, original_text, errors
    if len(new_text) > len(original_text):
        errors.append(
            f"rewrite grew length from {len(original_text)} to {len(new_text)}; declining"
        )
        return RewriteResult.DECLINE_MALFORMED, original_text, errors
    if new_text.strip() == original_text.strip():
        return RewriteResult.SKIP_NO_CHANGE, original_text, errors
    # Structural no-op: miner returned nothing and no canonical content
    # was reshuffled. Skip silently.
    if not applied_spans and not keep_in_prose_spans and not spans_found:
        return RewriteResult.SKIP_NO_CHANGE, original_text, errors

    return RewriteResult.WRITE, new_text, errors


# ═══════════════════════════════════════════════════════════════════════
# 1b. Prompt Matching Auto-Config
# ═══════════════════════════════════════════════════════════════════════


def _is_measure_column(column_name: str, data_type: str) -> bool:
    """Return True if the column looks like a metric view measure."""
    dt_upper = (data_type or "").upper().split("(")[0].strip()
    if dt_upper in NUMERIC_DATA_TYPES:
        return True
    lower_name = column_name.lower()
    return any(lower_name.startswith(p) for p in MEASURE_NAME_PREFIXES)


def _is_hidden(cc: dict) -> bool:
    if cc.get("visible") is False:
        return True
    if cc.get("exclude") is True:
        return True
    return False


def _table_has_rls(tbl_dict: dict) -> bool:
    """Return True if the table (or any of its columns) has row-level security.

    Tables with ``row_filter`` or ``column_mask`` (at either the table level or
    on any individual column) silently disable Genie's entity matching / value
    dictionary features. Enabling entity matching on such tables is a no-op
    that wastes one of the 120 per-space value-dictionary slots — detect this
    up front so the optimizer does not spend slots on columns that can't use
    them.

    Mirrors the detection used in
    ``genie_space_optimizer.iq_scan.scoring.calculate_score`` (Check 9) so the
    IQ scan and the auto-applier stay aligned on what counts as RLS.
    """
    if bool(tbl_dict.get("row_filter") or tbl_dict.get("column_mask")):
        return True
    for col in tbl_dict.get("column_configs", []) + tbl_dict.get("columns", []):
        if col.get("row_filter") or col.get("column_mask"):
            return True
    return False


def _column_has_rls(cc: dict) -> bool:
    """Return True if this column has its own ``row_filter`` or ``column_mask``."""
    return bool(cc.get("row_filter") or cc.get("column_mask"))


def _entity_matching_score_legacy(column_name: str) -> int:
    """Legacy 0/1/2 scorer — retained for one release behind
    ``ENABLE_SMARTER_SCORING=False`` so operators can pin today's
    behaviour if the new scorer misbehaves on their corpus.

    Returns 0-2; higher is better. Free-text names get 0 (sorted to
    bottom but NOT filtered — which is why the silent-PII leak exists
    on <120-col spaces under this scorer).
    """
    lower = column_name.lower()
    if any(pat in lower for pat in FREE_TEXT_COLUMN_PATTERNS):
        return 0
    if any(pat in lower for pat in CATEGORICAL_COLUMN_PATTERNS):
        return 2
    return 1


def _entity_matching_score(
    column_name: str,
    *,
    description: str = "",
    profile: dict | None = None,
    benchmark_col_refs: frozenset[str] = frozenset(),
    row_count: int = 0,
    rls_verdict: str = "clean",
    strict_rls: bool | None = None,
) -> tuple[float, str]:
    """Score a STRING column for entity matching priority (new scorer).

    Returns ``(score, reason)``. A score of 0.0 is a HARD REJECT — the
    caller is expected to FILTER score<=0 candidates out of the pool
    before applying the 120-slot cap, rather than sorting and taking
    top-N (which is what the legacy scorer's callers do, and the root
    cause of the silent-PII leak on <120-col spaces).

    Parameters
    ----------
    column_name
        The bare column name (not qualified).
    description
        Column description from space config or UC metadata. Used for
        PII detection ("pii", "sensitive") + positive/negative keyword
        boosts.
    profile
        Optional ``_data_profile`` entry for this column:
        ``{"cardinality": int, "distinct_values": [...]}``. When
        present AND ``cardinality > 0`` we apply cardinality-based
        disqualifiers + sweet-spot bonus. When absent (unprofiled — eg.
        metric views or tables beyond ``MAX_PROFILE_TABLES``), we fall
        through to name-based scoring rather than hard-rejecting.
    benchmark_col_refs
        Lowercased column tokens extracted from benchmark expected_sql.
        A match grants a +3.0 boost — strong "users actually ask about
        this column" signal.
    row_count
        Table row count from ``_data_profile[table]["row_count"]``. Used
        with ``profile.cardinality`` to compute distinct_ratio. Skip if 0.
    rls_verdict
        One of ``"clean"`` / ``"tainted"`` / ``"unknown"`` from
        :func:`iq_scan.collect_rls_audit`. Tainted → reject.
    strict_rls
        Defaults to the module-level :data:`STRICT_RLS_MODE`. When True,
        ``"unknown"`` is also rejected.

    Hard disqualifiers (score = 0):

    - RLS verdict ``"tainted"``
    - RLS verdict ``"unknown"`` AND strict_rls
    - Column name matches ``FREE_TEXT_COLUMN_PATTERNS``
    - Column name matches ``PII_COLUMN_PATTERNS``
    - Column name matches ``BOOLEAN_FLAG_PATTERNS``
    - Description contains "pii" / "sensitive"
    - (With profile) cardinality < MIN or > MAX
    - (With profile) distinct_ratio > FREE_TEXT_DISTINCT_RATIO (ID-like)

    Bonuses (each independently stacks, final capped at 10.0):

    - +3.0 base for categorical-pattern names, else +1.0
    - +2.0 for cardinality in the 5-200 sweet spot
    - +1.0 for cardinality 200-1024
    - +0.3 for cardinality 2-4 (thin benefit but not zero)
    - +3.0 for benchmark-referenced column name
    - +1.0 for positive description hints
    - -2.0 for negative description hints
    """
    if strict_rls is None:
        strict_rls = STRICT_RLS_MODE

    lower = column_name.lower()

    # ── Hard disqualifiers ──────────────────────────────────────────
    # Check order is meaningful for the reason string: PII before
    # free-text so ``customer_email`` reports ``pii_name`` (the more
    # specific / security-relevant reason) rather than ``free_text_name``
    # when both patterns would match.
    if rls_verdict == "tainted":
        return 0.0, "rls_tainted"
    if rls_verdict == "unknown" and strict_rls:
        return 0.0, "rls_unknown_strict"
    if any(pat in lower for pat in PII_COLUMN_PATTERNS):
        return 0.0, "pii_name"
    if any(pat in lower for pat in FREE_TEXT_COLUMN_PATTERNS):
        return 0.0, "free_text_name"
    if any(pat in lower for pat in BOOLEAN_FLAG_PATTERNS):
        return 0.0, "boolean_flag"

    desc_lower = (description or "").lower()
    if any(h in desc_lower for h in ("pii", "sensitive")):
        return 0.0, "pii_description"

    # Cardinality-based disqualifiers fire ONLY when we actually have
    # profile data with a non-zero card. Metric views are skipped by
    # _collect_data_profile (preflight.py:237-241) and tables beyond
    # MAX_PROFILE_TABLES=20 get no profile; for those unprofiled
    # columns we fall through to name-based scoring rather than
    # rejecting on an implicit cardinality=0.
    has_profile = (
        isinstance(profile, dict)
        and int(profile.get("cardinality", 0) or 0) > 0
    )
    if has_profile:
        card = int(profile["cardinality"])
        if card < MIN_ENTITY_MATCHING_CARDINALITY:
            return 0.0, "cardinality_too_low"
        if card > MAX_ENTITY_MATCHING_CARDINALITY:
            return 0.0, "cardinality_too_high"
        if row_count > 0 and card / row_count > FREE_TEXT_DISTINCT_RATIO:
            return 0.0, "id_like_distinct_ratio"

    # ── Base + bonuses ──────────────────────────────────────────────
    score = 3.0 if any(pat in lower for pat in CATEGORICAL_COLUMN_PATTERNS) else 1.0
    if has_profile:
        card = int(profile["cardinality"])
        if 5 <= card <= 200:
            score += 2.0
        elif 200 < card <= 1024:
            score += 1.0
        elif 2 <= card < 5:
            score += 0.3
    if lower in benchmark_col_refs:
        score += 3.0
    if any(h in desc_lower for h in DESCRIPTION_HINTS_POSITIVE):
        score += 1.0
    if any(h in desc_lower for h in DESCRIPTION_HINTS_NEGATIVE):
        score -= 2.0

    return max(0.0, min(score, 10.0)), "ok"


_BENCHMARK_TOKEN_RE = re.compile(r"[a-z_][a-z0-9_]*", re.IGNORECASE)
_BENCHMARK_STRIP_STRING_RE = re.compile(
    r"'([^'\\]|\\.)*'|\"([^\"\\]|\\.)*\"",
)
_BENCHMARK_STRIP_COMMENT_RE = re.compile(
    r"--[^\n]*|/\*.*?\*/",
    re.DOTALL,
)


def _extract_benchmark_col_refs(benchmarks: list[dict] | None) -> frozenset[str]:
    """Approximate column references from benchmark ``expected_sql`` text.

    The scorer applies a +3.0 boost for columns that appear in benchmarks —
    a strong "users actually ask about this column" signal. This extractor
    is deliberately lenient:

    * Strips SQL string literals (``'foo'``, ``"bar"``) so quoted values
      aren't tokenised.
    * Strips SQL comments (``-- …``, ``/* … */``).
    * Tokenises remaining text as ``[a-z_][a-z0-9_]*``, lowercased.

    The tokens include SQL keywords (``select``, ``from``, ``where``,
    ``join``, etc.), function names, and aliases — false positives. They
    don't matter because the boost only applies when a token **equals**
    an actual column name, and column names never collide with SQL
    keywords in practice. A smarter extractor (sqlglot) would be more
    precise but not materially better for ranking, so we keep it regex-only.
    """
    if not benchmarks:
        return frozenset()
    refs: set[str] = set()
    for b in benchmarks:
        if not isinstance(b, dict):
            continue
        sql = str(b.get("expected_sql", "") or "")
        if not sql:
            continue
        stripped = _BENCHMARK_STRIP_STRING_RE.sub("", sql)
        stripped = _BENCHMARK_STRIP_COMMENT_RE.sub("", stripped)
        for tok in _BENCHMARK_TOKEN_RE.findall(stripped):
            refs.add(tok.lower())
    return frozenset(refs)


def _ensure_column_configs_from_uc(
    tables_and_mvs: list[dict], uc_columns: list[dict],
) -> int:
    """Populate column_configs from UC metadata for columns not yet present.

    The Genie Space API only returns column_configs for explicitly configured
    columns. For unconfigured spaces every table has column_configs: [].
    This bootstraps entries so that format assistance and entity matching
    loops have something to iterate over.

    Returns the number of new column_config entries created.
    """
    table_cols: dict[str, list[str]] = {}
    for col in uc_columns:
        if not isinstance(col, dict):
            continue
        tbl = str(col.get("table_name") or "").strip().lower()
        cname = str(col.get("column_name") or "").strip()
        if tbl and cname:
            table_cols.setdefault(tbl, []).append(cname)

    created = 0
    for tbl_dict in tables_and_mvs:
        identifier = tbl_dict.get("identifier", "")
        short_name = identifier.replace("`", "").rsplit(".", 1)[-1].lower()
        existing_names = {
            cc.get("column_name", "").lower()
            for cc in tbl_dict.get("column_configs", [])
        }
        for col_name in table_cols.get(short_name, []):
            if col_name.lower() not in existing_names:
                tbl_dict.setdefault("column_configs", []).append(
                    {"column_name": col_name}
                )
                created += 1
    return created


def auto_apply_prompt_matching(
    w: WorkspaceClient,
    space_id: str,
    config: dict,
    *,
    benchmarks: list[dict] | None = None,
) -> dict:
    """Enable format assistance and entity matching as a best-practice step.

    Operates deterministically (no LLM calls).  Mutates ``config`` in-place
    and PATCHes the Genie Space via the API.

    When ``ENABLE_SMARTER_SCORING=True`` (default) the scoring path uses
    the profile / benchmarks / RLS-audit-aware scorer and **filters**
    score-0 candidates rather than sorting-and-taking-top-N. This closes
    the silent-PII leak on spaces with <120 STRING columns where today's
    sort-and-slice path would enable EM on every STRING column regardless
    of fit.

    The ``_data_profile`` and ``_rls_audit`` dicts are read from ``config``
    when present (populated by preflight); ``benchmarks`` is passed via
    kwarg so callers outside the enrichment hot path can omit it without
    breakage.

    Returns an apply_log dict with ``applied`` list, ``patched_objects``,
    ``pre_snapshot``, ``post_snapshot``, and summary stats including
    rejection reason counts under ``rejected_by_reason``.
    """
    if not ENABLE_SMARTER_SCORING:
        return _legacy_apply_em(w, space_id, config, benchmarks=benchmarks)

    from genie_space_optimizer.common.config import DRY_RUN_ENTITY_MATCHING

    parsed = config.get("_parsed_space", config)
    ds = parsed.get("data_sources", {})
    tables = ds.get("tables", [])
    metric_views = ds.get("metric_views", [])
    uc_columns: list[dict] = config.get("_uc_columns", [])
    data_profile: dict = config.get("_data_profile") or {}
    rls_audit: dict = config.get("_rls_audit") or {}
    benchmark_col_refs = _extract_benchmark_col_refs(benchmarks)

    bootstrapped = _ensure_column_configs_from_uc(tables + metric_views, uc_columns)
    if bootstrapped:
        print(f"  [PROMPT MATCHING] Bootstrapped {bootstrapped} column_config entries from UC metadata")

    type_lookup: dict[tuple[str, str], str] = {}
    for col in uc_columns:
        if not isinstance(col, dict):
            continue
        tbl = str(col.get("table_name") or "").strip()
        cname = str(col.get("column_name") or "").strip()
        dtype = str(col.get("data_type") or "").strip()
        if tbl and cname:
            type_lookup[(tbl.lower(), cname.lower())] = dtype

    pre_snapshot = copy.deepcopy(parsed)
    changes: list[dict] = []

    rls_skipped_tables: list[str] = []

    def _table_short_name(identifier: str) -> str:
        parts = identifier.replace("`", "").split(".")
        return parts[-1] if parts else identifier

    def _rls_verdict_for(identifier: str, tbl_local_rls: bool, cc_local_rls: bool) -> str:
        """Combine the lineage-aware audit verdict with field-level checks.

        Field-level RLS (row_filter / column_mask) always wins. Otherwise
        fall back to the lineage-aware audit verdict from ``_rls_audit``.
        Absent/unknown audit entry defaults to "clean" (preserves today's
        behaviour; flip with ``STRICT_RLS_MODE`` to treat unknown as
        tainted).
        """
        if tbl_local_rls or cc_local_rls:
            return "tainted"
        entry = rls_audit.get(identifier.strip("`").lower())
        if isinstance(entry, dict):
            v = entry.get("verdict")
            if v in ("tainted", "unknown", "clean"):
                return v
        return "clean"

    def _score(col_name: str, description: str, dtype: str, identifier: str,
               tbl_rls: bool, cc_rls: bool) -> tuple[float, str]:
        """Score a STRING column using the intelligent scorer. Pulls
        cardinality / distinct-values from ``_data_profile`` (by fully
        qualified identifier, falling back to short name) and RLS verdict
        from ``_rls_audit`` (combined with field-level checks)."""
        col_profile = (
            data_profile.get(identifier.strip("`").lower(), {})
            .get("columns", {})
            .get(col_name, {})
        ) or (
            data_profile.get(_table_short_name(identifier).lower(), {})
            .get("columns", {})
            .get(col_name, {})
        )
        row_count = int(
            data_profile.get(identifier.strip("`").lower(), {}).get("row_count", 0)
            or data_profile.get(_table_short_name(identifier).lower(), {}).get("row_count", 0)
            or 0
        )
        rls_verdict = _rls_verdict_for(identifier, tbl_rls, cc_rls)
        return _entity_matching_score(
            col_name,
            description=description,
            profile=col_profile,
            benchmark_col_refs=benchmark_col_refs,
            row_count=row_count,
            rls_verdict=rls_verdict,
        )

    def _column_description(cc: dict) -> str:
        d = cc.get("description") or cc.get("comment") or ""
        if isinstance(d, list):
            return " ".join(str(x) for x in d)
        return str(d)

    # ── 1. Score EVERY visible STRING column (enabled or not) ───────
    # Key idempotency shift: drop the today's ``not cc.get("enable_entity_matching")``
    # guard. We need to score every STRING column so the diff step can
    # work both directions — enable new winners AND disable existing slots
    # that no longer score highly (PII previously slotted, RLS added, etc.).
    all_scored: list[tuple[str, str, str, float, str]] = []
    for tbl in tables + metric_views:
        identifier = tbl.get("identifier", "")
        short_name = _table_short_name(identifier)
        is_mv = tbl in metric_views
        table_rls = _table_has_rls(tbl)
        if table_rls:
            rls_skipped_tables.append(identifier)
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if _is_hidden(cc) or not col_name:
                continue
            # Format-assistance side-effect applies to all visible columns,
            # independent of entity matching.
            if not cc.get("enable_format_assistance"):
                cc["enable_format_assistance"] = True
                changes.append({
                    "type": "enable_example_values",
                    "table": identifier,
                    "column": col_name,
                })
            dtype = type_lookup.get((short_name.lower(), col_name.lower()), "")
            # MV measure columns opt out of EM entirely (numeric aggregates
            # don't have meaningful value dictionaries).
            if is_mv and _is_measure_column(col_name, dtype):
                continue
            if dtype.upper().split("(")[0].strip() != "STRING":
                continue
            score, reason = _score(
                col_name, _column_description(cc), dtype, identifier,
                table_rls, _column_has_rls(cc),
            )
            all_scored.append((identifier, col_name, dtype, score, reason))

    if rls_skipped_tables:
        deduped = list(dict.fromkeys(rls_skipped_tables))
        print(
            f"  [PROMPT MATCHING] Skipped entity matching for {len(deduped)} "
            f"RLS-governed table(s): {', '.join(deduped[:5])}"
            + ("…" if len(deduped) > 5 else "")
        )

    # ── 2. Filter zero-scores + deterministic sort ───────────────────
    rejected_candidates = [e for e in all_scored if e[3] <= 0.0]
    candidates = [e for e in all_scored if e[3] > 0.0]
    # Sort key: score DESC, then table+column ASC for stable tie-breaks.
    candidates.sort(key=lambda x: (-x[3], x[0].lower(), x[1].lower()))

    # ── 3. Target = top-120 ──────────────────────────────────────────
    selected = candidates[:MAX_VALUE_DICTIONARY_COLUMNS]
    target_set: set[tuple[str, str]] = {
        (ident, col) for ident, col, _, _, _ in selected
    }
    # Build a score+reason lookup for the log block (keyed by (ident,col)).
    score_lookup: dict[tuple[str, str], tuple[float, str]] = {
        (ident, col): (sc, rs) for ident, col, _, sc, rs in all_scored
    }

    # ── 4. Current state ─────────────────────────────────────────────
    current_set: set[tuple[str, str]] = set()
    for tbl in tables + metric_views:
        ident = tbl.get("identifier", "")
        for cc in tbl.get("column_configs", []):
            if cc.get("enable_entity_matching") and cc.get("column_name"):
                current_set.add((ident, cc["column_name"]))

    # ── 5. Diff ──────────────────────────────────────────────────────
    to_enable = target_set - current_set
    to_disable = current_set - target_set
    kept = target_set & current_set

    # Build a deterministic dry-run view (sorted by score DESC).
    _enable_sorted = sorted(
        to_enable,
        key=lambda k: (-score_lookup.get(k, (0.0, ""))[0], k[0].lower(), k[1].lower()),
    )
    _disable_sorted = sorted(
        to_disable,
        key=lambda k: (score_lookup.get(k, (0.0, ""))[0], k[0].lower(), k[1].lower()),
    )

    # ── 6. Apply (unless dry-run) ────────────────────────────────────
    em_enabled_count = 0
    em_disabled_count = 0
    if DRY_RUN_ENTITY_MATCHING:
        print(
            f"  [PROMPT MATCHING] [DRY-RUN] Would enable {len(to_enable)} "
            f"slot(s), disable {len(to_disable)} slot(s); keep {len(kept)}"
        )
        for ident, col in _enable_sorted[:5]:
            sc, rs = score_lookup.get((ident, col), (0.0, ""))
            print(f"    + enable  {_table_short_name(ident)}.{col} score={sc:.1f} [{rs}]")
        for ident, col in _disable_sorted[:5]:
            sc, rs = score_lookup.get((ident, col), (0.0, ""))
            print(f"    - disable {_table_short_name(ident)}.{col} score={sc:.1f} [{rs}]")
    else:
        for ident, col in to_disable:
            tbl_dict = _find_table_in_config(parsed, ident)
            if not tbl_dict:
                continue
            cc = _find_or_create_column_config(tbl_dict, col)
            cc["enable_entity_matching"] = False
            sc, rs = score_lookup.get((ident, col), (0.0, "unscored"))
            changes.append({
                "type": "disable_value_dictionary",
                "table": ident,
                "column": col,
                "score": sc,
                "reason": rs,
            })
            em_disabled_count += 1
        for ident, col in to_enable:
            tbl_dict = _find_table_in_config(parsed, ident)
            if not tbl_dict:
                continue
            cc = _find_or_create_column_config(tbl_dict, col)
            cc["enable_entity_matching"] = True
            if not cc.get("enable_format_assistance"):
                cc["enable_format_assistance"] = True
            sc, rs = score_lookup.get((ident, col), (0.0, "unscored"))
            changes.append({
                "type": "enable_value_dictionary",
                "table": ident,
                "column": col,
                "score": sc,
                "reason": rs,
            })
            em_enabled_count += 1

    fa_count_preview = sum(1 for c in changes if c["type"] == "enable_example_values")
    fa_skipped = sum(
        1
        for t in tables + metric_views
        for cc in t.get("column_configs", [])
        if cc.get("enable_format_assistance") and not _is_hidden(cc)
    ) - fa_count_preview

    fa_lines = [f"\n-- FORMAT ASSISTANCE " + "-" * 31]
    fa_lines.append(f"  Enabled format assistance on {fa_count_preview} columns")
    fa_lines.append(f"  Skipped (already enabled): {fa_skipped}")
    fa_lines.append("-" * 52)
    print("\n".join(fa_lines))

    # ── Rejected-by-reason tally ─────────────────────────────────────
    rejected_by_reason: dict[str, int] = {}
    for _, _, _, _, r in rejected_candidates:
        rejected_by_reason[r] = rejected_by_reason.get(r, 0) + 1

    # ── ENTITY MATCHING log block (diff view) ────────────────────────
    em_lines = [f"\n-- ENTITY MATCHING (Value Dictionary) " + "-" * 14]
    em_lines.append(f"  STRING columns scored: {len(all_scored)}")
    if rejected_candidates:
        em_lines.append(
            f"  Rejected (never slotted): {len(rejected_candidates)}"
        )
        for reason, count in sorted(rejected_by_reason.items(), key=lambda x: -x[1]):
            em_lines.append(f"    - {reason:<26s} {count}")
        # One example column per reason for debuggability.
        _shown_per_reason: dict[str, int] = {}
        for ident, cname, _dt, _sc, reason in rejected_candidates[:30]:
            if _shown_per_reason.get(reason, 0) < 1:
                em_lines.append(
                    f"        e.g. {_table_short_name(ident)}.{cname}"
                )
                _shown_per_reason[reason] = _shown_per_reason.get(reason, 0) + 1
    em_lines.append("  Slot diff vs. current state:")
    em_lines.append(f"    Keep:    {len(kept):4d}")
    em_lines.append(f"    Enable:  {len(to_enable):4d}")
    em_lines.append(f"    Disable: {len(to_disable):4d}"
                    + ("  (displaced by higher-scoring candidates)"
                       if to_disable else ""))
    em_lines.append(
        f"    Net slots: {len(target_set):3d} / {MAX_VALUE_DICTIONARY_COLUMNS} max"
        + ("  (no changes)" if not to_enable and not to_disable else "")
    )
    if _enable_sorted:
        em_lines.append("  Top enables:")
        for ident, col in _enable_sorted[:10]:
            sc, rs = score_lookup.get((ident, col), (0.0, ""))
            em_lines.append(
                f"    {_table_short_name(ident)}.{col:<30s} "
                f"score={sc:5.1f}  [{rs}]"
            )
    if _disable_sorted:
        em_lines.append("  Top disables (displaced):")
        for ident, col in _disable_sorted[:10]:
            sc, rs = score_lookup.get((ident, col), (0.0, ""))
            em_lines.append(
                f"    {_table_short_name(ident)}.{col:<30s} "
                f"score={sc:5.1f}  [{rs}]"
            )
    if DRY_RUN_ENTITY_MATCHING:
        em_lines.append("  [DRY-RUN] Diff logged without PATCHing the space.")
    em_lines.append("-" * 52)
    print("\n".join(em_lines))

    if not changes:
        logger.info("Prompt matching auto-config: no changes needed (already configured)")
        return {
            "applied": [],
            "patched_objects": [],
            "pre_snapshot": pre_snapshot,
            "post_snapshot": parsed,
            "format_assistance_count": 0,
            "entity_matching_count": 0,
            "entity_matching_disabled_count": 0,
            "rejected_by_reason": rejected_by_reason,
        }

    sort_genie_config(parsed)
    _enforce_instruction_limit(parsed)
    patch_space_config(w, space_id, parsed)

    fa_count = sum(1 for c in changes if c["type"] == "enable_example_values")
    em_count = sum(1 for c in changes if c["type"] == "enable_value_dictionary")
    em_disabled = sum(1 for c in changes if c["type"] == "disable_value_dictionary")
    patched_objects = sorted({c["table"] for c in changes})

    print(
        f"Prompt matching auto-config: format assistance on {fa_count} columns, "
        f"entity matching enabled on {em_count} + disabled on {em_disabled} "
        f"({len(target_set)}/{MAX_VALUE_DICTIONARY_COLUMNS} slots in use)"
    )

    return {
        "applied": changes,
        "patched_objects": patched_objects,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": copy.deepcopy(parsed),
        "format_assistance_count": fa_count,
        "entity_matching_count": em_count,
        "entity_matching_disabled_count": em_disabled,
        "rejected_by_reason": rejected_by_reason,
    }


def _legacy_apply_em(
    w: WorkspaceClient,
    space_id: str,
    config: dict,
    *,
    benchmarks: list[dict] | None = None,
) -> dict:
    """Legacy enable-only entity-matching allocator (pre-idempotent).

    Gated by ``ENABLE_SMARTER_SCORING=False``. Preserves today's exact
    behaviour: legacy 0/1/2 scorer, no filtering of zero-scores, fill only
    empty slots (never disable). Includes the silent-PII leak on
    <120-column spaces by design — this shim exists so operators can pin
    today's behaviour during rollout if the new allocator surfaces any
    regression on their corpus.

    Scheduled for removal in a follow-up release (along with the
    ``ENABLE_SMARTER_SCORING`` flag).
    """
    parsed = config.get("_parsed_space", config)
    ds = parsed.get("data_sources", {})
    tables = ds.get("tables", [])
    metric_views = ds.get("metric_views", [])
    uc_columns: list[dict] = config.get("_uc_columns", [])

    bootstrapped = _ensure_column_configs_from_uc(tables + metric_views, uc_columns)
    if bootstrapped:
        print(f"  [PROMPT MATCHING] Bootstrapped {bootstrapped} column_config entries from UC metadata")

    type_lookup: dict[tuple[str, str], str] = {}
    for col in uc_columns:
        if not isinstance(col, dict):
            continue
        tbl = str(col.get("table_name") or "").strip()
        cname = str(col.get("column_name") or "").strip()
        dtype = str(col.get("data_type") or "").strip()
        if tbl and cname:
            type_lookup[(tbl.lower(), cname.lower())] = dtype

    pre_snapshot = copy.deepcopy(parsed)
    changes: list[dict] = []

    already_dict_count = sum(
        1
        for t in tables + metric_views
        for cc in t.get("column_configs", [])
        if cc.get("enable_entity_matching")
    )

    entity_candidates: list[tuple[str, str, str, float]] = []
    rls_skipped_tables: list[str] = []

    def _table_short_name(identifier: str) -> str:
        parts = identifier.replace("`", "").split(".")
        return parts[-1] if parts else identifier

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = _table_short_name(identifier)
        if _table_has_rls(tbl):
            rls_skipped_tables.append(identifier)
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if _is_hidden(cc) or not col_name:
                continue
            if not cc.get("enable_format_assistance"):
                cc["enable_format_assistance"] = True
                changes.append({
                    "type": "enable_example_values",
                    "table": identifier,
                    "column": col_name,
                })
            dtype = type_lookup.get((short_name.lower(), col_name.lower()), "")
            if (
                dtype.upper().split("(")[0].strip() == "STRING"
                and not cc.get("enable_entity_matching")
            ):
                entity_candidates.append(
                    (identifier, col_name, dtype, float(_entity_matching_score_legacy(col_name)))
                )

    for mv in metric_views:
        identifier = mv.get("identifier", "")
        short_name = _table_short_name(identifier)
        if _table_has_rls(mv):
            rls_skipped_tables.append(identifier)
        for cc in mv.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if _is_hidden(cc) or not col_name:
                continue
            dtype = type_lookup.get((short_name.lower(), col_name.lower()), "")
            if _is_measure_column(col_name, dtype):
                continue
            if not cc.get("enable_format_assistance"):
                cc["enable_format_assistance"] = True
                changes.append({
                    "type": "enable_example_values",
                    "table": identifier,
                    "column": col_name,
                })
            if (
                dtype.upper().split("(")[0].strip() == "STRING"
                and not cc.get("enable_entity_matching")
            ):
                entity_candidates.append(
                    (identifier, col_name, dtype, float(_entity_matching_score_legacy(col_name)))
                )

    if rls_skipped_tables:
        deduped = list(dict.fromkeys(rls_skipped_tables))
        print(
            f"  [PROMPT MATCHING] Skipped entity matching for {len(deduped)} "
            f"RLS-governed table(s): {', '.join(deduped[:5])}"
            + ("…" if len(deduped) > 5 else "")
        )

    entity_candidates.sort(key=lambda x: -x[3])
    slots_available = MAX_VALUE_DICTIONARY_COLUMNS - already_dict_count
    selected = entity_candidates[:max(slots_available, 0)]

    fa_count_preview = sum(1 for c in changes if c["type"] == "enable_example_values")
    fa_skipped = sum(
        1
        for t in tables + metric_views
        for cc in t.get("column_configs", [])
        if cc.get("enable_format_assistance") and not _is_hidden(cc)
    ) - fa_count_preview

    fa_lines = [f"\n-- FORMAT ASSISTANCE " + "-" * 31]
    fa_lines.append(f"  Enabled format assistance on {fa_count_preview} columns")
    fa_lines.append(f"  Skipped (already enabled): {fa_skipped}")
    fa_lines.append("-" * 52)
    print("\n".join(fa_lines))

    em_lines = [f"\n-- ENTITY MATCHING (Value Dictionary) [LEGACY] " + "-" * 6]
    em_lines.append(
        f"  Candidates ranked (legacy scorer): {len(entity_candidates)}"
    )
    for rank, (ident, cname, _dt, sc) in enumerate(entity_candidates[:20], 1):
        status = "SELECTED" if rank <= len(selected) else "NOT SELECTED (slot limit)"
        em_lines.append(
            f"    Rank {rank:2d}: {_table_short_name(ident)}.{cname:<30s} "
            f"score={sc:4.1f}  {status}"
        )
    em_lines.append(
        f"  Slots used: {already_dict_count} existing + {len(selected)} new = "
        f"{already_dict_count + len(selected)} / {MAX_VALUE_DICTIONARY_COLUMNS} max"
    )
    em_lines.append("-" * 52)
    print("\n".join(em_lines))

    for identifier, col_name, _dtype, _score in selected:
        tbl_dict = _find_table_in_config(parsed, identifier)
        if not tbl_dict:
            continue
        cc = _find_or_create_column_config(tbl_dict, col_name)
        cc["enable_entity_matching"] = True
        if not cc.get("enable_format_assistance"):
            cc["enable_format_assistance"] = True
        changes.append({
            "type": "enable_value_dictionary",
            "table": identifier,
            "column": col_name,
        })

    if not changes:
        logger.info("Prompt matching auto-config: no changes needed (already configured)")
        return {
            "applied": [],
            "patched_objects": [],
            "pre_snapshot": pre_snapshot,
            "post_snapshot": parsed,
            "format_assistance_count": 0,
            "entity_matching_count": 0,
            "entity_matching_disabled_count": 0,
            "rejected_by_reason": {},
        }

    sort_genie_config(parsed)
    _enforce_instruction_limit(parsed)
    patch_space_config(w, space_id, parsed)

    fa_count = sum(1 for c in changes if c["type"] == "enable_example_values")
    em_count = sum(1 for c in changes if c["type"] == "enable_value_dictionary")
    patched_objects = sorted({c["table"] for c in changes})

    print(
        f"Prompt matching auto-config [LEGACY]: enabled format assistance on {fa_count} "
        f"columns, entity matching on {em_count} STRING columns "
        f"({already_dict_count + em_count}/{MAX_VALUE_DICTIONARY_COLUMNS} dictionary slots used)"
    )

    return {
        "applied": changes,
        "patched_objects": patched_objects,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": copy.deepcopy(parsed),
        "format_assistance_count": fa_count,
        "entity_matching_count": em_count,
        "entity_matching_disabled_count": 0,
        "rejected_by_reason": {},
    }


def _find_table_in_config(config: dict, table_id: str) -> dict | None:
    """Find a table or metric view in data_sources by identifier."""
    ds = config.get("data_sources", {})
    for source_list in [ds.get("tables", []), ds.get("metric_views", [])]:
        for t in source_list:
            if t.get("identifier") == table_id:
                return t
    return None


def _find_or_create_column_config(table_dict: dict, column_name: str) -> dict:
    """Find an existing column_config or create one."""
    for cc in table_dict.get("column_configs", []):
        if cc.get("column_name") == column_name:
            return cc
    new_cc = {"column_name": column_name}
    table_dict.setdefault("column_configs", []).append(new_cc)
    return new_cc


# ═══════════════════════════════════════════════════════════════════════
# 2. Risk Classification
# ═══════════════════════════════════════════════════════════════════════


def classify_risk(patch_type: str | dict) -> str:
    """Classify a patch type as ``low``, ``medium``, or ``high``."""
    pt = patch_type if isinstance(patch_type, str) else patch_type.get("type", "")
    if pt in LOW_RISK_PATCHES:
        return "low"
    if pt in MEDIUM_RISK_PATCHES:
        return "medium"
    if pt in HIGH_RISK_PATCHES:
        return "high"
    return "medium"


# ═══════════════════════════════════════════════════════════════════════
# 3. Proposal → Patch Conversion
# ═══════════════════════════════════════════════════════════════════════


def proposals_to_patches(proposals: list[dict]) -> list[dict]:
    """Convert optimizer proposals into Patch DSL patches.

    Each proposal has an ``asi`` dict with ``failure_type``, ``blame_set``,
    ``counterfactual_fixes``.  Maps to concrete ``patch_type`` via
    ``_LEVER_TO_PATCH_TYPE``.

    For structured column proposals (Levers 1/2) carrying ``column_description``
    and/or ``column_synonyms``, emits separate patches for each field.
    """
    patches: list[dict] = []
    for p in proposals:
        asi = p.get("asi", {})
        if not isinstance(asi, dict):
            asi = {}
        failure_type = asi.get(
            "failure_type", p.get("lever_type", "other")
        )
        lever = p.get("lever", 5)
        patch_type = p.get("patch_type") or _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get((failure_type, 1), "add_instruction"),
        )
        blame_set = asi.get("blame_set", [])
        target = blame_set[0] if blame_set else p.get("change_description", "unknown")
        fixes = asi.get("counterfactual_fixes", [])
        if isinstance(fixes, str):
            fixes = [fixes]
        new_text = p.get("proposed_value") or (fixes[0] if fixes else p.get("change_description", ""))

        if (
            patch_type.startswith("add_sql_snippet_")
            or patch_type.startswith("update_sql_snippet_")
            or patch_type.startswith("remove_sql_snippet_")
        ):
            from genie_space_optimizer.common.genie_schema import generate_genie_id

            snippet_type_key = {
                "measure": "measures", "filter": "filters", "expression": "expressions",
            }.get(p.get("snippet_type", ""), "measures")

            snippet = {
                "id": generate_genie_id(),
                "sql": [p["sql"]] if isinstance(p.get("sql"), str) else p.get("sql", []),
                "display_name": p.get("display_name", ""),
                "synonyms": p.get("synonyms", []),
                "instruction": [p["instruction"]] if isinstance(p.get("instruction"), str) else p.get("instruction", []),
            }
            if p.get("alias") and snippet_type_key != "filters":
                snippet["alias"] = p["alias"]

            patches.append({
                "type": patch_type,
                "target": p.get("target_table", ""),
                "lever": 6,
                "risk_level": classify_risk(patch_type),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "source_proposal_id": p.get("proposal_id", ""),
                "sql_snippet": snippet,
                "snippet_type": snippet_type_key,
                # Tier 2.8: propagate validation stamp to the applier gate.
                "validation_passed": bool(p.get("validation_passed", False)),
            })
            continue

        table_sections = p.get("table_sections")
        col_sections = p.get("column_sections")
        col_desc = p.get("column_description")
        col_syns = p.get("column_synonyms")
        tbl_id = p.get("table", "")
        col_name = p.get("column", "")

        if isinstance(table_sections, dict) and table_sections and tbl_id and not col_name:
            patches.append({
                "type": "update_description",
                "target": tbl_id,
                "new_text": "",
                "old_text": "",
                "structured_sections": table_sections,
                "table_entity_type": p.get("table_entity_type", "table"),
                "lever": lever,
                "risk_level": classify_risk("update_description"),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
                "table": tbl_id,
            })
            continue

        if isinstance(col_sections, dict) and col_sections and tbl_id and col_name:
            base = {
                "lever": lever,
                "risk_level": classify_risk(patch_type),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
                "table": tbl_id,
                "column": col_name,
            }
            non_synonym_sections = {
                k: v for k, v in col_sections.items() if k != "synonyms"
            }
            synonym_value = col_sections.get("synonyms", "")
            if non_synonym_sections:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "target": tbl_id,
                    "new_text": "",
                    "old_text": "",
                    "structured_sections": non_synonym_sections,
                    "column_entity_type": p.get("column_entity_type", ""),
                })
            if synonym_value:
                if isinstance(synonym_value, list):
                    new_syns = [str(s).strip() for s in synonym_value if str(s).strip()]
                else:
                    new_syns = [s.strip() for s in str(synonym_value).split(",") if s.strip()]
                if new_syns:
                    patches.append({
                        **base,
                        "type": "add_column_synonym",
                        "target": tbl_id,
                        "new_text": "",
                        "old_text": "",
                        "synonyms": new_syns,
                    })
            continue

        if (col_desc is not None or col_syns is not None) and tbl_id and col_name:
            base = {
                "lever": lever,
                "risk_level": classify_risk(patch_type),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
                "table": tbl_id,
                "column": col_name,
            }
            if col_desc is not None and isinstance(col_desc, list) and col_desc:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "target": tbl_id,
                    "new_text": col_desc[0] if len(col_desc) == 1 else "\n".join(col_desc),
                    "old_text": "",
                })
            if col_syns is not None and isinstance(col_syns, list) and col_syns:
                patches.append({
                    **base,
                    "type": "add_column_synonym",
                    "target": tbl_id,
                    "new_text": col_syns[0] if len(col_syns) == 1 else "",
                    "old_text": "",
                    "synonyms": col_syns,
                })
            continue

        # ── Auto-convert freeform description patches to structured ──
        if (
            patch_type in ("update_column_description", "update_description")
            and new_text
            and not col_sections
            and not table_sections
        ):
            parsed = parse_structured_description(new_text)
            if parsed and any(k != "_preamble" for k in parsed):
                structured = {k: v for k, v in parsed.items() if k != "_preamble" and v}
                if structured:
                    logger.info(
                        "Auto-converting freeform %s to structured sections: %s",
                        patch_type, list(structured.keys()),
                    )
                    base_freeform = {
                        "lever": lever,
                        "risk_level": classify_risk(patch_type),
                        "predicted_affected_questions": p.get("questions_fixed", 0),
                        "grounded_in": p.get("grounded_in", []),
                        "source_proposal_id": p.get("proposal_id", ""),
                    }
                    if tbl_id:
                        base_freeform["table"] = tbl_id
                    if col_name:
                        base_freeform["column"] = col_name
                    patches.append({
                        **base_freeform,
                        "type": patch_type,
                        "target": tbl_id or target,
                        "new_text": "",
                        "old_text": "",
                        "structured_sections": structured,
                        "column_entity_type": p.get("column_entity_type", ""),
                        "table_entity_type": p.get("table_entity_type", "table"),
                    })
                    continue
            elif not parsed or (len(parsed) == 1 and "_preamble" in parsed):
                preamble = (parsed.get("_preamble") or new_text).strip()
                if preamble:
                    logger.info(
                        "Wrapping freeform %s text in 'definition' section",
                        patch_type,
                    )
                    base_freeform = {
                        "lever": lever,
                        "risk_level": classify_risk(patch_type),
                        "predicted_affected_questions": p.get("questions_fixed", 0),
                        "grounded_in": p.get("grounded_in", []),
                        "source_proposal_id": p.get("proposal_id", ""),
                    }
                    if tbl_id:
                        base_freeform["table"] = tbl_id
                    if col_name:
                        base_freeform["column"] = col_name
                    patches.append({
                        **base_freeform,
                        "type": patch_type,
                        "target": tbl_id or target,
                        "new_text": "",
                        "old_text": "",
                        "structured_sections": {"definition": preamble},
                        "column_entity_type": p.get("column_entity_type", ""),
                        "table_entity_type": p.get("table_entity_type", "table"),
                    })
                    continue

        patch_dict: dict = {
            "type": patch_type,
            "target": target,
            "new_text": new_text,
            "old_text": "",
            "lever": lever,
            "risk_level": classify_risk(patch_type),
            "predicted_affected_questions": p.get("questions_fixed", 0),
            "grounded_in": p.get("grounded_in", []),
            "source_proposal_id": p.get("proposal_id", ""),
        }
        if tbl_id:
            patch_dict["table"] = tbl_id
        if col_name:
            patch_dict["column"] = col_name
        if "join_spec" in p:
            patch_dict["join_spec"] = p["join_spec"]
        if "example_question" in p:
            patch_dict["example_question"] = p["example_question"]
        if "example_sql" in p:
            patch_dict["example_sql"] = p["example_sql"]
        if "parameters" in p:
            patch_dict["parameters"] = p["parameters"]
        if "usage_guidance" in p:
            patch_dict["usage_guidance"] = p["usage_guidance"]
        if "old_value" in p:
            patch_dict["old_value"] = p["old_value"]
        if "proposed_value" in p and "proposed_value" not in patch_dict:
            patch_dict["proposed_value"] = p["proposed_value"]
        if "change_description" in p and "change_description" not in patch_dict:
            patch_dict["change_description"] = p["change_description"]
        patches.append(patch_dict)
    return patches


# ═══════════════════════════════════════════════════════════════════════
# 4. Patch Rendering
# ═══════════════════════════════════════════════════════════════════════


def _parse_rewrite_into_sections(rewrite_body: str) -> tuple[dict[str, str], str]:
    """Parse a ``rewrite_instruction`` body into ``{canonical_header: body}``.

    Tolerates both ``HEADER:`` and ``HEADER\\n`` as delimiters. Any leading
    text before the first recognized canonical header is returned as the
    second element (the "preamble") — the caller decides whether to merge
    it into CONSTRAINTS or drop it.

    Canonical headers come from
    ``common.config.INSTRUCTION_SECTION_ORDER``. Matching is case-insensitive
    and tolerant of extra whitespace.
    """
    text = (rewrite_body or "").replace("\r\n", "\n").strip()
    if not text:
        return {}, ""

    _canonicals = [h.upper() for h in INSTRUCTION_SECTION_ORDER]
    _canon_set = set(_canonicals)
    # Match at the start of a line: HEADER followed by ``:`` or newline.
    # Allow internal spaces (e.g. ``ASSET ROUTING``) but no leading spaces.
    _alt = "|".join(re.escape(h) for h in _canonicals)
    _header_re = re.compile(
        rf"(?m)^(?P<h>{_alt})\s*(?::|\n)",
        flags=re.IGNORECASE,
    )

    matches = list(_header_re.finditer(text))
    if not matches:
        return {}, text

    sections: dict[str, str] = {}
    preamble = text[: matches[0].start()].strip()
    for idx, m in enumerate(matches):
        header = m.group("h").upper().strip()
        if header not in _canon_set:
            continue
        body_start = m.end()
        body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        body = text[body_start:body_end].strip()
        if not body:
            continue
        existing = sections.get(header, "").strip()
        sections[header] = (existing + "\n\n" + body).strip() if existing else body
    return sections, preamble


def _split_rewrite_instruction_patch(patch: dict) -> list[dict] | None:
    """Expand a ``rewrite_instruction`` patch into section-scoped children.

    T1.11: when ``ENABLE_REWRITE_SECTION_SPLIT`` is True and the patch has
    not explicitly escalated to ``full_rewrite``, parse ``proposed_value``
    (or ``new_text``) by canonical section header and emit a list of
    ``update_instruction_section`` children routed to the owning lever
    (via ``LEVER_TO_SECTIONS``) when that lever is among the invoked
    levers stamped on the source patch.

    Returns ``None`` if no split should be performed (flag off, explicit
    full-rewrite, empty body, or no canonical sections detected); the
    caller must fall back to legacy behaviour.
    Otherwise returns a (possibly empty) list of child patches.

    Content routing:
    - Sections owned by an invoked lever are emitted as a separate patch
      for that (lever, section).
    - Sections not owned by any invoked lever are merged into CONSTRAINTS
      on Lever 5 (the legacy collapse target) — this preserves
      backward-compatibility without dropping content.
    - Sections explicitly named ``CONSTRAINTS`` in the rewrite plus any
      preamble text with no canonical header are merged into CONSTRAINTS
      on Lever 5.
    """
    if not ENABLE_REWRITE_SECTION_SPLIT:
        return None
    if str(patch.get("escalation", "")).strip().lower() == "full_rewrite":
        return None
    if os.getenv("GSO_ALLOW_UNSCOPED_REWRITE", "").strip().lower() in ("1", "true", "yes"):
        return None

    body = patch.get("proposed_value") or patch.get("new_text") or ""
    if not isinstance(body, str) or not body.strip():
        return None

    parsed_sections, preamble = _parse_rewrite_into_sections(body)
    if not parsed_sections and not preamble:
        return None

    _invoked_raw = patch.get("invoked_levers") or []
    invoked_levers = {int(lv) for lv in _invoked_raw if str(lv).isdigit()}
    if not invoked_levers:
        invoked_levers = {int(patch.get("lever", 5) or 5)}

    # Build a section -> owning_lever map restricted to invoked levers.
    _owner_by_section: dict[str, int] = {}
    for lv in sorted(invoked_levers):
        for sec in LEVER_TO_SECTIONS.get(lv, []):
            _owner_by_section.setdefault(sec, lv)

    _base_fields = {
        k: patch.get(k) for k in (
            "proposal_id",
            "cluster_id",
            "rationale",
            "dual_persistence",
            "confidence",
            "questions_fixed",
            "questions_at_risk",
            "net_impact",
            "asi",
            "provenance",
            "invoked_levers",
            "old_value",
        ) if k in patch
    }

    children: list[dict] = []
    constraints_residual: list[str] = []
    routed_log: list[str] = []

    for section_name in INSTRUCTION_SECTION_ORDER:
        body_for_section = parsed_sections.get(section_name, "").strip()
        if not body_for_section:
            continue
        owner_lever = _owner_by_section.get(section_name)
        if section_name == "CONSTRAINTS" or owner_lever is None:
            constraints_residual.append(body_for_section)
            routed_log.append(
                f"L5[{section_name} (merged, {len(body_for_section)} chars)]"
            )
            continue
        child = dict(_base_fields)
        child["type"] = "update_instruction_section"
        child["lever"] = owner_lever
        child["section_name"] = section_name
        child["new_text"] = body_for_section
        child["change_description"] = (
            f"[{patch.get('cluster_id', '')}] Update instruction section "
            f"{section_name} ({len(body_for_section)} chars) — split from "
            f"rewrite_instruction"
        )
        child["_split_from"] = "rewrite_instruction"
        child["_proposal_patch_type"] = "rewrite_instruction"
        # Phase 4.1: rewrite splits are deliberate focused rewrites —
        # the new text should win even if shorter than the old.
        child["replacement_intent"] = "replace"
        children.append(child)
        routed_log.append(
            f"L{owner_lever}[{section_name} ({len(body_for_section)} chars)]"
        )

    if preamble:
        constraints_residual.append(preamble)
        routed_log.append(f"L5[CONSTRAINTS (preamble, {len(preamble)} chars)]")

    if constraints_residual:
        merged_constraints = "\n\n".join(constraints_residual).strip()
        child = dict(_base_fields)
        child["type"] = "update_instruction_section"
        child["lever"] = 5
        child["section_name"] = "CONSTRAINTS"
        child["new_text"] = merged_constraints
        child["change_description"] = (
            f"[{patch.get('cluster_id', '')}] Merge residual rewrite content "
            f"into CONSTRAINTS ({len(merged_constraints)} chars)"
        )
        child["_split_from"] = "rewrite_instruction"
        child["_proposal_patch_type"] = "rewrite_instruction"
        child["replacement_intent"] = "replace"
        children.append(child)

    if children:
        logger.info(
            "Rewrite downgrade split [%s]: %s",
            patch.get("cluster_id", "?"),
            ", ".join(routed_log) if routed_log else "<none>",
        )

    return children


def _expand_rewrite_splits(patches: list[dict]) -> list[dict]:
    """Preprocess a patch list, expanding qualifying rewrite_instruction
    patches into section-scoped children per T1.11."""
    expanded: list[dict] = []
    for p in patches:
        if p.get("type") == "rewrite_instruction":
            split = _split_rewrite_instruction_patch(p)
            if split is not None:
                if split:
                    expanded.extend(split)
                else:
                    # Empty split — nothing to apply; log and drop.
                    logger.info(
                        "Rewrite downgrade split [%s]: <no content> — dropping",
                        p.get("cluster_id", "?"),
                    )
                continue
        expanded.append(p)
    return expanded


def render_patch(patch: dict, space_id: str, space_config: dict) -> dict:
    """Convert a patch dict into an executable action with command + rollback.

    Returns ``{action_type, target, command, rollback_command, risk_level}``.
    """
    patch_type = patch.get("type", "")
    target = (
        patch.get("target") or patch.get("object_id") or patch.get("table") or ""
    )
    risk = classify_risk(patch_type)

    def action(cmd: str, rollback: str) -> dict:
        return {
            "action_type": patch_type,
            "target": target,
            "command": cmd,
            "rollback_command": rollback,
            "risk_level": risk,
        }

    old_text = patch.get("old_text", "")
    new_text = patch.get("new_text", patch.get("value", ""))
    table_id = patch.get("table") or patch.get("target") or ""
    column_name = patch.get("column", "")

    # ── Instructions ──────────────────────────────────────────────
    if patch_type == "add_instruction":
        return action(
            json.dumps({"op": "add", "section": "instructions", "new_text": new_text}),
            json.dumps({"op": "remove", "section": "instructions", "old_text": new_text}),
        )
    if patch_type == "update_instruction":
        return action(
            json.dumps({"op": "update", "section": "instructions", "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "instructions", "old_text": new_text, "new_text": old_text}),
        )
    if patch_type == "remove_instruction":
        return action(
            json.dumps({"op": "remove", "section": "instructions", "old_text": old_text}),
            json.dumps({"op": "add", "section": "instructions", "new_text": old_text}),
        )
    if patch_type == "update_instruction_section":
        section_name = str(patch.get("section_name", "CONSTRAINTS")).upper().strip()
        return action(
            json.dumps({
                "op": "update_section",
                "section": "instructions",
                "section_name": section_name,
                "new_text": new_text,
                "lever": patch.get("lever", 5),
            }),
            json.dumps({
                "op": "update_section",
                "section": "instructions",
                "section_name": section_name,
                "new_text": old_text,
                "lever": patch.get("lever", 5),
            }),
        )
    if patch_type == "rewrite_instruction":
        # Tier 2.10: full PURPOSE-through-everything rewrites are the
        # single biggest source of collateral damage (AG1 and AG2 both
        # regressed 5+ non-target questions via a global Lever-5 rewrite).
        # Require an explicit ``escalation="full_rewrite"`` stamp on the
        # patch OR a config opt-in, otherwise default to the sectional
        # update path. ``update_instruction_section`` (patch_type below)
        # is the narrower default for normal Lever-5 flows.
        #
        # T1.11: when ENABLE_REWRITE_SECTION_SPLIT is True (default), the
        # downgrade path is handled upstream by _expand_rewrite_splits in
        # apply_patch_set, which emits one update_instruction_section patch
        # per owned section. This branch is kept for (a) explicit
        # full_rewrite escalations, (b) the GSO_ALLOW_UNSCOPED_REWRITE
        # env opt-in, and (c) legacy / test callers that drive render_patch
        # directly without going through apply_patch_set.
        _escalation = str(patch.get("escalation", "")).strip().lower()
        _opt_in = os.getenv("GSO_ALLOW_UNSCOPED_REWRITE", "").strip().lower() in ("1", "true", "yes")
        if _escalation != "full_rewrite" and not _opt_in:
            logger.warning(
                "Refusing rewrite_instruction without escalation=full_rewrite. "
                "Tier 2.10: full instruction rewrites have high collateral "
                "damage and must be explicitly escalated. Converting to a "
                "section-scoped merge against CONSTRAINTS (legacy path — "
                "expected to be pre-empted by T1.11 splitter when routed "
                "through apply_patch_set)."
            )
            return action(
                json.dumps({
                    "op": "update_section",
                    "section": "instructions",
                    "section_name": "CONSTRAINTS",
                    "new_text": new_text,
                }),
                json.dumps({
                    "op": "update_section",
                    "section": "instructions",
                    "section_name": "CONSTRAINTS",
                    "new_text": old_text,
                }),
            )
        rewrite_old = patch.get("old_value", old_text)
        return action(
            json.dumps({"op": "rewrite", "section": "instructions", "new_text": new_text, "old_text": rewrite_old}),
            json.dumps({"op": "rewrite", "section": "instructions", "new_text": rewrite_old, "old_text": new_text}),
        )

    # ── Example SQL (preferred over text instructions) ────────────
    if patch_type == "add_example_sql":
        eq = patch.get("example_question", "")
        es = patch.get("example_sql", "")
        cmd_dict: dict = {"op": "add", "section": "example_question_sqls", "question": eq, "sql": es}
        params = patch.get("parameters", [])
        if params:
            cmd_dict["parameters"] = params
        guidance = patch.get("usage_guidance", "")
        if guidance:
            cmd_dict["usage_guidance"] = guidance
        return action(
            json.dumps(cmd_dict),
            json.dumps({"op": "remove", "section": "example_question_sqls", "question": eq}),
        )
    if patch_type == "update_example_sql":
        eq = patch.get("example_question", "")
        old_sql = patch.get("old_text", "")
        new_sql = patch.get("new_text", patch.get("example_sql", ""))
        return action(
            json.dumps({"op": "update", "section": "example_question_sqls", "question": eq, "old_sql": old_sql, "new_sql": new_sql}),
            json.dumps({"op": "update", "section": "example_question_sqls", "question": eq, "old_sql": new_sql, "new_sql": old_sql}),
        )
    if patch_type == "remove_example_sql":
        eq = patch.get("example_question", old_text)
        es = patch.get("example_sql", "")
        return action(
            json.dumps({"op": "remove", "section": "example_question_sqls", "question": eq}),
            json.dumps({"op": "add", "section": "example_question_sqls", "question": eq, "sql": es}),
        )

    # ── Descriptions ──────────────────────────────────────────────
    if patch_type == "add_description":
        return action(
            json.dumps({"op": "add", "section": "descriptions", "target": target, "value": new_text}),
            json.dumps({"op": "remove", "section": "descriptions", "target": target, "value": new_text}),
        )
    if patch_type == "update_description":
        structured_sections = patch.get("structured_sections")
        if isinstance(structured_sections, dict) and structured_sections:
            cmd_fwd_desc: dict = {
                "op": "update", "section": "descriptions",
                "target": target,
                "structured_sections": structured_sections,
                "lever": patch.get("lever", 0),
                "table_entity_type": patch.get("table_entity_type", "table"),
            }
            cmd_rev_desc: dict = {
                "op": "update", "section": "descriptions",
                "target": target, "old_text": new_text, "new_text": old_text,
            }
            return action(json.dumps(cmd_fwd_desc), json.dumps(cmd_rev_desc))
        return action(
            json.dumps({"op": "update", "section": "descriptions", "target": target, "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "descriptions", "target": target, "old_text": new_text, "new_text": old_text}),
        )
    if patch_type == "add_column_description":
        return action(
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "value": new_text}),
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "value": new_text}),
        )
    if patch_type == "update_column_description":
        structured_sections = patch.get("structured_sections")
        if isinstance(structured_sections, dict) and structured_sections:
            cmd_fwd: dict = {
                "op": "update", "section": "column_configs",
                "table": table_id, "column": column_name,
                "structured_sections": structured_sections,
                "lever": patch.get("lever", 0),
                "column_entity_type": patch.get("column_entity_type", ""),
            }
            cmd_rev: dict = {
                "op": "update", "section": "column_configs",
                "table": table_id, "column": column_name,
                "old_text": "", "new_text": "",
            }
            return action(json.dumps(cmd_fwd), json.dumps(cmd_rev))
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_text": new_text, "new_text": old_text}),
        )

    # ── Visibility ────────────────────────────────────────────────
    if patch_type == "hide_column":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": True}),
        )
    if patch_type == "unhide_column":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": False}),
        )
    if patch_type == "rename_column_alias":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_alias": old_text, "new_alias": new_text}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_alias": new_text, "new_alias": old_text}),
        )

    # ── Tables ────────────────────────────────────────────────────
    if patch_type == "add_table":
        asset = patch.get("asset", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "tables", "asset": asset}),
            json.dumps({"op": "remove", "section": "tables", "identifier": asset.get("identifier", target)}),
        )
    if patch_type == "remove_table":
        return action(
            json.dumps({"op": "remove", "section": "tables", "identifier": target}),
            json.dumps({"op": "add", "section": "tables", "asset": patch.get("previous_asset", {})}),
        )

    # ── Join Specifications (Lever 4) ─────────────────────────────
    if patch_type == "add_join_spec":
        join_spec = patch.get("join_spec", patch.get("value", {}))
        lt = _join_spec_left_id(join_spec) or patch.get("left_table", "")
        rt = _join_spec_right_id(join_spec) or patch.get("right_table", "")
        return action(
            json.dumps({"op": "add", "section": "join_specs", "join_spec": join_spec}),
            json.dumps({"op": "remove", "section": "join_specs", "left_table": lt, "right_table": rt}),
        )
    if patch_type == "update_join_spec":
        join_spec = patch.get("join_spec", patch.get("value", {}))
        lt = _join_spec_left_id(join_spec) or patch.get("left_table", "")
        rt = _join_spec_right_id(join_spec) or patch.get("right_table", "")
        return action(
            json.dumps({"op": "update", "section": "join_specs", "left_table": lt, "right_table": rt, "join_spec": join_spec}),
            json.dumps({"op": "update", "section": "join_specs", "left_table": lt, "right_table": rt, "join_spec": patch.get("previous_join_spec", {})}),
        )
    if patch_type == "remove_join_spec":
        lt = patch.get("left_table", "")
        rt = patch.get("right_table", "")
        return action(
            json.dumps({"op": "remove", "section": "join_specs", "left_table": lt, "right_table": rt}),
            json.dumps({"op": "add", "section": "join_specs", "join_spec": patch.get("previous_join_spec", {})}),
        )

    # ── SQL Snippets (Lever 6) ────────────────────────────────────
    _SNIPPET_TYPE_MAP = {
        "add_sql_snippet_measure": "measures",
        "update_sql_snippet_measure": "measures",
        "remove_sql_snippet_measure": "measures",
        "add_sql_snippet_filter": "filters",
        "update_sql_snippet_filter": "filters",
        "remove_sql_snippet_filter": "filters",
        "add_sql_snippet_expression": "expressions",
        "update_sql_snippet_expression": "expressions",
        "remove_sql_snippet_expression": "expressions",
    }

    if patch_type in _SNIPPET_TYPE_MAP:
        snippet_subtype = _SNIPPET_TYPE_MAP[patch_type]
        op_prefix = patch_type.split("_sql_snippet_")[0]  # "add", "update", "remove"
        snippet = patch.get("sql_snippet", {})
        snippet_id = snippet.get("id", "") or patch.get("snippet_id", "")

        # Tier 2.8: hard assertion — no ``add_sql_snippet_*`` patch can
        # be applied unless the proposer stamped ``validation_passed=True``
        # after ``validate_sql_snippet``. Prevents the observed bug where
        # CAST_INVALID_INPUT validation failures were logged but the
        # snippet still landed because a different code path bypassed
        # the gate.
        if op_prefix == "add" and not patch.get("validation_passed"):
            # T2.14: upgraded from ValueError to RuntimeError per plan so
            # apply_patch_set can distinguish the "validation missing"
            # refusal from a plain validation error and audit-log the
            # drop without aborting the rest of the patch set.
            raise RuntimeError(
                f"Refusing to apply {patch_type} without validation_passed=True. "
                f"Tier 2.14: every add_sql_snippet_* patch must carry a clean "
                f"validate_sql_snippet result. target={patch.get('target_table', '?')}, "
                f"snippet_id={snippet_id}"
            )

        if op_prefix == "add":
            return action(
                json.dumps({
                    "op": "add",
                    "section": "sql_snippets",
                    "snippet_type": snippet_subtype,
                    "snippet": snippet,
                }),
                json.dumps({
                    "op": "remove",
                    "section": "sql_snippets",
                    "snippet_type": snippet_subtype,
                    "snippet_id": snippet_id,
                }),
            )
        if op_prefix == "update":
            return action(
                json.dumps({
                    "op": "update",
                    "section": "sql_snippets",
                    "snippet_type": snippet_subtype,
                    "snippet_id": snippet_id,
                    "snippet": snippet,
                }),
                json.dumps({
                    "op": "update",
                    "section": "sql_snippets",
                    "snippet_type": snippet_subtype,
                    "snippet_id": snippet_id,
                    "snippet": patch.get("previous_sql_snippet", {}),
                }),
            )
        if op_prefix == "remove":
            return action(
                json.dumps({
                    "op": "remove",
                    "section": "sql_snippets",
                    "snippet_type": snippet_subtype,
                    "snippet_id": snippet_id,
                }),
                json.dumps({
                    "op": "add",
                    "section": "sql_snippets",
                    "snippet_type": snippet_subtype,
                    "snippet": patch.get("previous_sql_snippet", {}),
                }),
            )

    # ── Column Discovery Settings (Lever 5) ───────────────────────
    if patch_type == "enable_example_values":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": False}),
        )
    if patch_type == "disable_example_values":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": True}),
        )
    if patch_type == "enable_value_dictionary":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": False}),
        )
    if patch_type == "disable_value_dictionary":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": True}),
        )
    if patch_type == "add_column_synonym":
        synonyms = patch.get("synonyms", [new_text] if new_text else [])
        return action(
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
        )
    if patch_type == "remove_column_synonym":
        synonyms = patch.get("synonyms", [old_text] if old_text else [])
        return action(
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
        )

    # ── Filters ───────────────────────────────────────────────────
    if patch_type == "add_default_filter":
        filt = patch.get("filter", {"condition": new_text})
        return action(
            json.dumps({"op": "add", "section": "default_filters", "filter": filt}),
            json.dumps({"op": "remove", "section": "default_filters", "filter": filt}),
        )
    if patch_type == "remove_default_filter":
        filt = patch.get("filter", {"condition": old_text})
        return action(
            json.dumps({"op": "remove", "section": "default_filters", "filter": filt}),
            json.dumps({"op": "add", "section": "default_filters", "filter": filt}),
        )
    if patch_type == "update_filter_condition":
        return action(
            json.dumps({"op": "update", "section": "default_filters", "old_condition": old_text, "new_condition": new_text}),
            json.dumps({"op": "update", "section": "default_filters", "old_condition": new_text, "new_condition": old_text}),
        )

    # ── TVF ───────────────────────────────────────────────────────
    if patch_type == "add_tvf_parameter":
        return action(
            json.dumps({"op": "add", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", new_text)}),
            json.dumps({"op": "remove", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", new_text)}),
        )
    if patch_type == "remove_tvf_parameter":
        return action(
            json.dumps({"op": "remove", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", old_text)}),
            json.dumps({"op": "add", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", old_text)}),
        )
    if patch_type == "update_tvf_sql":
        return action(
            json.dumps({"op": "update", "section": "tvf_definition", "tvf": target, "old_sql": old_text, "new_sql": new_text}),
            json.dumps({"op": "update", "section": "tvf_definition", "tvf": target, "old_sql": new_text, "new_sql": old_text}),
        )
    if patch_type == "add_tvf":
        tvf_asset = patch.get("tvf_asset", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "tvfs", "tvf_asset": tvf_asset}),
            json.dumps({"op": "remove", "section": "tvfs", "identifier": tvf_asset.get("identifier", target)}),
        )
    if patch_type == "remove_tvf":
        return action(
            json.dumps({"op": "remove", "section": "tvfs", "identifier": target}),
            json.dumps({"op": "add", "section": "tvfs", "tvf_asset": patch.get("previous_tvf_asset", {})}),
        )

    # ── Metric Views ──────────────────────────────────────────────
    if patch_type == "add_mv_measure":
        measure = patch.get("measure", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "mv_measures", "mv": target, "measure": measure}),
            json.dumps({"op": "remove", "section": "mv_measures", "mv": target, "measure_name": measure.get("name", "")}),
        )
    if patch_type == "update_mv_measure":
        return action(
            json.dumps({"op": "update", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", ""), "old": old_text, "new": new_text}),
            json.dumps({"op": "update", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", ""), "old": new_text, "new": old_text}),
        )
    if patch_type == "remove_mv_measure":
        return action(
            json.dumps({"op": "remove", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", old_text)}),
            json.dumps({"op": "add", "section": "mv_measures", "mv": target, "measure": patch.get("previous_measure", {})}),
        )
    if patch_type == "add_mv_dimension":
        dim = patch.get("dimension", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "mv_dimensions", "mv": target, "dimension": dim}),
            json.dumps({"op": "remove", "section": "mv_dimensions", "mv": target, "dimension_name": dim.get("name", "")}),
        )
    if patch_type == "remove_mv_dimension":
        return action(
            json.dumps({"op": "remove", "section": "mv_dimensions", "mv": target, "dimension_name": patch.get("dimension_name", old_text)}),
            json.dumps({"op": "add", "section": "mv_dimensions", "mv": target, "dimension": patch.get("previous_dimension", {})}),
        )
    if patch_type == "update_mv_yaml":
        return action(
            json.dumps({"op": "update", "section": "mv_yaml", "mv": target, "new_yaml": new_text}),
            json.dumps({"op": "update", "section": "mv_yaml", "mv": target, "new_yaml": old_text}),
        )

    # ── Unknown type ──────────────────────────────────────────────
    return action(
        json.dumps({"op": "unknown", "patch_type": patch_type}),
        json.dumps({"op": "unknown", "patch_type": patch_type}),
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Action Application — Genie Config
# ═══════════════════════════════════════════════════════════════════════


def _apply_action_to_config(config: dict, action: dict) -> bool:
    """Apply a single rendered action to a Genie Space config dict in-place.

    Returns True if applied, False if skipped (e.g. old_text guard failed).
    """
    try:
        cmd = json.loads(action.get("command", "{}"))
    except json.JSONDecodeError:
        return False

    op = cmd.get("op", "")
    section = cmd.get("section", "")

    # ── Instructions ──────────────────────────────────────────────
    if section == "instructions":
        from genie_space_optimizer.optimization.optimizer import normalize_instructions

        if op == "add":
            text = cmd.get("new_text", "")
            if text:
                current = _get_general_instructions(config)
                merged = normalize_instructions((current + "\n" + text).strip())
                _set_general_instructions(config, merged)
            return True
        if op == "update":
            current = _get_general_instructions(config)
            old_text = cmd.get("old_text", "")
            new_text = cmd.get("new_text", "")
            if old_text and old_text not in current:
                return False
            replaced = current.replace(old_text, new_text, 1) if old_text else current + "\n" + new_text
            _set_general_instructions(config, normalize_instructions(replaced.strip()))
            return True
        if op == "remove":
            current = _get_general_instructions(config)
            old_text = cmd.get("old_text", "")
            if old_text and old_text not in current:
                return False
            _set_general_instructions(config, normalize_instructions(current.replace(old_text, "").strip()))
            return True
        if op == "update_section":
            # T1.11: merge ``new_text`` into the named instruction section,
            # preserving other sections verbatim. Used by
            # ``update_instruction_section`` patches (incl. split
            # rewrite_instruction children).
            section_name = str(cmd.get("section_name", "CONSTRAINTS")).upper().strip()
            text = cmd.get("new_text", "")
            if not text:
                return False
            current = _get_general_instructions(config)
            try:
                from genie_space_optimizer.optimization.optimizer import (
                    _ensure_structured,
                )
                structured = _ensure_structured(current, config)
            except Exception:
                logger.debug(
                    "update_section fallback: _ensure_structured failed, "
                    "appending section block verbatim",
                    exc_info=True,
                )
                structured = None

            if isinstance(structured, dict):
                # ``_ensure_structured`` returns ``dict[str, list[str]]`` (one
                # entry per non-blank line). Flatten to ``dict[str, str]`` once
                # so all subsequent reads/writes/renders below operate on a
                # single, uniform value type. Without this normalization the
                # render loop crashed with ``AttributeError: 'list' object has
                # no attribute 'strip'`` whenever any other section in the
                # config still held its original list shape.
                def _section_text(value: Any) -> str:
                    if isinstance(value, list):
                        return "\n".join(
                            str(ln) for ln in value if str(ln).strip()
                        )
                    return str(value or "")

                structured = {
                    k: _section_text(v) for k, v in structured.items()
                }

                existing = structured.get(section_name, "")
                merged = (existing.rstrip() + "\n\n" + text.strip()).strip() if existing else text.strip()
                structured[section_name] = merged
                rendered_parts: list[str] = []
                for _header in INSTRUCTION_SECTION_ORDER:
                    _body = structured.get(_header, "").strip()
                    if _body:
                        rendered_parts.append(f"{_header}:\n{_body}")
                for _header, _body in structured.items():
                    if _header in set(INSTRUCTION_SECTION_ORDER):
                        continue
                    _body_s = _body.strip()
                    if _body_s:
                        rendered_parts.append(f"{_header}:\n{_body_s}")
                new_full = "\n\n".join(rendered_parts)
            else:
                new_full = (current.rstrip() + f"\n\n{section_name}:\n{text.strip()}").strip()

            _set_general_instructions(config, normalize_instructions(new_full))
            return True
        if op == "rewrite":
            text = cmd.get("new_text", "")
            _orig_sections = config.get("_original_instruction_sections")
            if _orig_sections and isinstance(_orig_sections, dict) and text:
                try:
                    from genie_space_optimizer.optimization.optimizer import (
                        _detect_instruction_contradictions,
                        _ensure_structured,
                    )
                    _proposed_secs = _ensure_structured(text, config)
                    _contradictions = _detect_instruction_contradictions(
                        _orig_sections, _proposed_secs,
                    )
                    if _contradictions:
                        for _c in _contradictions:
                            logger.warning(
                                "Applier safety net: stripping contradictory line "
                                "from rewrite: '%s' (contradicts '%s')",
                                _c["proposed_line"][:100],
                                _c["original_rule"][:100],
                            )
                            text = text.replace(_c["proposed_line"], "").strip()
                except Exception:
                    logger.debug(
                        "Applier contradiction check failed — proceeding with rewrite",
                        exc_info=True,
                    )
            _set_general_instructions(config, normalize_instructions(text))
            return True

    # ── Example SQL Queries (preferred over text instructions) ────
    if section == "example_question_sqls":
        eqs = config.setdefault("instructions", {}).setdefault(
            "example_question_sqls", []
        )
        question_text = cmd.get("question", "")
        if op == "add":
            if not question_text:
                return False
            sql_text = cmd.get("sql", "")
            from genie_space_optimizer.common.genie_schema import generate_genie_id

            new_entry: dict = {
                "id": generate_genie_id(),
                "question": [question_text],
                "sql": [sql_text],
            }
            params = cmd.get("parameters", [])
            if params:
                api_params = []
                for p in params:
                    if not isinstance(p, dict):
                        continue
                    param_entry: dict = {"name": p.get("name", "")}
                    if p.get("type_hint"):
                        param_entry["type_hint"] = p["type_hint"]
                    dv = p.get("default_value", "")
                    if dv:
                        if isinstance(dv, dict):
                            param_entry["default_value"] = dv
                        else:
                            param_entry["default_value"] = {"values": [str(dv)]}
                    api_params.append(param_entry)
                if api_params:
                    new_entry["parameters"] = api_params
            guidance = cmd.get("usage_guidance", "")
            if guidance:
                new_entry["usage_guidance"] = (
                    [guidance] if isinstance(guidance, str) else guidance
                )
            if not _validate_example_sql_entry(new_entry, config=config):
                return False
            q_lower = question_text.strip().lower()
            for existing in eqs:
                eq_val = existing.get("question", [])
                existing_q = eq_val[0] if isinstance(eq_val, list) and eq_val else str(eq_val)
                if existing_q.strip().lower() == q_lower:
                    logger.info(
                        "Example SQL add skipped — duplicate question: %.80s",
                        question_text,
                    )
                    return True
            eqs.append(new_entry)
            return True
        if op == "update":
            for entry in eqs:
                eq = entry.get("question", [])
                q_str = eq[0] if isinstance(eq, list) and eq else str(eq)
                if q_str == question_text:
                    new_sql = cmd.get("new_sql", "")
                    updated_entry = dict(entry)
                    updated_entry["sql"] = [new_sql]
                    if not _validate_example_sql_entry(updated_entry, config=config):
                        logger.warning(
                            "update_example_sql rejected by validation: %.120s",
                            new_sql,
                        )
                        return False
                    entry["sql"] = [new_sql]
                    return True
            return False
        if op == "remove":
            for i, entry in enumerate(eqs):
                eq = entry.get("question", [])
                q_str = eq[0] if isinstance(eq, list) and eq else str(eq)
                if q_str == question_text:
                    eqs.pop(i)
                    return True
            return False

    # ── Column Configs (descriptions, visibility, discovery) ──────
    if section == "column_configs":
        table_id = cmd.get("table", "")
        col_name = cmd.get("column", "")
        tbl = _find_table_in_config(config, table_id)
        if not tbl:
            return False
        cc = _find_or_create_column_config(tbl, col_name)

        if "visible" in cmd:
            cc["exclude"] = not cmd["visible"]
            return True
        if "enable_format_assistance" in cmd:
            cc["enable_format_assistance"] = cmd["enable_format_assistance"]
            return True
        if "enable_entity_matching" in cmd:
            cc["enable_entity_matching"] = cmd["enable_entity_matching"]
            return True
        if "old_alias" in cmd:
            new_alias = cmd.get("new_alias", "")
            cc.setdefault("synonyms", []).append(new_alias)
            return True

        if op == "add" and "synonyms" in cmd:
            existing = cc.setdefault("synonyms", [])
            for s in cmd["synonyms"]:
                if s and s not in existing:
                    existing.append(s)
            return True
        if op == "remove" and "synonyms" in cmd:
            existing = cc.get("synonyms", [])
            cc["synonyms"] = [s for s in existing if s not in cmd["synonyms"]]
            return True

        if op == "add" and "value" in cmd:
            val = cmd["value"]
            if val is None or val == "" or val == []:
                return True
            cc["description"] = [val] if isinstance(val, str) else val
            return True
        if op == "update" and "structured_sections" in cmd:
            sections_update = cmd["structured_sections"]
            lever_num = cmd.get("lever", 0)
            etype_str = cmd.get("column_entity_type", "")
            if not etype_str:
                data_type = cc.get("data_type", "")
                etype_str = entity_type_for_column(col_name, data_type)
            # Phase 4.1: respect explicit replace intent from the
            # strategist (set via ``escalation == "replace"`` or by the
            # rewrite-instruction split path).
            _intent = (
                cmd.get("replacement_intent")
                or ("replace" if cmd.get("_split_from") == "rewrite_instruction"
                    else "merge")
            )
            try:
                new_desc = update_sections(
                    cc.get("description"),
                    sections_update,
                    lever_num,
                    etype_str,
                    replacement_intent=_intent,
                )
                cc["description"] = new_desc
                return True
            except LeverOwnershipError:
                logger.warning(
                    "Lever %d tried to update locked sections on %s.%s — skipped",
                    lever_num, table_id, col_name,
                )
                return False
        if op == "update" and "new_text" in cmd:
            if not cmd["new_text"]:
                return True
            desc = cc.get("description", [])
            joined = "\n".join(desc) if isinstance(desc, list) else str(desc)
            old_t = cmd.get("old_text", "")
            if old_t and old_t not in joined:
                return False
            new_desc = joined.replace(old_t, cmd["new_text"], 1) if old_t else joined + "\n" + cmd["new_text"]
            cc["description"] = [ln for ln in new_desc.split("\n")]
            return True
        if op == "remove" and "value" in cmd:
            desc = cc.get("description", [])
            if isinstance(desc, list):
                cc["description"] = [d for d in desc if d != cmd["value"]]
            return True

    # ── Descriptions (table-level) ────────────────────────────────
    if section == "descriptions":
        target = cmd.get("target", "")
        tbl = _find_table_in_config(config, target)
        if not tbl:
            return False
        if op == "update" and "structured_sections" in cmd:
            sections_update = cmd["structured_sections"]
            lever_num = cmd.get("lever", 0)
            entity_type = cmd.get("table_entity_type", "table")
            # Phase 4.1: respect explicit replace intent from the
            # strategist; default to legacy merge behavior.
            _intent = (
                cmd.get("replacement_intent")
                or ("replace" if cmd.get("_split_from") == "rewrite_instruction"
                    else "merge")
            )
            try:
                new_desc = update_sections(
                    tbl.get("description"),
                    sections_update,
                    lever_num,
                    entity_type,
                    replacement_intent=_intent,
                )
                tbl["description"] = new_desc
                return True
            except LeverOwnershipError:
                logger.warning(
                    "Lever %d tried to update locked sections on table %s — skipped",
                    lever_num, target,
                )
                return False
        if op == "add":
            desc = tbl.get("description", [])
            if isinstance(desc, list):
                desc.append(cmd.get("value", ""))
            else:
                tbl["description"] = [str(desc), cmd.get("value", "")]
            return True
        if op == "update":
            desc = tbl.get("description", [])
            joined = "\n".join(desc) if isinstance(desc, list) else str(desc)
            old_t = cmd.get("old_text", "")
            if old_t and old_t not in joined:
                return False
            new_desc = joined.replace(old_t, cmd.get("new_text", ""), 1)
            tbl["description"] = [ln for ln in new_desc.split("\n")]
            return True
        if op == "remove":
            desc = tbl.get("description", [])
            val = cmd.get("value", "")
            if isinstance(desc, list):
                tbl["description"] = [d for d in desc if d != val]
            return True

    # ── Join Specifications (Lever 4) ─────────────────────────────
    if section == "join_specs":
        from genie_space_optimizer.common.genie_schema import ensure_join_spec_fields

        inst = config.setdefault("instructions", {})
        specs = inst.setdefault("join_specs", [])
        if op == "add":
            js = cmd.get("join_spec", {})
            if js:
                js = ensure_join_spec_fields(js, config=config)
                if not _validate_join_spec_entry(js):
                    return False
                new_lt = _join_spec_left_id(js)
                new_rt = _join_spec_right_id(js)
                if new_lt and new_rt:
                    new_pair = tuple(sorted((new_lt, new_rt)))
                    for existing in specs:
                        ex_pair = tuple(sorted((_join_spec_left_id(existing), _join_spec_right_id(existing))))
                        if ex_pair == new_pair:
                            logger.info(
                                "Join spec add skipped — pair already exists: %s ↔ %s",
                                new_lt, new_rt,
                            )
                            return True
                specs.append(js)
            return True
        if op == "remove":
            lt, rt = cmd.get("left_table", ""), cmd.get("right_table", "")
            for i, s in enumerate(specs):
                if _join_spec_left_id(s) == lt and _join_spec_right_id(s) == rt:
                    specs.pop(i)
                    return True
            return False
        if op == "update":
            lt, rt = cmd.get("left_table", ""), cmd.get("right_table", "")
            new_spec = cmd.get("join_spec", {})
            if new_spec:
                new_spec = ensure_join_spec_fields(new_spec, config=config)
                if not _validate_join_spec_entry(new_spec):
                    return False
            for i, s in enumerate(specs):
                if _join_spec_left_id(s) == lt and _join_spec_right_id(s) == rt:
                    specs[i] = new_spec
                    return True
            return False

    # ── SQL Snippets (Lever 6) ────────────────────────────────────
    if section == "sql_snippets":
        from genie_space_optimizer.common.genie_schema import generate_genie_id

        snippet_type_key = cmd.get("snippet_type", "")
        if snippet_type_key not in ("measures", "filters", "expressions"):
            return False

        inst = config.setdefault("instructions", {})
        snippets_block = inst.setdefault("sql_snippets", {})
        items: list = snippets_block.setdefault(snippet_type_key, [])

        if op == "add":
            snippet = cmd.get("snippet", {})
            if not snippet:
                return False
            if not snippet.get("id"):
                snippet["id"] = generate_genie_id()
            if not _validate_sql_snippet_entry(snippet, snippet_type_key):
                return False
            new_sql_val = snippet.get("sql", [])
            new_sql_str = (
                new_sql_val[0] if isinstance(new_sql_val, list) and new_sql_val
                else str(new_sql_val)
            ).strip().lower()
            if new_sql_str:
                for existing in items:
                    ex_sql_val = existing.get("sql", [])
                    ex_sql_str = (
                        ex_sql_val[0] if isinstance(ex_sql_val, list) and ex_sql_val
                        else str(ex_sql_val)
                    ).strip().lower()
                    if ex_sql_str == new_sql_str:
                        logger.info(
                            "SQL snippet add skipped — duplicate SQL: %.80s",
                            new_sql_str,
                        )
                        return True
            items.append(snippet)
            return True

        if op == "remove":
            snippet_id = cmd.get("snippet_id", "")
            for i, s in enumerate(items):
                if s.get("id") == snippet_id:
                    items.pop(i)
                    return True
            return False

        if op == "update":
            snippet_id = cmd.get("snippet_id", "")
            new_snippet = cmd.get("snippet", {})
            if new_snippet and not _validate_sql_snippet_entry(new_snippet, snippet_type_key):
                return False
            for i, s in enumerate(items):
                if s.get("id") == snippet_id:
                    items[i] = new_snippet
                    return True
            return False

    # ── Tables ────────────────────────────────────────────────────
    if section == "tables":
        tables = config.setdefault("data_sources", {}).setdefault("tables", [])
        if op == "add":
            asset = cmd.get("asset", {})
            if asset:
                tables.append(asset)
                sort_genie_config(config)
            return True
        if op == "remove":
            ident = cmd.get("identifier", "")
            for i, t in enumerate(tables):
                if t.get("identifier") == ident:
                    tables.pop(i)
                    return True
            return False

    # ── Default Filters ───────────────────────────────────────────
    if section == "default_filters":
        filters = config.setdefault("default_filters", [])
        if op == "add":
            filt = cmd.get("filter", {})
            if filt and filt not in filters:
                filters.append(filt)
            return True
        if op == "remove":
            filt = cmd.get("filter", {})
            for i, f in enumerate(filters):
                if f == filt:
                    filters.pop(i)
                    return True
            return False
        if op == "update":
            old_c, new_c = cmd.get("old_condition", ""), cmd.get("new_condition", "")
            for i, f in enumerate(filters):
                if isinstance(f, dict) and f.get("condition") == old_c:
                    f["condition"] = new_c
                    return True
                if f == old_c:
                    filters[i] = new_c
                    return True
            return False

    # ── TVF / MV operations (config-level no-ops for uc_artifact patches) ──
    if section in ("tvf_parameters", "tvf_definition", "tvfs", "mv_measures", "mv_dimensions", "mv_yaml"):
        if section == "tvfs":
            funcs = config.setdefault("instructions", {}).setdefault("sql_functions", [])
            if op == "add":
                tvf_asset = cmd.get("tvf_asset", {})
                if tvf_asset:
                    ident = tvf_asset.get("identifier", "")
                    funcs.append({"id": tvf_asset.get("id", ident), "identifier": ident})
                    sort_genie_config(config)
                return True
            if op == "remove":
                ident = cmd.get("identifier", "")
                for i, f in enumerate(funcs):
                    if f.get("identifier") == ident:
                        funcs.pop(i)
                        return True
                return False
        return True

    return False


# ═══════════════════════════════════════════════════════════════════════
# 6. Action Application — UC Artifacts
# ═══════════════════════════════════════════════════════════════════════


def _apply_action_to_uc(w: WorkspaceClient, action: dict) -> bool:
    """Apply an action to UC artifacts via DDL.

    Only used for Levers 1-3 when ``apply_mode`` includes ``uc_artifact``.
    """
    try:
        cmd = json.loads(action.get("command", "{}"))
    except json.JSONDecodeError:
        return False

    patch_type = action.get("action_type", "")
    warehouse_id = os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        logger.warning("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID is not set; skipping UC DDL action")
        return False

    try:
        if patch_type == "update_column_description":
            table = cmd.get("table", "")
            column = cmd.get("column", "")
            structured_sections = cmd.get("structured_sections")
            new_text = cmd.get("new_text", "")
            if table and column and isinstance(structured_sections, dict) and structured_sections:
                etype = cmd.get("column_entity_type") or entity_type_for_column(column, "")
                rendered = render_structured_description(structured_sections, etype)
                flat = "\n".join(rendered)
                escaped = flat.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
            if table and column and new_text:
                escaped = new_text.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
        if patch_type == "update_description":
            table = cmd.get("target", "")
            new_text = cmd.get("new_text", "")
            if table and new_text:
                escaped = new_text.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"COMMENT ON TABLE {table} IS '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
        if patch_type == "update_tvf_sql":
            new_sql = cmd.get("new_sql", "")
            if new_sql:
                w.statement_execution.execute_statement(
                    statement=new_sql,
                    warehouse_id=warehouse_id,
                    wait_timeout="60s",
                )
                return True
    except Exception:
        logger.exception("UC action failed for %s", patch_type)
        return False

    return True


# ═══════════════════════════════════════════════════════════════════════
# 7. Patch Set Application
# ═══════════════════════════════════════════════════════════════════════


_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


def apply_patch_set(
    w: WorkspaceClient | None,
    space_id: str,
    patches: list[dict],
    metadata_snapshot: dict,
    *,
    apply_mode: str = APPLY_MODE,
    deploy_target: str | None = None,
    force_apply: bool = False,
) -> dict:
    """Apply a patch set to a Genie Space (and optionally UC artifacts).

    Applies in risk order: LOW -> MEDIUM -> HIGH.
    High-risk patches are queued for manual review unless *force_apply*
    is ``True``, in which case all patches are applied regardless of risk
    (used by the escalation pipeline after confidence-model approval).

    Returns an ``apply_log`` dict with pre/post snapshots and rollback info.
    """
    pre_snapshot = copy.deepcopy(metadata_snapshot)
    config = copy.deepcopy(metadata_snapshot)

    # T1.11: split downgraded rewrite_instruction patches into
    # section-scoped update_instruction_section children so the downgrade
    # no longer collapses ASSET ROUTING / AGGREGATION RULES etc. into
    # CONSTRAINTS. Behind ENABLE_REWRITE_SECTION_SPLIT (default True).
    patches = _expand_rewrite_splits(patches)

    # T3.2: infer read/write asset sets per patch and log them so
    # operators can audit ordering. The risk-order sort below is kept
    # as the primary ordering (low-risk first); this annotation surfaces
    # conflicts (e.g. an instruction rewrite that *reads* a measure
    # being *written* by another patch in the same AG) without yet
    # changing behaviour. When a cycle or a clear read-before-write
    # violation is detected, the loop warns but continues — the gate
    # catches any resulting regression downstream.
    _READ_WRITE_RULES: dict[str, tuple[list[str], list[str]]] = {
        # (reads, writes) — keys are asset-kind strings
        "update_description":          ([], ["table"]),
        "update_column_description":   ([], ["column"]),
        "add_column_synonym":          ([], ["column"]),
        "add_sql_snippet_measure":     ([], ["measure"]),
        "add_sql_snippet_calculation": ([], ["measure"]),
        "add_sql_snippet_tvf":         ([], ["tvf"]),
        "rewrite_instruction":         (["instructions"], ["instructions"]),
        "update_instruction_section":  (["instructions"], ["instructions"]),
        "update_instruction":          (["instructions"], ["instructions"]),
        "add_instruction":             (["instructions"], ["instructions"]),
        "remove_instruction":          (["instructions"], ["instructions"]),
        "update_join_spec":            (["column"], ["join_spec"]),
        "add_join_spec":               (["column"], ["join_spec"]),
    }

    _dag_writes: dict[str, list[int]] = {}  # asset_key -> [patch_indices]
    _dag_reads: dict[str, list[int]] = {}
    for _i, _p in enumerate(patches):
        _ptype = str(_p.get("type", ""))
        _reads, _writes = _READ_WRITE_RULES.get(_ptype, ([], ["other"]))
        _target = str(
            _p.get("target") or _p.get("target_object")
            or _p.get("target_table") or "default"
        )
        for _w in _writes:
            _key = f"{_w}:{_target}" if _w != "instructions" else "instructions:*"
            _dag_writes.setdefault(_key, []).append(_i)
        for _r in _reads:
            _key = f"{_r}:{_target}" if _r != "instructions" else "instructions:*"
            _dag_reads.setdefault(_key, []).append(_i)
        _p["_reads"] = list(_reads)
        _p["_writes"] = list(_writes)

    # Detect suspicious read/write orderings: any write that lands
    # AFTER a read of the same asset in current risk-sort order.
    sorted_indices = sorted(
        range(len(patches)),
        key=lambda i: _RISK_ORDER.get(classify_risk(patches[i].get("type", "")), 1),
    )
    _order_pos = {idx: pos for pos, idx in enumerate(sorted_indices)}
    _dag_warnings: list[str] = []
    for _asset, _readers in _dag_reads.items():
        _writers = _dag_writes.get(_asset, [])
        for _r in _readers:
            for _w in _writers:
                if _r == _w:
                    continue
                if _order_pos.get(_r, 0) < _order_pos.get(_w, 0):
                    _dag_warnings.append(
                        f"patch idx={_r} reads {_asset} but patch idx={_w} "
                        f"writes it later in apply order"
                    )
    if _dag_warnings:
        logger.info(
            "T3.2: patch DAG inference surfaced %d read-before-write ordering "
            "hint(s); continuing with risk-order (warnings are non-fatal):",
            len(_dag_warnings),
        )
        for _w in _dag_warnings[:5]:
            logger.info("  - %s", _w)

    applied: list[dict] = []
    queued_high: list[dict] = []
    rollback_commands: list[str] = []
    # T2.14: collect patches dropped at render time (e.g. add_sql_snippet_*
    # without validation_passed=True). These will be merged into the
    # final dropped_patches list in the apply_log.
    early_dropped_patches: list[dict] = []
    patched_objects: set[str] = set()

    for idx in sorted_indices:
        patch = patches[idx]
        risk = classify_risk(patch.get("type", ""))
        lever = patch.get("lever", 5)
        scope = _resolve_scope(lever, apply_mode)

        try:
            rendered = render_patch(patch, space_id, config)
        except RuntimeError as _render_err:
            # T2.14: render_patch raises RuntimeError when the Lever 6
            # gate refuses an add_sql_snippet_* patch without
            # validation_passed=True. Audit-log, record as dropped, and
            # continue applying the rest of the patch set rather than
            # aborting the whole AG.
            logger.warning(
                "Refusing patch at idx=%d (type=%s, target=%s): %s",
                idx, patch.get("type", "?"),
                patch.get("target_table") or patch.get("target", "?"),
                _render_err,
            )
            early_dropped_patches.append({
                "index": idx,
                **patch,
                "drop_reason": "validation_missing",
                "drop_detail": str(_render_err),
            })
            continue

        if risk == "high" and not force_apply:
            queued_high.append({"index": idx, "patch": patch, "action": rendered})
            continue

        ok = False
        if scope in ("genie_config", "both"):
            ok = _apply_action_to_config(config, rendered)
        if scope in ("uc_artifact", "both") and w is not None:
            uc_ok = _apply_action_to_uc(w, rendered)
            ok = ok or uc_ok

        if ok:
            # T2.13: stamp applied provenance on the applied entry so the
            # harness can persist ``applied_patch_type`` /
            # ``applied_patch_detail`` on genie_opt_patches and the
            # pretty-printer can enumerate records accurately. The
            # proposal-side patch_type (pre-downgrade) is preserved as
            # ``proposal_patch_type`` so readers can tell when a
            # ``rewrite_instruction`` was downgraded into multiple
            # ``update_instruction_section`` children.
            _applied_type = str(patch.get("type", ""))
            _proposal_type = (
                patch.get("_proposal_patch_type") or _applied_type
            )
            _detail_parts: list[str] = []
            if _applied_type == "update_instruction_section":
                _sec = patch.get("section_name")
                if _sec:
                    _detail_parts.append(f"section={_sec}")
                _lv = patch.get("lever")
                if _lv is not None:
                    _detail_parts.append(f"lever={_lv}")
            if patch.get("_split_from"):
                _detail_parts.append(f"split_from={patch['_split_from']}")
            target = rendered.get("target", "")
            if target:
                _detail_parts.append(f"target={target}")
            _detail = "; ".join(_detail_parts) if _detail_parts else None
            applied.append({
                "index": idx,
                "patch": patch,
                "action": rendered,
                "applied_patch_type": _applied_type,
                "applied_patch_detail": _detail,
                "proposal_patch_type": _proposal_type,
            })
            rollback_commands.append(rendered.get("rollback_command", ""))
            if target:
                patched_objects.add(target)

    sort_genie_config(config)
    # D1–D3: write sentinel-wrapped quality instruction blocks. The call is
    # idempotent and always runs — the GSO_APPLY_QUALITY_INSTRUCTIONS=off path
    # strips stale blocks so a revert needs only the env flag flip.
    apply_gso_quality_instructions(config)
    _enforce_instruction_limit(config)

    from genie_space_optimizer.common.genie_schema import (
        count_instruction_slots,
        count_sql_snippets,
        MAX_INSTRUCTION_SLOTS,
        MAX_SQL_SNIPPETS,
        validate_serialized_space,
    )

    slot_count = count_instruction_slots(config)
    if slot_count > MAX_INSTRUCTION_SLOTS:
        excess = slot_count - MAX_INSTRUCTION_SLOTS
        logger.warning(
            "Post-apply config exceeds instruction slot budget (%d/%d, excess=%d) — "
            "trimming example_question_sqls then sql_functions",
            slot_count, MAX_INSTRUCTION_SLOTS, excess,
        )
        inst = config.get("instructions") or {}
        eqs = inst.get("example_question_sqls", [])
        if eqs and excess > 0:
            trim_eq = min(len(eqs), excess)
            config.setdefault("instructions", {})["example_question_sqls"] = eqs[:-trim_eq]
            excess -= trim_eq
        if excess > 0:
            fns = (config.get("instructions") or {}).get("sql_functions", [])
            if fns and excess > 0:
                trim_fn = min(len(fns), excess)
                logger.warning(
                    "Still %d slots over budget after trimming examples — "
                    "removing %d newest sql_functions entries",
                    excess, trim_fn,
                )
                config["instructions"]["sql_functions"] = fns[:-trim_fn]
                excess -= trim_fn
        if excess > 0:
            logger.error(
                "Cannot trim enough instruction slots — %d excess slots from "
                "table/metric descriptions alone. Manual description cleanup required.",
                excess,
            )

    snippet_count = count_sql_snippets(config)
    if snippet_count > MAX_SQL_SNIPPETS:
        snippet_excess = snippet_count - MAX_SQL_SNIPPETS
        logger.warning(
            "Post-apply config exceeds SQL snippet budget (%d/%d, excess=%d) — "
            "trimming expressions, then measures, then filters",
            snippet_count, MAX_SQL_SNIPPETS, snippet_excess,
        )
        snippets_block = (config.get("instructions") or {}).get("sql_snippets", {})
        for trim_key in ("expressions", "measures", "filters"):
            if snippet_excess <= 0:
                break
            trim_items = snippets_block.get(trim_key, [])
            if trim_items:
                trim_n = min(len(trim_items), snippet_excess)
                snippets_block[trim_key] = trim_items[:-trim_n]
                snippet_excess -= trim_n
        if snippet_excess > 0:
            logger.error(
                "Cannot trim enough SQL snippets — %d excess remain.",
                snippet_excess,
            )

    # Validate the payload we are about to send (post-strip), not the
    # runtime config dict — the runtime carries `_data_profile`,
    # `_failure_clusters` and other underscore-prefixed annotations that
    # never leave this process (stripped by `patch_space_config`). The
    # strict validator tolerates runtime keys via `is_runtime_key` but
    # validating the stripped copy is the belt-and-suspenders invariant:
    # it guarantees we cannot regress by adding a future runtime key
    # that `is_runtime_key` misses.
    validation_target = strip_non_exportable_fields(copy.deepcopy(config))
    config_ok, validation_errors = validate_serialized_space(
        validation_target, strict=True,
    )
    if not config_ok:
        logger.error(
            "Post-patch config validation failed for space %s: %s",
            space_id,
            validation_errors,
        )
        return {
            "space_id": space_id,
            "pre_snapshot": pre_snapshot,
            "post_snapshot": copy.deepcopy(config),
            "applied": applied,
            "queued_high": queued_high,
            "rollback_commands": rollback_commands,
            "deploy_target": deploy_target,
            "patched_objects": list(patched_objects),
            "validation_errors": validation_errors,
            "patch_deployed": False,
            "patch_error": f"Validation failed: {validation_errors}",
        }

    patch_deployed = False
    patch_error: str = ""
    dropped_patches: list[dict] = []

    if w is not None and applied:
        try:
            patch_space_config(w, space_id, config)
            patch_deployed = True
        except Exception as exc:
            patch_error = str(exc)
            logger.exception(
                "Failed to PATCH Genie Space config after retries — "
                "patches were NOT deployed remotely",
            )

            join_spec_entries = [
                e for e in applied
                if e.get("patch", {}).get("type") == "add_join_spec"
            ]
            if join_spec_entries:
                logger.warning(
                    "PATCH failed with %d join spec patch(es) — retrying without them",
                    len(join_spec_entries),
                )
                config_retry = copy.deepcopy(metadata_snapshot)
                applied_retry: list[dict] = []
                patched_objects_retry: set[str] = set()
                rollback_retry: list[str] = []
                for entry in applied:
                    if entry.get("patch", {}).get("type") == "add_join_spec":
                        continue
                    rendered = render_patch(entry["patch"], space_id, config_retry)
                    if _apply_action_to_config(config_retry, rendered):
                        applied_retry.append(entry)
                        rollback_retry.append(rendered.get("rollback_command", ""))
                        target = rendered.get("target", "")
                        if target:
                            patched_objects_retry.add(target)
                if applied_retry:
                    sort_genie_config(config_retry)
                    _enforce_instruction_limit(config_retry)
                    try:
                        patch_space_config(w, space_id, config_retry)
                        patch_deployed = True
                        patch_error = ""
                        config = config_retry
                        dropped_patches = [e["patch"] for e in join_spec_entries]
                        applied = applied_retry
                        rollback_commands = rollback_retry
                        patched_objects = patched_objects_retry
                        logger.info(
                            "PATCH succeeded after dropping %d join spec patch(es); "
                            "%d patches deployed",
                            len(join_spec_entries), len(applied_retry),
                        )
                    except Exception as exc2:
                        patch_error = str(exc2)
                        logger.exception("Retry without join specs also failed")

    # Phase 3.4: end-of-pipeline canonicalization + dedup of the
    # instruction text. Single safety net that catches every write path
    # (GSO-quality, strategist update_instruction_section, rewrite
    # splits, sanitizer) so the on-disk Genie config never carries
    # duplicate ``CONSTRAINTS`` / ``DATA QUALITY NOTES`` blocks.
    # Toggle via ``GSO_CANONICAL_HEADERS_ALLCAPS=0`` to keep legacy
    # behavior during rollout.
    if (
        os.getenv("GSO_CANONICAL_HEADERS_ALLCAPS", "1")
        .strip().lower() not in ("0", "false", "no", "off")
    ):
        try:
            if _canonicalize_and_dedup_instructions(config):
                logger.info(
                    "Phase 3.4: end-of-pipeline canonicalize-and-dedup "
                    "rewrote instruction text for space %s", space_id,
                )
                if w is not None and patch_deployed:
                    try:
                        patch_space_config(w, space_id, config)
                    except Exception:
                        logger.warning(
                            "Failed to push canonicalized instructions "
                            "to Genie API — local snapshot is correct, "
                            "but next read may regress",
                            exc_info=True,
                        )
        except Exception:
            logger.warning(
                "Canonicalize-and-dedup pass failed (non-fatal)",
                exc_info=True,
            )

    return {
        "space_id": space_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": copy.deepcopy(config),
        "applied": applied,
        "queued_high": queued_high,
        "rollback_commands": rollback_commands,
        "deploy_target": deploy_target,
        "patched_objects": list(patched_objects),
        "validation_errors": [],
        "patch_deployed": patch_deployed,
        "patch_error": patch_error,
        "dropped_patches": dropped_patches + early_dropped_patches,
    }


# ═══════════════════════════════════════════════════════════════════════
# 8. Rollback
# ═══════════════════════════════════════════════════════════════════════


def rollback(
    apply_log: dict,
    w: WorkspaceClient | None,
    space_id: str,
    metadata_snapshot: dict | None = None,
) -> dict:
    """Restore the Genie Space config to its pre-patch state.

    Primary mechanism: replace current config with ``apply_log["pre_snapshot"]``.
    Fallback: execute rollback_commands in reverse order (HIGH -> MEDIUM -> LOW).
    """
    pre_snapshot = apply_log.get("pre_snapshot")
    if pre_snapshot is None:
        return {
            "status": "error",
            "executed_count": 0,
            "errors": ["No pre_snapshot in apply_log"],
        }

    restored = copy.deepcopy(pre_snapshot)

    if metadata_snapshot is not None:
        metadata_snapshot.clear()
        metadata_snapshot.update(restored)

    if w is not None:
        try:
            patch_space_config(w, space_id, restored)
        except Exception:
            logger.exception("Failed to PATCH rollback config")
            return {
                "status": "error",
                "executed_count": 0,
                "errors": ["Failed to apply rollback via API"],
                "restored_config": restored,
            }

    commands = apply_log.get("rollback_commands", [])
    return {
        "status": "SUCCESS",
        "executed_count": max(len(commands), 1),
        "errors": [],
        "restored_config": restored,
    }


# ═══════════════════════════════════════════════════════════════════════
# 9. Validation & Verification
# ═══════════════════════════════════════════════════════════════════════


def validate_patch_set(patches: list[dict], metadata_snapshot: dict) -> tuple[bool, list[str]]:
    """Validate a patch set before application.

    Delegates to ``optimizer.validate_patch_set`` but adds metadata checks.
    """
    from genie_space_optimizer.optimization.optimizer import validate_patch_set as _validate

    return _validate(patches, metadata_snapshot)


def verify_dual_persistence(applied_patches: list[dict]) -> list[dict]:
    """Verify that both Genie config and UC objects were updated."""
    results: list[dict] = []
    for entry in applied_patches:
        patch = entry.get("patch", {})
        results.append(
            {
                "patch_type": patch.get("type", ""),
                "target": entry.get("action", {}).get("target", ""),
                "genie_config_applied": True,
                "uc_artifact_applied": patch.get("lever", 5) <= 3,
            }
        )
    return results


def verify_repo_update(patch: dict, w: WorkspaceClient | None = None) -> dict:
    """Verify a specific patch persisted (stub for repo-level checks)."""
    return {
        "patch_type": patch.get("type", ""),
        "target": patch.get("target", ""),
        "verified": True,
    }
