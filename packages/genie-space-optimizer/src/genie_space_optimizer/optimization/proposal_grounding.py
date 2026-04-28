"""Proposal grounding: drop patches whose targets do not appear in any
failing question's surface, then keep small auditable bundles.

The retail run shipped 8-patch bundles whose targets did not match the
failures the strategist claimed to be addressing. AG2 in particular
patched ``zone_combination`` / ``market_combination`` column
descriptions even though the failing benchmarks (Q011's missing
``YEAR(date_key_2)`` GROUP BY, Q009's wrong customer-count measure)
had nothing to do with those columns. This module enforces a
deterministic relevance check before apply:

* Every patch declares its targets via standard fields (``column``,
  ``target``, ``metric``, ``join_target``, ``instruction_section``,
  ``table``, ``section_name``, ``snippet_name``).
* Every failing eval row exposes its surface — column names from
  generated/expected SQL plus tokenized NL response.
* A patch's relevance is the fraction of its targets that overlap
  the union of failing-row surfaces.

Crucially, the comparison is purely local — schema identifiers and
tokenized NL only. No benchmark text is sent to an LLM or a remote
service, so the leakage firewall (Bug #4) stays intact.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable

from genie_space_optimizer.common.config import (
    IGNORED_OPTIMIZATION_JUDGES as _CONFIG_IGNORED_OPTIMIZATION_JUDGES,
)

logger = logging.getLogger(__name__)

# Identifier-like tokens. We're deliberately permissive on what counts
# as an identifier to keep regex parity with sqlglot for unparseable
# SQL (the AG2 `GROUP BY ALL` case).
_IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")

# Common patch fields that name an identifier the patch claims to
# influence. Order does not matter; the union goes into the targets
# set.
_PATCH_TARGET_KEYS: tuple[str, ...] = (
    "column",
    "target",
    "target_object",
    "target_table",
    "metric",
    "join_target",
    "instruction_section",
    "table",
    "section_name",
    "snippet_name",
)

# Persisted MLflow eval rows use dotted keys (``outputs.predictions.sql``,
# ``inputs.expected_sql``); ad-hoc test fixtures and a few in-memory
# call sites use the flat ``generated_sql`` / ``expected_sql`` shape. We
# look up both — flat names stay in the chain so existing fixture-based
# tests remain valid, dotted names match what ``_get_failure_rows``
# loads from ``iterations.rows_json``.
_FAILURE_SURFACE_SQL_KEYS: tuple[str, ...] = (
    "outputs.predictions.sql",
    "inputs.expected_sql",
    "generated_sql",
    "expected_sql",
    "genie_sql",
)

_FAILURE_SURFACE_NL_KEYS: tuple[str, ...] = (
    "outputs.predictions.response_text",
    "nl_response",
)


def _normalize(token: str) -> str:
    return str(token).strip().lower()


_IGNORED_METADATA_PREFIXES: frozenset[str] = frozenset(
    _CONFIG_IGNORED_OPTIMIZATION_JUDGES
)
"""Judges whose ``*/metadata`` keys never seed grounding targets.

Sourced from ``common.config.IGNORED_OPTIMIZATION_JUDGES`` (driven by
``GSO_IGNORED_OPTIMIZATION_JUDGES``) so the optimizer engine has a
single, env-controllable ignored-judge policy.
"""


def _nested_get(row: dict, path: tuple[str, ...]) -> object:
    cur: object = row
    for part in path:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(part, "")
    return cur


def _row_qid(row: dict) -> str:
    inputs = row.get("inputs")
    nested_qid = inputs.get("question_id") if isinstance(inputs, dict) else ""
    return str(
        row.get("inputs.question_id")
        or row.get("question_id")
        or row.get("qid")
        or row.get("id")
        or nested_qid
        or ""
    )


def _iter_text_values(value: object) -> Iterable[str]:
    if isinstance(value, str):
        if value.strip():
            yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _iter_text_values(child)
    elif isinstance(value, (list, tuple, set)):
        for child in value:
            yield from _iter_text_values(child)


def _asi_metadata_surface(row: dict) -> set[str]:
    surface: set[str] = set()
    for key, value in (row or {}).items():
        if not isinstance(value, dict):
            continue
        if not isinstance(key, str):
            continue
        if not (key.endswith("/metadata") or key.endswith(".metadata")):
            continue
        judge = key.rsplit("/", 1)[0].rsplit(".", 1)[0]
        if judge in _IGNORED_METADATA_PREFIXES:
            continue
        for meta_key in (
            "failure_type",
            "wrong_clause",
            "blame_set",
            "counterfactual_fix",
            "expected_objects",
            "actual_objects",
            "rca_kind",
            "patch_family",
        ):
            for text in _iter_text_values(value.get(meta_key)):
                for tok in _IDENT_RE.findall(text):
                    surface.add(_normalize(tok))
    return surface


def extract_patch_targets(patch: dict) -> set[str]:
    """Return identifiers a patch claims to influence.

    Each value is split on whitespace to handle multi-word ``target``
    entries (e.g. ``"TIME GROUPING"`` becomes ``{"time grouping",
    "time", "grouping"}``) — we keep the joined form for exact-phrase
    matches and the parts for fuzzy ones.
    """
    targets: set[str] = set()
    for key in _PATCH_TARGET_KEYS:
        val = patch.get(key)
        if not isinstance(val, str) or not val.strip():
            continue
        joined = _normalize(val)
        targets.add(joined)
        if "." in joined:
            for dotted_part in joined.split("."):
                dotted_part = dotted_part.strip()
                if dotted_part:
                    targets.add(dotted_part)
        for part in _IDENT_RE.findall(joined):
            if part:
                targets.add(part)
    return targets


def extract_failure_surface(row: dict) -> set[str]:
    """Return identifiers + NL tokens visible in a failing eval row.

    Tries sqlglot first to recover columns, functions, and tables;
    falls back to regex on the raw SQL string when sqlglot can't
    parse. The NL response is always tokenized via regex.
    """
    surface: set[str] = set()
    try:
        import sqlglot
        from sqlglot import exp as _exp  # noqa: N812
    except Exception:
        sqlglot = None  # type: ignore[assignment]

    for sql_key in _FAILURE_SURFACE_SQL_KEYS:
        sql = row.get(sql_key) or ""
        if not isinstance(sql, str) or not sql.strip():
            continue

        if sqlglot is not None:
            try:
                parsed = sqlglot.parse_one(sql, read="databricks")
                if parsed is not None:
                    for col in parsed.find_all(_exp.Column):
                        if getattr(col, "name", None):
                            surface.add(_normalize(col.name))
                    for fn in parsed.find_all(_exp.Func):
                        try:
                            name = fn.sql_name()
                            if name:
                                surface.add(_normalize(name))
                        except Exception:
                            continue
                    for tbl in parsed.find_all(_exp.Table):
                        if getattr(tbl, "name", None):
                            surface.add(_normalize(tbl.name))
            except Exception:
                logger.debug(
                    "sqlglot parse failed for %s; using regex fallback",
                    sql_key,
                    exc_info=True,
                )

        # Regex fallback always runs — it costs nothing and catches
        # tokens sqlglot may miss (e.g. metric_view aliases inside
        # ``MEASURE(...)``).
        for tok in _IDENT_RE.findall(sql):
            if tok:
                surface.add(_normalize(tok))

    nested_sqls = (
        _nested_get(row, ("inputs", "expected_sql")),
        _nested_get(row, ("request", "expected_sql")),
        _nested_get(row, ("outputs", "predictions", "sql")),
        _nested_get(row, ("outputs", "predictions", "query")),
        _nested_get(row, ("response", "sql")),
    )
    for sql in nested_sqls:
        if not isinstance(sql, str) or not sql.strip():
            continue
        if sqlglot is not None:
            try:
                parsed = sqlglot.parse_one(sql, read="databricks")
                if parsed is not None:
                    for col in parsed.find_all(_exp.Column):
                        if getattr(col, "name", None):
                            surface.add(_normalize(col.name))
                    for tbl in parsed.find_all(_exp.Table):
                        if getattr(tbl, "name", None):
                            surface.add(_normalize(tbl.name))
                    for fn in parsed.find_all(_exp.Func):
                        try:
                            name = fn.sql_name()
                            if name:
                                surface.add(_normalize(name))
                        except Exception:
                            continue
            except Exception:
                logger.debug("sqlglot parse failed for nested SQL", exc_info=True)
        for tok in _IDENT_RE.findall(sql):
            if tok:
                surface.add(_normalize(tok))

    for nl_key in _FAILURE_SURFACE_NL_KEYS:
        nl = row.get(nl_key) or ""
        if not isinstance(nl, str) or not nl.strip():
            continue
        for tok in _IDENT_RE.findall(nl):
            if tok:
                surface.add(_normalize(tok))

    nested_nl_values = (
        _nested_get(row, ("inputs", "question")),
        _nested_get(row, ("outputs", "predictions", "response_text")),
    )
    for nl in nested_nl_values:
        if isinstance(nl, str):
            for tok in _IDENT_RE.findall(nl):
                surface.add(_normalize(tok))

    surface |= _asi_metadata_surface(row)

    return surface


def relevance_score(patch: dict, failing_rows: Iterable[dict]) -> float:
    """Fraction of patch targets that appear in any failing row's surface.

    Returns ``0.0`` when the patch has no targets or no failing rows
    are supplied. Always in ``[0.0, 1.0]``.
    """
    targets = extract_patch_targets(patch)
    if not targets:
        return 0.0
    rows = list(failing_rows or [])
    if not rows:
        return 0.0
    union_surface: set[str] = set()
    for row in rows:
        union_surface |= extract_failure_surface(row)
    if not union_surface:
        return 0.0
    overlap = targets & union_surface
    return len(overlap) / len(targets)


def explain_relevance(patch: dict, failing_rows: Iterable[dict]) -> dict:
    """Return debug details for why a patch did or did not ground.

    This mirrors :func:`relevance_score` while preserving the target,
    surface, overlap, and missing-target sets for audit rows and local
    troubleshooting.
    """
    targets = extract_patch_targets(patch)
    rows = list(failing_rows or [])
    union_surface: set[str] = set()
    for row in rows:
        union_surface |= extract_failure_surface(row)
    overlap = targets & union_surface
    missing = targets - union_surface
    score = (len(overlap) / len(targets)) if targets and rows and union_surface else 0.0
    return {
        "score": score,
        "targets": sorted(targets),
        "surface": sorted(union_surface),
        "overlap": sorted(overlap),
        "missing_targets": sorted(missing),
    }


def _filter_rows_for_qids(rows: Iterable[dict], qids: Iterable[str] | None) -> list[dict]:
    wanted = {str(q) for q in (qids or []) if str(q)}
    if not wanted:
        return [r for r in rows or [] if isinstance(r, dict)]
    out: list[dict] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if _row_qid(row) in wanted:
            out.append(row)
    return out


def causal_relevance_score(
    patch: dict,
    failure_rows: Iterable[dict],
    *,
    target_qids: Iterable[str] | None = None,
) -> float:
    """Score patch relevance against its causal target rows and ASI surface."""
    qids = tuple(target_qids or patch.get("target_qids") or ())
    scoped_rows = _filter_rows_for_qids(failure_rows, qids)
    return relevance_score(patch, scoped_rows)


def explain_causal_relevance(
    patch: dict,
    failure_rows: Iterable[dict],
    *,
    target_qids: Iterable[str] | None = None,
) -> dict:
    """Debug causal grounding with qid scope and ASI-enriched surfaces."""
    qids = tuple(target_qids or patch.get("target_qids") or ())
    scoped_rows = _filter_rows_for_qids(failure_rows, qids)
    details = explain_relevance(patch, scoped_rows)
    details["target_qids"] = list(qids)
    details["scoped_row_count"] = len(scoped_rows)
    return details


def select_patch_bundle(
    proposals: list[dict],
    *,
    max_patches: int,
    min_relevance: float = 0.0,
    failing_rows_by_proposal: dict[str, list[dict]] | None = None,
) -> list[dict]:
    """Drop ungrounded proposals; keep up to ``max_patches`` survivors.

    Order: by relevance score DESC (stable within a tie). The harness
    is responsible for any upstream diversity sorting; this function
    only enforces the relevance floor + size cap.

    When a proposal does not carry a precomputed ``relevance_score``,
    we compute it from ``failing_rows_by_proposal[proposal['id']]``.
    Empty input is a no-op.
    """
    if not proposals:
        return []
    rows_by_proposal = failing_rows_by_proposal or {}
    scored: list[tuple[dict, float]] = []
    for p in proposals:
        score = p.get("relevance_score")
        if score is None:
            failing = rows_by_proposal.get(str(p.get("id", "")), [])
            score = relevance_score(p, failing)
        scored.append((p, float(score)))

    grounded = [(p, s) for p, s in scored if s >= min_relevance]
    # Stable sort: Python's sort is stable, so equal-relevance
    # proposals retain their incoming order.
    grounded.sort(key=lambda ps: -ps[1])
    return [p for p, _s in grounded[:max_patches]]
