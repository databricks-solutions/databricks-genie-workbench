"""Deterministic teaching-safety gates for example SQLs.

These checks run *before* any LLM judge. They are pure functions over
``(question, sql, metadata_snapshot)`` and return a structured result
describing why a candidate is or is not safe to install as a teaching
example.

Lives separately from ``leakage`` (which is benchmark-isolation) and
from ``arbiter`` (LLM-based) so the cheap deterministic gates can be
audited and unit-tested in isolation.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TeachingSafetyResult:
    """Outcome of the deterministic teaching-safety gates.

    ``safe=True`` and an empty ``reasons`` list means every gate passed.
    ``safe=False`` means at least one gate failed; ``reasons`` contains
    machine-friendly tags (for counters/banners) and short human notes.
    """

    safe: bool
    reasons: list[str] = field(default_factory=list)


_FQN_RE = re.compile(r"\b([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\b")
_MEASURE_RE = re.compile(r"\bMEASURE\s*\(", re.IGNORECASE)
_DOUBLE_MEASURE_RE = re.compile(r"\bMEASURE\s*\(\s*MEASURE\s*\(", re.IGNORECASE)
_JOIN_RE = re.compile(
    r"\bJOIN\s+([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\b",
    re.IGNORECASE,
)
_FROM_RE = re.compile(
    r"\bFROM\s+([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\b",
    re.IGNORECASE,
)
_LITERAL_RE = re.compile(r"['\"]([^'\"]{2,})['\"]")
_WHERE_RE = re.compile(
    r"\bWHERE\b(.*?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|;|$)",
    re.IGNORECASE | re.DOTALL,
)


def _referenced_assets(sql: str) -> set[str]:
    return {m.group(1) for m in _FQN_RE.finditer(sql)}


def _asset_type(snapshot: dict, fqn: str) -> str:
    sem = snapshot.get("_asset_semantics") or {}
    info = sem.get(fqn)
    if isinstance(info, dict):
        return str(info.get("asset_type") or "unknown")
    return "unknown"


def _registered_join_pairs(snapshot: dict) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    inst = snapshot.get("instructions") or {}
    for spec in (inst.get("join_specs") or []):
        if not isinstance(spec, dict):
            continue
        lt = ((spec.get("left") or {}).get("identifier") or "")
        rt = ((spec.get("right") or {}).get("identifier") or "")
        if lt and rt:
            pairs.add(tuple(sorted((lt, rt))))
    for fk in (snapshot.get("_uc_foreign_keys") or []):
        if not isinstance(fk, dict):
            continue
        a = str(fk.get("left_table") or "")
        b = str(fk.get("right_table") or "")
        if a and b:
            pairs.add(tuple(sorted((a, b))))
    return pairs


def _extract_join_pairs_from_sql(sql: str) -> set[tuple[str, str]]:
    from_m = _FROM_RE.search(sql)
    if not from_m:
        return set()
    base = from_m.group(1)
    pairs: set[tuple[str, str]] = set()
    for jm in _JOIN_RE.finditer(sql):
        right = jm.group(1)
        pairs.add(tuple(sorted((base, right))))
    return pairs


def _extra_filters(question: str, sql: str) -> list[str]:
    where_m = _WHERE_RE.search(sql)
    if not where_m:
        return []
    where_clause = where_m.group(1)
    literals = [m.group(1) for m in _LITERAL_RE.finditer(where_clause)]
    q_lower = question.lower()
    return [lit for lit in literals if lit.lower() not in q_lower]


def check_teaching_safety(
    *,
    question: str,
    sql: str,
    metadata_snapshot: dict,
) -> TeachingSafetyResult:
    """Run all deterministic teaching-safety gates on a candidate.

    Gates checked, in order: anti-pattern syntax, MV/table routing,
    unknown-asset reference, unregistered joins, extra-filter risk.
    """
    reasons: list[str] = []

    if _DOUBLE_MEASURE_RE.search(sql):
        reasons.append("anti_pattern_double_measure: nested MEASURE() detected")

    if re.search(r"\bMEASURE\s*\(\s*\)", sql, flags=re.IGNORECASE):
        reasons.append("anti_pattern_empty_measure: MEASURE() with no argument")

    referenced = _referenced_assets(sql)
    semantics = metadata_snapshot.get("_asset_semantics") or {}
    for fqn in referenced:
        if fqn not in semantics:
            reasons.append(f"unknown_asset: {fqn} not in _asset_semantics")

    has_measure = bool(_MEASURE_RE.search(sql))
    for fqn in referenced:
        atype = _asset_type(metadata_snapshot, fqn)
        if atype == "metric_view" and not has_measure:
            reasons.append(
                f"metric_view_without_measure: {fqn} referenced without MEASURE()"
            )
        if atype == "table" and has_measure:
            reasons.append(
                f"table_used_with_measure: {fqn} is a table but SQL uses MEASURE()"
            )

    sql_pairs = _extract_join_pairs_from_sql(sql)
    if sql_pairs:
        registered = _registered_join_pairs(metadata_snapshot)
        for pair in sql_pairs:
            if pair not in registered:
                reasons.append(
                    f"unregistered_join: {pair[0]} <-> {pair[1]} "
                    "not in join_specs or UC FK"
                )

    extras = _extra_filters(question, sql)
    if extras:
        reasons.append(
            "extra_filter_not_in_question: literals "
            + ", ".join(repr(x) for x in extras[:3])
        )

    return TeachingSafetyResult(safe=not reasons, reasons=reasons)
