"""Typed RCA ledger primitives for lever-loop planning.

The ledger preserves judge, SQL, and regression evidence as patchable
RCA findings. It sits between evaluation/clustering and strategist
proposal generation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any


class RcaKind(str, Enum):
    METRIC_VIEW_ROUTING_CONFUSION = "metric_view_routing_confusion"
    MEASURE_SWAP = "measure_swap"
    CANONICAL_DIMENSION_MISSED = "canonical_dimension_missed"
    MISSING_REQUIRED_DIMENSION = "missing_required_dimension"
    EXTRA_DEFENSIVE_FILTER = "extra_defensive_filter"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RcaEvidence:
    source: str
    detail: str
    confidence: float = 0.0


@dataclass(frozen=True)
class RcaFinding:
    rca_id: str
    question_id: str
    rca_kind: RcaKind
    confidence: float
    expected_objects: tuple[str, ...] = ()
    actual_objects: tuple[str, ...] = ()
    evidence: tuple[RcaEvidence, ...] = ()
    recommended_levers: tuple[int, ...] = ()
    patch_family: str = ""
    target_qids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RcaPatchTheme:
    rca_id: str
    rca_kind: RcaKind
    patch_family: str
    patches: tuple[dict, ...]
    target_qids: tuple[str, ...]
    touched_objects: tuple[str, ...]
    risk_level: str = "medium"
    confidence: float = 0.0
    evidence_summary: str = ""


@dataclass(frozen=True)
class ThemeConflict:
    left_rca_id: str
    right_rca_id: str
    object_id: str
    reason: str


@dataclass(frozen=True)
class ThemeAttribution:
    rca_id: str
    target_qids: tuple[str, ...]
    fixed_qids: tuple[str, ...] = ()
    still_failing_qids: tuple[str, ...] = ()
    regressed_qids: tuple[str, ...] = ()


_MEASURE_RE = re.compile(
    r"MEASURE\s*\(\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*\)",
    re.IGNORECASE,
)
_FROM_RE = re.compile(r"\bFROM\s+`?([a-zA-Z0-9_.$`]+)`?", re.IGNORECASE)
_GROUP_BY_RE = re.compile(
    r"\bGROUP\s+BY\s+(.+?)(?:\bORDER\b|\bHAVING\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_WHERE_RE = re.compile(
    r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bHAVING\b|$)",
    re.IGNORECASE | re.DOTALL,
)


def _first_str(row: dict, *keys: str) -> str:
    for key in keys:
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val
    return ""


def _qid(row: dict) -> str:
    inputs = row.get("inputs")
    input_qid = inputs.get("question_id") if isinstance(inputs, dict) else ""
    return str(
        row.get("inputs.question_id")
        or row.get("inputs/question_id")
        or row.get("question_id")
        or row.get("id")
        or input_qid
        or ""
    )


def _expected_sql(row: dict) -> str:
    return _first_str(row, "inputs.expected_sql", "inputs/expected_sql", "expected_sql")


def _generated_sql(row: dict) -> str:
    return _first_str(
        row,
        "outputs.predictions.sql",
        "outputs/predictions/sql",
        "generated_sql",
        "genie_sql",
    )


def _measures(sql: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(m.lower() for m in _MEASURE_RE.findall(sql or "")))


def _tables(sql: str) -> tuple[str, ...]:
    out: list[str] = []
    for raw in _FROM_RE.findall(sql or ""):
        clean = raw.replace("`", "").split(".")[-1].lower()
        if clean:
            out.append(clean)
    return tuple(dict.fromkeys(out))


def _group_by_text(sql: str) -> str:
    m = _GROUP_BY_RE.search(sql or "")
    return m.group(1).lower() if m else ""


def _where_text(sql: str) -> str:
    m = _WHERE_RE.search(sql or "")
    return m.group(1).lower() if m else ""


def _mk_id(qid: str, kind: RcaKind) -> str:
    safe_qid = re.sub(r"[^a-zA-Z0-9_]+", "_", qid or "unknown")
    return f"rca_{safe_qid}_{kind.value}"


def extract_rca_findings_from_row(
    row: dict,
    *,
    metadata_snapshot: dict | None = None,
) -> list[RcaFinding]:
    """Extract typed RCA findings from one failed eval row.

    The first version focuses on the high-value failure shapes observed
    in the retail run. It is deterministic and never calls an LLM.
    """
    del metadata_snapshot  # Reserved for richer catalog-aware RCA.
    qid = _qid(row)
    expected = _expected_sql(row)
    generated = _generated_sql(row)
    if not qid or not expected or not generated:
        return []

    findings: list[RcaFinding] = []
    exp_measures = _measures(expected)
    gen_measures = _measures(generated)
    exp_tables = _tables(expected)
    gen_tables = _tables(generated)

    if exp_tables and gen_tables and set(exp_tables) != set(gen_tables):
        if exp_measures or gen_measures:
            kind = RcaKind.METRIC_VIEW_ROUTING_CONFUSION
            findings.append(RcaFinding(
                rca_id=_mk_id(qid, kind),
                question_id=qid,
                rca_kind=kind,
                confidence=0.85,
                expected_objects=tuple(exp_tables + exp_measures),
                actual_objects=tuple(gen_tables + gen_measures),
                evidence=(
                    RcaEvidence(
                        source="sql_diff",
                        detail=(
                            "expected and generated SQL use different metric "
                            "views or tables"
                        ),
                        confidence=0.85,
                    ),
                ),
                recommended_levers=(1, 5),
                patch_family="contrastive_metric_routing",
                target_qids=(qid,),
            ))

    if exp_measures and gen_measures and set(exp_measures) != set(gen_measures):
        kind = RcaKind.MEASURE_SWAP
        findings.append(RcaFinding(
            rca_id=_mk_id(qid, kind),
            question_id=qid,
            rca_kind=kind,
            confidence=0.8,
            expected_objects=exp_measures,
            actual_objects=gen_measures,
            evidence=(
                RcaEvidence(
                    source="sql_diff",
                    detail="expected and generated SQL use different measures",
                    confidence=0.8,
                ),
            ),
            recommended_levers=(1, 5),
            patch_family="contrastive_measure_disambiguation",
            target_qids=(qid,),
        ))

    if "calendar_month" in expected.lower() and "month(" in generated.lower():
        kind = RcaKind.CANONICAL_DIMENSION_MISSED
        findings.append(RcaFinding(
            rca_id=_mk_id(qid, kind),
            question_id=qid,
            rca_kind=kind,
            confidence=0.9,
            expected_objects=("calendar_month",),
            actual_objects=("MONTH(full_date)",),
            evidence=(
                RcaEvidence(
                    source="sql_shape",
                    detail=(
                        "generated SQL derives month instead of using "
                        "canonical calendar_month"
                    ),
                    confidence=0.9,
                ),
            ),
            recommended_levers=(1, 5),
            patch_family="canonical_dimension_guidance",
            target_qids=(qid,),
        ))

    exp_group = _group_by_text(expected)
    gen_group = _group_by_text(generated)
    if "time_window" in exp_group and "time_window" not in gen_group:
        kind = RcaKind.MISSING_REQUIRED_DIMENSION
        findings.append(RcaFinding(
            rca_id=_mk_id(qid, kind),
            question_id=qid,
            rca_kind=kind,
            confidence=0.85,
            expected_objects=("time_window",),
            actual_objects=(),
            evidence=(
                RcaEvidence(
                    source="sql_shape",
                    detail=(
                        "expected SQL groups by time_window but generated "
                        "SQL does not"
                    ),
                    confidence=0.85,
                ),
            ),
            recommended_levers=(1, 5),
            patch_family="required_dimension_guidance",
            target_qids=(qid,),
        ))

    exp_where = _where_text(expected)
    gen_where = _where_text(generated)
    if "is not null" in gen_where and "is not null" not in exp_where:
        kind = RcaKind.EXTRA_DEFENSIVE_FILTER
        findings.append(RcaFinding(
            rca_id=_mk_id(qid, kind),
            question_id=qid,
            rca_kind=kind,
            confidence=0.8,
            expected_objects=(),
            actual_objects=("IS NOT NULL",),
            evidence=(
                RcaEvidence(
                    source="sql_shape",
                    detail=(
                        "generated SQL adds defensive IS NOT NULL filters "
                        "absent from expected SQL"
                    ),
                    confidence=0.8,
                ),
            ),
            recommended_levers=(3, 5),
            patch_family="avoid_unrequested_defensive_filters",
            target_qids=(qid,),
        ))

    return findings
