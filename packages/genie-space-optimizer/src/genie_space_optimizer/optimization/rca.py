"""Typed RCA ledger primitives for lever-loop planning.

The ledger preserves judge, SQL, and regression evidence as patchable
RCA findings. It sits between evaluation/clustering and strategist
proposal generation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable


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
    target_regressed_qids: tuple[str, ...] = ()
    global_regressed_qids: tuple[str, ...] = ()
    regressed_qids: tuple[str, ...] = ()


_RCA_KIND_TO_LEVERS: dict[RcaKind, tuple[int, ...]] = {
    RcaKind.METRIC_VIEW_ROUTING_CONFUSION: (1, 5),
    RcaKind.MEASURE_SWAP: (1, 5),
    RcaKind.CANONICAL_DIMENSION_MISSED: (1, 5),
    RcaKind.MISSING_REQUIRED_DIMENSION: (1, 5),
    RcaKind.EXTRA_DEFENSIVE_FILTER: (3, 5),
    RcaKind.UNKNOWN: (5,),
}


def recommended_levers_for_rca_kind(kind: RcaKind) -> tuple[int, ...]:
    return _RCA_KIND_TO_LEVERS.get(kind, (5,))


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
                recommended_levers=recommended_levers_for_rca_kind(kind),
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
            recommended_levers=recommended_levers_for_rca_kind(kind),
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
            recommended_levers=recommended_levers_for_rca_kind(kind),
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
            recommended_levers=recommended_levers_for_rca_kind(kind),
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
            recommended_levers=recommended_levers_for_rca_kind(kind),
            patch_family="avoid_unrequested_defensive_filters",
            target_qids=(qid,),
        ))

    return findings


def _theme_patch_base(f: RcaFinding) -> dict:
    return {
        "rca_id": f.rca_id,
        "patch_family": f.patch_family,
        "target_qids": list(f.target_qids),
        "source": "rca_theme",
        "confidence": f.confidence,
    }


def _objects_for_theme(patches: Iterable[dict]) -> tuple[str, ...]:
    touched: list[str] = []
    for p in patches:
        for key in (
            "target",
            "target_object",
            "table",
            "table_id",
            "column",
            "instruction_section",
        ):
            val = p.get(key)
            if isinstance(val, str) and val:
                touched.append(val)
    return tuple(dict.fromkeys(touched))


def compile_patch_themes(
    findings: list[RcaFinding],
    *,
    metadata_snapshot: dict | None = None,
) -> list[RcaPatchTheme]:
    """Compile RCA findings into coherent patch themes.

    This function proposes patch intent only. Existing validators,
    grounding, leakage checks, and appliers still decide what persists.
    """
    del metadata_snapshot  # Reserved for catalog-aware patch shaping.
    themes: list[RcaPatchTheme] = []
    for f in findings:
        base = _theme_patch_base(f)
        patches: list[dict] = []

        if f.rca_kind is RcaKind.METRIC_VIEW_ROUTING_CONFUSION:
            for obj in f.expected_objects:
                if obj.startswith("mv_"):
                    patches.append({
                        **base,
                        "type": "update_description",
                        "target": obj,
                        "lever": 1,
                        "intent": (
                            "mark as default asset for unqualified business term"
                        ),
                    })
                elif obj:
                    patches.append({
                        **base,
                        "type": "update_column_description",
                        "column": obj,
                        "lever": 1,
                        "intent": "strengthen intended measure semantics",
                    })
            for obj in f.actual_objects:
                if obj.startswith("mv_"):
                    patches.append({
                        **base,
                        "type": "update_description",
                        "target": obj,
                        "lever": 1,
                        "intent": "clarify narrower channel-specific use",
                    })
                elif obj:
                    patches.append({
                        **base,
                        "type": "update_column_description",
                        "column": obj,
                        "lever": 1,
                        "intent": (
                            "clarify do-not-use-unless-explicit semantics"
                        ),
                    })

        elif f.rca_kind is RcaKind.MEASURE_SWAP:
            for obj in f.expected_objects:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "column": obj,
                    "lever": 1,
                    "intent": (
                        "strengthen intended measure description and synonyms"
                    ),
                })

        elif f.rca_kind is RcaKind.CANONICAL_DIMENSION_MISSED:
            for obj in f.expected_objects:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "column": obj,
                    "lever": 1,
                    "intent": (
                        "use canonical dimension instead of derived expression"
                    ),
                })

        elif f.rca_kind is RcaKind.MISSING_REQUIRED_DIMENSION:
            for obj in f.expected_objects:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "column": obj,
                    "lever": 1,
                    "intent": (
                        "required grouping dimension for comparison questions"
                    ),
                })

        elif f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER:
            patches.append({
                **base,
                "type": "add_instruction",
                "target": "QUERY CONSTRUCTION",
                "instruction_section": "QUERY CONSTRUCTION",
                "lever": 5,
                "intent": "do not add IS NOT NULL filters unless requested",
            })

        if not patches:
            continue
        themes.append(RcaPatchTheme(
            rca_id=f.rca_id,
            rca_kind=f.rca_kind,
            patch_family=f.patch_family,
            patches=tuple(patches),
            target_qids=f.target_qids,
            touched_objects=_objects_for_theme(patches),
            risk_level="medium",
            confidence=f.confidence,
            evidence_summary="; ".join(e.detail for e in f.evidence[:3]),
        ))
    return themes


def detect_theme_conflicts(themes: list[RcaPatchTheme]) -> list[ThemeConflict]:
    conflicts: list[ThemeConflict] = []
    owner: dict[str, str] = {}
    for theme in themes:
        for obj in theme.touched_objects:
            key = obj.lower()
            if key in owner and owner[key] != theme.rca_id:
                conflicts.append(ThemeConflict(
                    left_rca_id=owner[key],
                    right_rca_id=theme.rca_id,
                    object_id=obj,
                    reason="multiple RCA themes touch the same object",
                ))
            else:
                owner[key] = theme.rca_id
    return conflicts


def _theme_field(theme: Any, key: str, default: Any = "") -> Any:
    if isinstance(theme, dict):
        return theme.get(key, default)
    return getattr(theme, key, default)


def attribute_theme_outcomes(
    themes: list[RcaPatchTheme],
    *,
    prev_failure_qids: set[str],
    new_failure_qids: set[str],
) -> list[ThemeAttribution]:
    out: list[ThemeAttribution] = []
    prev_failures = {str(q) for q in (prev_failure_qids or set())}
    new_failures = {str(q) for q in (new_failure_qids or set())}
    global_regressions = sorted(new_failures - prev_failures)
    for theme in themes or []:
        targets = {str(q) for q in (_theme_field(theme, "target_qids", ()) or ())}
        fixed = sorted(targets & prev_failures - new_failures)
        still = sorted(targets & new_failures)
        target_regressions = sorted(targets & (new_failures - prev_failures))
        out.append(ThemeAttribution(
            rca_id=str(_theme_field(theme, "rca_id", "")),
            target_qids=tuple(sorted(targets)),
            fixed_qids=tuple(fixed),
            still_failing_qids=tuple(still),
            target_regressed_qids=tuple(target_regressions),
            global_regressed_qids=tuple(global_regressions),
            regressed_qids=tuple(target_regressions),
        ))
    return out


def select_compatible_themes(
    themes: list[RcaPatchTheme],
    *,
    max_themes: int,
    max_patches: int,
) -> list[RcaPatchTheme]:
    """Select a high-confidence non-conflicting set of RCA themes."""
    ordered = sorted(
        themes or [],
        key=lambda t: (
            -float(_theme_field(t, "confidence", 0.0) or 0.0),
            -len(_theme_field(t, "target_qids", ()) or ()),
            str(_theme_field(t, "rca_id", "")),
        ),
    )
    selected: list[RcaPatchTheme] = []
    touched: set[str] = set()
    patch_count = 0
    for theme in ordered:
        if len(selected) >= max_themes:
            break
        patches = _theme_field(theme, "patches", ()) or ()
        if patch_count + len(patches) > max_patches:
            continue
        theme_touched = {
            str(obj).lower()
            for obj in (_theme_field(theme, "touched_objects", ()) or ())
        }
        if touched & theme_touched:
            continue
        selected.append(theme)
        touched |= theme_touched
        patch_count += len(patches)
    return selected


def themes_for_strategy_context(
    themes: list[RcaPatchTheme],
    *,
    enable_selection: bool,
    max_themes: int,
    max_patches: int,
) -> list[RcaPatchTheme]:
    """Return all themes or a compatible subset for strategist context.

    This is prompt-context selection only. It does not mechanically
    constrain proposal generation, grounding, or apply.
    """
    if not enable_selection:
        return list(themes or [])
    return select_compatible_themes(
        themes,
        max_themes=max_themes,
        max_patches=max_patches,
    )


def rca_findings_from_regression_insights(
    insights: Iterable[Any],
) -> list[RcaFinding]:
    findings: list[RcaFinding] = []
    for ins in insights or []:
        if getattr(ins, "insight_type", "") != "column_confusion":
            continue
        qid = str(getattr(ins, "question_id", "") or "")
        intended = str(getattr(ins, "intended_column", "") or "")
        confused = str(getattr(ins, "confused_column", "") or "")
        if not qid or not intended or not confused:
            continue
        kind = RcaKind.MEASURE_SWAP
        findings.append(RcaFinding(
            rca_id=_mk_id(qid, kind),
            question_id=qid,
            rca_kind=kind,
            confidence=float(getattr(ins, "confidence", 0.0) or 0.0),
            expected_objects=(intended,),
            actual_objects=(confused,),
            evidence=(
                RcaEvidence(
                    "regression_mining",
                    str(getattr(ins, "rationale", "") or "column confusion"),
                    0.8,
                ),
            ),
            recommended_levers=recommended_levers_for_rca_kind(kind),
            patch_family="contrastive_measure_disambiguation",
            target_qids=(qid,),
        ))
    return findings


def _unique_tuple(values: Iterable[Any]) -> tuple:
    out: list[Any] = []
    seen: set[Any] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


def _dedupe_rca_findings(findings: Iterable[RcaFinding]) -> list[RcaFinding]:
    by_id: dict[str, RcaFinding] = {}
    for finding in findings or []:
        if not isinstance(finding, RcaFinding):
            continue
        existing = by_id.get(finding.rca_id)
        if existing is None:
            by_id[finding.rca_id] = finding
            continue
        by_id[finding.rca_id] = RcaFinding(
            rca_id=existing.rca_id,
            question_id=existing.question_id or finding.question_id,
            rca_kind=existing.rca_kind,
            confidence=max(float(existing.confidence), float(finding.confidence)),
            expected_objects=_unique_tuple(
                (*existing.expected_objects, *finding.expected_objects),
            ),
            actual_objects=_unique_tuple(
                (*existing.actual_objects, *finding.actual_objects),
            ),
            evidence=_unique_tuple((*existing.evidence, *finding.evidence)),
            recommended_levers=tuple(sorted(set(
                (*existing.recommended_levers, *finding.recommended_levers),
            ))),
            patch_family=existing.patch_family or finding.patch_family,
            target_qids=_unique_tuple((*existing.target_qids, *finding.target_qids)),
        )
    return list(by_id.values())


def build_rca_ledger(
    rows: list[dict],
    *,
    metadata_snapshot: dict | None = None,
    extra_findings: Iterable[RcaFinding] | None = None,
) -> dict[str, Any]:
    findings: list[RcaFinding] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        findings.extend(
            extract_rca_findings_from_row(
                row,
                metadata_snapshot=metadata_snapshot,
            )
        )
    findings.extend(f for f in (extra_findings or []) if isinstance(f, RcaFinding))
    findings = _dedupe_rca_findings(findings)
    themes = compile_patch_themes(findings, metadata_snapshot=metadata_snapshot)
    conflicts = detect_theme_conflicts(themes)
    return {
        "findings": findings,
        "themes": themes,
        "conflicts": conflicts,
        "finding_count": len(findings),
        "theme_count": len(themes),
        "conflict_count": len(conflicts),
    }
