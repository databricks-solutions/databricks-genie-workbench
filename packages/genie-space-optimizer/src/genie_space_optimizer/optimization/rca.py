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

from genie_space_optimizer.optimization.eval_row_access import (
    iter_asi_metadata as _iter_asi_metadata,
    row_expected_sql as _expected_sql,
    row_generated_sql as _generated_sql,
    row_qid as _qid,
)


class RcaKind(str, Enum):
    METRIC_VIEW_ROUTING_CONFUSION = "metric_view_routing_confusion"
    MEASURE_SWAP = "measure_swap"
    CANONICAL_DIMENSION_MISSED = "canonical_dimension_missed"
    MISSING_REQUIRED_DIMENSION = "missing_required_dimension"
    EXTRA_DEFENSIVE_FILTER = "extra_defensive_filter"
    JOIN_SPEC_MISSING_OR_WRONG = "join_spec_missing_or_wrong"
    FILTER_LOGIC_MISMATCH = "filter_logic_mismatch"
    GRAIN_OR_GROUPING_MISMATCH = "grain_or_grouping_mismatch"
    SYNONYM_OR_ENTITY_MATCH_MISSING = "synonym_or_entity_match_missing"
    SQL_EXPRESSION_MISSING = "sql_expression_missing"
    EXAMPLE_SQL_SHAPE_NEEDED = "example_sql_shape_needed"
    FUNCTION_OR_TVF_NOT_INVOKED = "function_or_tvf_not_invoked"
    FUNCTION_ROUTING_MISMATCH = "function_routing_mismatch"
    TOP_N_CARDINALITY_COLLAPSE = "top_n_cardinality_collapse"
    TIME_WINDOW_LOGIC_MISMATCH = "time_window_logic_mismatch"
    ASSET_TYPE_ROUTING_MISMATCH = "asset_type_routing_mismatch"
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
    recommended_levers: tuple[int, ...] = ()


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
    RcaKind.METRIC_VIEW_ROUTING_CONFUSION: (1, 2, 5),
    RcaKind.MEASURE_SWAP: (1, 2, 5, 6),
    RcaKind.CANONICAL_DIMENSION_MISSED: (1, 2, 5, 6),
    RcaKind.MISSING_REQUIRED_DIMENSION: (1, 5, 6),
    RcaKind.EXTRA_DEFENSIVE_FILTER: (5,),
    RcaKind.JOIN_SPEC_MISSING_OR_WRONG: (4, 5),
    RcaKind.FILTER_LOGIC_MISMATCH: (2, 5, 6),
    RcaKind.GRAIN_OR_GROUPING_MISMATCH: (1, 5, 6),
    RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING: (1,),
    RcaKind.SQL_EXPRESSION_MISSING: (6,),
    RcaKind.EXAMPLE_SQL_SHAPE_NEEDED: (5,),
    RcaKind.FUNCTION_OR_TVF_NOT_INVOKED: (3, 5, 6),
    RcaKind.FUNCTION_ROUTING_MISMATCH: (3, 5, 6),
    RcaKind.TOP_N_CARDINALITY_COLLAPSE: (1, 5, 6),
    RcaKind.TIME_WINDOW_LOGIC_MISMATCH: (2, 5, 6),
    RcaKind.ASSET_TYPE_ROUTING_MISMATCH: (5,),
    RcaKind.UNKNOWN: (5,),
}


def recommended_levers_for_rca_kind(kind: RcaKind) -> tuple[int, ...]:
    return _RCA_KIND_TO_LEVERS.get(kind, (5,))


_RCA_KIND_TO_PATCH_FAMILY: dict[RcaKind, str] = {
    RcaKind.METRIC_VIEW_ROUTING_CONFUSION: "contrastive_metric_routing",
    RcaKind.MEASURE_SWAP: "contrastive_measure_disambiguation",
    RcaKind.CANONICAL_DIMENSION_MISSED: "canonical_dimension_guidance",
    RcaKind.MISSING_REQUIRED_DIMENSION: "required_dimension_guidance",
    RcaKind.EXTRA_DEFENSIVE_FILTER: "avoid_unrequested_defensive_filters",
    RcaKind.JOIN_SPEC_MISSING_OR_WRONG: "join_spec_guidance",
    RcaKind.FILTER_LOGIC_MISMATCH: "filter_logic_guidance",
    RcaKind.GRAIN_OR_GROUPING_MISMATCH: "grain_grouping_guidance",
    RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING: "synonym_entity_matching_guidance",
    RcaKind.SQL_EXPRESSION_MISSING: "sql_expression_guidance",
    RcaKind.EXAMPLE_SQL_SHAPE_NEEDED: "example_sql_shape_guidance",
    RcaKind.FUNCTION_OR_TVF_NOT_INVOKED: "function_routing_guidance",
    RcaKind.FUNCTION_ROUTING_MISMATCH: "function_routing_guidance",
    RcaKind.TOP_N_CARDINALITY_COLLAPSE: "cardinality_preserving_top_n_guidance",
    RcaKind.TIME_WINDOW_LOGIC_MISMATCH: "time_window_logic_guidance",
    RcaKind.ASSET_TYPE_ROUTING_MISMATCH: "asset_type_routing_guidance",
    RcaKind.UNKNOWN: "generic_judge_guidance",
}


def patch_family_for_rca_kind(kind: RcaKind) -> str:
    return _RCA_KIND_TO_PATCH_FAMILY.get(kind, "generic_judge_guidance")


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


def _tuple_of_str(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value.strip() else ()
    if isinstance(value, Iterable):
        return tuple(str(v).strip() for v in value if str(v).strip())
    return ()


def _value_contains_function_or_tvf(*values: Any) -> bool:
    text = " ".join(str(v).lower() for v in values if v is not None)
    return (
        "tvf" in text
        or "udf" in text
        or "function" in text
        or "fn_" in text
        or "_fn_" in text
    )


def _safe_rca_kind(value: Any, failure_type: str = "", metadata: dict | None = None) -> RcaKind:
    raw = str(value or "").strip()
    if raw:
        try:
            return RcaKind(raw)
        except ValueError:
            pass
    metadata = metadata or {}
    failure = str(failure_type or "").strip().lower()
    if _value_contains_function_or_tvf(
        metadata.get("blame_set"),
        metadata.get("counterfactual_fix"),
        metadata.get("wrong_clause"),
        metadata.get("expected_objects"),
        metadata.get("actual_objects"),
    ):
        if failure in {
            "asset_routing_error",
            "wrong_column",
            "wrong_table",
            "missing_filter",
            "wrong_filter_condition",
            "other",
        }:
            return RcaKind.FUNCTION_OR_TVF_NOT_INVOKED
    if failure == "asset_routing_error":
        return RcaKind.ASSET_TYPE_ROUTING_MISMATCH
    if failure in {"plural_top_n_collapse", "top_n_cardinality_collapse"}:
        return RcaKind.TOP_N_CARDINALITY_COLLAPSE
    if failure in {"different_metric", "wrong_measure", "wrong_aggregation"}:
        return RcaKind.MEASURE_SWAP
    if failure in {"missing_join", "missing_join_spec", "wrong_join", "wrong_join_spec"}:
        return RcaKind.JOIN_SPEC_MISSING_OR_WRONG
    if failure in {"missing_filter", "wrong_filter", "wrong_filter_condition"}:
        return RcaKind.FILTER_LOGIC_MISMATCH
    if failure in {"missing_dimension", "wrong_grouping", "different_grain"}:
        return RcaKind.GRAIN_OR_GROUPING_MISMATCH
    if failure in {"missing_temporal_filter", "wrong_time_window", "time_window_logic_mismatch"}:
        return RcaKind.TIME_WINDOW_LOGIC_MISMATCH
    return RcaKind.UNKNOWN


def _asi_finding_from_metadata(
    qid: str,
    judge_name: str,
    metadata: dict,
) -> RcaFinding | None:
    failure_type = str(metadata.get("failure_type") or "").strip()
    if not qid or not failure_type:
        return None
    kind = _safe_rca_kind(metadata.get("rca_kind"), failure_type, metadata)
    expected_objects = _tuple_of_str(metadata.get("expected_objects"))
    actual_objects = _tuple_of_str(metadata.get("actual_objects"))
    if not expected_objects:
        expected_objects = _tuple_of_str(metadata.get("blame_set"))
    detail_parts = [
        f"judge={judge_name}",
        f"failure_type={failure_type}",
    ]
    if metadata.get("wrong_clause"):
        detail_parts.append(f"wrong_clause={metadata['wrong_clause']}")
    if metadata.get("counterfactual_fix"):
        detail_parts.append(str(metadata["counterfactual_fix"]))
    recommended = metadata.get("recommended_levers")
    if isinstance(recommended, Iterable) and not isinstance(recommended, str):
        levers = tuple(sorted({int(x) for x in recommended if str(x).isdigit()}))
    else:
        levers = recommended_levers_for_rca_kind(kind)
    return RcaFinding(
        rca_id=_mk_id(qid, kind),
        question_id=qid,
        rca_kind=kind,
        confidence=float(metadata.get("confidence") or 0.75),
        expected_objects=expected_objects,
        actual_objects=actual_objects,
        evidence=(
            RcaEvidence(
                source="judge_asi",
                detail="; ".join(detail_parts),
                confidence=float(metadata.get("confidence") or 0.75),
            ),
        ),
        recommended_levers=levers or recommended_levers_for_rca_kind(kind),
        patch_family=str(
            metadata.get("patch_family")
            or patch_family_for_rca_kind(kind)
        ),
        target_qids=(qid,),
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

    findings: list[RcaFinding] = []

    # Judge ASI metadata is always considered first — judges can fire
    # even when SQL is missing (NL-only failures, planner errors).
    if qid:
        for judge_name, metadata in _iter_asi_metadata(row):
            asi_finding = _asi_finding_from_metadata(qid, judge_name, metadata)
            if asi_finding is not None:
                findings.append(asi_finding)

    if not qid or not expected or not generated:
        return findings

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


_CLUSTER_ROOT_TO_RCA_KIND: dict[str, RcaKind] = {
    "plural_top_n_collapse": RcaKind.TOP_N_CARDINALITY_COLLAPSE,
    "top_n_cardinality_collapse": RcaKind.TOP_N_CARDINALITY_COLLAPSE,
    "time_window_pivot": RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
}


def _cluster_kind(cluster: dict) -> RcaKind | None:
    root = str(cluster.get("root_cause") or "").strip().lower()
    blame_text = " ".join(
        str(x).lower() for x in _tuple_of_str(cluster.get("asi_blame_set"))
    )
    fix_text = " ".join(
        str(x).lower()
        for x in _tuple_of_str(cluster.get("asi_counterfactual_fixes"))
    )
    if root in _CLUSTER_ROOT_TO_RCA_KIND:
        return _CLUSTER_ROOT_TO_RCA_KIND[root]
    if root == "wrong_filter_condition" and "time_window" in f"{blame_text} {fix_text}":
        return RcaKind.TIME_WINDOW_LOGIC_MISMATCH
    return None


def rca_findings_from_clusters(clusters: Iterable[dict]) -> list[RcaFinding]:
    """Promote cluster-resolved root causes into typed RCA findings.

    Failure clustering already labels structural defects (e.g.,
    ``plural_top_n_collapse``); the lever loop must carry that decision
    into the executable RCA ledger so patch families and grounding terms
    reflect the resolved root cause, not just per-row judge metadata.
    """
    findings: list[RcaFinding] = []
    for cluster in clusters or []:
        if not isinstance(cluster, dict):
            continue
        kind = _cluster_kind(cluster)
        if kind is None:
            continue
        qids = _tuple_of_str(cluster.get("question_ids"))
        if not qids:
            continue
        blame = _tuple_of_str(cluster.get("asi_blame_set"))
        fixes = _tuple_of_str(cluster.get("asi_counterfactual_fixes"))
        evidence_detail = (
            "; ".join(fixes[:3])
            or str(cluster.get("root_cause") or kind.value)
        )
        for qid in qids:
            findings.append(RcaFinding(
                rca_id=_mk_id(qid, kind),
                question_id=qid,
                rca_kind=kind,
                confidence=0.9,
                expected_objects=blame,
                actual_objects=(),
                evidence=(
                    RcaEvidence(
                        source="cluster_resolved_root_cause",
                        detail=evidence_detail,
                        confidence=0.9,
                    ),
                ),
                recommended_levers=recommended_levers_for_rca_kind(kind),
                patch_family=patch_family_for_rca_kind(kind),
                target_qids=(qid,),
            ))
    return findings


def _patch_intent(base: dict, *, ptype: str, lever: int, intent: str, **fields: Any) -> dict:
    return {
        **base,
        "type": ptype,
        "lever": lever,
        "intent": intent,
        **{k: v for k, v in fields.items() if v not in (None, "", [], {})},
    }


def _split_table_column(obj: str) -> tuple[str, str]:
    parts = str(obj or "").split(".")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return "", str(obj or "")


def _example_synthesis_intent(base: dict, finding: "RcaFinding", root_cause: str) -> dict:
    return _patch_intent(
        base,
        ptype="request_example_sql_synthesis",
        lever=5,
        intent="synthesize original non-benchmark example SQL for this RCA shape",
        root_cause=root_cause,
        blame_set=list(finding.expected_objects or finding.actual_objects),
    )


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
                table, column = _split_table_column(obj)
                patches.append(_patch_intent(
                    base,
                    ptype="update_column_description",
                    lever=1,
                    intent="strengthen intended measure description and contrast it with confused measures",
                    table=table,
                    column=column or obj,
                ))
                patches.append(_patch_intent(
                    base,
                    ptype="add_column_synonym",
                    lever=1,
                    intent="add business aliases that route users to the intended measure",
                    table=table,
                    column=column or obj,
                ))
                patches.append(_patch_intent(
                    base,
                    ptype="add_sql_snippet_measure",
                    lever=6,
                    intent="define reusable measure expression for the intended metric",
                    target_object=obj,
                    snippet_type="measure",
                ))
            patches.append(_example_synthesis_intent(
                base,
                f,
                root_cause="wrong_measure",
            ))

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

        elif f.rca_kind is RcaKind.JOIN_SPEC_MISSING_OR_WRONG:
            patches.append(_patch_intent(
                base,
                ptype="add_join_spec",
                lever=4,
                intent="add or repair join relationship required by the failed question pattern",
                expected_objects=list(f.expected_objects),
            ))
            patches.append(_example_synthesis_intent(
                base,
                f,
                root_cause="missing_join_spec",
            ))

        elif f.rca_kind is RcaKind.FILTER_LOGIC_MISMATCH:
            patches.append(_patch_intent(
                base,
                ptype="add_sql_snippet_filter",
                lever=6,
                intent="define reusable filter expression for the corrected condition",
                expected_objects=list(f.expected_objects),
                snippet_type="filter",
            ))
            patches.append(_patch_intent(
                base,
                ptype="add_instruction",
                lever=5,
                target="QUERY CONSTRUCTION",
                instruction_section="QUERY CONSTRUCTION",
                intent="clarify when this filter should and should not be applied",
            ))
            patches.append(_example_synthesis_intent(base, f, root_cause="missing_filter"))

        elif f.rca_kind is RcaKind.GRAIN_OR_GROUPING_MISMATCH:
            for obj in f.expected_objects:
                table, column = _split_table_column(obj)
                patches.append(_patch_intent(
                    base,
                    ptype="update_column_description",
                    lever=1,
                    intent="clarify required grain and grouping semantics",
                    table=table,
                    column=column or obj,
                ))
            patches.append(_patch_intent(
                base,
                ptype="add_sql_snippet_expression",
                lever=6,
                intent="define reusable grouping expression or projection pattern",
                expected_objects=list(f.expected_objects),
                snippet_type="expression",
            ))
            patches.append(_example_synthesis_intent(base, f, root_cause="wrong_grouping"))

        elif f.rca_kind is RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING:
            for obj in f.expected_objects:
                table, column = _split_table_column(obj)
                patches.append(_patch_intent(
                    base,
                    ptype="add_column_synonym",
                    lever=1,
                    intent="add missing business synonym or entity-match hint",
                    table=table,
                    column=column or obj,
                ))

        elif f.rca_kind is RcaKind.SQL_EXPRESSION_MISSING:
            patches.append(_patch_intent(
                base,
                ptype="add_sql_snippet_expression",
                lever=6,
                intent="define reusable SQL expression primitive for this RCA",
                expected_objects=list(f.expected_objects),
                snippet_type="expression",
            ))

        elif f.rca_kind is RcaKind.EXAMPLE_SQL_SHAPE_NEEDED:
            patches.append(_example_synthesis_intent(base, f, root_cause="wide_vs_long_shape"))

        elif f.rca_kind in {
            RcaKind.FUNCTION_OR_TVF_NOT_INVOKED,
            RcaKind.FUNCTION_ROUTING_MISMATCH,
        }:
            function_targets = tuple(
                obj for obj in (f.expected_objects or f.actual_objects)
                if "fn" in obj.lower() or "tvf" in obj.lower() or "function" in obj.lower()
            ) or f.expected_objects or f.actual_objects
            for obj in function_targets:
                patches.append({
                    **base,
                    "type": "add_instruction",
                    "lever": 3,
                    "target": obj,
                    "intent": f"Route requests that require {obj} to the registered function/TVF instead of inlining logic.",
                })
                patches.append(_patch_intent(
                    base,
                    ptype="add_sql_snippet_expression",
                    lever=6,
                    intent=f"Teach Genie the reusable SQL expression shape for {obj}.",
                    target=obj,
                ))
            patches.append({
                **base,
                "type": "add_instruction",
                "lever": 5,
                "target": "ASSET ROUTING",
                "intent": "Prefer the correct function or TVF asset type for matching user patterns.",
            })

        elif f.rca_kind is RcaKind.TOP_N_CARDINALITY_COLLAPSE:
            for obj in f.expected_objects or f.actual_objects:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "lever": 1,
                    "target": obj,
                    "intent": "Clarify this dimension should preserve cardinality for top-N/grouped questions.",
                })
            patches.append(_example_synthesis_intent(base, f, root_cause="top_n_cardinality_collapse"))

        elif f.rca_kind is RcaKind.TIME_WINDOW_LOGIC_MISMATCH:
            patches.append(_patch_intent(
                base,
                ptype="add_sql_snippet_filter",
                lever=6,
                intent="Encode the correct reusable time-window filter logic.",
                target="time_window",
            ))
            patches.append({
                **base,
                "type": "add_instruction",
                "lever": 5,
                "target": "QUERY RULES",
                "intent": "Explain how user time-window language maps to filters.",
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
            recommended_levers=f.recommended_levers,
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
