"""Plan 3 — typed RCA evidence carrier + deterministic helpers.

Three public symbols:

  * ``PerQidRcaEvidence`` — frozen+slots+JsonRoundTrip dataclass that
    travels through ``RcaEvidenceBundle.per_qid_evidence_typed`` and
    is read by Plan 4+ consumers without dict-probing.
  * ``rca_kind_from_repair_family`` — deterministic mapper from the
    LLM's open-vocab ``suggested_repair_family`` to the closed
    ``RcaKind`` enum (best-effort substring match; UNKNOWN when no
    family matches).
  * ``PerQidRcaEvidence.to_legacy_dict`` — projector from typed to
    the legacy ``per_qid_evidence`` dict shape every existing
    downstream consumer expects.

This module imports neither the extractor nor the stage — it is the
unidirectional type / helper layer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class PerQidRcaEvidence(JsonRoundTrip):
    """Typed per-qid RCA evidence — wire-stable carrier through stage
    I/O.

    Field names match ``PerQidRcaEvidenceOutput`` exactly; the
    Pydantic/dataclass alignment is pinned by
    ``test_per_qid_rca_evidence_pydantic_dataclass_alignment.py``.

    Why a tuple, not list, for sequence fields: frozen-dataclass
    semantics require hashable members. ``to_legacy_dict`` converts
    back to lists for the legacy consumers.
    """

    qid: str
    observed_failure: str
    generated_sql_issue: str
    expected_sql_shape: str
    blame_set: tuple[str, ...]
    suggested_repair_family: str
    repair_hint_patch_type: PatchType
    confidence: Literal["high", "medium", "low"]
    quoted_evidence: tuple[str, ...]

    def to_legacy_dict(
        self,
        *,
        judge: dict[str, Any],
        asi: dict[str, Any],
        sql: str,
    ) -> dict[str, Any]:
        """Project to the legacy ``RcaEvidenceBundle.per_qid_evidence``
        dict.

        Pass-through fields come from the (judge, asi, sql) inputs so
        the legacy consumers see byte-stable structure regardless of
        whether the LLM path or the deterministic fallback produced
        this evidence. The closed-enum derivation (rca_kind →
        recommended_levers → rca_id) uses
        ``rca_kind_from_repair_family``.
        """
        rca_kind = rca_kind_from_repair_family(self.suggested_repair_family)
        safe_qid = re.sub(r"[^a-zA-Z0-9_]+", "_", self.qid or "unknown")
        rca_id = f"rca_llm_{safe_qid}_{rca_kind.value}"
        judge_verdict = str(
            judge.get("verdict") or self.observed_failure or ""
        )
        actual_objects_raw = asi.get("actual_objects") or []
        if isinstance(actual_objects_raw, (list, tuple)):
            actual_objects = [str(x) for x in actual_objects_raw]
        else:
            actual_objects = []
        return {
            "rca_kind": rca_kind.value,
            "judge_verdict": judge_verdict,
            "sql_diff": str(sql or ""),
            "counterfactual_fix": asi.get("counterfactual_fix"),
            "asi_features": dict(asi or {}),
            "expected_objects": list(self.blame_set),
            "actual_objects": actual_objects,
            "recommended_levers": list(
                recommended_levers_for_rca_kind(rca_kind)
            ),
            "rca_id": rca_id,
        }


# ── Plan 3 Task 4 — open-vocab repair_family → closed RcaKind mapper. ──

from genie_space_optimizer.optimization.rca import (  # noqa: E402
    RcaKind,
    recommended_levers_for_rca_kind,
)

# Substring patterns mapping the LLM's open-vocab
# ``suggested_repair_family`` onto the closed ``RcaKind`` enum. Order
# matters — first match wins, so more-specific patterns
# (``extra_defensive`` / ``filter_removal``) precede more-general ones
# (``filter``).
_REPAIR_FAMILY_PATTERNS: tuple[tuple[str, RcaKind], ...] = (
    # Top-N
    ("top_n", RcaKind.TOP_N_CARDINALITY_COLLAPSE),
    ("topn", RcaKind.TOP_N_CARDINALITY_COLLAPSE),
    ("cardinality", RcaKind.TOP_N_CARDINALITY_COLLAPSE),
    # Joins
    ("join_spec", RcaKind.JOIN_SPEC_MISSING_OR_WRONG),
    ("missing_join", RcaKind.JOIN_SPEC_MISSING_OR_WRONG),
    ("wrong_join", RcaKind.JOIN_SPEC_MISSING_OR_WRONG),
    # Filters — extra/unrequested defensive removal precedes generic.
    ("extra_defensive", RcaKind.EXTRA_DEFENSIVE_FILTER),
    ("filter_removal", RcaKind.EXTRA_DEFENSIVE_FILTER),
    ("unrequested_predicate", RcaKind.EXTRA_DEFENSIVE_FILTER),
    ("filter_logic", RcaKind.FILTER_LOGIC_MISMATCH),
    ("filter", RcaKind.FILTER_LOGIC_MISMATCH),
    # Grain / grouping
    ("grain", RcaKind.GRAIN_OR_GROUPING_MISMATCH),
    ("grouping", RcaKind.GRAIN_OR_GROUPING_MISMATCH),
    # Time window
    ("time_window", RcaKind.TIME_WINDOW_LOGIC_MISMATCH),
    ("temporal", RcaKind.TIME_WINDOW_LOGIC_MISMATCH),
    # Measures
    ("measure_swap", RcaKind.MEASURE_SWAP),
    ("measure", RcaKind.MEASURE_SWAP),
    # SQL expression
    ("sql_expression", RcaKind.SQL_EXPRESSION_MISSING),
    # Synonym / entity matching
    ("synonym", RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING),
    ("entity_match", RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING),
    # Functions / TVFs
    ("function", RcaKind.FUNCTION_OR_TVF_NOT_INVOKED),
    ("tvf", RcaKind.FUNCTION_OR_TVF_NOT_INVOKED),
    # Example-SQL shape
    ("example_sql_shape", RcaKind.EXAMPLE_SQL_SHAPE_NEEDED),
    # Metric view
    ("metric_view", RcaKind.METRIC_VIEW_ROUTING_CONFUSION),
    # Asset-type routing
    ("asset_type", RcaKind.ASSET_TYPE_ROUTING_MISMATCH),
    # Canonical / required dimensions
    ("canonical_dimension", RcaKind.CANONICAL_DIMENSION_MISSED),
    ("required_dimension", RcaKind.MISSING_REQUIRED_DIMENSION),
)


def rca_kind_from_repair_family(family: str | None) -> RcaKind:
    """Deterministic substring map from open-vocab family to closed
    RcaKind.

    Case-insensitive, whitespace-tolerant. Returns ``RcaKind.UNKNOWN``
    when no pattern matches (including ``None`` and empty string).
    Postmortems should surface ``UNKNOWN`` rates as a signal that a
    new pattern may need to be added.
    """
    if not family:
        return RcaKind.UNKNOWN
    needle = family.strip().lower()
    if not needle:
        return RcaKind.UNKNOWN
    for pattern, kind in _REPAIR_FAMILY_PATTERNS:
        if pattern in needle:
            return kind
    return RcaKind.UNKNOWN
