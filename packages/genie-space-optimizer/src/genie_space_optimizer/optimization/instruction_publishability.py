"""Publishability checks for Genie-facing text instructions.

The optimizer carries several useful diagnostic strings: RCA summaries,
AFS summaries, ASI counterfactual fixes, and repair-plan rationales. Those
strings are for humans and the optimizer, not for Genie. This module defines
the positive contract for text that may be persisted to
instructions.text_instructions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from genie_space_optimizer.common.config import INSTRUCTION_SECTION_ORDER


SQL_SHAPE_FAILURE_TYPES: frozenset[str] = frozenset({
    "missing_filter",
    "missing_temporal_filter",
    "wrong_filter",
    "wrong_filter_condition",
    "missing_aggregation",
    "wrong_aggregation",
    "wrong_measure",
    "wrong_groupby",
    "wrong_grouping",
    "missing_dimension",
    "wrong_join",
    "missing_join_spec",
    "wrong_join_spec",
    "wrong_table",
    "select_star",
    "tvf_parameter_error",
})

_SECTION_BY_FAILURE_TYPE: dict[str, str] = {
    "asset_routing_error": "ASSET ROUTING",
    "ambiguous_question": "DISAMBIGUATION",
    "missing_instruction": "CONSTRAINTS",
    "missing_filter": "QUERY RULES",
    "missing_temporal_filter": "TEMPORAL FILTERS",
    "wrong_filter": "QUERY RULES",
    "wrong_filter_condition": "QUERY RULES",
    "missing_aggregation": "AGGREGATION RULES",
    "wrong_aggregation": "AGGREGATION RULES",
    "wrong_measure": "AGGREGATION RULES",
    "wrong_groupby": "AGGREGATION RULES",
    "wrong_grouping": "AGGREGATION RULES",
    "missing_dimension": "QUERY PATTERNS",
    "wrong_join": "JOIN GUIDANCE",
    "missing_join_spec": "JOIN GUIDANCE",
    "wrong_join_spec": "JOIN GUIDANCE",
    "wrong_table": "ASSET ROUTING",
    "select_star": "QUERY RULES",
    "tvf_parameter_error": "FUNCTION ROUTING",
}

_CANONICAL_SECTION_RE = re.compile(
    r"(?m)^("
    + "|".join(re.escape(section) for section in INSTRUCTION_SECTION_ORDER)
    + r"):\s*$"
)

_INTERNAL_DIAGNOSTIC_RE = re.compile(
    r"\b("
    r"root cause|blamed|affected|failure_type|counterfactual|"
    r"wrong_aggregation|wrong_measure|missing_filter|wrong_join|"
    r"missing_join_spec|asset_routing_error|Guidance for"
    r")\b",
    flags=re.IGNORECASE,
)

_REPAIR_PLAN_RE = re.compile(
    r"\b("
    r"add an instruction|add instruction|update .*description|"
    r"add .*synonym|add .*table description|clarify in .*metadata|"
    r"genie space should clarify|the genie space should|"
    r"metadata clarifying|patch the|change the metadata"
    r")\b",
    flags=re.IGNORECASE,
)

_SQL_IN_TEXT_RE = re.compile(
    r"\b(select\s+.+\s+from|from\s+[\w.]+\s+join|join\s+[\w.]+\s+on|"
    r"group\s+by\s+[\w., ]+|where\s+[\w.]+\s*(=|<>|!=|>|<|>=|<=))\b",
    flags=re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class InstructionPublishabilityResult:
    ok: bool
    reasons: list[str]


def instruction_section_for_failure(failure_type: str) -> str:
    """Return the canonical instruction section for a failure type."""
    return _SECTION_BY_FAILURE_TYPE.get(str(failure_type or "").strip(), "CONSTRAINTS")


def is_sql_shape_failure(failure_type: str) -> bool:
    """Return True when text-only fallback is the wrong remediation channel."""
    return str(failure_type or "").strip() in SQL_SHAPE_FAILURE_TYPES


def validate_publishable_instruction_text(
    text: str,
    *,
    known_assets: set[str] | None = None,
) -> InstructionPublishabilityResult:
    """Validate text before it can be written to Genie instructions."""
    candidate = str(text or "").strip()
    reasons: list[str] = []

    if not candidate:
        reasons.append("empty_instruction")
    if candidate and not _CANONICAL_SECTION_RE.search(candidate):
        reasons.append("missing_canonical_section")
    if _INTERNAL_DIAGNOSTIC_RE.search(candidate):
        reasons.append("internal_diagnostic_text")
    if _REPAIR_PLAN_RE.search(candidate):
        reasons.append("optimizer_repair_plan_voice")
    if _SQL_IN_TEXT_RE.search(candidate):
        reasons.append("sql_in_text_instruction")

    assets = {a.lower() for a in (known_assets or set()) if a}
    if assets and not any(asset in candidate.lower() for asset in assets):
        reasons.append("missing_known_asset")

    return InstructionPublishabilityResult(ok=not reasons, reasons=reasons)


def _candidate_assets(raw: Any) -> set[str]:
    if not raw:
        return set()
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, (list, tuple, set)):
        return {str(v) for v in raw if str(v).strip()}
    return set()


def compile_publishable_fallback(afs: dict[str, Any]) -> dict[str, Any] | None:
    """Compile explicit instruction candidates into a proposal.

    This intentionally does not compile ``counterfactual_fixes``. Those are
    optimizer-facing repair hints. A future caller that wants a text fallback
    must provide ``publishable_instruction_candidates`` with section, text,
    and assets already expressed in Genie-facing voice.
    """
    failure_type = str(afs.get("failure_type") or "").strip()
    if is_sql_shape_failure(failure_type):
        return None

    candidates = afs.get("publishable_instruction_candidates") or []
    if isinstance(candidates, dict):
        candidates = [candidates]
    if not isinstance(candidates, list):
        return None

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        section_name = str(
            candidate.get("section_name") or instruction_section_for_failure(failure_type)
        ).upper().strip()
        if section_name not in INSTRUCTION_SECTION_ORDER:
            section_name = instruction_section_for_failure(failure_type)
        text = str(candidate.get("text") or "").strip()
        if not text:
            continue
        assets = _candidate_assets(candidate.get("assets")) | _candidate_assets(afs.get("blame_set"))
        block = f"{section_name}:\n- {text}"
        result = validate_publishable_instruction_text(block, known_assets=assets)
        if not result.ok:
            continue
        return {
            "patch_type": "update_instruction_section",
            "section_name": section_name,
            "new_text": f"- {text}",
            "proposed_value": f"{section_name}:\n- {text}",
            "rationale": (
                "Published explicit Genie-facing instruction candidate after "
                "instruction publishability validation."
            ),
            "provenance": {
                "source": "synthesis_fallback",
                "cluster_id": afs.get("cluster_id", "?"),
                "failure_type": failure_type,
                "tier": "publishability_contract_v1",
                "suggested_fix_summary": afs.get("suggested_fix_summary", ""),
                "blame_set": list(afs.get("blame_set") or []),
            },
        }

    return None
