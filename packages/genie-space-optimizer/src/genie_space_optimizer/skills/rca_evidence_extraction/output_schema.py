"""Pydantic output contract for the rca-evidence-extraction skill.

Bound to Plan 2's ``AbstainableEnvelope[PerQidRcaEvidenceOutput]`` for
response_format generation. The dataclass-based carrier
``PerQidRcaEvidence`` (in ``optimization/rca_evidence_typed.py``) is a
separate type — kept in sync by
``test_per_qid_rca_evidence_pydantic_dataclass_alignment.py``.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.repair_intent import PatchType


class PerQidRcaEvidenceOutput(LLMOutputContract):
    """LLM output shape per roadmap.md:244-254.

    Field choices follow the Anthropic context-engineering guide:
    fields name the *signal* the LLM extracts (observed_failure,
    blame_set, quoted_evidence) rather than asking for free-form
    prose. ``suggested_repair_family`` is intentionally open-vocab so
    a new family can be introduced without a code change;
    ``repair_hint_patch_type`` is closed-vocab so the applier can
    dispatch on it directly.
    """

    qid: str = Field(
        description=(
            "The qid this evidence is for. Must echo the qid in the prompt."
        ),
    )
    observed_failure: str = Field(
        description=(
            "One sentence describing what went wrong in the LLM's own words."
        ),
    )
    generated_sql_issue: str = Field(
        description=(
            "One sentence describing the specific defect in the generated "
            "SQL. Be concrete: name the clause / column / function involved."
        ),
    )
    expected_sql_shape: str = Field(
        description=(
            "Structural description of what the SQL should look like. Use "
            "shape language rather than copying the benchmark expected_sql."
        ),
    )
    blame_set: list[str] = Field(
        description=(
            "Fully-qualified table.column references the failure is "
            "attributable to. Empty when the failure is metadata-level."
        ),
    )
    suggested_repair_family: str = Field(
        description=(
            "Open-vocab name for the repair shape. Examples: "
            "'top_n_with_ordering', 'join_spec_addition_with_disambiguation'."
        ),
    )
    repair_hint_patch_type: PatchType = Field(
        description=(
            "Closed-enum hint about which applier arm should land the fix. "
            "Pick the closest match; downstream synthesis is free to "
            "override."
        ),
    )
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "How confident you are in this evidence. 'high' = unambiguous "
            "failure signature; 'medium' = consistent but not definitive; "
            "'low' = best-effort given limited context."
        ),
    )
    quoted_evidence: list[str] = Field(
        description=(
            "Specific snippets from the failure context that support the "
            "above. Keep each ≤120 chars and label by source. Empty list "
            "is acceptable when no snippet adds signal."
        ),
    )
