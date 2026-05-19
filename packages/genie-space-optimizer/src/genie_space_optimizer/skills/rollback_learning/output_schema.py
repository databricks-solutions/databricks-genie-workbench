"""Pydantic output contract for the rollback-learning skill.

One class:

  * ``LlmNextAttemptHypothesisOutput`` — per-cluster shape the LLM emits.
    ``rolled_back_intent_id`` and ``cluster_id`` are framework-stamped
    and intentionally absent from the LLM output (mirrors Plan 4's
    ``cluster_id`` and Plan 5's ``intent_id`` and Plan 6's
    ``proposal_id`` discipline). ``declined`` lives on the
    ``AbstainableEnvelope[T]`` wrapper, not on the hypothesis body.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


class LlmNextAttemptHypothesisOutput(LLMOutputContract):
    """One NextAttemptHypothesis the LLM emitted for a single
    rolled-back cluster."""

    why_failed: str = Field(
        description=(
            "One paragraph (≤500 chars) naming the SPECIFIC failure "
            "signal that caused the deterministic acceptance gate to "
            "roll back. Cite the rollback_reason field from the input "
            "AND the most relevant per_qid_evidence entry. Examples: "
            "'patch added a top-N example using ORDER BY revenue but "
            "the gate dropped it because gs_044/gs_055 (passing on "
            "revenue without top-N) regressed to wrong-row-count'."
        ),
    )
    failure_mode: str = Field(
        description=(
            "Short snake_case label (≤40 chars) categorising the "
            "failure pattern. Free-form — the framework will collect "
            "telemetry on these labels and promote common ones to a "
            "closed enum in a follow-up. Examples: "
            "'overgeneralized_filter', 'wrong_join_direction', "
            "'shape_mismatch_top_n_vs_aggregation'."
        ),
    )
    revised_repair_shape: RepairShape | None = Field(
        description=(
            "If your hypothesis is that a DIFFERENT repair_shape would "
            "work, name it from the closed RepairShape enum (OTHER "
            "permitted). Pass None when the shape was correct but a "
            "narrower blame_set / different patch_type is needed."
        ),
    )
    revised_patch_type: PatchType | None = Field(
        description=(
            "If your hypothesis is that a DIFFERENT patch_type would "
            "work, name it from the closed PatchType enum. Pass None "
            "when the patch_type was correct. Cross-lever revisions "
            "allowed (e.g. add_example_sql → add_sql_snippet_filter)."
        ),
    )
    revised_blame_set: list[str] | None = Field(
        description=(
            "If your hypothesis is that a NARROWER or DIFFERENT "
            "blame_set would work, emit the revised list of "
            "fully-qualified catalog.schema.table.column references. "
            "MUST be a subset of identifier_allowlist (input field) — "
            "the framework's deterministic validator rejects "
            "references outside the allowlist and the entire "
            "hypothesis is dropped. Pass None when blame_set was "
            "correct."
        ),
    )
    additional_evidence_needed: list[str] = Field(
        description=(
            "List of evidence types the framework should collect "
            "before the next attempt. Free-form strings; downstream "
            "consumers may pattern-match on common values. Examples: "
            "'data_profile_for_sales.fact_sales.revenue', "
            "'join_cardinality_crm.customer_to_crm.orders'. Empty "
            "list when no additional evidence is needed."
        ),
    )
    forbidden_signatures: list[str] = Field(
        description=(
            "List of content_fingerprint strings (from the "
            "applied_patch_fingerprints input) the framework should "
            "add to the do-not-retry set. The LLM NOMINATES from the "
            "existing applied-patch fingerprints — the framework's "
            "deterministic validator rejects any signature NOT in "
            "applied_patch_fingerprints (you cannot invent "
            "fingerprints). Empty list when no signatures should be "
            "blocklisted."
        ),
    )
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "How confident your hypothesis is. 'high' = the rollback "
            "evidence points clearly at one revised dimension; "
            "'low' = best-effort. Plan 5's synthesizer weighs the "
            "hypothesis lower at 'low' confidence."
        ),
    )
