"""Pydantic output contract for the candidate-critique skill.

One class:

  * ``LlmCritiqueVerdictOutput`` — per-proposal shape the LLM emits.
    ``proposal_id`` is framework-stamped and intentionally absent
    from the LLM output (mirrors Plan 4's ``cluster_id`` and Plan 5's
    ``intent_id`` discipline). ``declined`` lives on the
    ``AbstainableEnvelope[T]`` wrapper, not on the verdict body.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class LlmCritiqueVerdictOutput(LLMOutputContract):
    """One CritiqueVerdict the LLM emitted for a single candidate
    proposal."""

    addresses_target_failure: bool = Field(
        description=(
            "True when the patch CONCRETELY addresses at least one "
            "qid's observed_failure (the failure signature flagged by "
            "Plan 3's per_qid_evidence). False when the patch is "
            "tangential — e.g. teaches a generic SQL pattern when the "
            "cluster needs a specific column."
        ),
    )
    is_overgeneralized: bool = Field(
        description=(
            "True when the patch's intent is BROADER than the cluster's "
            "blame_set warrants — e.g. an example_sql that demonstrates "
            "any GROUP BY when the cluster's blame_set is one specific "
            "column. Overgeneralized patches risk regressing nearby "
            "passing qids."
        ),
    )
    likely_neighbor_regressions: list[str] = Field(
        description=(
            "qids (from the passing_qids_at_risk input) that you "
            "predict may break if this patch ships. Empty list when "
            "no regression risk. Used by postmortem to verify the "
            "prediction after post-patch evaluation runs."
        ),
    )
    matches_intended_shape: bool = Field(
        description=(
            "True when the patch_body matches the intent's "
            "repair_shape (e.g. a top_n_by_metric intent produced an "
            "example_sql with ORDER BY ... LIMIT N). False when the "
            "patch drifted from the declared shape during synthesis."
        ),
    )
    overall_recommendation: Literal["proceed", "rework", "discard"] = Field(
        description=(
            "'proceed' — ship as-is (safety gates still run). "
            "'rework' — let through advisory; future plan may re-dispatch "
            "to the synthesizer with critique feedback. "
            "'discard' — block when GSO_CRITIQUE_GATE_ENFORCING=true; "
            "advisory otherwise. Set 'discard' only when the patch is "
            "actively harmful (overgeneralized + neighbor regressions "
            "OR addresses_target_failure=False AND matches_intended_shape=False)."
        ),
    )
    rationale: str = Field(
        description=(
            "One or two sentences (≤300 chars) citing the SPECIFIC "
            "input fields that drove the recommendation. Examples: "
            "'example_sql cleanly demonstrates top-N; matches "
            "blame_set'; 'patch generalizes from one qid to the whole "
            "fact_sales table — likely to regress gs_044, gs_055'."
        ),
    )
