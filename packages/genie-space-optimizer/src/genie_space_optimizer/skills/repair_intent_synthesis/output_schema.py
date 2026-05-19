"""Pydantic output contract for the repair-intent-synthesis skill.

One class:

  * ``LlmRepairProposalOutput`` — per-cluster shape the LLM emits.
    Loose on ``patch_body`` (dict[str, Any]) — per-patch-type field
    constraints enforced by the deterministic validator in
    ``optimization/repair_intent_synthesizer.py`` (Plan 2 pattern;
    avoids Databricks strict-mode JSON-schema pitfalls with
    discriminated unions). ``intent_id`` is framework-stamped and
    intentionally absent from the LLM output (mirrors Plan 4's
    no-cluster-id discipline).
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


class LlmRepairProposalOutput(LLMOutputContract):
    """One RepairProposal the LLM emitted for a single failure cluster.

    ``intent_id`` is intentionally absent — the framework stamps it
    deterministically (e.g. ``intent_H001_AG3_001``) after parse.
    """

    intent_name: str = Field(
        description=(
            "Short LLM-invented label for the repair (≤80 chars). "
            "Examples: 'top_n_revenue_by_region', "
            "'add_customer_orders_join'. Snake-case preferred."
        ),
    )
    intent_description: str = Field(
        description=(
            "One or two sentences describing what this repair does. "
            "Downstream lever prompts read this verbatim, so be "
            "concrete: name the structural shape and the "
            "columns/tables involved."
        ),
    )
    repair_shape: RepairShape = Field(
        description=(
            "Closed-enum repair shape from the Plan 1 catalog. "
            "``OTHER`` is the documented escape-hatch for novel "
            "structural patterns. Downstream OTHER triggers a "
            "relaxed validation gate."
        ),
    )
    patch_type: PatchType = Field(
        description=(
            "Closed-enum patch type the applier dispatches on. MAY "
            "cross lever boundaries (e.g. cluster came into L5b but "
            "the right patch is ADD_SQL_SNIPPET_EXPRESSION — the "
            "cross-lever router will re-dispatch with this intent "
            "attached)."
        ),
    )
    rationale: str = Field(
        description=(
            "One sentence (≤200 chars) explaining WHY this proposal "
            "fixes the cluster's failure signature. Cite specific "
            "qid evidence."
        ),
    )
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "How confident you are. 'high' = every qid in the "
            "cluster shares an obvious unifying signal; 'low' = "
            "best-effort."
        ),
    )
    patch_body: dict[str, Any] = Field(
        description=(
            "Per-patch-type payload. Field expectations vary by "
            "patch_type — see the SKILL.md <patch_body_shapes> "
            "section for canonical schemas. Deterministically "
            "validated post-LLM."
        ),
    )
    blame_set: list[str] = Field(
        description=(
            "Fully-qualified ``catalog.schema.table.column`` "
            "references the patch targets. MUST be a subset of the "
            "AG's identifier allowlist — references outside the "
            "allowlist are rejected by the validator and trigger "
            "fallback to ``intent_from_archetype``."
        ),
    )
