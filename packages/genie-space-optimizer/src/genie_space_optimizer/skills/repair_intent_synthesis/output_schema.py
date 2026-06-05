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

from pydantic import BaseModel, ConfigDict, Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


class LlmTargetObject(BaseModel):
    """Plan 9 Task 1 — LLM-emitted typed slice. Bridges to
    ``TargetObject`` dataclass in target_object_typed.py.

    Strict mode + extra=forbid mirror the ``LLMOutputContract`` base
    used by ``LlmRepairProposalOutput``.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    asset_kind: Literal["table", "metric_view", "column"] = Field(
        description=(
            "The kind of asset this slice points at. "
            "'table' for base tables; 'metric_view' for UC metric "
            "views; 'column' for a single named column."
        ),
    )
    identifier: str = Field(
        min_length=1,
        description=(
            "Fully qualified name. For 'table'/'metric_view': "
            "'catalog.schema.name'. For 'column': "
            "'catalog.schema.table.column_name'."
        ),
    )
    columns: list[str] = Field(
        default_factory=list,
        description=(
            "For 'table'/'metric_view': the subset of columns the "
            "repair will touch (typically 1-8). For 'column': empty "
            "list."
        ),
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
    target_objects: list[LlmTargetObject] = Field(
        default_factory=list,
        description=(
            "Plan 9 — typed slice replacing archetype-derived "
            "AssetSlice. The LLM emits the assets (tables, metric "
            "views, columns) the repair targets. Empty list is "
            "allowed only when repair_shape == 'other' or "
            "patch_type does not require a slice (e.g. "
            "'add_instruction'). After Plan 9 PR2 (catalog "
            "deletion), this becomes required for shape-keyed "
            "repairs."
        ),
    )
    required_constructs: list[str] = Field(
        default_factory=list,
        description=(
            "Plan 9 — SQL clause keywords your patch_body's SQL must "
            "contain. The deterministic validator reads this list and "
            "rejects the proposal if the generated SQL is missing "
            "any. Use uppercase clause names: SELECT, FROM, WHERE, "
            "GROUP_BY, ORDER_BY, LIMIT, JOIN, WINDOW, HAVING, CASE. "
            "For patch types that do not produce SQL (e.g. "
            "add_instruction, add_column_description), leave as []."
        ),
    )
    # ── Trial 17 — Lever Selection Contract ─────────────────────────
    # The LLM declares which of the 6 levers this proposal operates
    # on, an auditable behavioural-change hypothesis, and a fallback
    # lever for next-iteration pivot. All fields default to empty
    # string for backward compatibility with pre-Trial-17 prompts
    # that do not yet request them; the deterministic validator in
    # ``levers_contract.validate_plan_vs_proposal_consistency`` runs
    # only when ``selected_lever`` is non-empty.
    selected_lever: str = Field(
        default="",
        description=(
            "Trial 17 — Which of the 6 levers this proposal operates "
            "on: 'lever-1' (table/column descriptions), 'lever-2' "
            "(metric-view columns), 'lever-3' (TVF routing), "
            "'lever-4' (joins), 'lever-5' (instructions + example "
            "SQL), 'lever-6' (SQL snippets). MUST be consistent with "
            "patch_type; consult LEVER_TO_PATCH_TYPES in "
            "levers_contract.py for the membership table."
        ),
    )
    expected_behavioral_change: str = Field(
        default="",
        description=(
            "Trial 17 — Auditable hypothesis for what the generated "
            "SQL grammar will do differently after this patch lands. "
            "Be concrete: 'queries about top N customers will now "
            "use ORDER BY revenue DESC LIMIT N instead of "
            "MAX(revenue)'. Used by the next-iteration prompt when "
            "the patch failed at acceptance to help the LLM pivot."
        ),
    )
    fallback_lever: str = Field(
        default="",
        description=(
            "Trial 17 — Which lever to try next if sliced eval shows "
            "this patch did not change behavior (target_unchanged). "
            "Same closed enum as selected_lever. Optional but "
            "strongly recommended."
        ),
    )
    bundle_id: str = Field(
        default="",
        description=(
            "Trial 17 — Optional bundle identifier. Proposals sharing "
            "the same non-empty bundle_id are applied incrementally "
            "by the SM orchestration (one patch + sliced eval at a "
            "time). Empty string means single-proposal path."
        ),
    )
    # ── Trial 20 D1 — single-lever justification ────────────────────
    single_lever_justification: str = Field(
        default="",
        description=(
            "Trial 20 D1 — REQUIRED when emitting a single-lever "
            "proposal on iteration 1 (no prior insufficient "
            "signatures). One to three sentences naming the failure "
            "mode and the lever family, and explaining why a second "
            "lever would not materially reinforce the repair. "
            "Surfaced in the postmortem-joinable "
            "GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1 marker."
        ),
    )
