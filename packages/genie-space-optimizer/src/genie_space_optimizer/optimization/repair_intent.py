"""Typed RepairIntent contract — Plan 1 Foundation.

A ``RepairIntent`` captures what kind of repair the optimizer is
attempting for a single failure cluster: the high-level ``RepairShape``
(the LLM's view in Plan 2) and the concrete ``PatchType`` (the
applier's view). It travels end-to-end through the pipeline: stamped
at synthesis time onto each proposal dict, surfaced as a typed carrier
on every stage I/O dataclass, and read by postmortem + downstream LLM
calls.

Today (Plan 1) the only producer is the deterministic
``intent_from_archetype`` adapter — it wraps the existing ``Archetype``
catalog so structural synthesis emits typed intents without any LLM
call. Plan 2 replaces the L5b synthesis path with an LLM call that
returns a ``RepairIntent`` directly (with ``RepairShape.OTHER`` as the
escape hatch for shapes outside the closed vocabulary). Plans 3 and 4
build further LLM-driven producers on top.

Design constraints:
  * ``RepairShape`` is closed-with-escape-hatch — the LLM picks from
    the canonical set or falls back to ``OTHER``. Pure semantic
    vocabulary, not tied to applier arms.
  * ``PatchType`` is closed and pinned to ``applier.py`` dispatch
    arms (see ``test_patch_type_covers_applier_dispatch_arms``).
    Adding a new applier arm requires adding a new ``PatchType``
    member in the same commit.
  * ``RepairIntent`` is frozen+slots+JsonRoundTrip so it can
    round-trip through MLflow Phase H capture and stage I/O
    boundaries.
"""

from __future__ import annotations

from enum import StrEnum


class RepairShape(StrEnum):
    """High-level repair category — the vocabulary the LLM picks from.

    Closed-with-escape-hatch: ``OTHER`` lets Plan 2's LLM propose a
    repair shape the catalog doesn't enumerate (e.g. a long-tail
    structural pattern). Downstream code that inspects ``repair_shape``
    must always handle ``OTHER`` — it is the deliberate fallback, not
    a bug indicator.
    """

    TOP_N_BY_METRIC = "top_n_by_metric"
    ORDERED_LIST_BY_METRIC = "ordered_list_by_metric"
    RANK_WITHIN_GROUP = "rank_within_group"
    PERIOD_OVER_PERIOD = "period_over_period"
    FILTER_COMPOSE = "filter_compose"
    FILTER_REMOVE = "filter_remove"
    JOIN_DISCOVERY = "join_discovery"
    SQL_EXPRESSION = "sql_expression"
    COLUMN_DESCRIPTION = "column_description"
    METRIC_VIEW_REFINEMENT = "metric_view_refinement"
    INSTRUCTION = "instruction"
    OTHER = "other"


class PatchType(StrEnum):
    """Closed set of patch types the applier dispatches on.

    Pinned to ``applier.py`` dispatch arms by
    ``test_patch_type_covers_applier_dispatch_arms``. If a new applier
    arm is added, a corresponding ``PatchType`` member MUST be added
    in the same commit or the test will fail.
    """

    # Instructions
    ADD_INSTRUCTION = "add_instruction"
    UPDATE_INSTRUCTION = "update_instruction"
    UPDATE_INSTRUCTION_SECTION = "update_instruction_section"
    REWRITE_INSTRUCTION = "rewrite_instruction"
    REMOVE_INSTRUCTION = "remove_instruction"
    # Example SQLs
    ADD_EXAMPLE_SQL = "add_example_sql"
    UPDATE_EXAMPLE_SQL = "update_example_sql"
    REMOVE_EXAMPLE_SQL = "remove_example_sql"
    # Descriptions
    ADD_DESCRIPTION = "add_description"
    UPDATE_DESCRIPTION = "update_description"
    ADD_COLUMN_DESCRIPTION = "add_column_description"
    UPDATE_COLUMN_DESCRIPTION = "update_column_description"
    ADD_TVF_DESCRIPTION = "add_tvf_description"
    # Columns
    HIDE_COLUMN = "hide_column"
    UNHIDE_COLUMN = "unhide_column"
    RENAME_COLUMN_ALIAS = "rename_column_alias"
    ADD_COLUMN_SYNONYM = "add_column_synonym"
    REMOVE_COLUMN_SYNONYM = "remove_column_synonym"
    # Tables
    ADD_TABLE = "add_table"
    REMOVE_TABLE = "remove_table"
    # Joins
    ADD_JOIN_SPEC = "add_join_spec"
    UPDATE_JOIN_SPEC = "update_join_spec"
    REMOVE_JOIN_SPEC = "remove_join_spec"
    # Filters
    ADD_DEFAULT_FILTER = "add_default_filter"
    REMOVE_DEFAULT_FILTER = "remove_default_filter"
    UPDATE_FILTER_CONDITION = "update_filter_condition"
    # Genie feature toggles
    ENABLE_EXAMPLE_VALUES = "enable_example_values"
    DISABLE_EXAMPLE_VALUES = "disable_example_values"
    ENABLE_VALUE_DICTIONARY = "enable_value_dictionary"
    DISABLE_VALUE_DICTIONARY = "disable_value_dictionary"
    # TVFs
    ADD_TVF = "add_tvf"
    REMOVE_TVF = "remove_tvf"
    ADD_TVF_PARAMETER = "add_tvf_parameter"
    REMOVE_TVF_PARAMETER = "remove_tvf_parameter"
    UPDATE_TVF_SQL = "update_tvf_sql"
    # Metric views
    ADD_MV_MEASURE = "add_mv_measure"
    UPDATE_MV_MEASURE = "update_mv_measure"
    REMOVE_MV_MEASURE = "remove_mv_measure"
    ADD_MV_DIMENSION = "add_mv_dimension"
    REMOVE_MV_DIMENSION = "remove_mv_dimension"
    UPDATE_MV_YAML = "update_mv_yaml"
    # SQL snippets
    ADD_SQL_SNIPPET_FILTER = "add_sql_snippet_filter"
    ADD_SQL_SNIPPET_EXPRESSION = "add_sql_snippet_expression"
    ADD_SQL_SNIPPET_MEASURE = "add_sql_snippet_measure"


from dataclasses import dataclass
from typing import Literal

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class RepairIntent(JsonRoundTrip):
    """The typed unit of repair threaded end-to-end through the optimizer.

    Required fields (always populated by the producer):
      * ``intent_id`` — stable identifier, generated by the producer
        (e.g. ``"intent_H001_001"`` for the deterministic adapter).
      * ``intent_name`` — short canonical name (matches Archetype.name
        in the deterministic path; free-form for the LLM path).
      * ``intent_description`` — one or two sentences the LLM can read
        downstream when deciding whether this intent is on-shape.
      * ``repair_shape`` — RepairShape enum value.
      * ``patch_type`` — PatchType enum value (closed set the applier
        dispatches on).
      * ``rationale`` — one sentence explaining WHY this intent fits
        the cluster's failure signature.
      * ``confidence`` — Literal['high', 'medium', 'low']; used by
        Plan 4 learning to weight the next-action decision.
      * ``source`` — opaque producer string (e.g.
        ``"deterministic_archetype_adapter"``,
        ``"llm_l5b_synthesis"``). Postmortem can group intents by
        producer.
      * ``cluster_id``, ``target_qids``, ``blame_set``,
        ``rca_card_id``, ``ag_id`` — provenance fields letting
        downstream stages key the intent back to its originating
        cluster + RCA card + AG.

    Optional fields (populated by later stages as the intent flows):
      * ``applied_at_iter`` — iteration where this intent was applied.
      * ``applied_signature`` — the AppliedPatchSet.applied_signature
        snapshot at apply time (Plan 4 cycle-detection seam).
      * ``acceptance_outcome`` — verdict string from AgOutcomeRecord
        (``"accepted"`` / ``"rolled_back"`` / etc.).
      * ``rollback_reason`` — populated when acceptance_outcome ==
        ``"rolled_back"``.
    """

    intent_id: str
    intent_name: str
    intent_description: str
    repair_shape: RepairShape
    patch_type: PatchType
    rationale: str
    confidence: Literal["high", "medium", "low"]
    source: str
    cluster_id: str
    target_qids: tuple[str, ...]
    blame_set: tuple[str, ...]
    rca_card_id: str
    ag_id: str
    applied_at_iter: int | None = None
    applied_signature: str | None = None
    acceptance_outcome: str | None = None
    rollback_reason: str | None = None

    @classmethod
    def from_json(cls, payload: dict) -> "RepairIntent":  # type: ignore[override]
        return cls(
            intent_id=str(payload["intent_id"]),
            intent_name=str(payload["intent_name"]),
            intent_description=str(payload["intent_description"]),
            repair_shape=RepairShape(payload["repair_shape"]),
            patch_type=PatchType(payload["patch_type"]),
            rationale=str(payload["rationale"]),
            confidence=str(payload["confidence"]),  # type: ignore[arg-type]
            source=str(payload["source"]),
            cluster_id=str(payload["cluster_id"]),
            target_qids=tuple(payload.get("target_qids") or ()),
            blame_set=tuple(payload.get("blame_set") or ()),
            rca_card_id=str(payload.get("rca_card_id") or ""),
            ag_id=str(payload.get("ag_id") or ""),
            applied_at_iter=(
                int(payload["applied_at_iter"])
                if payload.get("applied_at_iter") is not None
                else None
            ),
            applied_signature=(
                str(payload["applied_signature"])
                if payload.get("applied_signature") is not None
                else None
            ),
            acceptance_outcome=(
                str(payload["acceptance_outcome"])
                if payload.get("acceptance_outcome") is not None
                else None
            ),
            rollback_reason=(
                str(payload["rollback_reason"])
                if payload.get("rollback_reason") is not None
                else None
            ),
        )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.archetypes import Archetype
    from genie_space_optimizer.optimization.failure_cluster import FailureCluster


# Closed mapping from Archetype.name → RepairShape. Plan 1 owns this
# table. New archetypes added to the catalog MUST add an entry here
# in the same commit (the catalog-drift test in
# ``test_repair_intent_archetype_adapter.py`` pins this).
_ARCHETYPE_NAME_TO_SHAPE: dict[str, RepairShape] = {
    "simple_enumerate": RepairShape.OTHER,
    "ordered_list_by_metric": RepairShape.ORDERED_LIST_BY_METRIC,
    "top_n_by_metric": RepairShape.TOP_N_BY_METRIC,
    "group_by_all_projected_keys": RepairShape.OTHER,
    "period_over_period": RepairShape.PERIOD_OVER_PERIOD,
    "correct_join_spec": RepairShape.JOIN_DISCOVERY,
    "cohort_retention": RepairShape.OTHER,
    "funnel_conversion": RepairShape.OTHER,
    "ratio_by_dimension": RepairShape.OTHER,
    "running_total": RepairShape.OTHER,
    "rank_within_group": RepairShape.RANK_WITHIN_GROUP,
    "pct_change": RepairShape.PERIOD_OVER_PERIOD,
    "filter_compose": RepairShape.FILTER_COMPOSE,
    "segment_compare": RepairShape.OTHER,
    "disambiguate_column": RepairShape.COLUMN_DESCRIPTION,
    "time_window_aggregate": RepairShape.PERIOD_OVER_PERIOD,
    "self_join_hierarchy": RepairShape.JOIN_DISCOVERY,
    "event_sequence": RepairShape.OTHER,
    "distinct_count_by_dim": RepairShape.OTHER,
    "pivot_wide": RepairShape.OTHER,
}


def intent_from_archetype(
    *,
    archetype: "Archetype",
    cluster: "FailureCluster",
    ag_id: str,
    seq: int,
) -> RepairIntent:
    """Deterministic Archetype → RepairIntent adapter.

    Pure function: same archetype + cluster + ag_id + seq always
    produces an equal RepairIntent. Used by every non-LLM synthesis
    path in Plan 1 to emit typed intents.

    Plan 2 replaces the L5b call site of this adapter with an LLM
    call; the adapter itself remains as the fallback producer for
    paths Plan 2 does not touch.

    Raises ``KeyError`` if ``archetype.name`` is not in
    ``_ARCHETYPE_NAME_TO_SHAPE`` — the catalog-drift detector.
    """
    if archetype.name not in _ARCHETYPE_NAME_TO_SHAPE:
        raise KeyError(
            f"intent_from_archetype: archetype {archetype.name!r} has no "
            f"RepairShape mapping. Add an entry to "
            f"repair_intent._ARCHETYPE_NAME_TO_SHAPE in the same commit "
            f"that introduced the archetype."
        )
    shape = _ARCHETYPE_NAME_TO_SHAPE[archetype.name]
    patch_type = PatchType(archetype.patch_type)

    intent_id = (
        f"intent_{cluster.cluster_id}_{ag_id}_{archetype.name}_{seq:03d}"
    )

    # intent_description = archetype.prompt_template clipped to one
    # sentence. Plan 2's LLM call replaces this with free-form text.
    description = archetype.prompt_template.split(". ")[0].strip()
    if not description.endswith("."):
        description += "."

    rationale = (
        f"Cluster {cluster.cluster_id} root_cause="
        f"{cluster.root_cause!r}; archetype {archetype.name!r} is the "
        f"deterministic shape match."
    )

    return RepairIntent(
        intent_id=intent_id,
        intent_name=archetype.name,
        intent_description=description,
        repair_shape=shape,
        patch_type=patch_type,
        rationale=rationale,
        confidence="medium",
        source="deterministic_archetype_adapter",
        cluster_id=cluster.cluster_id,
        target_qids=cluster.target_qids,
        blame_set=cluster.blame_set_normalized or cluster.blame_set_raw,
        rca_card_id=cluster.rca_card_id,
        ag_id=ag_id,
    )


from typing import Any


class RepairIntentCollisionError(ValueError):
    """Raised when two different intents are stamped on the same
    proposal dict. Indicates a synthesizer bug (two producers
    competing for the same proposal_id)."""


class RepairIntentPatchTypeMismatchError(ValueError):
    """Raised when a proposal's patch_type field disagrees with the
    intent's patch_type. Indicates the synthesizer's dispatcher picked
    a patch_type independently of the chosen archetype's intent."""


def stamp_repair_intent_on_proposal(
    proposal: dict[str, Any],
    intent: RepairIntent,
) -> None:
    """Mutate ``proposal`` in place to carry ``intent``.

    Adds two keys:
      * ``proposal["intent_id"]`` — stable string lookup key.
      * ``proposal["repair_intent"]`` — serialized intent dict (the
        output of ``intent.to_json()``).

    Idempotent for the same intent. Raises:
      * ``RepairIntentCollisionError`` if the proposal already carries
        a different intent_id.
      * ``RepairIntentPatchTypeMismatchError`` if proposal["patch_type"]
        (when present and non-empty) disagrees with intent.patch_type.
    """
    existing_id = str(proposal.get("intent_id") or "")
    if existing_id and existing_id != intent.intent_id:
        raise RepairIntentCollisionError(
            f"proposal_id={proposal.get('proposal_id', '?')!r} already "
            f"carries intent_id={existing_id!r}; refusing to overwrite "
            f"with {intent.intent_id!r}. Two producers competed for "
            f"the same proposal — investigate the synthesis dispatch."
        )
    proposal_pt = str(proposal.get("patch_type") or "")
    if proposal_pt and proposal_pt != intent.patch_type.value:
        raise RepairIntentPatchTypeMismatchError(
            f"proposal_id={proposal.get('proposal_id', '?')!r} has "
            f"patch_type={proposal_pt!r} but intent patch_type="
            f"{intent.patch_type.value!r}. The synthesizer's "
            f"dispatcher picked a patch_type independently of the "
            f"chosen archetype's intent."
        )
    proposal["intent_id"] = intent.intent_id
    proposal["repair_intent"] = intent.to_json()


def extract_repair_intent_from_proposal(
    proposal: dict[str, Any],
) -> RepairIntent | None:
    """Read a typed ``RepairIntent`` off a stamped proposal dict.

    Returns ``None`` when the proposal carries no ``repair_intent``
    field (legacy / unstamped proposals during the Plan 1 rollout
    window). After Plan 4 every proposal will carry one and a
    follow-up plan can promote this to non-Optional.
    """
    payload = proposal.get("repair_intent")
    if not payload or not isinstance(payload, dict):
        return None
    return RepairIntent.from_json(payload)
