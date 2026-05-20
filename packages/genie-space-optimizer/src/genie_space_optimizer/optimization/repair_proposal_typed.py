"""Plan 5 — typed RepairProposal carrier + per-patch-type projectors.

Public symbols:

  * ``RepairProposal`` — frozen+slots+JsonRoundTrip dataclass with
    the framework-stamped ``intent_id`` plus the eight LLM-emitted
    fields.
  * ``RepairProposal.to_proposal_dict()`` — per-patch-type projector
    to the proposal-dict shape the appropriate per-lever generator
    expects.
  * ``RepairProposal.to_repair_intent(cluster, ag_id)`` — bridge to
    Plan 1's RepairIntent for stamping onto the canonical proposal
    dict via ``stamp_repair_intent_on_proposal``.
  * ``RepairProposal.from_llm_output(pydantic_inst, intent_id)`` —
    bridge from Pydantic LlmRepairProposalOutput to the dataclass.
  * ``PatchBodyValidationError`` — raised by ``to_proposal_dict``
    and by ``_validate_patch_body_against_patch_type`` in the
    synthesizer when patch_body fails the per-patch-type
    required-field check.

Unidirectional: depends on ``repair_intent`` (Plan 1) only — no
imports from the synthesizer or the cross-lever router.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


class PatchBodyValidationError(ValueError):
    """patch_body is missing required field(s) for its patch_type."""


# Per-patch-type required field sets. Keep in sync with the SKILL.md
# <patch_body_shapes> section. Adding a new patch_type that Plan 5
# should emit requires adding an entry here.
_REQUIRED_PATCH_BODY_FIELDS: dict[PatchType, frozenset[str]] = {
    PatchType.ADD_EXAMPLE_SQL: frozenset({"example_question", "example_sql"}),
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: frozenset(
        {"name", "sql_expression"}
    ),
    PatchType.ADD_SQL_SNIPPET_FILTER: frozenset(
        {"name", "sql_expression"}
    ),
    PatchType.ADD_SQL_SNIPPET_MEASURE: frozenset(
        {"name", "sql_expression"}
    ),
    PatchType.ADD_INSTRUCTION: frozenset({"instruction_text"}),
    PatchType.UPDATE_INSTRUCTION: frozenset({"instruction_id", "new_text"}),
    PatchType.ADD_JOIN_SPEC: frozenset({"left", "right", "on"}),
    PatchType.ADD_COLUMN_DESCRIPTION: frozenset(
        {"table", "column", "description"}
    ),
}


def required_patch_body_fields(patch_type: PatchType) -> frozenset[str]:
    """Public accessor for the synthesizer's validator. Returns the
    empty frozenset for patch types Plan 5 has not enumerated yet."""
    return _REQUIRED_PATCH_BODY_FIELDS.get(patch_type, frozenset())


@dataclass(frozen=True, slots=True)
class RepairProposal(JsonRoundTrip):
    """Typed RepairProposal — wire-stable carrier through Plan 5."""

    intent_id: str
    intent_name: str
    intent_description: str
    repair_shape: RepairShape
    patch_type: PatchType
    rationale: str
    confidence: Literal["high", "medium", "low"]
    patch_body: dict[str, Any]
    blame_set: tuple[str, ...]
    # Plan 9 Task 1 — LLM-emitted typed slice. Replaces archetype-
    # derived AssetSlice picking (which lived in pick_archetype +
    # _derive_asset_slice_from_afs). Default () for backward
    # compatibility with pre-Plan-9 serialized proposals.
    target_objects: tuple[TargetObject, ...] = ()

    @classmethod
    def from_llm_output(
        cls,
        pydantic_inst: Any,
        *,
        intent_id: str,
    ) -> "RepairProposal":
        """Bridge from Pydantic LlmRepairProposalOutput to the
        dataclass."""
        target_objects_raw = (
            getattr(pydantic_inst, "target_objects", None) or []
        )
        target_objects = tuple(
            TargetObject(
                asset_kind=AssetKind(getattr(t, "asset_kind", "table")),
                identifier=str(getattr(t, "identifier", "")),
                columns=tuple(
                    str(c) for c in (getattr(t, "columns", None) or [])
                ),
            )
            for t in target_objects_raw
        )
        return cls(
            intent_id=str(intent_id),
            intent_name=str(pydantic_inst.intent_name),
            intent_description=str(pydantic_inst.intent_description),
            repair_shape=RepairShape(pydantic_inst.repair_shape),
            patch_type=PatchType(pydantic_inst.patch_type),
            rationale=str(pydantic_inst.rationale),
            confidence=pydantic_inst.confidence,
            patch_body=dict(pydantic_inst.patch_body or {}),
            blame_set=tuple(
                str(b) for b in pydantic_inst.blame_set or ()
            ),
            target_objects=target_objects,
        )

    def to_proposal_dict(self) -> dict[str, Any]:
        """Per-patch-type projection to the proposal-dict shape the
        chosen per-lever generator expects.

        Raises ``PatchBodyValidationError`` if patch_body is missing
        a required field for the patch_type.
        """
        required = required_patch_body_fields(self.patch_type)
        missing = sorted(required - self.patch_body.keys())
        if missing:
            raise PatchBodyValidationError(
                f"patch_body for patch_type={self.patch_type.value!r} "
                f"missing required field(s): {missing}"
            )

        pb = self.patch_body
        if self.patch_type == PatchType.ADD_EXAMPLE_SQL:
            return {
                "example_question": str(pb["example_question"]),
                "example_sql": str(pb["example_sql"]),
                "parameters": list(pb.get("parameters") or []),
                "usage_guidance": str(
                    pb.get("usage_guidance") or self.rationale
                ),
            }
        if self.patch_type in (
            PatchType.ADD_SQL_SNIPPET_EXPRESSION,
            PatchType.ADD_SQL_SNIPPET_FILTER,
            PatchType.ADD_SQL_SNIPPET_MEASURE,
        ):
            return {
                "name": str(pb["name"]),
                "sql_expression": str(pb["sql_expression"]),
                "usage_guidance": str(
                    pb.get("usage_guidance") or self.rationale
                ),
            }
        if self.patch_type == PatchType.ADD_INSTRUCTION:
            return {
                "instruction_text": str(pb["instruction_text"]),
                "rationale": self.rationale,
            }
        if self.patch_type == PatchType.UPDATE_INSTRUCTION:
            return {
                "instruction_id": str(pb["instruction_id"]),
                "new_text": str(pb["new_text"]),
                "rationale": self.rationale,
            }
        if self.patch_type == PatchType.ADD_JOIN_SPEC:
            return {
                "left": str(pb["left"]),
                "right": str(pb["right"]),
                "on": str(pb["on"]),
                "usage_guidance": str(
                    pb.get("usage_guidance") or self.rationale
                ),
            }
        if self.patch_type == PatchType.ADD_COLUMN_DESCRIPTION:
            return {
                "table": str(pb["table"]),
                "column": str(pb["column"]),
                "description": str(pb["description"]),
            }
        return dict(pb)

    def to_repair_intent(
        self,
        *,
        cluster: Any,
        ag_id: str,
    ) -> RepairIntent:
        """Project to Plan 1's RepairIntent for stamping onto the
        canonical proposal dict.

        ``intent_id`` is preserved verbatim — the framework already
        stamped it deterministically before this projector runs.
        """
        return RepairIntent(
            intent_id=self.intent_id,
            intent_name=self.intent_name,
            intent_description=self.intent_description,
            repair_shape=self.repair_shape,
            patch_type=self.patch_type,
            rationale=self.rationale,
            confidence=self.confidence,
            source="llm_l5b_synthesis",
            cluster_id=str(getattr(cluster, "cluster_id", "")),
            target_qids=tuple(
                str(q) for q in getattr(cluster, "target_qids", ())
            ),
            blame_set=self.blame_set,
            rca_card_id=str(getattr(cluster, "rca_card_id", "")),
            ag_id=str(ag_id),
            target_objects=self.target_objects,
        )

    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "intent_id": self.intent_id,
            "intent_name": self.intent_name,
            "intent_description": self.intent_description,
            "repair_shape": self.repair_shape.value,
            "patch_type": self.patch_type.value,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "patch_body": dict(self.patch_body),
            "blame_set": list(self.blame_set),
            "target_objects": [t.to_json() for t in self.target_objects],
        }

    @classmethod
    def from_json(cls, payload: dict) -> "RepairProposal":  # type: ignore[override]
        return cls(
            intent_id=str(payload["intent_id"]),
            intent_name=str(payload["intent_name"]),
            intent_description=str(payload["intent_description"]),
            repair_shape=RepairShape(payload["repair_shape"]),
            patch_type=PatchType(payload["patch_type"]),
            rationale=str(payload["rationale"]),
            confidence=str(payload["confidence"]),  # type: ignore[arg-type]
            patch_body=dict(payload.get("patch_body") or {}),
            blame_set=tuple(
                str(b) for b in payload.get("blame_set") or ()
            ),
            target_objects=tuple(
                TargetObject.from_json(t)
                for t in (payload.get("target_objects") or ())
            ),
        )
