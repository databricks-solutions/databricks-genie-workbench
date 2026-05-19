"""Plan 7 — typed NextAttemptHypothesis carrier.

Public symbols:

  * ``NextAttemptHypothesis`` — frozen+slots+JsonRoundTrip dataclass
    with framework-stamped provenance (rolled_back_intent_id,
    cluster_id, ag_id, iteration) plus the eight LLM-emitted fields.
  * ``NextAttemptHypothesis.from_llm_output(...)`` — bridge from
    Pydantic LlmNextAttemptHypothesisOutput to the dataclass.
  * ``NextAttemptHypothesis.reason_code()`` — maps confidence to the
    ReasonCode enum member so postmortem grouping is cardinality-
    bounded.

Unidirectional: depends on ``rca_decision_trace`` (ReasonCode),
``repair_intent`` (RepairShape, PatchType), and ``stages._json_io``
only — zero imports from the public entry, the synthesizer, or the
skill.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from genie_space_optimizer.optimization.rca_decision_trace import (
    ReasonCode,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class NextAttemptHypothesis(JsonRoundTrip):
    """Typed NextAttemptHypothesis — wire-stable carrier through Plan 7."""

    rolled_back_intent_id: str
    cluster_id: str
    ag_id: str
    iteration: int

    why_failed: str
    failure_mode: str
    revised_repair_shape: RepairShape | None
    revised_patch_type: PatchType | None
    revised_blame_set: tuple[str, ...] | None
    additional_evidence_needed: tuple[str, ...]
    forbidden_signatures: tuple[str, ...]
    confidence: Literal["high", "medium", "low"]

    @classmethod
    def from_llm_output(
        cls,
        pydantic_inst: Any,
        *,
        rolled_back_intent_id: str,
        cluster_id: str,
        ag_id: str,
        iteration: int,
    ) -> "NextAttemptHypothesis":
        """Bridge from Pydantic LlmNextAttemptHypothesisOutput to the
        dataclass."""
        revised_blame_set: tuple[str, ...] | None
        if pydantic_inst.revised_blame_set is None:
            revised_blame_set = None
        else:
            revised_blame_set = tuple(
                str(b) for b in pydantic_inst.revised_blame_set
            )
        return cls(
            rolled_back_intent_id=str(rolled_back_intent_id),
            cluster_id=str(cluster_id),
            ag_id=str(ag_id),
            iteration=int(iteration),
            why_failed=str(pydantic_inst.why_failed),
            failure_mode=str(pydantic_inst.failure_mode),
            revised_repair_shape=pydantic_inst.revised_repair_shape,
            revised_patch_type=pydantic_inst.revised_patch_type,
            revised_blame_set=revised_blame_set,
            additional_evidence_needed=tuple(
                str(e) for e in pydantic_inst.additional_evidence_needed or ()
            ),
            forbidden_signatures=tuple(
                str(s) for s in pydantic_inst.forbidden_signatures or ()
            ),
            confidence=pydantic_inst.confidence,
        )

    def reason_code(self) -> ReasonCode:
        """Map the hypothesis's confidence to a closed ReasonCode for
        postmortem grouping. Each confidence → exactly one code."""
        return _REASON_CODE_BY_CONFIDENCE[self.confidence]

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "rolled_back_intent_id": self.rolled_back_intent_id,
            "cluster_id": self.cluster_id,
            "ag_id": self.ag_id,
            "iteration": int(self.iteration),
            "why_failed": self.why_failed,
            "failure_mode": self.failure_mode,
            "revised_repair_shape": (
                self.revised_repair_shape.value
                if self.revised_repair_shape is not None
                else None
            ),
            "revised_patch_type": (
                self.revised_patch_type.value
                if self.revised_patch_type is not None
                else None
            ),
            "revised_blame_set": (
                list(self.revised_blame_set)
                if self.revised_blame_set is not None
                else None
            ),
            "additional_evidence_needed": list(self.additional_evidence_needed),
            "forbidden_signatures": list(self.forbidden_signatures),
            "confidence": self.confidence,
        }

    @classmethod
    def from_json(cls, payload: dict) -> "NextAttemptHypothesis":  # type: ignore[override]
        rrs = payload.get("revised_repair_shape")
        rpt = payload.get("revised_patch_type")
        rbs = payload.get("revised_blame_set")
        return cls(
            rolled_back_intent_id=str(payload["rolled_back_intent_id"]),
            cluster_id=str(payload["cluster_id"]),
            ag_id=str(payload["ag_id"]),
            iteration=int(payload["iteration"]),
            why_failed=str(payload["why_failed"]),
            failure_mode=str(payload["failure_mode"]),
            revised_repair_shape=(
                RepairShape(rrs) if rrs is not None else None
            ),
            revised_patch_type=(
                PatchType(rpt) if rpt is not None else None
            ),
            revised_blame_set=(
                tuple(str(b) for b in rbs)
                if rbs is not None
                else None
            ),
            additional_evidence_needed=tuple(
                str(e) for e in payload.get("additional_evidence_needed") or ()
            ),
            forbidden_signatures=tuple(
                str(s) for s in payload.get("forbidden_signatures") or ()
            ),
            confidence=str(payload["confidence"]),  # type: ignore[arg-type]
        )


_REASON_CODE_BY_CONFIDENCE: dict[str, ReasonCode] = {
    "high": ReasonCode.HYPOTHESIS_HIGH_CONFIDENCE,
    "medium": ReasonCode.HYPOTHESIS_MEDIUM_CONFIDENCE,
    "low": ReasonCode.HYPOTHESIS_LOW_CONFIDENCE,
}
