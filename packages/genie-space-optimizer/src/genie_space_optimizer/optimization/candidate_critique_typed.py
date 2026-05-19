"""Plan 6 — typed CritiqueVerdict carrier.

Public symbols:

  * ``CritiqueVerdict`` — frozen+slots+JsonRoundTrip dataclass with
    the framework-stamped ``proposal_id`` plus the six LLM-emitted
    fields.
  * ``CritiqueVerdict.is_blocking()`` — single source of truth for
    the enforcing decision. True iff ``overall_recommendation ==
    "discard"``.
  * ``CritiqueVerdict.reason_code()`` — maps the recommendation to
    the ``ReasonCode`` enum member (added by Task 13) so postmortem
    grouping is cardinality-bounded.
  * ``CritiqueVerdict.from_llm_output(pydantic_inst, proposal_id)``
    — bridge from Pydantic LlmCritiqueVerdictOutput to the dataclass.

Unidirectional: depends on ``rca_decision_trace`` (for ReasonCode)
and ``stages._json_io`` only — zero imports from the stage module,
the synthesizer, or the skill.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from genie_space_optimizer.optimization.rca_decision_trace import (
    ReasonCode,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class CritiqueVerdict(JsonRoundTrip):
    """Typed CritiqueVerdict — wire-stable carrier through Plan 6."""

    proposal_id: str
    addresses_target_failure: bool
    is_overgeneralized: bool
    likely_neighbor_regressions: tuple[str, ...]
    matches_intended_shape: bool
    overall_recommendation: Literal["proceed", "rework", "discard"]
    rationale: str

    @classmethod
    def from_llm_output(
        cls,
        pydantic_inst: Any,
        *,
        proposal_id: str,
    ) -> "CritiqueVerdict":
        """Bridge from Pydantic LlmCritiqueVerdictOutput to the dataclass."""
        return cls(
            proposal_id=str(proposal_id),
            addresses_target_failure=bool(pydantic_inst.addresses_target_failure),
            is_overgeneralized=bool(pydantic_inst.is_overgeneralized),
            likely_neighbor_regressions=tuple(
                str(q) for q in pydantic_inst.likely_neighbor_regressions or ()
            ),
            matches_intended_shape=bool(pydantic_inst.matches_intended_shape),
            overall_recommendation=pydantic_inst.overall_recommendation,
            rationale=str(pydantic_inst.rationale),
        )

    def is_blocking(self) -> bool:
        """True iff the verdict would block the proposal when
        ``GSO_CRITIQUE_GATE_ENFORCING=true``. Single source of truth —
        the stage module reads this rather than re-checking the
        recommendation string."""
        return self.overall_recommendation == "discard"

    def reason_code(self) -> ReasonCode:
        """Map the verdict's recommendation to a closed ReasonCode for
        postmortem grouping. Each recommendation → exactly one code."""
        return _REASON_CODE_BY_RECOMMENDATION[self.overall_recommendation]

    @classmethod
    def from_json(cls, payload: dict) -> "CritiqueVerdict":  # type: ignore[override]
        return cls(
            proposal_id=str(payload["proposal_id"]),
            addresses_target_failure=bool(payload["addresses_target_failure"]),
            is_overgeneralized=bool(payload["is_overgeneralized"]),
            likely_neighbor_regressions=tuple(
                str(q) for q in payload.get("likely_neighbor_regressions") or ()
            ),
            matches_intended_shape=bool(payload["matches_intended_shape"]),
            overall_recommendation=str(payload["overall_recommendation"]),  # type: ignore[arg-type]
            rationale=str(payload["rationale"]),
        )


_REASON_CODE_BY_RECOMMENDATION: dict[str, ReasonCode] = {
    "proceed": ReasonCode.CRITIQUE_PROCEED,
    "rework": ReasonCode.CRITIQUE_REWORK,
    "discard": ReasonCode.CRITIQUE_DISCARD,
}
