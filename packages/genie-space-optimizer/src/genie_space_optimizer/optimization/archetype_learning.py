"""Phase 2 Action 2.5 — In-loop archetype learning.

This module supplies the dataclasses and pure logic for the four-tier
lifecycle described in the Section E overview. It deliberately reuses
Section A's :class:`RepairArchetype` shape — :class:`ProvisionalArchetype`
adds two fields (``provenance``, ``lifecycle_state``) plus tracking
metadata (``signature_hash``, ``synthesis_iteration``) so the planner
can treat provisional archetypes identically to canonical ones.

The four lifecycle states for a provisional archetype:

* ``provisional`` — synthesised but not yet trialled, or trialled with
  ``diagnostic_hold`` outcome (eligible for re-trial up to a cap).
* ``confirmed_in_run`` — its kit cleared the acceptance gate at least
  once in this run; the archetype is reused for subsequent iterations.
* ``failed_in_run`` — its kit was rejected with ``loss``; the archetype
  is parked for this run.
* ``synthesis_declined`` — the synthesis LLM declined; the candidate is
  parked for the next iteration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.repair_archetypes import RepairArchetype

LifecycleState = Literal[
    "provisional",
    "confirmed_in_run",
    "failed_in_run",
    "synthesis_declined",
]


@dataclass(frozen=True)
class UnmatchedPatternRecord:
    """One record per cluster the planner could not match to any
    archetype (canonical or provisional). The signature is the
    grouping key for Tier 2 detection."""

    signature_hash: str
    cluster_id: str
    root_cause_label: str
    grounding_terms: frozenset[str]
    intended_patch_shape: str
    asi_question_intent: str
    qids: tuple[str, ...]


@dataclass(frozen=True)
class PatternCandidate:
    """A group of >= K unmatched-pattern records sharing the same
    signature. Eligible inputs to Tier 3 synthesis."""

    signature_hash: str
    member_cluster_ids: tuple[str, ...]
    union_qids: tuple[str, ...]
    root_cause_label: str
    grounding_terms: frozenset[str]
    intended_patch_shape: str
    asi_question_intent: str
    member_count: int


@dataclass(frozen=True)
class ProvisionalArchetype:
    """A RepairArchetype synthesised in-loop from a PatternCandidate.

    Mirrors Section A's RepairArchetype field-for-field so the planner
    can merge the two lists without branching. Adds provenance +
    lifecycle_state + tracking metadata for trial accounting and
    offline promotion.
    """

    name: str
    applicable_rca_kinds: frozenset[RcaKind]
    required_grounding_tokens: frozenset[str]
    evidence_predicates: frozenset[str]
    default_priority_step: str
    expected_causal_effect_template: str
    rationale: str
    provenance: Literal["provisional_archetype"]
    lifecycle_state: LifecycleState
    signature_hash: str
    synthesis_iteration: int
    trial_iterations: tuple[int, ...] = field(default_factory=tuple)
    last_outcome: str = ""

    def to_repair_archetype(self) -> RepairArchetype:
        """Project to Section A's :class:`RepairArchetype` shape so it
        can be passed through the planner's ``additional_archetypes``
        parameter without further adapter logic. The Section A registry
        carries the same provenance/lifecycle fields as defaults
        (``canonical`` / ``stable``); the projection overrides them with
        the provisional values so the kit dict still surfaces the
        provenance the operator transcript expects.
        """
        return RepairArchetype(
            name=self.name,
            applicable_rca_kinds=self.applicable_rca_kinds,
            required_grounding_tokens=self.required_grounding_tokens,
            evidence_predicates=self.evidence_predicates,
            default_priority_step=self.default_priority_step,
            expected_causal_effect_template=self.expected_causal_effect_template,
            rationale=self.rationale,
            provenance=self.provenance,
            lifecycle_state=self.lifecycle_state,
        )
