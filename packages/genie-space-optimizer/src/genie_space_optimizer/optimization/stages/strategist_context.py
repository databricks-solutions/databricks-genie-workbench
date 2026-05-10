"""Stage 3.5 (NEW for C15): Strategist Context.

The user's stated north-star goal in Cycle 15: 'the journey from MLflow
Judges Verdicts to Strategist Proposal Generation should be deterministic.'
This stage names every field the strategist sees as input — closing the
implicit kwargs-soup that lives in ``optimizer._build_context_data`` today.

This stage runs AFTER ``rca_evidence`` and BEFORE the strategist LLM
call. With the contract in place, the boundary fixture can capture the
exact JSON the LLM receives, and the strategist-context derivation is
a pure function from typed Input → typed Output (no implicit dict
reshapes).

This stage also ENFORCES the original Cycle 15 'Stage 2 → Stage 4'
arrow at the type level: ``rca_cards_grounded_only`` exposes only
grounded RCAs to the strategist. Ungrounded cards are recorded
separately for observability but are not surfaced in the prompt.

Production shape: the ``rca_cards`` slot accepts raw dicts (from
``RcaEvidenceBundle.per_qid_evidence``) because the legacy schema
carries fields beyond what a typed RcaCard would expose. The stage
filters by ``grounding == "grounded"`` using a string check, which
is tolerant of both StrEnum values and bare strings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


STAGE_KEY: str = "strategist_context"


@dataclass(frozen=True, slots=True)
class StrategistContextInput(JsonRoundTrip):
    """The typed upstream boundary feeding the strategist.

    Collects every piece of upstream evidence the strategist LLM needs.
    All RCA-grounding filtering happens in the Output, not here — the
    Input is a faithful capture of what was computed.
    """

    hard_failure_qids: tuple[str, ...] = ()
    soft_failure_qids: tuple[str, ...] = ()
    passing_qids: tuple[str, ...] = ()
    clusters_by_qid: dict[str, str] = field(default_factory=dict)
    # rca_cards: raw dicts from RcaEvidenceBundle.per_qid_evidence.
    # Each dict may carry: rca_id, cluster_id, grounding, evidence_qids,
    # rca_kind, judge_verdict, sql_diff, counterfactual_fix, etc.
    rca_cards: tuple[dict, ...] = ()
    forbidden_ag_ids: tuple[str, ...] = ()
    reflection_buffer: tuple[dict, ...] = ()
    baseline_accuracy: float = 0.0
    candidate_accuracy: float = 0.0
    iteration: int = 0


@dataclass(frozen=True, slots=True)
class StrategistContextOutput(JsonRoundTrip):
    """The exact logical boundary the strategist LLM receives.

    Field order is deterministic so JSON serialization is byte-stable
    across runs. Renaming any field requires bumping the strategist
    prompt version; the strategist's prompt template references these
    names verbatim.

    ``rca_cards_grounded_only`` is the Stage 2→4 arrow enforcement:
    only RCA cards where grounding == "grounded" reach the strategist.
    ``rca_cards_ungrounded_count`` records dropped cards for
    observability / audit.
    """

    iteration: int = 0
    baseline_accuracy: float = 0.0
    hard_failure_qids: tuple[str, ...] = ()
    soft_failure_qids: tuple[str, ...] = ()
    passing_qids: tuple[str, ...] = ()
    clusters_by_qid: dict[str, str] = field(default_factory=dict)
    rca_cards_grounded_only: tuple[dict, ...] = ()
    rca_cards_ungrounded_count: int = 0
    forbidden_ag_ids: tuple[str, ...] = ()
    reflection_buffer: tuple[dict, ...] = ()


def build_strategist_context(
    ctx: Any,
    inp: StrategistContextInput,
) -> StrategistContextOutput:
    """Pure function: StrategistContextInput → StrategistContextOutput.

    Filters RCA cards to grounded-only (Stage 2→4 arrow) and copies
    all other fields verbatim. No LLM calls, no I/O, no side effects.
    """
    grounded = tuple(
        c for c in inp.rca_cards
        if str(c.get("grounding", "")).lower() == "grounded"
    )
    ungrounded_n = sum(
        1 for c in inp.rca_cards
        if str(c.get("grounding", "")).lower() == "ungrounded"
    )
    return StrategistContextOutput(
        iteration=inp.iteration,
        baseline_accuracy=inp.baseline_accuracy,
        hard_failure_qids=inp.hard_failure_qids,
        soft_failure_qids=inp.soft_failure_qids,
        passing_qids=inp.passing_qids,
        clusters_by_qid=dict(inp.clusters_by_qid),
        rca_cards_grounded_only=grounded,
        rca_cards_ungrounded_count=ungrounded_n,
        forbidden_ag_ids=inp.forbidden_ag_ids,
        reflection_buffer=inp.reflection_buffer,
    )


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = StrategistContextInput
OUTPUT_CLASS = StrategistContextOutput


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = build_strategist_context
