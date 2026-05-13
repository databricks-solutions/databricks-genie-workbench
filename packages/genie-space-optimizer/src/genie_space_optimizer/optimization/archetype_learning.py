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


# ---------------------------------------------------------------------------
# Phase 2 Action 2.5 Tier 1 — signature + unmatched-pattern record emission.
# ---------------------------------------------------------------------------

import hashlib


def compute_unmatched_signature(
    *,
    card: "RCACard",
    asi_question_intent: str,
) -> str:
    """Deterministic 16-char hex signature derived from the cluster
    shape — the grouping key for Tier 2 candidate detection.

    The signature MUST be stable across iterations and runs for the
    same input shape, otherwise pattern_candidate detection cannot
    accumulate evidence across iterations.
    """
    rc = card.root_cause
    rc_label = rc.name if hasattr(rc, "name") else str(rc)
    parts = (
        str(rc_label),
        ",".join(sorted(card.grounding_terms or ())),
        str(card.intended_patch_shape or ""),
        str(asi_question_intent or ""),
    )
    raw = "|".join(parts).encode("utf-8")
    return hashlib.blake2s(raw, digest_size=8).hexdigest()


def emit_unmatched_pattern_record(
    *,
    run_id: str,
    card: "RCACard",
    cluster: dict,
) -> UnmatchedPatternRecord:
    """Tier 1: build the record, append it to the run state, and
    return it so the caller can also emit a decision record.

    Pure with respect to the LLM and the catalog — only mutates
    the in-memory run state.
    """
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
    )

    intent = str(cluster.get("asi_question_intent") or "")
    sig = compute_unmatched_signature(card=card, asi_question_intent=intent)
    rc = card.root_cause
    rc_label = rc.name if hasattr(rc, "name") else str(rc)
    rec = UnmatchedPatternRecord(
        signature_hash=sig,
        cluster_id=str(cluster.get("cluster_id") or ""),
        root_cause_label=str(rc_label),
        grounding_terms=frozenset(card.grounding_terms or ()),
        intended_patch_shape=str(card.intended_patch_shape or ""),
        asi_question_intent=intent,
        qids=tuple(
            str(q) for q in (cluster.get("question_ids") or []) if str(q)
        ),
    )
    get_state(run_id).unmatched_pattern_records.append(rec)
    return rec


# ---------------------------------------------------------------------------
# Phase 2 Action 2.5 Tier 2 — pattern-candidate detection.
# ---------------------------------------------------------------------------

from collections import defaultdict
from typing import Iterable


def detect_pattern_candidates(
    *,
    records: Iterable[UnmatchedPatternRecord],
    exclude_signature_hashes: frozenset[str] = frozenset(),
) -> tuple[PatternCandidate, ...]:
    """Tier 2: group records by signature; emit a PatternCandidate for
    every signature whose member count meets the configured threshold
    AND whose signature is not in ``exclude_signature_hashes``.

    The exclusion set is the union of ``signature_hash`` values
    already attached to provisional archetypes (states ``provisional``,
    ``confirmed_in_run``, ``failed_in_run``) — we do not re-detect
    a pattern for which we already have a provisional in flight.

    Output is sorted by ``member_count`` descending, then by
    ``signature_hash`` ascending for deterministic tie-break.
    """
    from genie_space_optimizer.common.config import (
        pattern_candidate_member_threshold,
    )

    threshold = pattern_candidate_member_threshold()
    grouped: dict[str, list[UnmatchedPatternRecord]] = defaultdict(list)
    for r in records:
        if r.signature_hash in exclude_signature_hashes:
            continue
        grouped[r.signature_hash].append(r)

    out: list[PatternCandidate] = []
    for sig, members in grouped.items():
        if len(members) < threshold:
            continue
        union_qids: list[str] = []
        seen: set[str] = set()
        for m in members:
            for q in m.qids:
                if q not in seen:
                    seen.add(q)
                    union_qids.append(q)
        head = members[0]
        out.append(
            PatternCandidate(
                signature_hash=sig,
                member_cluster_ids=tuple(m.cluster_id for m in members),
                union_qids=tuple(union_qids),
                root_cause_label=head.root_cause_label,
                grounding_terms=head.grounding_terms,
                intended_patch_shape=head.intended_patch_shape,
                asi_question_intent=head.asi_question_intent,
                member_count=len(members),
            )
        )
    out.sort(key=lambda c: (-c.member_count, c.signature_hash))
    return tuple(out)


# ---------------------------------------------------------------------------
# Phase 2 Action 2.5 Tier 3 — provisional-archetype synthesis (capped LLM).
# ---------------------------------------------------------------------------


def _call_llm_for_provisional_archetype_synthesis(
    *,
    candidate: PatternCandidate,
    counterfactual_examples: tuple[dict, ...] = (),
    w=None,
) -> dict | None:
    """Phase 2 Action 2.5 — single capped LLM call that proposes a
    provisional archetype synthesised from a PatternCandidate.

    Returns the parsed JSON payload (a ``dict``) on success, or
    ``None`` on decline / parse failure / LLM error.

    Production implementation will issue the real LLM call via the
    project's chat-completions client and parse a constrained JSON
    response. The stub here returns ``None`` so the Tier 3 pipeline
    short-circuits to "synthesis declined" when the harness invokes
    it in production WITHOUT a custom LLM hook. Tests patch this
    function directly on the module to supply deterministic payloads.
    """
    del candidate, counterfactual_examples, w
    return None


def synthesize_provisional_archetype(
    *,
    run_id: str,
    candidate: PatternCandidate,
    iteration: int,
    counterfactual_examples: tuple[dict, ...] = (),
    w=None,
) -> "ProvisionalArchetype | None":
    """Tier 3: optionally call the LLM and convert its payload into a
    :class:`ProvisionalArchetype`.

    Returns ``None`` when:
      * the master flag (``GSO_ARCHETYPE_LEARNING``) is OFF, or
      * the LLM sub-flag (``GSO_PROVISIONAL_SYNTHESIS_LLM``) is OFF, or
      * the per-iteration cap is reached, or
      * the LLM declines / returns an unparseable payload.

    Side-effects: appends to ``run_state.provisional_archetypes`` on
    success, and increments ``run_state.synthesis_calls_this_iteration``
    on every actual LLM invocation (not on flag-skip paths).
    """
    from genie_space_optimizer.common.config import (
        archetype_learning_enabled,
        provisional_synthesis_llm_enabled,
        provisional_synthesis_max_per_iteration,
    )
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
    )

    if not archetype_learning_enabled():
        return None
    if not provisional_synthesis_llm_enabled():
        return None
    state = get_state(run_id)
    if state.synthesis_calls_this_iteration >= provisional_synthesis_max_per_iteration():
        return None

    state.synthesis_calls_this_iteration += 1
    payload = _call_llm_for_provisional_archetype_synthesis(
        candidate=candidate,
        counterfactual_examples=counterfactual_examples,
        w=w,
    )
    if payload is None:
        return None

    try:
        rca_kinds = frozenset({RcaKind[k] for k in payload["applicable_rca_kinds"]})
    except (KeyError, TypeError):
        return None
    pa = ProvisionalArchetype(
        name=str(payload["name"]),
        applicable_rca_kinds=rca_kinds,
        required_grounding_tokens=frozenset(payload.get("required_grounding_tokens") or ()),
        evidence_predicates=frozenset(payload.get("evidence_predicates") or ()),
        default_priority_step=str(payload["default_priority_step"]),
        expected_causal_effect_template=str(payload["expected_causal_effect_template"]),
        rationale=str(payload["rationale"]),
        provenance="provisional_archetype",
        lifecycle_state="provisional",
        signature_hash=candidate.signature_hash,
        synthesis_iteration=iteration,
    )
    state.provisional_archetypes.append(pa)
    return pa


# ---------------------------------------------------------------------------
# Phase 2 Action 2.5 Tier 4 — trial-outcome bookkeeping.
# ---------------------------------------------------------------------------

import dataclasses


_PROMOTING_TIERS: frozenset[str] = frozenset({"strict_win", "net_win_with_debt"})
_FAILING_TIERS: frozenset[str] = frozenset({"loss"})
_HOLDING_TIERS: frozenset[str] = frozenset({"diagnostic_hold"})


def record_provisional_archetype_trial_outcome(
    *,
    run_id: str,
    signature_hash: str,
    iteration: int,
    acceptance_tier: str,
) -> "ProvisionalArchetype | None":
    """Tier 4: update the provisional archetype's lifecycle based on the
    iteration's acceptance-gate decision.

    Returns the (possibly mutated) :class:`ProvisionalArchetype`, or
    ``None`` when no provisional with ``signature_hash`` is registered
    for ``run_id``. Mutation is via ``dataclasses.replace`` because
    :class:`ProvisionalArchetype` is frozen.

    Lifecycle transitions:
      * ``strict_win``, ``net_win_with_debt`` → ``confirmed_in_run``
      * ``loss`` → ``failed_in_run``
      * ``diagnostic_hold`` → stays ``provisional`` (eligible for retrial)
    """
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
    )

    state = get_state(run_id)
    for idx, pa in enumerate(state.provisional_archetypes):
        if pa.signature_hash != signature_hash:
            continue
        if acceptance_tier in _PROMOTING_TIERS:
            new_state = "confirmed_in_run"
        elif acceptance_tier in _FAILING_TIERS:
            new_state = "failed_in_run"
        elif acceptance_tier in _HOLDING_TIERS:
            new_state = "provisional"
        else:
            return pa  # unknown tier — no transition
        updated = dataclasses.replace(
            pa,
            lifecycle_state=new_state,
            trial_iterations=tuple(pa.trial_iterations) + (iteration,),
            last_outcome=acceptance_tier,
        )
        state.provisional_archetypes[idx] = updated
        return updated
    return None


# ---------------------------------------------------------------------------
# Phase 2 Action 2.5 Tier 1 wiring helper.
# ---------------------------------------------------------------------------


def emit_unmatched_pattern_records_for_unmatched_clusters(
    *,
    run_id: str,
    clusters: list[dict],
) -> list[UnmatchedPatternRecord]:
    """Walk ``clusters`` and emit one Tier 1 record for every cluster
    that (a) has an ``rca_card`` AND (b) has no ``_repair_kit`` (i.e.
    the planner returned no archetype match for it).

    Returns the list of emitted records so the caller can also push
    decision records onto its iteration's ``decision_records`` buffer.
    """
    from genie_space_optimizer.optimization.rca import RCACard

    out: list[UnmatchedPatternRecord] = []
    for cluster in clusters or []:
        card = cluster.get("rca_card")
        if not isinstance(card, RCACard):
            continue
        if cluster.get("_repair_kit") is not None:
            continue
        out.append(
            emit_unmatched_pattern_record(
                run_id=run_id, card=card, cluster=cluster,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Phase 2 Action 2.5 — Tiers 2-3 iteration-prelude driver.
# ---------------------------------------------------------------------------


def run_iteration_prelude_tiers_2_to_3(
    *,
    run_id: str,
    iteration: int,
    w=None,
) -> tuple[tuple[ProvisionalArchetype, ...], tuple[PatternCandidate, ...]]:
    """Run Tier 2 detection + Tier 3 synthesis at the start of an
    iteration. Returns ``(new_provisionals, candidates)`` so the caller
    can emit decision records for both.

    Honours ``provisional_synthesis_max_per_iteration()`` — synthesis
    stops once the cap is reached.

    Excludes signatures already covered by an in-flight provisional
    archetype (states ``provisional``, ``confirmed_in_run``,
    ``failed_in_run``).
    """
    from genie_space_optimizer.common.config import archetype_learning_enabled
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
        reset_iteration_counters,
    )

    if not archetype_learning_enabled():
        return (), ()

    reset_iteration_counters(run_id)
    state = get_state(run_id)
    excluded = frozenset(p.signature_hash for p in state.provisional_archetypes)
    candidates = detect_pattern_candidates(
        records=state.unmatched_pattern_records,
        exclude_signature_hashes=excluded,
    )
    new_provisionals: list[ProvisionalArchetype] = []
    for c in candidates:
        pa = synthesize_provisional_archetype(
            run_id=run_id, candidate=c, iteration=iteration,
            counterfactual_examples=(),
            w=w,
        )
        if pa is not None:
            new_provisionals.append(pa)
    return tuple(new_provisionals), candidates
