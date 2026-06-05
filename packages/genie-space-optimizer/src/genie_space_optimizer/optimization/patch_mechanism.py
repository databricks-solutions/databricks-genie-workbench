"""P4 C2 — Coarse patch-mechanism taxonomy + mechanism-repeat guard.

The d139 postmortem showed the optimizer emitting ``add_example_sql``
four iterations in a row against the same QID, each time landing
``kept_insufficient``, and never pivoting to a different mechanism.
The Plan 12 pivot decider had no signal beyond ``prior_patch_family``
to drive a mechanism switch; nothing prevented the LLM from selecting
the same mechanism again.

This module owns a *coarse*, behavior-grounded taxonomy of patch
mechanisms (six values) and the typed guard that prevents repeating
the same mechanism after an ``unproductive`` outcome on the same
``(qid, behavior_delta)``. Unlike a deterministic
``KIT_FOR_RCA`` map, the taxonomy is mechanism-level (how the patch
acts on the space), not RCA-kind-level, so the guard generalizes to
clusters the planner did not anticipate.

The guard fires when:

  1. A proposal targets the same ``(qid, behavior_delta_hash,
     mechanism)`` tuple as a prior attempt whose outcome was
     ``kept_insufficient`` or ``no_applied_patches``, AND
  2. The proposal does NOT pair the repeated mechanism with at
     least one *additional* new mechanism (e.g. ``example_sql +
     metadata_description``).

The mechanism-change justification field on the proposal
(``mechanism_change_justification``) is LLM-authored free text the
guard records but does NOT machine-validate. The audit marker
``GSO_MECHANISM_REPEAT_GUARD_V1`` lets postmortems verify the
justification was emitted.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum


class PatchMechanism(StrEnum):
    """Coarse mechanism taxonomy. Six closed values.

    The taxonomy is at the LEVEL OF HOW THE PATCH ACTS ON THE
    SPACE — not what RCA-kind it targets. The same RCA kind can be
    addressed by multiple mechanisms; the same mechanism can target
    multiple RCA kinds. That deliberate separation is what makes the
    guard generalize.

      * ``INSTRUCTION_TEXT``     — adds/edits free-text instructions.
      * ``EXAMPLE_SQL``          — adds/edits example-SQL pairs.
      * ``SQL_SNIPPET``          — adds add_sql_snippet_* (filter,
        expression, measure).
      * ``METADATA_DESCRIPTION`` — adds/edits column / table /
        TVF descriptions or synonyms.
      * ``METADATA_JOIN``        — adds/edits join specs.
      * ``ROUTING``              — table-add / table-remove / view
        toggle (changes which assets the space can route to).

    ``ROUTING`` covers the asset_routing_error RCA kind that
    instruction-only patches cannot fix. The taxonomy maps to
    PatchType values via :func:`mechanism_for_patch_type`.
    """

    INSTRUCTION_TEXT = "instruction_text"
    EXAMPLE_SQL = "example_sql"
    SQL_SNIPPET = "sql_snippet"
    METADATA_DESCRIPTION = "metadata_description"
    METADATA_JOIN = "metadata_join"
    ROUTING = "routing"


# Wire-string patch_type → coarse mechanism. The mapping is closed —
# adding a new patch_type without an entry here triggers
# ``mechanism_for_patch_type`` to return ``None``, which downstream
# treats as "unclassified mechanism" rather than silently routing it
# to a default bucket.
_PATCH_TYPE_TO_MECHANISM: dict[str, PatchMechanism] = {
    # Instructions
    "add_instruction": PatchMechanism.INSTRUCTION_TEXT,
    "update_instruction": PatchMechanism.INSTRUCTION_TEXT,
    "update_instruction_section": PatchMechanism.INSTRUCTION_TEXT,
    "rewrite_instruction": PatchMechanism.INSTRUCTION_TEXT,
    "remove_instruction": PatchMechanism.INSTRUCTION_TEXT,
    # Example SQLs (positive + negative use the same applier dispatch
    # arm and the same behavioral mechanism even though the optimizer
    # tracks negatives separately).
    "add_example_sql": PatchMechanism.EXAMPLE_SQL,
    "add_example_sql_negative": PatchMechanism.EXAMPLE_SQL,
    "update_example_sql": PatchMechanism.EXAMPLE_SQL,
    "remove_example_sql": PatchMechanism.EXAMPLE_SQL,
    # SQL snippets
    "add_sql_snippet_filter": PatchMechanism.SQL_SNIPPET,
    "add_sql_snippet_expression": PatchMechanism.SQL_SNIPPET,
    "add_sql_snippet_measure": PatchMechanism.SQL_SNIPPET,
    # Filter / measure / view edits land on SQL_SNIPPET because they
    # express the same kind of "shape" the snippet vocabulary owns.
    "add_default_filter": PatchMechanism.SQL_SNIPPET,
    "remove_default_filter": PatchMechanism.SQL_SNIPPET,
    "update_filter_condition": PatchMechanism.SQL_SNIPPET,
    "add_mv_measure": PatchMechanism.SQL_SNIPPET,
    "update_mv_measure": PatchMechanism.SQL_SNIPPET,
    "remove_mv_measure": PatchMechanism.SQL_SNIPPET,
    "add_mv_dimension": PatchMechanism.SQL_SNIPPET,
    "remove_mv_dimension": PatchMechanism.SQL_SNIPPET,
    "update_mv_yaml": PatchMechanism.SQL_SNIPPET,
    "update_tvf_sql": PatchMechanism.SQL_SNIPPET,
    # Metadata descriptions / synonyms
    "add_description": PatchMechanism.METADATA_DESCRIPTION,
    "update_description": PatchMechanism.METADATA_DESCRIPTION,
    "add_column_description": PatchMechanism.METADATA_DESCRIPTION,
    "update_column_description": PatchMechanism.METADATA_DESCRIPTION,
    "add_tvf_description": PatchMechanism.METADATA_DESCRIPTION,
    "hide_column": PatchMechanism.METADATA_DESCRIPTION,
    "unhide_column": PatchMechanism.METADATA_DESCRIPTION,
    "rename_column_alias": PatchMechanism.METADATA_DESCRIPTION,
    "add_column_synonym": PatchMechanism.METADATA_DESCRIPTION,
    "remove_column_synonym": PatchMechanism.METADATA_DESCRIPTION,
    # Joins
    "add_join_spec": PatchMechanism.METADATA_JOIN,
    "update_join_spec": PatchMechanism.METADATA_JOIN,
    "remove_join_spec": PatchMechanism.METADATA_JOIN,
    # Routing
    "add_table": PatchMechanism.ROUTING,
    "remove_table": PatchMechanism.ROUTING,
    "add_tvf": PatchMechanism.ROUTING,
    "remove_tvf": PatchMechanism.ROUTING,
    "add_tvf_parameter": PatchMechanism.ROUTING,
    "remove_tvf_parameter": PatchMechanism.ROUTING,
    "enable_example_values": PatchMechanism.ROUTING,
    "disable_example_values": PatchMechanism.ROUTING,
    "enable_value_dictionary": PatchMechanism.ROUTING,
    "disable_value_dictionary": PatchMechanism.ROUTING,
}


def mechanism_for_patch_type(
    patch_type_wire: str,
) -> PatchMechanism | None:
    """Resolve a wire-string patch_type to its mechanism.

    Returns ``None`` for unknown patch types — callers treat that as
    ``"unclassified mechanism"`` and do NOT default to a bucket. Any
    new patch_type must add an entry to ``_PATCH_TYPE_TO_MECHANISM``
    in the same commit; the test in ``test_patch_mechanism.py`` pins
    the coverage.
    """
    return _PATCH_TYPE_TO_MECHANISM.get(
        str(patch_type_wire or "").strip().lower()
    )


# Outcomes that count as "unproductive" for the repeat guard.
# ``kept_insufficient`` = the patch was kept by the applier but did
# not move the target QID. ``no_applied_patches`` = the applier
# dropped the patch entirely.
UNPRODUCTIVE_OUTCOMES: frozenset[str] = frozenset(
    {"kept_insufficient", "no_applied_patches"}
)


def behavior_delta_hash(behavior_delta: str) -> str:
    """Stable 8-char hash of a behavior_delta string.

    The hash is the *key* the guard uses to compare two proposals'
    behavior_deltas — exact string equality would be too brittle
    (whitespace, punctuation, paraphrase). The 8-char prefix is
    deterministic and short enough to inline in marker payloads.

    Normalization: lowercase + strip + collapse internal whitespace.
    """
    normalized = " ".join(str(behavior_delta or "").lower().split())
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]


@dataclass(frozen=True, slots=True)
class MechanismAttempt:
    """One past attempt: ``(qid, behavior_delta_hash, mechanism,
    outcome)``."""

    qid: str
    behavior_delta_hash: str
    mechanism: PatchMechanism
    outcome: str


@dataclass(frozen=True, slots=True)
class MechanismRepeatVerdict:
    """Outcome of :func:`check_mechanism_repeat_guard`.

    ``outcome`` ∈ {``"allowed"``, ``"blocked"``}.

      * ``"allowed"``  — the proposal does NOT repeat an
        unproductive mechanism, OR it repeats but pairs the
        repeated mechanism with at least one additional new
        mechanism that has not been tried for this
        ``(qid, behavior_delta_hash)``.
      * ``"blocked"``  — the proposal repeats a mechanism with no
        new mechanism paired in. The synthesizer MUST either:
          - switch to a different mechanism, or
          - add a new mechanism alongside the repeated one and
            re-submit with a non-empty
            ``mechanism_change_justification``.

    ``forbidden_mechanism`` names the repeated mechanism (when
    blocked) so the marker payload is self-describing.
    """

    outcome: str
    forbidden_mechanism: PatchMechanism | None
    feedback: str
    prior_unproductive_outcome: str = ""


def check_mechanism_repeat_guard(
    *,
    qid: str,
    behavior_delta: str,
    proposed_mechanisms: tuple[PatchMechanism, ...],
    mechanism_change_justification: str,
    prior_attempts: tuple[MechanismAttempt, ...],
) -> MechanismRepeatVerdict:
    """Return the repeat-guard verdict for one proposal.

    Args:
      qid: the target question id.
      behavior_delta: the structured repair-intent behavior_delta
        from C1.
      proposed_mechanisms: the mechanisms this proposal includes
        (length ≥ 1).
      mechanism_change_justification: LLM-authored justification
        the proposal carries. Required iff the proposal repeats a
        mechanism with an additional new mechanism paired in;
        otherwise empty is fine.
      prior_attempts: all prior attempts for this ``qid``.

    Predicate:

      For each ``m in proposed_mechanisms``:
        if any prior attempt has the same ``(qid,
        behavior_delta_hash, mechanism=m, outcome ∈
        UNPRODUCTIVE_OUTCOMES)`` AND
        no other proposed mechanism is new w.r.t. the prior
        ``(qid, behavior_delta_hash)`` set:
          block(m)

    "New mechanism paired in" = some other proposed mechanism is
    NOT in the set of mechanisms tried before for this
    ``(qid, behavior_delta_hash)``.
    """
    bdh = behavior_delta_hash(behavior_delta)
    # Set of mechanisms already attempted (any outcome) for this
    # (qid, behavior_delta_hash) pair.
    tried_mechanisms = frozenset(
        a.mechanism
        for a in prior_attempts
        if a.qid == qid and a.behavior_delta_hash == bdh
    )
    # Set of mechanisms that landed unproductively for this pair.
    unproductive_mechanisms: dict[PatchMechanism, str] = {}
    for a in prior_attempts:
        if (
            a.qid == qid
            and a.behavior_delta_hash == bdh
            and a.outcome in UNPRODUCTIVE_OUTCOMES
        ):
            unproductive_mechanisms[a.mechanism] = a.outcome

    proposed_set = frozenset(proposed_mechanisms)
    new_mechanisms_paired = proposed_set - tried_mechanisms

    for m in proposed_mechanisms:
        if m not in unproductive_mechanisms:
            continue
        # Is some OTHER proposed mechanism new?
        other_new = new_mechanisms_paired - {m}
        if other_new:
            # Repeated mechanism is paired with a new one — allowed.
            # We require justification on this path. Empty is still
            # allowed (the marker records it) but the audit field
            # is the loud signal for postmortems.
            continue
        return MechanismRepeatVerdict(
            outcome="blocked",
            forbidden_mechanism=m,
            feedback=(
                f"mechanism {m.value!r} already attempted on this "
                f"(qid, behavior_delta) with outcome "
                f"{unproductive_mechanisms[m]!r}; either switch "
                f"mechanism or pair {m.value!r} with at least one "
                f"new mechanism and emit a "
                f"mechanism_change_justification"
            ),
            prior_unproductive_outcome=unproductive_mechanisms[m],
        )
    return MechanismRepeatVerdict(
        outcome="allowed",
        forbidden_mechanism=None,
        feedback="",
    )


def mechanism_repeat_guard_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    behavior_delta: str,
    proposed_mechanisms: tuple[PatchMechanism, ...],
    verdict: MechanismRepeatVerdict,
    mechanism_change_justification: str,
) -> str:
    """Return one ``GSO_MECHANISM_REPEAT_GUARD_V1`` marker line."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        marker_line,
    )

    return marker_line(
        "GSO_MECHANISM_REPEAT_GUARD_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "behavior_delta_hash": behavior_delta_hash(behavior_delta),
            "proposed_mechanisms": [m.value for m in proposed_mechanisms],
            "outcome": verdict.outcome,
            "forbidden_mechanism": (
                verdict.forbidden_mechanism.value
                if verdict.forbidden_mechanism
                else ""
            ),
            "prior_unproductive_outcome": (
                verdict.prior_unproductive_outcome
            ),
            "mechanism_change_justification": (
                str(mechanism_change_justification or "")
            ),
            "feedback": verdict.feedback,
        },
    )
