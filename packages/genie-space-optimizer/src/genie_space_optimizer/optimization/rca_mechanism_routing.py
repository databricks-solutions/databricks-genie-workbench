"""Trial 23 W4 — RCA-kind to mechanism routing (correct at source).

The d139 / e943 postmortems showed the optimizer defaulting to
``add_example_sql`` for RCA kinds it cannot fix. An example-SQL pair
anchors the SQL *shape* but is behaviorally inert against root causes
that need a different lever entirely:

  * ``extra_defensive_filter``      — the Genie planner injects an
    unwanted defensive predicate (e.g. ``WHERE x IS NOT NULL``). An
    exemplar does not suppress it; an instruction telling the planner
    not to add the filter, or a sql_snippet that overrides the WHERE
    clause, does.
  * ``top_n_cardinality_collapse``  — a top-N / ranking query collapses
    cardinality because the planner aggregates at the wrong grain. A
    structural snippet/expression (or a measure description) fixes the
    grammar; another exemplar repeats the collapse.
  * ``canonical_dimension_missed``  — the planner never routes to the
    canonical dimension because it is undescribed / unsynonymed. A
    metadata description (or a routing change exposing the dimension)
    fixes it; an exemplar cannot conjure a dimension the planner does
    not know exists.

This module is the *pure* routing brain. It maps each example-SQL-
insufficient RCA kind to the mechanism(s) that DO fix it, detects the
"defaulted to example_sql" anti-pattern, and builds the
``GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1`` anti-success
marker. The synthesizer prompt (and the ``plan11_synthesize`` skill)
carry the same routing guidance so the LLM is corrected at source; this
module is the runtime detector that proves whether the guidance worked.

The map is deliberately mechanism-level (``PatchMechanism``), not
patch_type-level, so it generalises across the patch_type vocabulary.
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping

from genie_space_optimizer.optimization.patch_mechanism import (
    PatchMechanism,
    mechanism_for_patch_type,
)


# RCA kinds where the EXAMPLE_SQL mechanism alone is behaviorally inert,
# mapped to the mechanism(s) that actually fix them. EXAMPLE_SQL is
# never a member of a value set here — that is the whole point.
RCA_KIND_TO_FIXING_MECHANISMS: Mapping[str, frozenset[PatchMechanism]] = {
    "extra_defensive_filter": frozenset(
        {PatchMechanism.INSTRUCTION_TEXT, PatchMechanism.SQL_SNIPPET}
    ),
    "top_n_cardinality_collapse": frozenset(
        {PatchMechanism.SQL_SNIPPET, PatchMechanism.METADATA_DESCRIPTION}
    ),
    "canonical_dimension_missed": frozenset(
        {PatchMechanism.METADATA_DESCRIPTION, PatchMechanism.ROUTING}
    ),
}


# Plan shorthand → canonical routing-brain key. The Track B plan and
# the optimizer prompts use compact RCA labels (``top_n_collapse``,
# ``defensive_filter``, ``dimension_disambiguation``); the routing brain
# keys are the longer descriptive forms. Resolving aliases at
# normalization keeps both vocabularies pointing at the SAME fixing-
# mechanism contract so a label drift cannot silently un-map an RCA.
_RCA_KIND_ALIASES: Mapping[str, str] = {
    "top_n_collapse": "top_n_cardinality_collapse",
    "defensive_filter": "extra_defensive_filter",
    "dimension_disambiguation": "canonical_dimension_missed",
}


# Trial 26 W26.2 — extend coverage to the live airline RCA distribution
# observed across the canonical anchors (TRIAL24_KIT_FORCED_V1=0 on
# both anchors because zero RCAs landed inside the Trial 24 map).
#
# Mechanism families follow the same shape as the Trial 23 entries:
# each fixing set picks the coarse mechanisms that actually reshape the
# generated SQL for that RCA. Trial 24's lever-projection
# (INSTRUCTION_TEXT → lever-5a, SQL_SNIPPET → lever-6,
# METADATA_DESCRIPTION → lever-1) then produces the matched companion
# levers in ``_TRIAL26_KIT_FOR_RCA`` (see ``stages/action_groups``).
_TRIAL26_RCA_KIND_TO_FIXING_MECHANISMS: Mapping[str, frozenset[PatchMechanism]] = {
    # Aggregation defect → snippet (corrected measure) + metadata
    # description (clarify column / measure semantics).
    "wrong_aggregation": frozenset(
        {PatchMechanism.SQL_SNIPPET, PatchMechanism.METADATA_DESCRIPTION}
    ),
    # Wrong column selected → metadata description (disambiguate the
    # canonical column) + instruction text (route to the correct one).
    "wrong_column": frozenset(
        {PatchMechanism.METADATA_DESCRIPTION, PatchMechanism.INSTRUCTION_TEXT}
    ),
}


# Trial 26 W26.2 alias — ``plural_top_n_collapse`` is the same defect as
# ``top_n_cardinality_collapse`` (live runs use both labels
# interchangeably). Aliasing avoids duplicating the routing entry.
_TRIAL26_RCA_KIND_ALIASES: Mapping[str, str] = {
    "plural_top_n_collapse": "top_n_cardinality_collapse",
}


def _trial26_kit_map_expanded() -> bool:
    """Lazy + safe accessor for the Trial 26 W26.2 sub-flag.

    Returns False on any import error so a broken trial26_flags module
    cannot regress the Trial 24 baseline.
    """
    try:
        from genie_space_optimizer.optimization.trial26_flags import (
            trial26_kit_map_expanded_enabled,
        )
        return bool(trial26_kit_map_expanded_enabled())
    except Exception:
        return False


def _aliases() -> Mapping[str, str]:
    """Aliases active for the current flag state. Trial 26 W26.2
    aliases are merged in only when the sub-flag is ON.
    """
    if not _trial26_kit_map_expanded():
        return _RCA_KIND_ALIASES
    merged: dict[str, str] = dict(_RCA_KIND_ALIASES)
    merged.update(_TRIAL26_RCA_KIND_ALIASES)
    return merged


def _fixing_map() -> Mapping[str, frozenset[PatchMechanism]]:
    """Fixing-mechanism map active for the current flag state.

    Trial 26 W26.2 entries (``wrong_aggregation``, ``wrong_column``)
    are merged in only when the sub-flag is ON. The canonical Trial 23
    map is never mutated; this returns a fresh merged view.
    """
    if not _trial26_kit_map_expanded():
        return RCA_KIND_TO_FIXING_MECHANISMS
    merged: dict[str, frozenset[PatchMechanism]] = dict(
        RCA_KIND_TO_FIXING_MECHANISMS
    )
    merged.update(_TRIAL26_RCA_KIND_TO_FIXING_MECHANISMS)
    return merged


def _normalize_rca_kind(rca_kind: str | None) -> str:
    """Collapse a free-text RCA label to the closed key vocabulary.

    Mirrors :func:`action_groups._normalize_rca_kind`: lowercase +
    strip, then resolve any plan-shorthand alias to its canonical key;
    unknown values pass through unchanged so the caller can use a simple
    ``in RCA_KIND_TO_FIXING_MECHANISMS`` membership test.

    Trial 26 W26.2 widens the alias table when the sub-flag is ON so
    ``plural_top_n_collapse`` reduces to ``top_n_cardinality_collapse``.
    """
    if not rca_kind:
        return ""
    key = str(rca_kind).strip().lower()
    return _aliases().get(key, key)


def example_sql_is_insufficient_for(rca_kind: str | None) -> bool:
    """True when ``add_example_sql`` alone cannot fix this RCA kind."""
    return _normalize_rca_kind(rca_kind) in _fixing_map()


def _structural_fix_mechanisms(rca_kind: str | None) -> frozenset[PatchMechanism]:
    """Fixing mechanisms for this RCA EXCLUDING prose ``INSTRUCTION_TEXT``.

    Track B / B1. The "structural companion" set — the non-prose levers
    that actually reshape the generated SQL (snippet, measure/description,
    routing). A lone ``add_instruction`` is admissible for a SQL-shape RCA
    only when paired with one of these. Empty for RCAs outside the map.
    """
    fixing = _fixing_map().get(_normalize_rca_kind(rca_kind))
    if not fixing:
        return frozenset()
    return frozenset(fixing) - {PatchMechanism.INSTRUCTION_TEXT}


def mechanisms_for_rejected_levers(
    rejected: Iterable[str],
) -> frozenset[PatchMechanism]:
    """Normalize rejected lever-id / patch_type tokens to PatchMechanism.

    Trial 30 W30.1b. ``AcceptanceDecisionRecord.rejected_mechanism``
    stores a lever-id (``"lever-5"``) — or, when the lever was inferred
    from a patch_type, a patch_type wire token (``"add_example_sql"``).
    The enforcement guard compares on the *behavioral* unit
    (:class:`PatchMechanism`), not the lever-id, so lever-5 / 5a / 5b
    aliasing cannot let a re-emit slip through.

    Returns the union of mechanisms reachable from each token. Unknown
    tokens contribute nothing (empty), so the guard fails open (keeps
    the proposal) rather than mis-dropping on an unrecognised label.
    """
    # Imported lazily: ``levers_contract`` is a heavier module and this
    # keeps the pure routing brain free of a load-time dependency on it.
    from genie_space_optimizer.optimization.levers_contract import (
        LEVER_TO_PATCH_TYPES,
    )

    out: set[PatchMechanism] = set()
    for token in rejected:
        t = str(token or "").strip()
        if not t:
            continue
        # Direct patch_type token form.
        mech = mechanism_for_patch_type(t)
        if mech is not None:
            out.add(mech)
            continue
        # Lever-id form: expand to its patch_types, then to mechanisms.
        for patch_type in LEVER_TO_PATCH_TYPES.get(t, frozenset()):
            pm = mechanism_for_patch_type(str(getattr(patch_type, "value", patch_type)))
            if pm is not None:
                out.add(pm)
    return frozenset(out)


def instruction_text_is_insufficient_for(rca_kind: str | None) -> bool:
    """True when a *lone* ``add_instruction`` is behaviorally inert here.

    Track B / B1. A natural-language instruction cannot, on its own,
    change the LLM's generated SQL *shape* for ANY SQL-shape RCA — the
    planner keeps emitting the same grain/clause skeleton. This holds for
    every kind in :data:`RCA_KIND_TO_FIXING_MECHANISMS`, including
    ``extra_defensive_filter``: the e943 ``live-llm-only`` run proved a
    lone ``add_instruction`` for a defensive-filter RCA left the SQL shape
    unchanged (``behavioral_diff=unchanged``) and was phantom-accepted as
    ``GSO_ACCEPTANCE_KEPT_INSUFFICIENT_V1``. The reliable fix is the
    structural companion (snippet / description / routing); an instruction
    may *contribute* but is inert alone.

    ``False`` for RCAs outside the map (no contract).
    """
    return _normalize_rca_kind(rca_kind) in _fixing_map()


def recommended_mechanisms_for_rca(rca_kind: str | None) -> tuple[str, ...]:
    """Sorted mechanism *values* that fix this RCA kind.

    Returns ``()`` for RCA kinds outside the map (no contract). The
    sorted order makes prompt rendering and marker payloads stable.
    """
    fixing = _fixing_map().get(_normalize_rca_kind(rca_kind))
    if not fixing:
        return ()
    return tuple(sorted(m.value for m in fixing))


def rca_mechanism_default_reason(
    rca_kind: str | None,
    mechanisms: Iterable[PatchMechanism],
) -> str:
    """Return the typed anti-pattern reason, or ``""`` when admissible.

    Fires ``rca_mechanism_defaulted_to_example_sql:rca=<kind>`` when ALL
    of the following hold:

      1. ``rca_kind`` is in :data:`RCA_KIND_TO_FIXING_MECHANISMS`
         (example_sql is known to be insufficient for it), AND
      2. ``EXAMPLE_SQL`` is among the observed mechanisms (the proposal
         reached for the exemplar lever), AND
      3. NONE of the observed mechanisms is a fixing mechanism for the
         RCA (the exemplar was not paired with a lever that can fix it).

    Returns ``""`` otherwise — unmapped RCAs, sets without example_sql,
    and example_sql paired with an adequate fixing mechanism are all
    admissible.
    """
    key = _normalize_rca_kind(rca_kind)
    fixing = _fixing_map().get(key)
    if not fixing:
        return ""
    observed = {m for m in mechanisms if m is not None}
    if PatchMechanism.EXAMPLE_SQL not in observed:
        return ""
    if observed & fixing:
        return ""
    return f"rca_mechanism_defaulted_to_example_sql:rca={key}"


def rca_instruction_default_reason(
    rca_kind: str | None,
    mechanisms: Iterable[PatchMechanism],
) -> str:
    """Return the typed lone-instruction anti-pattern reason, or ``""``.

    Track B / B1. The instruction-text analogue of
    :func:`rca_mechanism_default_reason`. Fires
    ``rca_mechanism_defaulted_to_instruction_text:rca=<kind>`` when ALL
    of the following hold:

      1. ``rca_kind`` is one where INSTRUCTION_TEXT is insufficient
         (:func:`instruction_text_is_insufficient_for`), AND
      2. ``INSTRUCTION_TEXT`` is among the observed mechanisms (the
         proposal reached for the prose lever), AND
      3. NONE of the observed mechanisms is a fixing mechanism for the
         RCA (the instruction was not paired with a lever that can fix
         the SQL shape).

    Returns ``""`` otherwise — unmapped RCAs, sets without instruction,
    and an instruction paired with a *structural* fixing mechanism (a
    non-prose companion such as ``sql_snippet`` / ``metadata_description``
    / ``routing``) are admissible. A lone instruction — even for
    ``extra_defensive_filter``, where prose is a contributing but
    non-sufficient fix — fires the reason.
    """
    if not instruction_text_is_insufficient_for(rca_kind):
        return ""
    key = _normalize_rca_kind(rca_kind)
    fixing = _fixing_map().get(key)
    if not fixing:
        return ""
    observed = {m for m in mechanisms if m is not None}
    if PatchMechanism.INSTRUCTION_TEXT not in observed:
        return ""
    # Admissible only when paired with a structural (non-prose) companion
    # that can actually reshape the SQL. A lone instruction is inert.
    if observed & _structural_fix_mechanisms(key):
        return ""
    return f"rca_mechanism_defaulted_to_instruction_text:rca={key}"


def rca_instruction_defaulted_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    rca_kind: str,
    mechanisms: Iterable[PatchMechanism],
) -> str:
    """Build the
    ``GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_INSTRUCTION_TEXT_V1``
    anti-success marker line (Track B / B1).

    Mirrors :func:`rca_mechanism_defaulted_marker`: pins the RCA kind,
    the observed (inert) mechanisms, and the mechanisms that WOULD have
    fixed it so postmortems can verify the lone-instruction false-fix and
    the next iteration's forbidden signature.
    """
    payload = {
        "anti_success": True,
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "rca_kind": _normalize_rca_kind(rca_kind),
        "observed_mechanisms": sorted(
            m.value for m in mechanisms if m is not None
        ),
        "recommended_mechanisms": list(
            recommended_mechanisms_for_rca(rca_kind)
        ),
    }
    return (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_INSTRUCTION_TEXT_V1 "
        + json.dumps(payload, sort_keys=True)
    )


def rca_mechanism_defaulted_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    rca_kind: str,
    mechanisms: Iterable[PatchMechanism],
) -> str:
    """Build the ``GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1``
    anti-success marker line.

    The payload pins the RCA kind, the observed (inert) mechanisms, and
    the mechanisms that WOULD have fixed it so postmortems can verify the
    routing guidance and the next iteration's forbidden signature.
    """
    payload = {
        "anti_success": True,
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "rca_kind": _normalize_rca_kind(rca_kind),
        "observed_mechanisms": sorted(
            m.value for m in mechanisms if m is not None
        ),
        "recommended_mechanisms": list(
            recommended_mechanisms_for_rca(rca_kind)
        ),
    }
    return (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1 "
        + json.dumps(payload, sort_keys=True)
    )


__all__ = [
    "RCA_KIND_TO_FIXING_MECHANISMS",
    "example_sql_is_insufficient_for",
    "instruction_text_is_insufficient_for",
    "_structural_fix_mechanisms",
    "recommended_mechanisms_for_rca",
    "rca_mechanism_default_reason",
    "rca_mechanism_defaulted_marker",
    "rca_instruction_default_reason",
    "rca_instruction_defaulted_marker",
]
