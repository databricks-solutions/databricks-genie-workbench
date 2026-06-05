"""Trial 24 — Kit at Source for example-SQL-insufficient RCAs.

Module pattern mirrors :mod:`trial23_flags` so flag helpers are easy to
grep and so callsites that import them are not mistaken for legacy-
synthesis imports by the Plan 11 callsite-coverage guardrail.

Trial 24 closes the e943 gap (the faithful replay landed a corrective
``add_instruction`` that died as an ungrounded single lever before any
Trial 23 repair hook could reach it). The fix routes example-SQL-
insufficient RCA kinds to a MANDATORY multi-lever kit at synthesis time
(``KIT_FOR_RCA``) so the corrective patch is born as a >= 2-family kit
that survives both the slate ``required_assets`` and bundle-invariants
contracts.

A single master gate :func:`trial24_kit_at_source_enabled` ANDs over
every sub-flag so emergency rollback to pre-Trial-24 behaviour is a
single ``export GSO_TRIAL24_KIT_AT_SOURCE=0``.

Default ON (promoted from opt-in): the deterministic e943 kit-at-source
replay gate is green and wired into the CI merge gate
(``trial24_replay_gate``), so the corrective kit provably survives the
slate compiler with the flag on and rolls back byte-stably with it off.
NOTE: the live ``behavioral_diff != unchanged`` applier proof (the other
half of the original promotion bar) is still owed; default-on was
accepted as a monitored ship ahead of that live signal.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables the flag. Env unset, empty, or any
other value enables. This now mirrors the Trial 23 default-on pattern.
"""
from __future__ import annotations

import os


_TRIAL24_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL24_FLAG_OFF_VALUES


def trial24_kit_at_source_enabled() -> bool:
    """Trial 24 master flag. Default ON.

    Opt out with ``export GSO_TRIAL24_KIT_AT_SOURCE=0`` for emergency
    rollback to the pre-Trial-24 behaviour. When OFF, every Trial 24
    sub-flag is forced OFF regardless of its own env var, so the base
    ``KIT_FOR_RCA`` map and the slate ``required_assets`` check behave
    exactly as they did pre-Trial-24 (byte-stable rollback).
    """
    return _flag_enabled("GSO_TRIAL24_KIT_AT_SOURCE")


def _subflag_opt_out(env_name: str) -> bool:
    """Shared opt-out helper for Trial 24 sub-flags.

    Returns True (enabled) when the master is on AND the sub-flag has not
    been explicitly disabled. Only the explicit OFF vocabulary
    (``0`` / ``false`` / ``no`` / ``off``) disables it, so a sub-flag
    defaults ON whenever the master is ON.
    """
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in frozenset({"0", "false", "no", "off"})
    return trial24_kit_at_source_enabled() and not off


def trial24_required_assets_kit_waiver_enabled() -> bool:
    """W24.3 — waive the instruction-family ``justification`` requirement
    when the proposal belongs to a valid multi-lever kit.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL24_REQUIRED_ASSETS_KIT_WAIVER=0`` to keep the kit
    forced at synthesis (W24.1) but leave the per-proposal
    ``_check_required_assets`` justification gate strict — useful to
    isolate whether the waiver (not the kit mandate) is what lands the
    corrective instruction.

    The kit IS the justification: an ``add_instruction`` that ships as a
    member of a >= 2-lever-family kit is justified by construction (the
    companion lever supplies the structural anchor), so the
    ``UNJUSTIFIED_SINGLE_LEVER`` drop must not fire on it.
    """
    return _subflag_opt_out("GSO_TRIAL24_REQUIRED_ASSETS_KIT_WAIVER")


def trial24_mechanism_aware_kit_enabled() -> bool:
    """Follow-on A — recognise a kit by patch_type-derived mechanism
    family, not only by the LLM-declared ``selected_levers``.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL24_MECHANISM_AWARE_KIT=0``.

    The live e943 replay showed the LLM emitting an
    ``add_instruction`` + ``add_sql_snippet_filter`` bundle but tagging
    BOTH members ``lever-5`` — so the declared-lever union was
    ``{lever-5}`` (< 2) and the kit was not recognised. Two distinct
    *mechanisms* (INSTRUCTION_TEXT + SQL_SNIPPET) are present regardless
    of the mis-tagged levers, so this flag adds the mechanism-derived
    signal as an ADDITIONAL acceptance path (OR with the lever union):
    the kit is admitted when ``union_levers >= 2`` OR
    ``distinct_mechanisms >= 2``. Never weakens the lever path.
    """
    return _subflag_opt_out("GSO_TRIAL24_MECHANISM_AWARE_KIT")


def trial24_filter_removal_solo_enabled() -> bool:
    """Follow-on B — treat ``extra_defensive_filter`` as a single-
    mechanism (instruction) fix that lands SOLO.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL24_FILTER_REMOVAL_SOLO=0``.

    ``extra_defensive_filter`` is a filter-REMOVAL RCA: the planner
    injected an unwanted predicate and the fix is an instruction telling
    it not to. A positive ``lever-6`` snippet companion cannot express
    "remove this filter" — the LLM emits a no-op ``1=1`` / ``TRUE`` that
    the producer validator rejects, and the cohesion cascade then drops
    the corrective instruction too. This flag (1) removes
    ``extra_defensive_filter`` from the Trial 24 forced-kit map so a lone
    justified instruction is admissible, (2) grounds the instruction's
    justification at synthesis, and (3) degrades a no-op suppression
    snippet to an instruction-only solo BEFORE the slate so it never
    cascades. ``top_n_cardinality_collapse`` stays a kit.
    """
    return _subflag_opt_out("GSO_TRIAL24_FILTER_REMOVAL_SOLO")


def trial24_general_instruction_grounding_enabled() -> bool:
    """Replay-readiness generalization of Follow-on B's FB2 grounding.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING=0``.

    FB2 only grounds a solo corrective instruction's justification when
    the cluster ``rca_kind`` is in the Trial 24 forced-kit map
    (``extra_defensive_filter`` / ``top_n_cardinality_collapse``). The
    live e943 fix therefore covers only those two RCAs: for ANY other
    ``rca_kind`` an LLM that emits a lone ``add_instruction`` with an
    empty ``single_lever_justification`` still dies at
    ``_check_required_assets`` as ``unjustified_single_lever`` — the
    exact death mode that blocked e943 before Trial 24.

    This flag widens the FB2 fallback (``single_lever_justification`` ->
    ``expected_behavioral_change`` -> ``rationale``) to ANY
    instruction-family (``INSTRUCTION_TEXT`` mechanism) proposal,
    regardless of ``rca_kind``, so a grounded solo corrective
    instruction lands across a broader multi-RCA replay. It never
    fabricates justification: when all three sources are empty the
    proposal still drops. Byte-stable when off (the narrow FB2 path is
    unchanged).
    """
    return _subflag_opt_out("GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING")


__all__ = [
    "trial24_kit_at_source_enabled",
    "trial24_required_assets_kit_waiver_enabled",
    "trial24_mechanism_aware_kit_enabled",
    "trial24_filter_removal_solo_enabled",
    "trial24_general_instruction_grounding_enabled",
]
