"""Trial 30 — enforced inert-mechanism switch + rerouted-QID carry-forward.

Mirror of :mod:`trial29_flags`. Same default-ON / OFF-vocabulary
semantics. Single emergency rollback knob is
``GSO_TRIAL30_ENFORCED_SWITCH``.

Trial 30 closes the W29.4 PARTIAL: the ``kit_forced_inert_reroute``
lane fires live (detection works) but the W29.1 feedback channel was
never wired into production, so the LLM re-emitted the rejected
mechanism. W30.1a wires the channel; W30.1b adds a deterministic
post-LLM enforcement guard; W30.2 carries the rerouted QID forward.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables.
"""
from __future__ import annotations

import os

_TRIAL30_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL30_FLAG_OFF_VALUES


def trial30_enforced_switch_enabled() -> bool:
    """Trial 30 master flag. Default ON.

    Opt out with ``export GSO_TRIAL30_ENFORCED_SWITCH=0`` for emergency
    rollback. When OFF, every Trial 30 sub-flag is forced OFF
    regardless of its own env var (byte-stable rollback to Trial 29).
    """
    return _flag_enabled("GSO_TRIAL30_ENFORCED_SWITCH")


def _subflag_opt_out(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL30_FLAG_OFF_VALUES
    return trial30_enforced_switch_enabled() and not off


def trial30_inert_harvest_wire_enabled() -> bool:
    """W30.1a + W30.2(a)/(c) — wire the InertMechanismHistory channel
    (harvest -> ctx -> prompt render), union member_qids into
    target_qids_union, and write the same-iteration live bucket.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL30_INERT_HARVEST_WIRE=0``.
    """
    return _subflag_opt_out("GSO_TRIAL30_INERT_HARVEST_WIRE")


def trial30_enforce_guard_enabled() -> bool:
    """W30.1b — deterministic post-LLM enforcement guard that hard-drops
    a re-emitted rejected mechanism when a structural fallback exists.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL30_ENFORCE_GUARD=0`` to disable the hard drop
    independently of the wiring (e.g. if it over-rejects live).
    """
    return _subflag_opt_out("GSO_TRIAL30_ENFORCE_GUARD")


def trial30_bundle_completeness_enabled() -> bool:
    """W30.3 — evidence-bundle completeness for inert reroutes.

    When ON, every ``kit_forced_inert_reroute`` decision (a) persists a
    typed ``Trial29InertPatchDiagnostic`` JSONL record into the run's
    evidence bundle and (b) projects a row into
    ``genie_eval_lever_loop_decisions`` so the postmortem reads the
    decision from the table rather than falling back to log-grep. This
    is what lets the W29.5 ``bundle_completeness_invariants_held``
    sub-invariant go green.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL30_BUNDLE_COMPLETENESS=0`` to disable persistence
    and projection independently of the enforcement guard.
    """
    return _subflag_opt_out("GSO_TRIAL30_BUNDLE_COMPLETENESS")


__all__ = [
    "trial30_enforced_switch_enabled",
    "trial30_inert_harvest_wire_enabled",
    "trial30_enforce_guard_enabled",
    "trial30_bundle_completeness_enabled",
]
