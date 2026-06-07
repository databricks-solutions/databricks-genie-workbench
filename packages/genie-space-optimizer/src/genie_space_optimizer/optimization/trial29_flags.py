"""Trial 29 — behaviour-changing structural lever for kit-forced RCAs.

Mirror of :mod:`trial28_flags`. Same default-ON / OFF-vocabulary
semantics. Single emergency rollback knob is ``GSO_TRIAL29_BEHAVIOR_DELTA``.

Trial 29 closes the W28.4 inert-patch blocker isolated by the W28.1
live verification: ``GSO_TRIAL24_KIT_FORCED_V1`` fired on 7now and the
patch applied, but the post-eval ``behavioral_diff`` stayed
``unchanged`` because the kit-mapped ``add_sql_snippet_filter`` was a
no-op for the existing query body.

1. **Post-apply behaviour gate + structural-lever routing (W29.1).**
   When the kit gate fires + the patch applies + the post-eval
   ``behavioral_diff == "unchanged"``, route to a new
   ``kit_forced_inert_reroute`` acceptance lane (sibling of
   ``kept_insufficient``) and feed the rejected mechanism back into
   the next iteration's Stage 3 synthesis via
   ``InertMechanismHistory`` so the LLM picks from
   ``_structural_fix_mechanisms(rca) - rejected``.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables. Mirrors Trial 26/27/28 default-on pattern.
"""
from __future__ import annotations

import os


_TRIAL29_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL29_FLAG_OFF_VALUES


def trial29_behavior_delta_enabled() -> bool:
    """Trial 29 master flag. Default ON.

    Opt out with ``export GSO_TRIAL29_BEHAVIOR_DELTA=0`` for emergency
    rollback. When OFF, every Trial 29 sub-flag is forced OFF
    regardless of its own env var, so the new acceptance lane is
    skipped and inert kit-forced patches drop into the existing
    ``kept_insufficient`` lane (byte-stable rollback).
    """
    return _flag_enabled("GSO_TRIAL29_BEHAVIOR_DELTA")


def _subflag_opt_out(env_name: str) -> bool:
    """Shared opt-out helper for Trial 29 sub-flags.

    Returns True (enabled) when the master is on AND the sub-flag has
    not been explicitly disabled. Only the explicit OFF vocabulary
    (``0`` / ``false`` / ``no`` / ``off``) disables it, so a sub-flag
    defaults ON whenever the master is ON.
    """
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL29_FLAG_OFF_VALUES
    return trial29_behavior_delta_enabled() and not off


def trial29_inert_reroute_enabled() -> bool:
    """W29.1 — enable the ``kit_forced_inert_reroute`` acceptance lane
    + ``inert_mechanism_history`` lever-loop feedback channel.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL29_INERT_REROUTE=0``.

    When OFF, inert kit-forced patches drop into ``kept_insufficient``
    exactly as before Trial 29 (byte-stable). The acceptance-gate test
    suite covers both branches.
    """
    return _subflag_opt_out("GSO_TRIAL29_INERT_REROUTE")


__all__ = [
    "trial29_behavior_delta_enabled",
    "trial29_inert_reroute_enabled",
]
