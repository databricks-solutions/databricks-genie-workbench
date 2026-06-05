"""Trial 21 — feature-flag home for the Evidence Actuator cutover.

Trial 21 collapses six fragmented observe-only P4 gates into a single
:mod:`proposal_slate_compiler` boundary between Stage 3 raw
``RepairProposals`` and the applier. Each P4 detector remains in place
as the per-check helper the compiler calls; what changes is who owns
the decision verb. Before Trial 21: each detector emits a marker, the
runtime ignores it. After Trial 21: the compiler reads each verdict
and drops the proposal with a typed :class:`DropReason`.

Module pattern mirrors :mod:`trial19_flags` and :mod:`trial20_flags`
so flag helpers are easy to grep and the callsites that import them
don't trip the legacy-synthesis import scanner.

Default: ON. Opt-out: any of ``0`` / ``false`` / ``no`` / ``off``
(case-insensitive) on ``GSO_TRIAL21_ACTUATOR`` disables the actuator
and the producer falls back to the P4 observe-only path.
"""
from __future__ import annotations

import os


_TRIAL21_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL21_FLAG_OFF_VALUES


def trial21_actuator_enabled() -> bool:
    """Trial 21 master flag.

    When ON (default), :func:`proposal_slate_compiler.compile_slate`
    is the single decision boundary between Stage 3 and the applier:
    every raw proposal flows through the ordered check pipeline and
    either lands in ``surviving_proposals`` or in
    ``dropped_proposals`` with a typed :class:`DropReason`.

    When OFF (``GSO_TRIAL21_ACTUATOR=0``), the producer reverts to the
    P4 observe-only path: each detector emits its marker but the
    proposal continues unchanged. This is the emergency rollback knob
    for production deployments that need to disable enforce-mode
    without code changes.
    """
    return _flag_enabled("GSO_TRIAL21_ACTUATOR")
