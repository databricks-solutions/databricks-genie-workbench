"""Trial 18 — central home for the acceptance-overhaul feature flag.

Module pattern mirrors :mod:`trial17_flags` so flag helpers are
easy to grep and so callsites that import them don't look like
"legacy synthesis import" calls to the Plan 11 callsite coverage
guardrail (which scans for ``cluster_driven_synthesis`` /
``forced_synthesis_dispatch`` / ``three_stage_pipeline`` imports
that bypass the plan11 flag branch).

The single flag gates the entire Trial 18 acceptance overhaul:

* Canonical ``row_semantic_score`` use in ``evaluated_gate`` and
  ``acceptance_gate`` (Steps 1+2).
* ``KEPT_INSUFFICIENT`` outcome and the typed
  ``insufficient_repair_signature`` sibling channel (Step 3).
* Stage 3 metadata-target preflight (Step 4).
* Plan 12 pivot ordering observability marker (Step 5).

When the flag is OFF the gates revert byte-for-byte to the
pre-Trial-18 behaviour: raw byte-match scoring, two-lane acceptance
verdict, no insufficient_repair_signature emission, no Stage 3
metadata preflight. Critical for emergency rollback.

Opt-out semantics: any of ``0`` / ``false`` / ``no`` / ``off``
(case insensitive) disables the flag. Any other value, or env
unset, enables.
"""
from __future__ import annotations

import os


_TRIAL18_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL18_FLAG_OFF_VALUES


def trial18_acceptance_overhaul_enabled() -> bool:
    """Trial 18 — gate the acceptance contract overhaul.

    Default: ON. Opt out with
    ``export GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0`` for emergency
    rollback. The flag is consumed by ``evaluated_gate``,
    ``acceptance_gate``, ``dispatch_input``, ``stages.synthesize``,
    and ``harness`` (Plan 12 pivot observability).
    """
    return _flag_enabled("GSO_TRIAL18_ACCEPTANCE_OVERHAUL")


__all__ = [
    "trial18_acceptance_overhaul_enabled",
]
