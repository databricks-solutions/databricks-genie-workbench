"""Trial 17 — central home for the two opt-out feature flags.

Lives in its own tiny module so callsites importing the flag helpers
don't look like "legacy synthesis import" calls to the Plan 11 callsite
coverage guardrail (which scans for ``cluster_driven_synthesis`` /
``forced_synthesis_dispatch`` / ``three_stage_pipeline`` imports that
bypass the plan11 flag branch).

Both flags default to **ON**. They were flag-gated during Trial 17
development to allow staging-only validation; Trial 17.1 ships the
flags default-on so operators can keep deploying without setting envs.

Opt-out semantics: any of ``0`` / ``false`` / ``no`` / ``off`` (case
insensitive) disables the flag. Any other value, or env unset, enables.
"""
from __future__ import annotations

import os


_TRIAL17_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL17_FLAG_OFF_VALUES


def trial17_lever_led_synthesis_enabled() -> bool:
    """Trial 17 Step 7 — deprioritise ``pick_archetype`` as a control-flow
    gate and let the Stage 3 LLM pick from the lever menu.

    Default: ON. Opt out with
    ``export GSO_TRIAL17_LEVER_LED_SYNTHESIS=0`` for emergency rollback.
    """
    return _flag_enabled("GSO_TRIAL17_LEVER_LED_SYNTHESIS")


def trial17_bundles_enabled() -> bool:
    """Trial 17 Step 5 — enable the multi-lever bundle path.

    Default: ON. Legacy single-proposal callers ignore the flag entirely
    (proposals with ``bundle_id == ""`` are never grouped), so flipping
    this on is a no-op for clusters that don't emit a bundle. Opt out
    with ``export GSO_TRIAL17_BUNDLES=0``.
    """
    return _flag_enabled("GSO_TRIAL17_BUNDLES")


__all__ = [
    "trial17_lever_led_synthesis_enabled",
    "trial17_bundles_enabled",
]
