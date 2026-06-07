"""Trial 28 — Wire the owed RCA-canonicaliser LLM tier (the confirmed
kit-gate blocker) + iter-0 Stage-3 de-starvation.

Module mirrors :mod:`trial27_flags` so a single ``export
GSO_TRIAL28_KIT_REACHABILITY=0`` rolls back every Trial 28 path
byte-stably and each sub-flag is surgically rollable per the
``_subflag_opt_out`` shape.

Trial 28 closes the gap the Trial 27 live verification isolated with
fresh marker payloads:

1. **RCA canonicaliser LLM tier (W28.1).** On both anchors the live
   ``GSO_TRIAL26_RCA_CANONICAL_V1`` distribution left the majority of
   Stage-2 routing narratives at ``unknown_kind`` (66.7% airline /
   71.4% 7now, both above the 30% anti-marker), so
   ``_kit_for_rca_companions`` returned ``None``,
   ``GSO_TRIAL24_KIT_FORCED_V1`` never fired, and no structural lever
   was ever selected. The owed tier-4 LLM call in
   :mod:`optimization.rca_kind_canonical` (``_invoke_llm_tier``,
   previously ``raise NotImplementedError``) is the principled fix: an
   LLM categorises the free-text narrative against the closed
   ``RCA_CANONICAL_KEY_SET`` enum and deterministic code clamps the
   output to the canonical set. When this sub-flag is ON the
   canonicaliser may lazily acquire a workspace client (via
   :func:`genie_space_optimizer._workspace_client.make_workspace_client`)
   so the kit-gate call site does not need a ``w`` threaded through its
   many callers. When OFF, the LLM tier is reachable only when a
   workspace client is explicitly supplied, exactly as before Trial 28
   (byte-stable rollback).
2. **Iter-0 seed Stage-3 de-starvation (W28.2).** The Trial 27 W27.1
   relaxation fixed in-loop Stage-3 starvation but the pre-loop
   iteration-0 SEED synthesis pass still declined ``prompt_too_large``;
   W28.2 extends the partitioned re-dispatch to the seed pass.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables. Mirrors Trial 26/27 default-on pattern.
"""
from __future__ import annotations

import os


_TRIAL28_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL28_FLAG_OFF_VALUES


def trial28_kit_reachability_enabled() -> bool:
    """Trial 28 master flag. Default ON.

    Opt out with ``export GSO_TRIAL28_KIT_REACHABILITY=0`` for
    emergency rollback. When OFF, every Trial 28 sub-flag is forced
    OFF regardless of its own env var, so the canonicaliser LLM tier
    stays gated to explicit-``w`` callers and the seed Stage-3 pass
    keeps its pre-Trial-28 assembly (byte-stable rollback).
    """
    return _flag_enabled("GSO_TRIAL28_KIT_REACHABILITY")


def _subflag_opt_out(env_name: str) -> bool:
    """Shared opt-out helper for Trial 28 sub-flags.

    Returns True (enabled) when the master is on AND the sub-flag has
    not been explicitly disabled. Only the explicit OFF vocabulary
    (``0`` / ``false`` / ``no`` / ``off``) disables it, so a sub-flag
    defaults ON whenever the master is ON.
    """
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL28_FLAG_OFF_VALUES
    return trial28_kit_reachability_enabled() and not off


def trial28_rca_llm_tier_enabled() -> bool:
    """W28.1 — enable the RCA-canonicaliser tier-4 LLM call AND its
    lazy workspace-client acquisition so the kit-gate call site can
    resolve free-text Stage-2 routing narratives to canonical keys
    without threading a ``w`` through every ``_normalize_rca_kind``
    caller.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL28_RCA_LLM_TIER=0``.

    When OFF, the canonicaliser's LLM tier is reachable only when the
    caller explicitly passes a workspace client (pre-Trial-28
    behaviour); the deterministic + alias + keyword tiers are
    unchanged either way, so the offline alignment corpus stays
    byte-stable.
    """
    return _subflag_opt_out("GSO_TRIAL28_RCA_LLM_TIER")


def trial28_seed_destarve_enabled() -> bool:
    """W28.2 — extend the Trial 27 W6 partitioned re-dispatch to the
    iteration-0 SEED synthesis pass so the pre-loop pass also fits the
    single-call cap.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL28_SEED_DESTARVE=0``.
    """
    return _subflag_opt_out("GSO_TRIAL28_SEED_DESTARVE")


__all__ = [
    "trial28_kit_reachability_enabled",
    "trial28_rca_llm_tier_enabled",
    "trial28_seed_destarve_enabled",
]
