"""Trial 27 — Stage 3 Synthesis De-Starvation (prompt_too_large) + Kit-Gate
Reachability on a Sub-Threshold Anchor.

Module mirrors :mod:`trial26_flags` so a single ``export
GSO_TRIAL27_STAGE3_DESTARVE=0`` rolls back every Trial 27 path
byte-stably and each sub-flag is surgically rollable per the
``_subflag_opt_out`` shape.

Trial 27 closes the two gaps exposed by the Trial 26 W26.5 live
verification:

1. **7now — Stage 3 synthesis starvation (W27.1).** 20/24
   ``plan11_synthesize`` calls declined ``prompt_too_large``
   (73170 tokens vs the 40000 cap). The Trial 23 W6 partitioned
   re-dispatch already exists in
   :mod:`optimization.stages.synthesize` but is gated to
   ``_w6_is_subcluster`` (cluster_id contains ``"subcluster"``),
   so it never engaged on the regular clusters that overflowed
   on 7now. W27.1 relaxes that gate so the partition fires on any
   cluster with ``sub_cluster_split_needed=True``. Bright-line #5
   (H001 cluster path) is preserved because the existing
   ``if len(_w6_parts) > 1:`` guard at synthesize.py falls through
   to single-call when partition cannot split further.
2. **airline — Starting Point Gate skips lever_loop (W27.3).**
   Baseline 91.3% already ``thresholds_met=true`` so the lever
   loop is skipped (verdict
   ``LEVER_LOOP_SKIPPED_POST_ENRICHMENT_MEETS_THRESHOLDS``), making
   the kit gate unverifiable on airline at its current threshold.
   W27.3 adds a verification-only env flag honoured by the
   lever_loop notebook gate: when ON, the gate emits the skip
   reason for observability but does NOT skip (lever_loop runs
   anyway). Pure harness/trial-design knob; no ``src/`` per-anchor
   hardcode.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables. Mirrors Trial 26 default-on pattern.
"""
from __future__ import annotations

import os


_TRIAL27_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL27_FLAG_OFF_VALUES


def trial27_stage3_destarve_enabled() -> bool:
    """Trial 27 master flag. Default ON.

    Opt out with ``export GSO_TRIAL27_STAGE3_DESTARVE=0`` for
    emergency rollback. When OFF, every Trial 27 sub-flag is forced
    OFF regardless of its own env var, so the W6 partition stays
    gated to subcluster builders and the lever_loop Starting Point
    Gate honours its skip thresholds without any verification
    override (byte-stable rollback).
    """
    return _flag_enabled("GSO_TRIAL27_STAGE3_DESTARVE")


def _subflag_opt_out(env_name: str) -> bool:
    """Shared opt-out helper for Trial 27 sub-flags.

    Returns True (enabled) when the master is on AND the sub-flag
    has not been explicitly disabled. Only the explicit OFF
    vocabulary (``0`` / ``false`` / ``no`` / ``off``) disables it,
    so a sub-flag defaults ON whenever the master is ON.
    """
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL27_FLAG_OFF_VALUES
    return trial27_stage3_destarve_enabled() and not off


def trial27_w6_extend_nonsubcluster_enabled() -> bool:
    """W27.1 — extend Trial 23 W6 partitioned re-dispatch to fire on
    any cluster with ``sub_cluster_split_needed=True``.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER=0``.

    Pre-Trial-27 behaviour gated W6 to clusters whose ``cluster_id``
    contained ``"subcluster"`` (bright-line #5 protecting the H001
    cluster path). The 7now live verification showed regular
    (non-subcluster) clusters overflowed the Stage 3 cap and got
    declined ``prompt_too_large``, never producing a structural
    proposal. W27.1 relaxes the cluster-id gate so the partition
    fires whenever the size verdict reports
    ``sub_cluster_split_needed=True``; bright-line #5 is preserved
    structurally by the existing ``if len(_w6_parts) > 1:`` guard
    (when the cluster has too few QIDs to actually split, the path
    falls through to the single-call branch byte-stably). When OFF,
    the original ``_w6_is_subcluster`` gate is restored.
    """
    return _subflag_opt_out("GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER")


def trial27_force_lever_loop_override_enabled() -> bool:
    """W27.3 — verification-only override that forces the lever_loop
    Starting Point Gate to NOT skip even when post-enrichment
    accuracy meets thresholds.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE=0``.

    NOTE: This flag enables the *capability* in the gate function.
    The harness still selects per-run whether to engage the
    override (via a separate per-run signal — the
    ``GSO_TRIAL27_FORCE_LEVER_LOOP`` env var set by
    ``gso-lever-loop-replay`` only on verification runs that need
    it). Default-ON here means the gate function honours the
    per-run signal when set; default-OFF would short-circuit the
    capability regardless of the per-run signal. Pure harness/
    trial-design lever — no ``src/`` per-anchor hardcode. Engaged
    only on verification runs that target unreachable kit-gate
    anchors (airline at 91.3% baseline today).
    """
    return _subflag_opt_out("GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE")


__all__ = [
    "trial27_force_lever_loop_override_enabled",
    "trial27_stage3_destarve_enabled",
    "trial27_w6_extend_nonsubcluster_enabled",
]
