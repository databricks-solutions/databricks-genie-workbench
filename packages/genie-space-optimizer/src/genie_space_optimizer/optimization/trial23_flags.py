"""Trial 23 — Optimizer Loop Repair feature flags.

Module pattern mirrors :mod:`trial20_flags` / :mod:`trial22_flags` so
flag helpers are easy to grep and so callsites that import them are not
mistaken for legacy-synthesis imports by the Plan 11 callsite-coverage
guardrail.

Trial 23 fixes the optimizer *control loop* (Trials 20-22 fixed patch
delivery). The organizing laws are correct-at-source (the patch is right
the first time) and repair-not-drop (a wrong patch is repaired or
redirected, never silently dropped), plus loop authority (kept-
insufficient is authoritative and forces a pivot) and honest acceptance
(global drift with unresolved target debt is non-deployable).

A single master gate :func:`trial23_loop_repair_enabled` ANDs over every
sub-flag so emergency rollback to pre-Trial-23 behaviour is a single
``export GSO_TRIAL23_LOOP_REPAIR=0``.

Opt-out semantics: any of ``0`` / ``false`` / ``no`` / ``off`` (case
insensitive) disables the flag. Any other value, or env unset, enables.
"""
from __future__ import annotations

import os


_TRIAL23_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL23_FLAG_OFF_VALUES


def trial23_loop_repair_enabled() -> bool:
    """Trial 23 master flag. Default ON.

    Opt out with ``export GSO_TRIAL23_LOOP_REPAIR=0`` for emergency
    rollback to the pre-Trial-23 behaviour. When OFF, every Trial 23
    sub-flag is forced OFF regardless of its own env var.
    """
    return _flag_enabled("GSO_TRIAL23_LOOP_REPAIR")


def trial23_kept_insufficient_authoritative_enabled() -> bool:
    """W1 — kept_insufficient is the authoritative iteration terminal.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_KEPT_INSUFFICIENT_AUTHORITATIVE=0``.

    Gates the new top-precedence branch in
    :func:`iteration_terminal.compute_iteration_terminal_reason`:
    when an iteration applied >= 1 patch AND recorded >= 1
    ``kept_insufficient`` acceptance, the terminal reason MUST be
    ``KEPT_INSUFFICIENT`` and MUST NOT be ``NO_APPLIED_PATCHES``.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_KEPT_INSUFFICIENT_AUTHORITATIVE")
    )


def trial23_target_honest_acceptance_enabled() -> bool:
    """W2 — demote attribution-drift accept with unresolved target debt.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_TARGET_HONEST_ACCEPTANCE=0``.

    Gates the acceptance-policy change: a candidate with non-empty
    ``unresolved_target_debt_qids`` is demoted from a deployable accept
    to a non-deployable diagnostic tier (the global delta_pp is still
    recorded as evidence).
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_TARGET_HONEST_ACCEPTANCE")
    )


def trial23_pivot_inputs_enabled() -> bool:
    """W3 — populate reliable prior_patch_family / prior_lever_set.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_PIVOT_INPUTS=0``.

    Gates threading the prior patch family + lever set onto the terminal
    signature so the Trial 20 C pivot graph has a non-empty ``from``
    state to escape ``add_example_sql``.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_PIVOT_INPUTS")
    )


def trial23_rca_mechanism_routing_enabled() -> bool:
    """W4 — RCA-kind to mechanism routing (correct at source).

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_RCA_MECHANISM_ROUTING=0``.

    Gates the synthesis-time guidance that maps each RCA kind to a
    mechanism that can actually fix it, instead of defaulting to
    ``add_example_sql`` for RCAs it cannot fix.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_RCA_MECHANISM_ROUTING")
    )


def trial23_asset_grounding_enabled() -> bool:
    """W5 — pre-generation asset grounding for SQL-shape repairs.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_ASSET_GROUNDING=0``.

    Gates injecting resolved table/column slices when the repair
    diagnosis is missing ``implicated_assets``. NOTE: the blocking
    promotion of the repair-diagnosis gate stays behind
    :func:`trial23_asset_grounding_blocking_enabled` (default OFF) so we
    do not re-create the all-dropped flatline before the repair paths
    (W7-W9) exist.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_ASSET_GROUNDING")
    )


def trial23_asset_grounding_blocking_enabled() -> bool:
    """W5 (blocking promotion) — flip the repair-diagnosis gate from
    observe-only to blocking. Default OFF; enable only after W7-W9
    repair paths are verified. ``export GSO_TRIAL23_ASSET_GROUNDING_BLOCKING=1``.
    """
    raw = os.environ.get(
        "GSO_TRIAL23_ASSET_GROUNDING_BLOCKING", ""
    ).strip().lower()
    return trial23_loop_repair_enabled() and raw in ("1", "true", "yes", "on")


def trial23_subcluster_real_slice_enabled() -> bool:
    """W6 — actually slice oversized RCA-subcluster Stage 3 prompts.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_SUBCLUSTER_REAL_SLICE=0``.

    Gates replacing the W7 observe-only marker with real partitioned
    re-dispatch: N smaller LLM calls instead of one oversized call that
    is declined as ``prompt_too_large``.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_SUBCLUSTER_REAL_SLICE")
    )


def trial23_snippet_repair_enabled() -> bool:
    """W7 — repair invalid snippets before dropping.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_SNIPPET_REPAIR=0``.

    Gates a single re-prompt with the canonical validator error +
    resolved schema on ``snippet_invalid`` before the proposal is
    dropped.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_SNIPPET_REPAIR")
    )


def trial23_pivot_destination_enabled() -> bool:
    """W8 — give the sole-lever pivot a destination.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_PIVOT_DESTINATION=0``.

    Gates synthesizing a replacement multi-lever bundle when the
    Trial 20 D3 sole-lever drop would otherwise empty the slate.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_PIVOT_DESTINATION")
    )


def trial23_bundle_repair_enabled() -> bool:
    """W9 — recompose/dissolve bundles instead of dropping.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_BUNDLE_REPAIR=0``.

    Gates recomposing a cohesion-failing bundle (extends the W2.1
    singleton dissolution) instead of dropping every member as
    ``bundle_invariant_violated``.
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_BUNDLE_REPAIR")
    )


def trial23_phase_h_contract_gate_enabled() -> bool:
    """W10 — Phase H assembly/upload failure makes candidate non-deployable.

    Default ON when master is ON. Opt out with
    ``export GSO_TRIAL23_PHASE_H_CONTRACT_GATE=0``.

    Gates propagating ``assembly_failed`` / ``upload_failed`` / stale
    scoreboard to a non-deployable verdict (the job may stay effort-
    successful, but the deploy gate must see the failure).
    """
    return (
        trial23_loop_repair_enabled()
        and _flag_enabled("GSO_TRIAL23_PHASE_H_CONTRACT_GATE")
    )


__all__ = [
    "trial23_loop_repair_enabled",
    "trial23_kept_insufficient_authoritative_enabled",
    "trial23_target_honest_acceptance_enabled",
    "trial23_pivot_inputs_enabled",
    "trial23_rca_mechanism_routing_enabled",
    "trial23_asset_grounding_enabled",
    "trial23_asset_grounding_blocking_enabled",
    "trial23_subcluster_real_slice_enabled",
    "trial23_snippet_repair_enabled",
    "trial23_pivot_destination_enabled",
    "trial23_bundle_repair_enabled",
    "trial23_phase_h_contract_gate_enabled",
]
