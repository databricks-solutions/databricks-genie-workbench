"""Trial 20 — central home for the outer-rails feature flags.

Module pattern mirrors :mod:`trial19_flags` so flag helpers are easy
to grep and so callsites that import them don't look like "legacy
synthesis import" calls to the Plan 11 callsite coverage guardrail.

Trial 20 closes the contract splits surfaced by the Trial 19
postmortems by aligning every outer decision rail with the inner
state-machine contracts already established in Trials 16-19:

* Workstream A — pre-arbiter veto fix. ``decide_pre_arbiter_
  regression_guardrail`` rolled back an arbiter-improving candidate
  (airline ``519131527536322`` iteration 2: post-arbiter 87.5% to
  91.7%, pre-arbiter byte-match 87.5% to 75.0%). The actual fix
  surface is determined by the A1 root-cause replay (row-shape
  stripping, target-attribution drift, or baseline mismatch). Gated
  by :func:`trial20_pre_arbiter_veto_fix_enabled`.
* Workstream B — terminal taxonomy unification. The acceptance-gate
  ``kept_insufficient`` decision (Trial 18) now projects to a typed
  ``TerminalReason.KEPT_INSUFFICIENT`` via a new selector contract
  on the iteration terminal router. Plan 12 treats it as a survival
  failure. Gated by :func:`trial20_kept_insufficient_terminal_enabled`.
* Workstream C — cycle-aware patch-family pivot graph. Replaces the
  degenerate single-element ``_PIVOT_FROM_FAMILY_AFTER_FAILURE``
  constant with a five-family cycle plus prior-family inference from
  the latest kept-insufficient signature. Gated by
  :func:`trial20_family_pivot_graph_enabled`.
* Workstream D — multi-lever bundles as default strategy. Iteration
  1 the LLM MAY emit single-lever with a free-text
  ``single_lever_justification``; bundle MANDATORY when prior
  insufficient signatures exist. Canonical templates surface as
  examples, not deterministic mappings. Gated by
  :func:`trial20_multi_lever_bundle_default_enabled`.
* Workstream E — blast-radius gate goes from advisory to mandatory.
  ``passing_dependents`` stamped unconditionally into state-machine
  ctx; safe-by-default fallback flipped to unsafe-by-default. No new
  routing — narrow-replacement-gate is already registered after
  blast-radius-batch at ``registry.py``. Gated by
  :func:`trial20_blast_radius_mandatory_enabled`.

A single master gate :func:`trial20_enforce_enabled` ANDs over all
five sub-flags so emergency rollback to pre-Trial-20 behaviour is a
single ``export GSO_TRIAL20_ENFORCE=0``.

When the master flag is OFF every Trial 20 surface reverts byte-for-
byte to the pre-Trial-20 behaviour:

* No A2 pre-arbiter veto fix; ``decide_pre_arbiter_regression_
  guardrail`` reads today's row shape / target attribution.
* No B2 selector; iteration terminal emits ``NO_APPLIED_PATCHES``
  catch-all even when the SM final state recorded
  ``kept_insufficient``.
* No C1 pivot graph; ``next_patch_family_for_cluster`` returns the
  single ``add_example_sql`` constant.
* No D1 bundle-default; Stage 3 prompt keeps ``bundle_id (optional)``.
* No E2 mandatory blast-radius; ``patch_blast_radius_is_safe`` keeps
  ``no_passing_dependents_field`` safe-by-default fallback.

Opt-out semantics: any of ``0`` / ``false`` / ``no`` / ``off`` (case
insensitive) disables the flag. Any other value, or env unset,
enables.
"""
from __future__ import annotations

import os


_TRIAL20_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL20_FLAG_OFF_VALUES


def trial20_enforce_enabled() -> bool:
    """Trial 20 master flag.

    Default: ON. Opt out with
    ``export GSO_TRIAL20_ENFORCE=0`` for emergency rollback to the
    pre-Trial-20 behaviour (Trial 19 admission + RCA lanes preserved).

    When OFF, every Trial 20 sub-flag is forced OFF regardless of
    its own env var. This is the single rollback knob — callsites
    in the full-eval acceptance path, iteration terminal selector,
    Plan 12 pivot policy, Stage 3 prompt, and blast-radius gate all
    consult their sub-flag through this gate.
    """
    return _flag_enabled("GSO_TRIAL20_ENFORCE")


def trial20_pre_arbiter_veto_fix_enabled() -> bool:
    """Workstream A — pre-arbiter veto fix on full-eval acceptance.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL20_PRE_ARBITER_VETO_FIX=0``.

    Gates the A2 fix surface (determined by A1 root-cause replay).
    Candidates for the actual fix surface include:

    * Row-shape: ensure full-eval rows retain ``arbiter/value`` from
      sliced eval so ``row_is_hard_failure`` reads the field.
    * Target attribution: ensure the AG's ``target_qids`` resolution
      includes the arbiter-rescued QID.
    * Baseline selection: ensure pre/post row sets used by the
      guardrail are the same set producing the arbiter metrics.

    Also gates the A3 ``GSO_TRIAL20_SHADOW_DECISION_V1`` marker that
    audits every full-eval acceptance for postmortem joins.

    When OFF, ``decide_pre_arbiter_regression_guardrail`` and the
    upstream row-projection / target-attribution / baseline-selection
    paths revert to today's behaviour (Trial 19 byte-stable).
    """
    return (
        trial20_enforce_enabled()
        and _flag_enabled("GSO_TRIAL20_PRE_ARBITER_VETO_FIX")
    )


def trial20_kept_insufficient_terminal_enabled() -> bool:
    """Workstream B — typed ``KEPT_INSUFFICIENT`` iteration terminal.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL=0``.

    Gates:

    * ``TerminalReason.KEPT_INSUFFICIENT`` consumption in the
      iteration terminal selector (B2).
    * The new precedence rule: if any QID's SM final state has
      ``accepted.decision == "kept_insufficient"`` for the iteration,
      ``terminal_reason`` MUST be ``KEPT_INSUFFICIENT``, overriding
      any catch-all ``NO_APPLIED_PATCHES`` assignment.
    * Plan 12 pivot-membership for ``kept_insufficient`` (B3).

    The new enum value (B1) is always present; the flag only gates
    consumption / projection so the value is observable in tape
    fixtures even with the master flag OFF.

    When OFF, the iteration terminal emits ``NO_APPLIED_PATCHES``
    via the catch-all sites as today (Trial 19 byte-stable).
    """
    return (
        trial20_enforce_enabled()
        and _flag_enabled("GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL")
    )


def trial20_family_pivot_graph_enabled() -> bool:
    """Workstream C — cycle-aware patch-family pivot graph.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL20_FAMILY_PIVOT_GRAPH=0``.

    Gates:

    * ``next_patch_family_for_cluster`` consults ``_PIVOT_GRAPH``
      dict instead of returning the single
      ``_PIVOT_FROM_FAMILY_AFTER_FAILURE`` constant (C1).
    * Prior-family inference from the latest kept-insufficient or
      applier-record signature when ``prior_patch_family`` is empty
      or unknown (C2).

    When OFF, ``next_patch_family_for_cluster`` returns
    ``add_example_sql`` as today — leading to ``pivot_recommended=
    false`` when the cluster is already on that family.
    """
    return (
        trial20_enforce_enabled()
        and _flag_enabled("GSO_TRIAL20_FAMILY_PIVOT_GRAPH")
    )


def trial20_multi_lever_bundle_default_enabled() -> bool:
    """Workstream D — multi-lever bundles as default strategy.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT=0``.

    Gates:

    * Stage 3 prompt directive (D1): iteration 1 LLM MAY emit
      single-lever with free-text ``single_lever_justification``;
      bundle MANDATORY when ``insufficient_repair_signatures`` is
      non-empty for the cluster.
    * Canonical bundle templates surfaced as ``Curated Example
      Patterns`` in the prompt (D2) — illustrative, not mandatory.
    * Strategist gate (D3): refuse sole-lever proposal that reuses
      the same lever family as a rejected ``rejected_insufficient_
      repeat`` signature.
    * Markers (D4): ``GSO_TRIAL20_BUNDLE_EMITTED_V1`` on every
      multi-lever bundle; ``GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1``
      carrying the LLM's free-text justification when single-lever.

    When OFF, Stage 3 prompt keeps the ``bundle_id (optional)``
    wording; no single-lever justification is required; strategist
    gate is advisory.
    """
    return (
        trial20_enforce_enabled()
        and _flag_enabled("GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT")
    )


def trial20_blast_radius_mandatory_enabled() -> bool:
    """Workstream E — mandatory blast-radius gate.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL20_BLAST_RADIUS_MANDATORY=0``.

    Gates:

    * Replicate harness-direct ``passing_dependents`` stamping into
      state-machine ``TransformerContext`` so every proposal
      reaching ``blast_radius_batch._assess_blast_radius`` carries
      the field (E1).
    * Flip ``patch_blast_radius_is_safe``'s ``no_passing_dependents_
      field`` fallback from safe-by-default to unsafe-by-default;
      emit ``GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1`` (E2).
    * (E3 is read-only verification — no flag consumption.)

    No new transformer routing — ``narrow_replacement_gate`` is
    already registered after ``blast_radius_batch`` for
    ``FunnelStage.NORMALIZED`` per ``registry.py`` lines 35-66.
    E1+E2 close the plumbing gap so the existing narrow-replacement
    cycle is actually exercised instead of bypassed by the
    safe-by-default fallback.

    When OFF, ``patch_blast_radius_is_safe`` keeps
    ``no_passing_dependents_field`` safe-by-default fallback as
    today (Trial 19 byte-stable).
    """
    return (
        trial20_enforce_enabled()
        and _flag_enabled("GSO_TRIAL20_BLAST_RADIUS_MANDATORY")
    )


__all__ = [
    "trial20_enforce_enabled",
    "trial20_pre_arbiter_veto_fix_enabled",
    "trial20_kept_insufficient_terminal_enabled",
    "trial20_family_pivot_graph_enabled",
    "trial20_multi_lever_bundle_default_enabled",
    "trial20_blast_radius_mandatory_enabled",
]
