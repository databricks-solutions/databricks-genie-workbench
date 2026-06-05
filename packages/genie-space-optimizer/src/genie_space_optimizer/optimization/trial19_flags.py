"""Trial 19 — central home for the enforce-decisions feature flags.

Module pattern mirrors :mod:`trial18_flags` so flag helpers are easy
to grep and so callsites that import them don't look like "legacy
synthesis import" calls to the Plan 11 callsite coverage guardrail
(which scans for ``cluster_driven_synthesis`` /
``forced_synthesis_dispatch`` / ``three_stage_pipeline`` imports that
bypass the plan11 flag branch).

Trial 19 turns Trial 18's honest labels into enforced control
decisions and flips the RCA classification surface to LLM-first
end-to-end:

* Workstream A — admission gate, Stage 3 plumbing for
  ``insufficient_repair_signatures``, AG regenerator wire-in,
  ``fallback_no_new_strategy`` terminal, ``GSO_INSUFFICIENT_
  SIGNATURES_IN_CONTEXT_V1`` audit marker. Gated by
  :func:`trial19_enforce_insufficient_enabled`.
* Workstream B — free-text ``rca_kind_label`` and
  ``intended_patch_shape`` authoritative end-to-end;
  ``_INTENDED_PATCH_SHAPE`` / ``_FORBIDDEN_FAMILIES`` dicts become
  back-compat readers; structural repair gate emits typed retry
  instead of admitting ``absent`` shape. Gated by
  :func:`trial19_llm_first_rca_enabled`.
* Workstream C1+C2 — hard-QID admission filter at dispatcher entry
  and mid-run reclassification of ``already_correct_under_arbiter``.
  Gated by :func:`trial19_already_correct_filter_enabled`.
* Workstream C3 — GT correction candidates written to
  ``pending_review`` (no swap, no corpus mutation). Gated by
  :func:`trial19_gt_pending_review_enabled`.

A single master gate :func:`trial19_enforce_enabled` ANDs over all
four sub-flags so emergency rollback to pre-Trial-19 behaviour is a
single ``export GSO_TRIAL19_ENFORCE=0``.

When the master flag is OFF every Trial 19 surface reverts byte-for-
byte to the pre-Trial-19 behaviour:

* No admission gate; ``insufficient_repair_signatures`` stay
  advisory-only prompt context (Trial 18 semantics).
* RCA classification reads the closed ``RcaKind`` enum and the
  ``_INTENDED_PATCH_SHAPE`` / ``_FORBIDDEN_FAMILIES`` dicts; the
  structural repair gate admits ``absent`` shapes as today.
* Hard-QID list flows into iteration 1 unchanged; ``acceptance_gate``
  never emits ``already_correct_under_arbiter``.
* No ``pending_review`` writes from the hard-QID side; the existing
  ``write_gt_correction_candidates`` pipeline keeps its prior
  callsites.

Opt-out semantics: any of ``0`` / ``false`` / ``no`` / ``off`` (case
insensitive) disables the flag. Any other value, or env unset,
enables.
"""
from __future__ import annotations

import os


_TRIAL19_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL19_FLAG_OFF_VALUES


def trial19_enforce_enabled() -> bool:
    """Trial 19 master flag.

    Default: ON. Opt out with
    ``export GSO_TRIAL19_ENFORCE=0`` for emergency rollback to the
    pre-Trial-19 behaviour (Trial 18 acceptance lanes preserved).

    When OFF, every Trial 19 sub-flag is forced OFF regardless of
    its own env var. This is the single rollback knob — callsites
    in ``admission_gate``, ``stages.synthesize``, ``rca_card_builder``,
    ``structural_repair_gate``, ``acceptance_gate``, ``harness``, and
    ``state`` (GT pending_review) all consult their sub-flag through
    this gate.
    """
    return _flag_enabled("GSO_TRIAL19_ENFORCE")


def trial19_enforce_insufficient_enabled() -> bool:
    """Workstream A — admission gate + insufficient signal enforcement.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL19_ENFORCE_INSUFFICIENT=0``.

    Gates:

    * ``admission_gate.evaluate_admission`` (A1).
    * Stage 3 ``insufficient_repair_signatures`` wire-in (A2).
    * AG regenerator wrapper invocation (A3).
    * ``GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1`` audit marker (A5).
    * ``FALLBACK_NO_NEW_STRATEGY`` terminal (A6).

    Prompt edits in ``plan11_cluster/SKILL.md`` and
    ``plan11_synthesize/SKILL.md`` (A4) are not gated — prompts are
    static text. The runtime consumers above are what observe the
    gate.
    """
    return (
        trial19_enforce_enabled()
        and _flag_enabled("GSO_TRIAL19_ENFORCE_INSUFFICIENT")
    )


def trial19_llm_first_rca_enabled() -> bool:
    """Workstream B — LLM-first RCA classification end-to-end.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL19_LLM_FIRST_RCA=0``.

    Gates:

    * ``dominant_root_cause_label`` free-text aggregator (B1).
    * ``intended_patch_shape_for_root_cause`` LLM-first read (B2).
    * Stage 3 ``allowed_and_forbidden_patch_families`` prompt-based
      enforcement (B3).
    * ``structural_repair_gate`` retry-with-typed-feedback when
      ``intended_patch_shape`` is named (B4).

    When OFF, callers fall back to the closed ``RcaKind`` enum +
    ``_INTENDED_PATCH_SHAPE`` / ``_FORBIDDEN_FAMILIES`` dicts
    (back-compat path) for byte-stable replay of pre-Trial-19
    fixtures.
    """
    return (
        trial19_enforce_enabled()
        and _flag_enabled("GSO_TRIAL19_LLM_FIRST_RCA")
    )


def trial19_already_correct_filter_enabled() -> bool:
    """Workstream C1+C2 — already-correct-under-arbiter detection.

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL19_ALREADY_CORRECT_FILTER=0``.

    Gates:

    * Hard-QID admission filter at dispatcher entry (C1) — baseline
      ``row_semantic_score`` per hard QID, exclude when
      ``arbiter_verdict == both_correct`` AND raw byte-match
      disagrees.
    * Mid-run reclassification in ``acceptance_gate``'s
      ``kept_insufficient`` branch (C2) — emits
      ``already_correct_under_arbiter`` decision and suppresses
      future proposals for the QID.

    When OFF, the hard-QID list flows into iteration 1 unchanged
    and ``acceptance_gate`` never emits the new decision.
    """
    return (
        trial19_enforce_enabled()
        and _flag_enabled("GSO_TRIAL19_ALREADY_CORRECT_FILTER")
    )


def trial19_gt_pending_review_enabled() -> bool:
    """Workstream C3 — GT correction candidate capture (no swap).

    Default: ON when master is ON. Opt out with
    ``export GSO_TRIAL19_GT_PENDING_REVIEW=0``.

    Gates the write of ``gt_correction_candidate`` rows with status
    ``pending_review`` via
    ``state.write_gt_correction_candidates`` when C1 / C2 detect
    arbiter-correct-but-GT-disagrees baselines.

    Trial 19 only captures; the four-state machine
    (``pending_review`` / ``accepted_corpus_fix`` /
    ``rejected_keep_gt`` / ...) at
    ``state.write_gt_correction_candidates`` already exists and is
    unchanged by this flag — Trial 19 just adds new producers
    behind this gate. No corpus mutation.
    """
    return (
        trial19_enforce_enabled()
        and _flag_enabled("GSO_TRIAL19_GT_PENDING_REVIEW")
    )


__all__ = [
    "trial19_enforce_enabled",
    "trial19_enforce_insufficient_enabled",
    "trial19_llm_first_rca_enabled",
    "trial19_already_correct_filter_enabled",
    "trial19_gt_pending_review_enabled",
]
