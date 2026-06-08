"""Trial 31 — land the structural fallback the enforced switch demands
(airline L6-decline) + RCA-groundedness funnel de-death (7now) +
invariant/slice consistency.

Mirror of :mod:`trial30_flags`. Same default-ON / OFF-vocabulary
semantics. Single emergency rollback knob is ``GSO_TRIAL31``; each
sub-flag is forced OFF when the master is OFF (byte-stable rollback to
Trial 30).

Trial 31 closes the W30.5 PARTIAL gaps:
  * W31.2 — the SM->dispatch RCA-grounding projection dropped the
    cluster's named causal assets (stored them in
    ``RcaFinding.expected_objects``, a field the groundedness gate's
    ``_finding_terms`` never reads), so 7now's gs_013/gs_026 died at
    ``rca_ungrounded -> no_causal_target``. The projection now carries
    the blame set + counterfactual fixes (and the card's own
    ``grounding_terms`` when present) onto the finding's grounding
    surface.
  * W31.4 — the post-apply empty-slice fail-fast
    (``post_apply_eval_empty_slice_for_requested_qid``) fired for an
    already-correct QID that legitimately has no benchmark row. Now the
    invariant only fires for a *benchmark-expected* (live-hard) QID; a
    requested set that is entirely already-correct / no-benchmark is a
    benign skip, not an ``OPTIMIZER_INVARIANT_VIOLATION``. This matters
    because W31.3 fails the lever_loop task on an invariant violation,
    so a spurious empty-slice would otherwise fail the whole run.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables.
"""
from __future__ import annotations

import os

_TRIAL31_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL31_FLAG_OFF_VALUES


def trial31_enabled() -> bool:
    """Trial 31 master flag. Default ON.

    Opt out with ``export GSO_TRIAL31=0`` for emergency rollback. When
    OFF, every Trial 31 sub-flag below is forced OFF regardless of its
    own env var (byte-stable rollback to Trial 30).
    """
    return _flag_enabled("GSO_TRIAL31")


def _subflag_opt_out(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL31_FLAG_OFF_VALUES
    return trial31_enabled() and not off


def trial31_no_structural_candidate_terminal_enabled() -> bool:
    """W31.1(b) — at SM Stage-3 finalization, when the RCA mandates a
    structural mechanism but the surviving slate is inert-only (the
    forced-L6 / plan11 structural synthesis declined), emit a typed
    ``no_structural_candidate`` no-op instead of letting the inert patch
    survive to application (which trips
    ``rca_mechanism_defaulted_to_instruction_text`` ->
    ``OPTIMIZER_INVARIANT_VIOLATION`` — failed by W31.3).

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL31_NO_STRUCTURAL_CANDIDATE_TERMINAL=0`` to restore
    the Trial 30 flatline-avoidance (inert sole-survivor kept).
    """
    return _subflag_opt_out("GSO_TRIAL31_NO_STRUCTURAL_CANDIDATE_TERMINAL")


def trial31_rca_grounding_projection_enabled() -> bool:
    """W31.2 — carry the cluster's named causal assets (ASI blame set +
    counterfactual fixes + the RCA card's ``grounding_terms``) onto the
    projected ``RcaFinding.grounding_terms`` so the proposal gate can
    see them.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL31_RCA_GROUNDING_PROJECTION=0`` — the projection
    then leaves ``grounding_terms`` empty (byte-stable Trial 30
    behaviour).
    """
    return _subflag_opt_out("GSO_TRIAL31_RCA_GROUNDING_PROJECTION")


def trial31_empty_slice_excludes_correct_enabled() -> bool:
    """W31.4 — the post-apply empty-slice invariant only fires for a
    benchmark-expected (live-hard) requested QID; a requested set that
    is entirely already-correct / no-benchmark is a benign skip.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL31_EMPTY_SLICE_EXCLUDE_CORRECT=0`` to restore the
    Trial 30 fail-fast that raised for any empty slice.
    """
    return _subflag_opt_out("GSO_TRIAL31_EMPTY_SLICE_EXCLUDE_CORRECT")


__all__ = [
    "trial31_enabled",
    "trial31_no_structural_candidate_terminal_enabled",
    "trial31_rca_grounding_projection_enabled",
    "trial31_empty_slice_excludes_correct_enabled",
]
