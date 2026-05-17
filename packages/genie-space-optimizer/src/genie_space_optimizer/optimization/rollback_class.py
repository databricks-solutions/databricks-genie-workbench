"""Rollback-reason taxonomy for the adaptive lever loop.

The lever loop records a free-form ``rollback_reason`` string on every
reflection entry that represents a rolled-back iteration. Historically
that string was only used for logging. After the router-and-resilience
plan (Phases C1-C3), it is also classified into a small enum so loop
bookkeeping can distinguish content regressions from infrastructure
errors:

* ``CONTENT_REGRESSION`` — a gate (slice, p0, or full eval) flagged a
  score regression. This is evidence that the chosen strategy did not
  work. It counts toward ``_diminishing_returns`` and toward
  ``_consecutive_rb`` in the lever-loop budget.

* ``INFRA_FAILURE`` — a transient API / deploy error unrelated to the
  content of the patch (5xx, network flake, rate limit, etc.). Does
  not count against content budget but a separate
  ``INFRA_RETRY_BUDGET`` caps stuck-on-infra runs.

* ``SCHEMA_FAILURE`` — a deterministic Genie API rejection due to
  payload structure (``Invalid serialized_space`` / ``Cannot find
  field``). Retrying the same payload will always fail, so the loop
  exits immediately with ``LEVER_LOOP_SCHEMA_FATAL``.

* ``PROPAGATION_FAILURE`` — reserved. No producer is mapped yet, but
  the enum value exists for a future ``propagation_gate`` that checks
  whether a patched Genie space reaches steady state before the next
  iteration.

* ``ACCEPTED_WITH_DEBT`` — Cycle 14B-T2. The iteration accepted the
  candidate under ``RegressionDebtPolicy`` with one or more
  out-of-target regressions counted as bounded debt. Not a rollback
  in the strict sense — the enum is the reflection-classification
  axis used by the forbidden-AG admission predicate (so a
  debt-accepting AG is not unconditionally retried with the same
  signature on the next iteration).

* ``MULTI_PATCH_REGRESSION`` — Cycle 14B-T3. Patch-subset isolation
  could not isolate the regression to a single patch — multiple
  applied patches contributed. The candidate is fully discarded;
  this reason names the diagnosis honestly so the postmortem can
  act on it (instead of silently falling back to ``OTHER``).

* ``NO_ACTION`` — Cycle 13 + Defect Plan 2 + Phase 0.5. The
  iteration emitted no patches: either the strategist generated
  zero proposals (``no_proposals``), was intercepted by the
  collision guard (``ag_collision_with_forbidden_set``), grounded
  proposals but the applier dropped every patch via blast-radius
  (``no_applied_patches``), or the grounder itself dropped every
  patch and no candidate state exists (``no_grounded_patches``).
  Not a rollback in the strict sense; the enum is the
  reflection-classification axis used by the forbidden-AG
  admission predicate so a same-signature AG is not
  unconditionally retried with zero proposals on the next
  iteration. Behind ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION``
  (default-off); when off, ``NO_ACTION`` is classified
  correctly but the admission predicate excludes it (legacy
  behaviour, replay byte-stable).

* ``OTHER`` — catch-all for escalation_handled entries, ``no_proposals``
  skips, and anything the classifier doesn't recognise. These do not
  participate in the diminishing-returns / consecutive-rollback gates.

The classifier is a pure string-prefix matcher. It is intentionally
strict about known prefixes — unknown reasons fall into ``OTHER``
rather than silently defaulting to ``INFRA_FAILURE``, so accidentally
introducing a new producer prefix will show up in the ``OTHER`` bucket
of the observability logs rather than poisoning the infra budget.
"""

from __future__ import annotations

from enum import Enum


class RollbackClass(str, Enum):
    """Classification of a rollback reason string."""

    CONTENT_REGRESSION = "content_regression"
    INFRA_FAILURE = "infra_failure"
    SCHEMA_FAILURE = "schema_failure"
    PROPAGATION_FAILURE = "propagation_failure"  # reserved; no producer yet
    # Cycle 14B-T2: an iteration that accepted a candidate with
    # bounded out-of-target regression debt under
    # RegressionDebtPolicy. Not a rollback in the strict sense; the
    # enum is the reflection-classification axis (see module
    # docstring).
    ACCEPTED_WITH_DEBT = "accepted_with_debt"
    # Cycle 14B-T3: patch-subset isolation could not isolate the
    # regression to a single patch — multiple patches contributed.
    # The candidate is fully discarded; this reason names the
    # diagnosis honestly so the postmortem can act on it.
    MULTI_PATCH_REGRESSION = "multi_patch_regression"
    # Cycle 13: an iteration that produced no patches (no_proposals)
    # or was intercepted by the strategist collision guard
    # (ag_collision_with_forbidden_set). Reflection-classification
    # axis used by the forbidden-AG admission predicate.
    NO_ACTION = "no_action"
    OTHER = "other"


# Schema-failure signatures are case-insensitive substring matches. These
# are the deterministic API rejections that mean "retrying the same
# payload will always fail, stop the loop."
_SCHEMA_FAILURE_SIGNATURES: tuple[str, ...] = (
    "invalid serialized_space",
    "cannot find field",
)


# Content-regression prefixes are the reasons emitted by the three gate
# functions in the harness: slice_gate, p0_gate, full_eval.
_CONTENT_REGRESSION_PREFIXES: tuple[str, ...] = (
    "slice_gate:",
    "p0_gate:",
    "full_eval:",
)


def classify_rollback_reason(reason: str | None) -> RollbackClass:
    """Map a rollback reason string to a :class:`RollbackClass`.

    ``None``, empty string, ``"unknown"``, ``"no_proposals"``, and any
    ``escalation:*`` string all classify as ``OTHER``. Unknown strings
    also classify as ``OTHER`` so that a new producer can't silently
    start consuming the infra retry budget.
    """
    if not reason:
        return RollbackClass.OTHER
    lowered = str(reason).strip().lower()
    if not lowered or lowered == "unknown":
        return RollbackClass.OTHER

    # Check schema signatures first — they appear inside
    # ``patch_deploy_failed:`` messages so the generic ``patch_deploy_failed:``
    # branch below shouldn't steal them.
    if any(sig in lowered for sig in _SCHEMA_FAILURE_SIGNATURES):
        return RollbackClass.SCHEMA_FAILURE

    # Cycle 14B-T2: accept-with-debt iterations are reflected with an
    # ``accepted_with_debt:<qid>`` (or bare ``accepted_with_debt``)
    # rollback-reason string so the forbidden-AG admission predicate
    # can pick them up.
    if lowered == "accepted_with_debt" or lowered.startswith("accepted_with_debt:"):
        return RollbackClass.ACCEPTED_WITH_DEBT

    # Cycle 14B-T3: patch-subset isolation halts with a
    # ``multi_patch_regression`` reason (optionally suffixed with
    # the contributing qids) when more than one applied patch is
    # implicated in an out-of-target regression.
    if (
        lowered == "multi_patch_regression"
        or lowered.startswith("multi_patch_regression:")
    ):
        return RollbackClass.MULTI_PATCH_REGRESSION

    if any(lowered.startswith(prefix) for prefix in _CONTENT_REGRESSION_PREFIXES):
        return RollbackClass.CONTENT_REGRESSION

    if lowered.startswith("patch_deploy_failed"):
        return RollbackClass.INFRA_FAILURE

    if lowered.startswith("escalation:"):
        # Escalations are already routed through ``escalation_handled=True``
        # in the reflection entry and should not contribute to the
        # content / infra budgets.
        return RollbackClass.OTHER

    # Cycle 13 / Defect Plan 2 (2026-05-12) / Phase 0.5 (2026-05-16) —
    # reflection-axis classifications. The iteration produced no
    # patch-applying action (no candidate state reached the gate). They
    # route to NO_ACTION so the forbidden-AG admission predicate picks
    # them up when GSO_FORBIDDEN_AG_ADMITS_NO_ACTION is enabled.
    #
    # Producers admitted on this axis:
    #
    # * ``no_proposals`` (Cycle 13) — strategist generated zero proposals.
    # * ``ag_collision_with_forbidden_set`` (Cycle 13) — strategist re-
    #   proposed a previously-rejected ``(root_cause, blame_set,
    #   lever_set)`` tuple and the collision guard intercepted it.
    # * ``no_applied_patches`` (Defect Plan 2) — proposals were generated
    #   and grounded but ``apply_log.applied`` is empty (the applier
    #   dropped every patch, typically via the blast-radius gate). Pre-
    #   Defect-2 this fell to OTHER, leaving the airline reflection out
    #   of the forbidden set and letting the same AG re-emit on the next
    #   iteration. See run 31ecd96f-5d56-4b5a-af8e-38e9e5c549af.
    # * ``no_grounded_patches`` (Phase 0.5) — proposals were generated
    #   but the grounder dropped every patch and no candidate state
    #   exists (producer at ``harness.py:12297-12301``). Pre-Phase-0.5
    #   this fell to OTHER, closing the same repetition pattern as
    #   ``no_applied_patches`` but on the grounding-failure axis.
    if lowered in {
        "no_proposals",
        "ag_collision_with_forbidden_set",
        "no_applied_patches",
        "no_grounded_patches",
    }:
        return RollbackClass.NO_ACTION

    return RollbackClass.OTHER
