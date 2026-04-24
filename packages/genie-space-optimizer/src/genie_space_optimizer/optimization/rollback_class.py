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

    if any(lowered.startswith(prefix) for prefix in _CONTENT_REGRESSION_PREFIXES):
        return RollbackClass.CONTENT_REGRESSION

    if lowered.startswith("patch_deploy_failed"):
        return RollbackClass.INFRA_FAILURE

    if lowered.startswith("escalation:"):
        # Escalations are already routed through ``escalation_handled=True``
        # in the reflection entry and should not contribute to the
        # content / infra budgets.
        return RollbackClass.OTHER

    if lowered == "no_proposals":
        return RollbackClass.OTHER

    return RollbackClass.OTHER
