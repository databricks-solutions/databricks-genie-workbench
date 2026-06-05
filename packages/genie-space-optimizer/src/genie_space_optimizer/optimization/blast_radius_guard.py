"""Phase 3 P3.4 — production guard against unstamped blast-radius.

Background — Trial 20 Workstream E1 added a safe-by-default
``setdefault("passing_dependents", [])`` in the synthesize-llm
transformer and the static-judge replay so the blast-radius gate
would not crash on tape-replay paths that don't run the
counterfactual scanner. That fallback was correct for offline
testing but became a silent foot-gun in production: a patch arriving
at the applier with NO ``passing_dependents`` key was treated as "no
dependents → safe", masking proposals where the counterfactual
scanner had silently failed and never stamped the field.

P3.4 closes that gap. When the run carries a LIVE benchmark
(``ctx.benchmarks`` is non-empty), an unstamped ``passing_dependents``
key on a candidate patch_body is a contract violation: the
synthesize-llm transformer was supposed to stamp it and didn't. The
applier MUST refuse to apply such patches and surface a typed
``BLAST_RADIUS_UNSTAMPED_UNSAFE`` rejection reason so the next
iteration's postmortem can attribute the failure to the stamping
gap (vs. masking it as ``blast_radius_rejected``).

Workbench / tape-replay paths (``ctx.benchmarks`` empty) keep the
Trial 20 E1 ``setdefault`` semantics — the offline judge and
fuzzer have no benchmark catalog to compute dependents against.

This module exposes :func:`is_blast_radius_unstamped_in_production`
as the single predicate the applier consults. The predicate is
deterministic and side-effect-free.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence


# Sentinel rejection reason — emitted on the rejected
# ProposalAttempt.outcome_reason and on the
# ``BLAST_RADIUS_UNSTAMPED_UNSAFE`` postmortem marker so postmortems
# can grep one canonical string. Keeping the string in this module
# (rather than re-defining inline at the applier) lets the SM tests
# pin the contract without importing from the applier monolith.
BLAST_RADIUS_UNSTAMPED_UNSAFE: str = "blast_radius_unstamped_unsafe"


def is_blast_radius_unstamped_in_production(
    *,
    patch_body: Mapping[str, Any] | None,
    benchmarks: Sequence[Mapping[str, Any]] | None,
) -> bool:
    """Return ``True`` when the applier MUST refuse to apply this
    patch because the synthesize-llm transformer forgot to stamp
    ``passing_dependents`` AND the run has a live benchmark catalog.

    Decision logic:

      * ``benchmarks`` is None / empty → workbench / tape-replay
        path; the legacy safe-by-default fallback applies and the
        guard returns ``False``. The applier path that calls this
        helper continues to call ``patch_body.setdefault("passing_dependents", [])``
        for byte-stable behaviour with Trial 20 E1.
      * ``patch_body`` is None / missing → defensive ``False``;
        upstream validators have already rejected the patch on
        empty body and the guard need not re-litigate.
      * ``passing_dependents`` key absent from ``patch_body`` →
        ``True``. Note: an EMPTY list is fine — that means the
        scanner ran and found no dependents. An absent KEY means
        the scanner DID NOT RUN, which is the violation.
      * ``passing_dependents`` key present (even ``[]`` or
        ``None``) → ``False``; the scanner left its witness.

    The asymmetry between "key absent" (violation) and
    "key=None or []" (safe) is intentional: the synthesize-llm
    transformer's setdefault call writes ``[]`` precisely so the
    downstream gates can tell "scanner ran, found nothing" from
    "scanner did not run". P3.4 enforces that semantic at the
    applier boundary in production.
    """
    if not benchmarks:
        return False
    if patch_body is None:
        return False
    return "passing_dependents" not in patch_body


def blast_radius_unstamped_rejection_reason(
    *,
    patch_type: str,
    intent_id: str,
) -> str:
    """Format the canonical rejection_reason string for the
    rejection path. Centralized so the applier, the postmortem
    grader, and the workbench fuzzer (Phase 4 K6/K7/K8) can all
    agree on the format::

        blast_radius_unstamped_unsafe:<patch_type>:<intent_id>

    Both arguments default to ``"unknown"`` when empty so the
    postmortem grep never matches a malformed marker.
    """
    pt = str(patch_type or "").strip() or "unknown"
    iid = str(intent_id or "").strip() or "unknown"
    return f"{BLAST_RADIUS_UNSTAMPED_UNSAFE}:{pt}:{iid}"
