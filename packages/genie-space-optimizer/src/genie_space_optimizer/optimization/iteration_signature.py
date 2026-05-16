"""Stable signature for iteration-level proposal failures.

Used by the stalemate detector in
``harness._emit_proposal_failure_decided`` to determine whether the
current iteration is a repeat of a prior one — when it is, the policy
escalates to ``ESCALATE_STALEMATE`` instead of looping forever (the
C4 contract break Trial-5 exposed).
"""

from __future__ import annotations

import hashlib


def iteration_failure_signature(
    *,
    ag_id: str,
    failure_mode: str,
    root_cause: str,
    lever_set: tuple[int, ...],
    tried_lever_families: tuple[int, ...],
    cluster_signature: str,
) -> str:
    """Return a 16-char hex digest summarizing the failure context.

    The signature is order-insensitive for ``lever_set`` and
    ``tried_lever_families`` (both are semantically sets). It is
    sensitive to every other input. Two failures with identical
    signatures mean the lever-loop is going to make the same proposal-
    generation call with the same context — i.e. a stalemate.
    """
    payload = "|".join((
        str(ag_id),
        str(failure_mode),
        str(root_cause),
        ",".join(str(x) for x in sorted(set(lever_set))),
        ",".join(str(x) for x in sorted(set(tried_lever_families))),
        str(cluster_signature),
    ))
    digest = hashlib.blake2b(payload.encode("utf-8"), digest_size=8).hexdigest()
    return digest
