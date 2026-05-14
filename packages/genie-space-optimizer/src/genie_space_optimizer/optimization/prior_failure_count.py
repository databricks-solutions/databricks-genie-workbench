"""Phase 1.4 — compute prior_failure_count from reflection_buffer.

Replaces the literal ``prior_failure_count=0`` (or stale value)
that the harness passes into the strategist constraint filter and
the AG-retire decision.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence


def compute_prior_failure_count(
    *,
    cluster_signature: tuple[tuple[str, tuple[str, ...]], ...],
    reflection_buffer: Sequence[Mapping[str, Any]],
) -> int:
    """Return the number of reflection_buffer entries whose
    ``cluster_signature`` matches the given cluster_signature and
    whose ``accepted`` flag is False.

    Pure: no I/O, no harness imports.

    ``cluster_signature`` MUST be canonical (sorted by cluster_id,
    qids sorted ascending) — see
    :func:`terminal_signature.build_terminal_signature`.
    """
    target = tuple(cluster_signature)
    count = 0
    for entry in (reflection_buffer or ()):
        if bool(entry.get("accepted")):
            continue
        cs = entry.get("cluster_signature")
        if cs is None:
            continue
        if tuple(cs) == target:
            count += 1
    return count
