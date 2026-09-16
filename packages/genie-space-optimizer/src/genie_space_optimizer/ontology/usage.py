"""Popularity normalizer (Signal Authority Stage 1, MV-D93) — pure, I/O-free.

Turns per-table read-demand rows (``{fqn, reads, users}``) from the batch lineage
reader into the pre-normalized ``{fqn -> demand in [0, 1]}`` map the L6 blend's usage
factor reads (``rank.FACTOR_WEIGHTS["usage"] == 0.40`` — the highest-weight lever, dark
until now because ``usage_signals()`` returned ``{}``). It does NOT re-weight or re-tier
anything: it feeds ``rank.RankSignals.usage`` a better input; ``rank.blend`` is unchanged.

**Normalization: percentile rank** of a combined demand score over the in-scope tables.
Query volume is heavily right-skewed (a few hot tables), so a max-normalize would crush
the field to ~0; percentile rank keeps the ordering legible — the busiest table lands at
the top percentile (1.0), the least-busy at 0.0, ties share the average percentile.

**Honest-gap (MV-D43 / architecture §5).** A table with zero reads in the window is
OMITTED (absent, never scored ``0.0`` — a table nobody queried has no usage entry, so it
simply leaves the blend and lowers coverage). Empty or unparseable input ⇒ ``{}``.

**Deterministic (MV-D82).** Sorted output, average-rank ties, no wall-clock — the same
rows always yield the same map.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

# Weight on the distinct-user breadth term relative to raw read volume in the combined
# demand score. Reads are the primary demand signal; user breadth lifts a table many
# people touch over one a single job hammers. Both are log-scaled first so neither
# dominates by raw magnitude, and the percentile rank then erases the absolute scale
# entirely — this constant only shapes the *ordering* when reads and breadth disagree.
_BREADTH_WEIGHT = 0.5


def _demand(reads: float, users: float) -> float:
    """Combined, log-scaled read demand for one table (higher = more in demand)."""
    return math.log1p(max(reads, 0.0)) + _BREADTH_WEIGHT * math.log1p(max(users, 0.0))


def normalize_usage(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    """Percentile-rank per-table read demand into a sorted ``{fqn -> [0, 1]}`` map.

    ``rows`` are ``{fqn, reads, users}`` (one per table, from the trailing-window
    lineage read). Zero-read tables are omitted (honest-gap); the busiest table lands
    at the top percentile (``1.0``) and the least-busy at ``0.0``; ties share the
    average percentile. Empty or fully-degenerate input ⇒ ``{}``.
    """
    demands: dict[str, float] = {}
    for row in rows or ():
        fqn = str(row.get("fqn") or "").strip()
        if not fqn:
            continue
        try:
            reads = float(row.get("reads") or 0.0)
            users = float(row.get("users") or 0.0)
        except (TypeError, ValueError):
            continue  # an unparseable row is dropped, never faked to 0 (MV-D43)
        if reads <= 0.0:
            continue  # a table nobody queried has NO usage entry (absent, not 0.0)
        # The reader GROUP BYs, so fqns are unique in practice; keep the max to stay
        # order-stable if a caller ever passes a table twice.
        demands[fqn] = max(demands.get(fqn, 0.0), _demand(reads, users))

    n = len(demands)
    if n == 0:
        return {}
    if n == 1:
        # A lone in-scope table is, trivially, the busiest ⇒ the top percentile.
        return {next(iter(demands)): 1.0}

    # Average-rank percentile (ascending): the smallest demand → 0.0, the largest → 1.0,
    # tied values share their averaged rank. Fully deterministic — no wall-clock, no
    # dependence on dict iteration order.
    ordered = sorted(demands.values())
    avg_rank: dict[float, float] = {}
    i = 0
    while i < n:
        j = i
        while j < n and ordered[j] == ordered[i]:
            j += 1
        # 0-based positions i..j-1 are 1-based ranks (i+1)..j; average them for ties.
        avg_rank[ordered[i]] = (i + 1 + j) / 2.0
        i = j

    return {
        fqn: round((avg_rank[d] - 1.0) / (n - 1), 6)
        for fqn, d in sorted(demands.items())
    }
