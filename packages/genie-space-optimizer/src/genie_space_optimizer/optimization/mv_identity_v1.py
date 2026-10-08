"""The frozen v1 measure grouping, read only by the identity-v2 dual read (MV-D123).

Identity v1 grouped a measure's occurrences by table *leaf* and keyed each group
with MV-D7 over the union of the table names as they were spelled. Identity v2
keys a measure by its calculation and its fully qualified tables, so a dismissal
recorded under a v1 key would stop hiding its measure. For one deployed release
the advisor also computes each measure's v1 keys here, hides the measure when
any of them is suppressed, and copies the dismissal forward to the v2 key.

The grouping below is copied verbatim from ``mv_fingerprint`` at 3e71d66f
(``table_leaves`` and the partitioning in ``_measure_buckets``), and this module
imports nothing from the live grouping, so it stays v1 when that grouping
changes. It freezes the grouping, not the extractor: an occurrence is whatever
the caller extracted, with its table names as spelled.

M8 — delete the v1 identity read after one deployed release: this module, its
test and the v1 branch of the suppression check go in one commit citing MV-D123.
"""

from __future__ import annotations

from collections.abc import Hashable, Iterable, Mapping
from typing import TYPE_CHECKING, NamedTuple

from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.mv_fingerprint import MeasureRef


def table_leaves(tables: Iterable[str]) -> frozenset[str]:
    """Lowercase unqualified table names — the grain MV-D116 compares tables at.

    One table spelled two ways (``samples.tpch.orders``, ``tpch.orders``) stays
    one table. The same name in two catalogs also reads as one table.
    """
    leaves: set[str] = set()
    for table in tables:
        leaf = str(table or "").strip().rsplit(".", 1)[-1].strip().strip("`").lower()
        if leaf:
            leaves.add(leaf)
    return frozenset(leaves)


class _MeasureOccurrence(NamedTuple):
    id: Hashable
    measure: MeasureRef


def _measure_partitions(
    occurrences: dict[str, list[_MeasureOccurrence]],
) -> list[list[_MeasureOccurrence]]:
    """One partition per expression — or per table grain when it spans several (MV-D116).

    With at most one distinct non-empty set of table names, the partition holds
    every occurrence. With several, each set gets its own partition, and
    occurrences that resolved to no table form a table-less remainder.
    """
    buckets: list[list[_MeasureOccurrence]] = []
    for found in occurrences.values():
        groups: dict[frozenset[str], list[_MeasureOccurrence]] = {}
        for occurrence in found:
            groups.setdefault(table_leaves(occurrence.measure.source_tables), []).append(occurrence)
        named = [leaves for leaves in groups if leaves]
        if len(named) <= 1:
            partitions = [found]
        else:
            partitions = [groups[leaves] for leaves in named]
            if frozenset() in groups:
                partitions.append(groups[frozenset()])
        buckets.extend(partitions)
    return buckets


def v1_member_keys(
    space_id: str,
    occurrences: Mapping[Hashable, MeasureRef],
) -> dict[Hashable, str]:
    """Return ``{occurrence id: v1 MV-D7 key}`` for every occurrence.

    ``occurrences`` maps an id to an extracted measure; only its ``fingerprint``,
    ``canonical_expr`` and spelled ``source_tables`` are read. The id is the
    caller's, returned as given. Occurrences group by expression fingerprint,
    then by table leaf, and each group's key is MV-D7 over its first
    occurrence's ``canonical_expr`` and the union of its spelled table names —
    the key the advisor gave that group's corpus row before identity v2.
    """
    by_fingerprint: dict[str, list[_MeasureOccurrence]] = {}
    for occurrence_id, measure in occurrences.items():
        by_fingerprint.setdefault(measure.fingerprint, []).append(
            _MeasureOccurrence(occurrence_id, measure)
        )
    keys: dict[Hashable, str] = {}
    for partition in _measure_partitions(by_fingerprint):
        tables = {table for occurrence in partition for table in occurrence.measure.source_tables}
        key = mv_candidate_fingerprint(space_id, partition[0].measure.canonical_expr, tables)
        for occurrence in partition:
            keys[occurrence.id] = key
    return keys


__all__ = ["table_leaves", "v1_member_keys"]
