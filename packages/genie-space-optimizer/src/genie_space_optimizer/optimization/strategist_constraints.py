"""Cross-iteration AG constraints for the strategist (T5).

Today the lever loop forgets *why* a previous iteration's proposals
failed when it asks the strategist to try again. This module keeps a
small ledger of structured constraints — initially just
``forbid_tables`` per AG-id — that gets serialized into the strategist
prompt context on the next call.

Cycle 9 motivation: the airline run had `BLAST-RADIUS GATE` drop both
patches under `AG_DECOMPOSED_H001` because `tkt_payment` had passing
dependents outside the AG's targets. The strategist then re-proposed
the same patch shape against the same table on every subsequent
iteration, with no signal that the table was off-limits. This module
captures the table as a forbidden target so the next call carries
that constraint through the prompt.

Phase B will extend this with ``forbid_filters``,
``required_root_cause_families``, and rollback-driven negative
constraints.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T5.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class StrategistConstraints:
    """Per-AG constraints that survive across iterations."""

    _forbid_tables: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    def forbid_table_for_ag(self, ag_id: str, table: str) -> None:
        ag = str(ag_id or "").strip()
        tbl = str(table or "").strip()
        if not ag or not tbl:
            return
        self._forbid_tables[ag].add(tbl)

    def forbid_tables_for_ag(self, ag_id: str) -> set[str]:
        return set(self._forbid_tables.get(str(ag_id or "").strip(), set()))

    def to_strategist_context(self) -> dict[str, dict[str, list[str]]]:
        """Render constraints as a stable JSON-friendly dict.

        Lists are sorted so the prompt-cache key is stable across runs.
        AGs with no constraints are omitted (callers can use truthiness
        of the returned dict to skip the prompt-context insertion).
        """
        out: dict[str, dict[str, list[str]]] = {}
        for ag, tables in self._forbid_tables.items():
            if not tables:
                continue
            out[ag] = {"forbid_tables": sorted(tables)}
        return out


def record_blast_radius_drop(
    *,
    constraints: StrategistConstraints,
    ag_id: str,
    dropped_patches: list[dict],
) -> None:
    """Mirror the blast-radius gate's drop list into the constraint store.

    ``dropped_patches`` is the list of dropped-patch dicts the gate
    already builds (each carries ``target`` = fully-qualified table).
    Patches without a ``target`` field are silently skipped (e.g.
    ``add_instruction`` patches don't bind to a table).
    """
    for p in dropped_patches or []:
        target = str(p.get("target") or "").strip()
        if target:
            constraints.forbid_table_for_ag(ag_id, target)
