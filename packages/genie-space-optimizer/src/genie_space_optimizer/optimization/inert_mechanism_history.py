"""Trial 29 W29.1 — InertMechanismHistory typed accumulator.

The lever-loop feedback channel for kit-forced inert patches. Mirrors
:mod:`forbidden_signatures` (same harvest/extend shape) but the
per-iteration record is a typed Pydantic model carrying the
``(qid, rca_kind)`` key and the ordered list of rejected mechanisms
the system already tried for that pair.

Stage 3 synthesis reads
``TransformerContext.inert_mechanism_history`` and instructs the LLM
to pick from
``_structural_fix_mechanisms(rca_kind) - rejected_mechanisms`` so the
next iteration cannot re-emit a mechanism we already proved inert.
"""
from __future__ import annotations

from typing import Iterable, Sequence

from pydantic import BaseModel, ConfigDict, Field

from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


class InertMechanismHistory(BaseModel):
    """Per ``(qid, rca_kind)`` record of mechanisms proven inert."""

    model_config = ConfigDict(frozen=True)

    qid: str
    rca_kind: str  # canonical key (must be in RCA_CANONICAL_KEY_SET)
    rejected_mechanisms: tuple[str, ...] = Field(default_factory=tuple)
    signatures: tuple[str, ...] = Field(default_factory=tuple)


def harvest_sm_inert_mechanism_history(
    records: Sequence[AcceptanceDecisionRecord],
    *,
    qid_rca_pairs: Sequence[tuple[str, str]],
) -> tuple[InertMechanismHistory, ...]:
    """Extract InertMechanismHistory entries from this iteration's
    AcceptanceDecisionRecord stream.

    ``records`` and ``qid_rca_pairs`` are positionally paired (the
    caller threads them so we don't have to re-extract the qid/rca
    from inside the SM transformer that already had them in scope).
    Only records with ``decision == "kit_forced_inert_reroute"``
    contribute; everything else is ignored.
    """
    if len(records) != len(qid_rca_pairs):
        # Defensive: silently truncate to the shorter sequence so a
        # threading bug does not crash the harvest pass.
        n = min(len(records), len(qid_rca_pairs))
        records = list(records)[:n]
        qid_rca_pairs = list(qid_rca_pairs)[:n]

    harvested: list[InertMechanismHistory] = []
    for record, (qid, rca_kind) in zip(records, qid_rca_pairs):
        if record.decision != "kit_forced_inert_reroute":
            continue
        if not record.rejected_mechanism:
            continue
        harvested.append(
            InertMechanismHistory(
                qid=str(qid),
                rca_kind=str(rca_kind),
                rejected_mechanisms=(str(record.rejected_mechanism),),
                signatures=(str(record.insufficient_repair_signature or ""),),
            )
        )
    return tuple(harvested)


def extend_sm_inert_mechanism_history(
    prior: Iterable[InertMechanismHistory],
    fresh: Iterable[InertMechanismHistory],
) -> tuple[InertMechanismHistory, ...]:
    """Merge a fresh iteration's harvest into the cumulative history,
    keyed by ``(qid, rca_kind)``.

    Dedupes mechanisms + signatures within each pair. Order is
    preserved (FIFO: earliest insertion wins).
    """
    by_key: dict[tuple[str, str], InertMechanismHistory] = {}
    for entry in list(prior) + list(fresh):
        key = (entry.qid, entry.rca_kind)
        if key not in by_key:
            by_key[key] = entry
            continue
        existing = by_key[key]
        new_mechanisms = tuple(
            m
            for m in entry.rejected_mechanisms
            if m not in existing.rejected_mechanisms
        )
        new_signatures = tuple(
            s for s in entry.signatures if s not in existing.signatures
        )
        by_key[key] = InertMechanismHistory(
            qid=existing.qid,
            rca_kind=existing.rca_kind,
            rejected_mechanisms=existing.rejected_mechanisms + new_mechanisms,
            signatures=existing.signatures + new_signatures,
        )
    return tuple(by_key.values())


__all__ = [
    "InertMechanismHistory",
    "harvest_sm_inert_mechanism_history",
    "extend_sm_inert_mechanism_history",
]
