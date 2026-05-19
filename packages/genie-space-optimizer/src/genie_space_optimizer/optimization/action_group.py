"""ActionGroup typed view — Plan 1 Task 6.

Read-only typed projection of the legacy AG dict shape every
strategist + harness path produces today.
``ActionGroup.from_legacy(ag)`` is the adapter; the dict remains the
source of truth in Plan 1. Plan 4 promotes ActionGroup to the source
of truth and removes the dict path.

Carried as a sidecar on ``ActionGroupSlate.ag_records`` (Task 12).
The legacy ``ags: tuple[Mapping, ...]`` field continues to flow
alongside unchanged, so existing downstream readers are byte-stable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class ActionGroup(JsonRoundTrip):
    """Typed view of a single action group.

    Field-derivation rules in ``from_legacy``:
      * ``ag_id`` reads ``id`` first then ``ag_id`` (canonical harness
        order).
      * ``target_qids`` reads ``target_qids`` first then
        ``affected_questions`` (per the strategist emission shape).
      * ``source_cluster_ids`` is normalised to a tuple.
      * ``lever_directives`` is normalised to an int-tuple.
    """

    ag_id: str
    ag_kind: str
    primary_cluster_id: str
    source_cluster_ids: tuple[str, ...]
    target_qids: tuple[str, ...]
    affected_questions: tuple[str, ...]
    rca_id: str
    lever_directives: tuple[int, ...]
    needs_rca_regeneration: bool

    @classmethod
    def from_legacy(cls, ag: Mapping) -> "ActionGroup":
        """Project a legacy AG dict into a typed ActionGroup.

        Raises ``ValueError`` when neither ``id`` nor ``ag_id`` is set
        — an empty-id record is a data-integrity bug we surface at
        the boundary.
        """
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id:
            raise ValueError(
                f"ActionGroup.from_legacy: ag_id missing from input "
                f"(neither 'id' nor 'ag_id' set). ag keys={sorted(ag)}"
            )
        target_qids = tuple(
            str(q) for q in (ag.get("target_qids") or ()) if str(q)
        )
        affected = tuple(
            str(q) for q in (ag.get("affected_questions") or ()) if str(q)
        )
        if not target_qids:
            target_qids = affected
        lever_raw = ag.get("lever_directives") or ag.get("levers") or ()
        lever_directives = tuple(
            int(lk) for lk in lever_raw if str(lk).strip().isdigit()
        )
        source_cluster_ids = tuple(
            str(c) for c in (ag.get("source_cluster_ids") or ()) if str(c)
        )
        return cls(
            ag_id=ag_id,
            ag_kind=str(ag.get("ag_kind") or ""),
            primary_cluster_id=str(ag.get("primary_cluster_id") or ""),
            source_cluster_ids=source_cluster_ids,
            target_qids=target_qids,
            affected_questions=affected or target_qids,
            rca_id=str(ag.get("rca_id") or ""),
            lever_directives=lever_directives,
            needs_rca_regeneration=bool(
                ag.get("needs_rca_regeneration") or False
            ),
        )

    @classmethod
    def from_json(cls, payload: dict) -> "ActionGroup":  # type: ignore[override]
        return cls(
            ag_id=str(payload["ag_id"]),
            ag_kind=str(payload.get("ag_kind") or ""),
            primary_cluster_id=str(payload.get("primary_cluster_id") or ""),
            source_cluster_ids=tuple(payload.get("source_cluster_ids") or ()),
            target_qids=tuple(payload.get("target_qids") or ()),
            affected_questions=tuple(payload.get("affected_questions") or ()),
            rca_id=str(payload.get("rca_id") or ""),
            lever_directives=tuple(
                int(lk) for lk in (payload.get("lever_directives") or ())
            ),
            needs_rca_regeneration=bool(
                payload.get("needs_rca_regeneration") or False
            ),
        )
