"""Plan 12 — typed carrier for blast-radius-dropped patches.

Replaces the dict-shaped drop records that lost ``original_patch_type``
/ ``causal_target`` / ``original_patch_body`` and broke
``narrow_replacement_with_llm`` (the ``narrow_skipped_no_original_patch_type``
record both 2026-05-20 postmortems observed).

Carries every field ``narrow_replacement_with_llm`` needs so the
narrow-replacement LLM can produce a scoped patch that no longer
collides with the protected ``collateral_qids``. The
:func:`narrow_replacement_inputs_from` helper projects the record into
the exact kwargs the existing loop accepts.

``__hash__`` is intentionally unsupported: the dict-valued fields
``original_patch_body`` and ``protected_sql_by_qid`` are not hashable,
so we set ``eq=True, unsafe_hash=False`` (the dataclass defaults).
Postmortem consumers compare records by field-equality only.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True)
class BlastRadiusDropRecord(JsonRoundTrip):
    intent_id: str
    original_patch_type: str
    original_patch_body: Mapping[str, Any]
    causal_target: str
    failing_sql_anchor: str
    target_qids: tuple[str, ...]
    collateral_qids: tuple[str, ...]
    protected_sql_by_qid: Mapping[str, str]
    rca_card_id: str
    cluster_id: str
    ag_id: str

    def to_json(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "original_patch_type": self.original_patch_type,
            "original_patch_body": dict(self.original_patch_body),
            "causal_target": self.causal_target,
            "failing_sql_anchor": self.failing_sql_anchor,
            "target_qids": list(self.target_qids),
            "collateral_qids": list(self.collateral_qids),
            "protected_sql_by_qid": dict(self.protected_sql_by_qid),
            "rca_card_id": self.rca_card_id,
            "cluster_id": self.cluster_id,
            "ag_id": self.ag_id,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "BlastRadiusDropRecord":
        return cls(
            intent_id=str(payload["intent_id"]),
            original_patch_type=str(payload.get("original_patch_type", "")),
            original_patch_body=dict(payload.get("original_patch_body") or {}),
            causal_target=str(payload.get("causal_target", "")),
            failing_sql_anchor=str(payload.get("failing_sql_anchor", "")),
            target_qids=tuple(
                str(q) for q in payload.get("target_qids", [])
            ),
            collateral_qids=tuple(
                str(q) for q in payload.get("collateral_qids", [])
            ),
            protected_sql_by_qid=dict(
                payload.get("protected_sql_by_qid") or {}
            ),
            rca_card_id=str(payload.get("rca_card_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            ag_id=str(payload.get("ag_id", "")),
        )


def narrow_replacement_inputs_from(
    record: BlastRadiusDropRecord,
) -> dict[str, Any]:
    """Project a :class:`BlastRadiusDropRecord` into the exact kwargs
    ``narrow_replacement_with_llm`` needs (the entry point that
    actually drives the LLM loop). The wrapper in
    ``stages/narrow_replacement.py`` consumes this and reconstructs
    a typed :class:`RepairProposal` from ``original_patch_body``.
    """
    return {
        "original_patch_type": record.original_patch_type,
        "original_patch_body": dict(record.original_patch_body),
        "causal_target": record.causal_target,
        "failing_sql_anchor": record.failing_sql_anchor,
        "target_qids": tuple(record.target_qids),
        "collateral_qids": tuple(record.collateral_qids),
        "protected_sql_by_qid": dict(record.protected_sql_by_qid),
        "rca_card_id": record.rca_card_id,
        "cluster_id": record.cluster_id,
        "ag_id": record.ag_id,
        "intent_id": record.intent_id,
    }
