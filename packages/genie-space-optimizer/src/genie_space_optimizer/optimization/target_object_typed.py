"""Plan 9 Task 1 — TargetObject typed contract.

A TargetObject is the LLM-emitted typed slice that replaces the
archetype-derived AssetSlice. Plan 5's repair-intent-synthesis SKILL
emits a tuple of TargetObjects per RepairProposal; the
llm_direct_slice_resolver (T6) resolves each one to a concrete
table / metric_view / column entry from metadata_snapshot.

Pure data + JsonRoundTrip; no logic, no LLM call, no dependency
on archetypes.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


class AssetKind(StrEnum):
    """Closed set of asset kinds the LLM can name in target_objects."""

    TABLE = "table"
    METRIC_VIEW = "metric_view"
    COLUMN = "column"


@dataclass(frozen=True, slots=True)
class TargetObject(JsonRoundTrip):
    """One typed slice the LLM emits in RepairProposal.target_objects.

    Fields:
      * ``asset_kind`` — TABLE / METRIC_VIEW / COLUMN.
      * ``identifier`` — fully qualified name (catalog.schema.name for
        tables and metric views; catalog.schema.table.column for
        columns). MUST be non-empty.
      * ``columns`` — for TABLE / METRIC_VIEW kinds, the subset of
        columns the LLM intends the repair to touch (often a small
        top-K). Empty tuple is allowed for COLUMN kind (the column
        itself is the identifier).
    """

    asset_kind: AssetKind
    identifier: str
    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.identifier or not self.identifier.strip():
            raise ValueError(
                "TargetObject.identifier must be non-empty"
            )

    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "asset_kind": self.asset_kind.value,
            "identifier": self.identifier,
            "columns": list(self.columns),
        }

    @classmethod
    def from_json(cls, payload: dict) -> "TargetObject":  # type: ignore[override]
        return cls(
            asset_kind=AssetKind(payload["asset_kind"]),
            identifier=str(payload["identifier"]),
            columns=tuple(str(c) for c in payload.get("columns") or ()),
        )
