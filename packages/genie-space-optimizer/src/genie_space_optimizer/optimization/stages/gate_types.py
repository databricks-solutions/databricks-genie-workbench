"""RCO-4 — typed input/output dataclasses for the three Stage-6 pure
helpers extracted out of harness.py.

Three pairs:
  * BlastRadiusProductionInput / BlastRadiusProductionOutcome
  * NarrowReplacementInput / NarrowReplacementOutcome
  * ApplyabilityGateInput / ApplyabilityGateOutcome

All six dataclasses are frozen and JSON-roundtrippable so fixture
pairs can serialize them cleanly. Lives in its own module to keep
``stages/gates.py`` under the hold-in-context threshold.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True)
class BlastRadiusProductionInput(JsonRoundTrip):
    ag_id: str
    ag_target_qids: tuple[str, ...]
    live_hard_qids: tuple[str, ...]
    max_outside_target: int
    patches: tuple[dict[str, Any], ...]

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "ag_id": self.ag_id,
            "ag_target_qids": list(self.ag_target_qids),
            "live_hard_qids": list(self.live_hard_qids),
            "max_outside_target": int(self.max_outside_target),
            "patches": [dict(p) for p in self.patches],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "BlastRadiusProductionInput":  # type: ignore[override]
        return cls(
            ag_id=str(payload.get("ag_id") or ""),
            ag_target_qids=tuple(str(q) for q in (payload.get("ag_target_qids") or [])),
            live_hard_qids=tuple(str(q) for q in (payload.get("live_hard_qids") or [])),
            max_outside_target=int(payload.get("max_outside_target") or 0),
            patches=tuple(dict(p) for p in (payload.get("patches") or [])),
        )


@dataclass(frozen=True)
class BlastRadiusProductionOutcome(JsonRoundTrip):
    kept: tuple[dict[str, Any], ...] = ()
    dropped: tuple[dict[str, Any], ...] = ()

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "kept": [dict(p) for p in self.kept],
            "dropped": [dict(p) for p in self.dropped],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "BlastRadiusProductionOutcome":  # type: ignore[override]
        return cls(
            kept=tuple(dict(p) for p in (payload.get("kept") or [])),
            dropped=tuple(dict(p) for p in (payload.get("dropped") or [])),
        )


@dataclass(frozen=True)
class NarrowReplacementInput(JsonRoundTrip):
    ag_id: str
    ag_rca_id: str
    ag_target_qids: tuple[str, ...]
    ag_root_cause: str
    blast_dropped: tuple[dict[str, Any], ...]
    qid_to_question_text: dict[str, str] = field(default_factory=dict)
    qid_to_reference_sql: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "ag_id": self.ag_id,
            "ag_rca_id": self.ag_rca_id,
            "ag_target_qids": list(self.ag_target_qids),
            "ag_root_cause": self.ag_root_cause,
            "blast_dropped": [dict(d) for d in self.blast_dropped],
            "qid_to_question_text": dict(self.qid_to_question_text),
            "qid_to_reference_sql": dict(self.qid_to_reference_sql),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "NarrowReplacementInput":  # type: ignore[override]
        return cls(
            ag_id=str(payload.get("ag_id") or ""),
            ag_rca_id=str(payload.get("ag_rca_id") or ""),
            ag_target_qids=tuple(
                str(q) for q in (payload.get("ag_target_qids") or [])
            ),
            ag_root_cause=str(payload.get("ag_root_cause") or ""),
            blast_dropped=tuple(
                dict(d) for d in (payload.get("blast_dropped") or [])
            ),
            qid_to_question_text=dict(payload.get("qid_to_question_text") or {}),
            qid_to_reference_sql=dict(payload.get("qid_to_reference_sql") or {}),
        )


@dataclass(frozen=True)
class NarrowReplacementOutcome(JsonRoundTrip):
    narrow_survivors: tuple[dict[str, Any], ...] = ()
    structural_causal_dropped: tuple[dict[str, Any], ...] = ()
    halt_no_structural_alternative: bool = False

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "narrow_survivors": [dict(s) for s in self.narrow_survivors],
            "structural_causal_dropped": [
                dict(d) for d in self.structural_causal_dropped
            ],
            "halt_no_structural_alternative": bool(self.halt_no_structural_alternative),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "NarrowReplacementOutcome":  # type: ignore[override]
        return cls(
            narrow_survivors=tuple(
                dict(s) for s in (payload.get("narrow_survivors") or [])
            ),
            structural_causal_dropped=tuple(
                dict(d) for d in (payload.get("structural_causal_dropped") or [])
            ),
            halt_no_structural_alternative=bool(
                payload.get("halt_no_structural_alternative") or False
            ),
        )


@dataclass(frozen=True)
class ApplyabilityGateInput(JsonRoundTrip):
    candidates: tuple[dict[str, Any], ...]
    metadata_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "candidates": [dict(c) for c in self.candidates],
            "metadata_snapshot": dict(self.metadata_snapshot),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ApplyabilityGateInput":  # type: ignore[override]
        return cls(
            candidates=tuple(dict(c) for c in (payload.get("candidates") or [])),
            metadata_snapshot=dict(payload.get("metadata_snapshot") or {}),
        )


@dataclass(frozen=True)
class ApplyabilityGateOutcome(JsonRoundTrip):
    applyable: tuple[dict[str, Any], ...] = ()
    rejected: tuple[dict[str, Any], ...] = ()

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "applyable": [dict(c) for c in self.applyable],
            "rejected": [dict(d) for d in self.rejected],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ApplyabilityGateOutcome":  # type: ignore[override]
        return cls(
            applyable=tuple(dict(c) for c in (payload.get("applyable") or [])),
            rejected=tuple(dict(d) for d in (payload.get("rejected") or [])),
        )
