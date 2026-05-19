"""Plan 4 — typed LlmCluster carrier + helpers.

Three public symbols:

  * ``LlmCluster`` — frozen+slots+JsonRoundTrip dataclass with the
    framework-stamped ``cluster_id`` plus the six LLM-emitted fields.
  * ``LlmCluster.to_legacy_dict()`` — projects to the dict shape the
    legacy ``cluster_failures`` returns (byte-stable downstream
    consumers).
  * ``LlmCluster.from_llm_output(pydantic_inst, cluster_id)`` —
    bridge from the Pydantic per-cluster instance to the dataclass,
    stamping ``cluster_id``.
  * ``ClusterValidationError`` — raised by the validators in
    ``cluster_llm.py`` when an LLM cluster fails post-hoc rules.

Unidirectional: depends on ``repair_intent`` only — no imports from
``cluster_llm`` or ``optimizer``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


class ClusterValidationError(ValueError):
    """An LLM-emitted cluster violated a deterministic post-hoc rule.

    The driver catches this exception per-cluster, rejects that
    cluster, and routes its qids back through the deterministic
    fallback path.
    """


@dataclass(frozen=True, slots=True)
class LlmCluster(JsonRoundTrip):
    """Typed semantic cluster — wire-stable carrier through
    clustering code."""

    cluster_id: str
    semantic_theme: str
    member_qids: tuple[str, ...]
    unifying_evidence: str
    suggested_repair_shape: RepairShape
    primary_blame_set: tuple[str, ...]
    confidence: Literal["high", "medium", "low"]

    @classmethod
    def from_llm_output(
        cls,
        pydantic_inst: Any,  # LlmClusterOutput; Any avoids cycles
        *,
        cluster_id: str,
    ) -> "LlmCluster":
        """Bridge from Pydantic LlmClusterOutput to the dataclass.

        Stamps cluster_id (framework-deterministic; the LLM never
        mints cluster IDs). Converts sequence fields to tuples for
        the frozen dataclass.
        """
        return cls(
            cluster_id=str(cluster_id),
            semantic_theme=str(pydantic_inst.semantic_theme),
            member_qids=tuple(str(q) for q in pydantic_inst.member_qids),
            unifying_evidence=str(pydantic_inst.unifying_evidence),
            suggested_repair_shape=RepairShape(
                pydantic_inst.suggested_repair_shape
            ),
            primary_blame_set=tuple(
                str(b) for b in pydantic_inst.primary_blame_set
            ),
            confidence=pydantic_inst.confidence,
        )

    def to_legacy_dict(
        self,
        *,
        signal_type: str = "hard",
    ) -> dict[str, Any]:
        """Project to the dict shape ``cluster_failures`` returns.

        Existing downstream consumers (repair_planner,
        lever_rotation, cluster_driven_synthesis, etc.) read this
        dict shape; new consumers can read the typed ``LlmCluster``
        directly when wired up in Plan 5+. The legacy dict carries
        the new fields ``semantic_theme`` and
        ``suggested_repair_shape`` as additional keys — existing
        consumers that don't read them are unaffected.
        """
        return {
            "cluster_id": self.cluster_id,
            "question_ids": list(self.member_qids),
            "asi_blame_set": list(self.primary_blame_set),
            "asi_blame_set_normalized": list(self.primary_blame_set),
            "root_cause": self.semantic_theme,
            "asi_failure_type": self.suggested_repair_shape.value,
            "failure_keys": [
                self.semantic_theme,
                self.suggested_repair_shape.value,
            ],
            "semantic_theme": self.semantic_theme,
            "suggested_repair_shape": self.suggested_repair_shape.value,
            "llm_confidence": self.confidence,
            "llm_rationale": self.unifying_evidence,
            "source": "llm",
            "signal_type": signal_type,
        }
