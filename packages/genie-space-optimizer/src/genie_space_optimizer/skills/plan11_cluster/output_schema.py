"""Plan 11 Stage 2 — LLM output contract for failure clustering.

Free-text ``repair_hypothesis`` replaces the closed ``RepairShape`` enum
that the legacy ``failure_clustering`` skill used.

Trial 13 Track 4 — field caps relaxed 5× over Trial 12 and replaced
with a graceful-truncate validator (see :mod:`plan11_diagnose.output_schema`
for the architectural rationale).
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


_CLUSTER_FIELD_CAPS = {
    "unifying_evidence": 2000,
    "repair_hypothesis": 1500,
}


def _truncate_with_ellipsis(text: str, cap: int) -> str:
    if len(text) <= cap:
        return text
    return text[: cap - 3] + "..."


class ClusterItem(LLMOutputContract):
    semantic_theme: str
    member_qids: list[str]
    unifying_evidence: str = Field(max_length=_CLUSTER_FIELD_CAPS["unifying_evidence"])
    repair_hypothesis: str = Field(max_length=_CLUSTER_FIELD_CAPS["repair_hypothesis"])
    primary_blame_set: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"]

    @field_validator(*_CLUSTER_FIELD_CAPS.keys(), mode="before")
    @classmethod
    def _truncate_oversize_field(cls, v, info):
        if not isinstance(v, str):
            return v
        cap = _CLUSTER_FIELD_CAPS.get(info.field_name)
        if cap is None:
            return v
        return _truncate_with_ellipsis(v, cap)


class Plan11ClusterOutput(LLMOutputContract):
    """LLMOutputContract for plan11_cluster skill."""

    clusters: list[ClusterItem]
