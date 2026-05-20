"""Plan 11 Stage 2 — LLM output contract for failure clustering.

Free-text ``repair_hypothesis`` replaces the closed ``RepairShape`` enum
that the legacy ``failure_clustering`` skill used.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class ClusterItem(LLMOutputContract):
    semantic_theme: str
    member_qids: list[str]
    unifying_evidence: str = Field(max_length=400)
    repair_hypothesis: str = Field(max_length=300)
    primary_blame_set: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"]


class Plan11ClusterOutput(LLMOutputContract):
    """LLMOutputContract for plan11_cluster skill."""

    clusters: list[ClusterItem]
