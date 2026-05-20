"""Plan 11 repair loop — LLM output contract (one revised proposal)."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class Plan11RepairOutput(LLMOutputContract):
    """LLMOutputContract for plan11_repair skill. One revised proposal."""

    intent_name: str = Field(max_length=80)
    intent_description: str
    repair_hypothesis: str
    patch_type: str
    rationale: str
    confidence: Literal["high", "medium", "low"]
    patch_body: dict[str, Any]
    blame_set: list[str] = Field(default_factory=list)
    target_qids: list[str] = Field(default_factory=list)
