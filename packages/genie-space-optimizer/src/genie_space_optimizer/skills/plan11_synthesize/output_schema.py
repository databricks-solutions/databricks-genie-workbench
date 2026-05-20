"""Plan 11 Stage 3 — LLM output contract for patch synthesis.

The LLM emits a list of RepairProposals (1–3 per cluster). ``patch_type``
is the closed PatchType enum (the applier dispatches on it); ``patch_body``
is free-form per patch_type and validated by the Plan 11 dispatcher
``validate_patch.py``.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class ProposalItem(LLMOutputContract):
    intent_name: str = Field(max_length=80)
    intent_description: str
    repair_hypothesis: str
    patch_type: str
    rationale: str
    confidence: Literal["high", "medium", "low"]
    patch_body: dict[str, Any]
    blame_set: list[str] = Field(default_factory=list)
    target_qids: list[str] = Field(default_factory=list)


class Plan11SynthesizeOutput(LLMOutputContract):
    """LLMOutputContract for plan11_synthesize skill."""

    proposals: list[ProposalItem]
