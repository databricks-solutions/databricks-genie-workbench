"""Plan 11 Stage 3 — LLM output contract for patch synthesis.

The LLM emits a list of RepairProposals (1–3 per cluster). ``patch_type``
is the closed PatchType enum (the applier dispatches on it); ``patch_body``
is free-form per patch_type and validated by the Plan 11 dispatcher
``validate_patch.py``.

Trial 13 Track 4 — ``intent_name`` cap relaxed 5× over Trial 12 (80 →
200) and replaced with a graceful-truncate validator (see
:mod:`plan11_diagnose.output_schema` for the architectural rationale).
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, field_validator

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


_SYNTHESIZE_FIELD_CAPS = {
    "intent_name": 200,
}


def _truncate_with_ellipsis(text: str, cap: int) -> str:
    if len(text) <= cap:
        return text
    return text[: cap - 3] + "..."


class ProposalItem(LLMOutputContract):
    intent_name: str = Field(max_length=_SYNTHESIZE_FIELD_CAPS["intent_name"])
    intent_description: str
    repair_hypothesis: str
    patch_type: str
    rationale: str
    confidence: Literal["high", "medium", "low"]
    patch_body: dict[str, Any]
    blame_set: list[str] = Field(default_factory=list)
    target_qids: list[str] = Field(default_factory=list)
    # ── Trial 17 — Lever Selection Contract ─────────────────────────
    # Per-proposal lever declaration. ``selected_lever`` MUST be in
    # the closed lever_id set (``lever-1`` … ``lever-6``) and MUST be
    # consistent with ``patch_type`` according to ``LEVER_TO_PATCH_TYPES``
    # in ``levers_contract.py``. The deterministic validator in
    # ``synthesize.py`` drops inconsistent proposals and emits a typed
    # ``forbidden_signature`` the next iteration's LLM sees.
    # Defaults to empty string for backward compatibility with
    # pre-Trial-17 prompts.
    selected_lever: str = ""
    expected_behavioral_change: str = ""
    fallback_lever: str = ""
    bundle_id: str = ""

    @field_validator(*_SYNTHESIZE_FIELD_CAPS.keys(), mode="before")
    @classmethod
    def _truncate_oversize_field(cls, v, info):
        if not isinstance(v, str):
            return v
        cap = _SYNTHESIZE_FIELD_CAPS.get(info.field_name)
        if cap is None:
            return v
        return _truncate_with_ellipsis(v, cap)


class Plan11SynthesizeOutput(LLMOutputContract):
    """LLMOutputContract for plan11_synthesize skill."""

    proposals: list[ProposalItem]
