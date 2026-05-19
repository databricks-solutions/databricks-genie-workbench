"""Plan 8 Task 11 — ReasoningSkillMetadata no longer carries the
model_override field; one system-wide LLM_MODEL is the single knob."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.skills._loader import ReasoningSkillMetadata
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)


def test_reasoning_skill_metadata_has_no_model_override():
    fields = {f.name for f in dataclasses.fields(ReasoningSkillMetadata)}
    assert "model_override" not in fields, (
        "Plan 8 Task 11 — model_override field removed"
    )


def test_llm_reasoning_request_has_no_model_override():
    fields = {f.name for f in dataclasses.fields(LlmReasoningRequest)}
    assert "model_override" not in fields


def test_model_override_scope_does_not_exist():
    from genie_space_optimizer.optimization import llm_reasoning_call
    assert not hasattr(llm_reasoning_call, "_model_override_scope")
