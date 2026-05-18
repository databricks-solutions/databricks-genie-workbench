"""Unit tests for TapeBackedLLMCaller."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.tape import (
    LeverLoopTape,
    TapeEntry,
    TapeKey,
    TapeMissError,
    prompt_sha256,
)
from genie_space_optimizer.optimization.tape_llm_caller import (
    PRE_LOOP_HELPER_STAGES_ALLOWLIST,
    TapeCallContext,
)


def _tape_with_one_entry(*, stage: str, iteration: int, prompt: str, response: str):
    return LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage=stage, iteration=iteration,
                    ag_id="", cluster_id="",
                    prompt_sha256=prompt_sha256(prompt),
                ),
                prompt=prompt,
                response_text=response,
                response_metadata={"model": "claude-sonnet-4-6"},
            )
        ],
    )


def test_caller_returns_recorded_response_for_exact_key():
    tape = _tape_with_one_entry(
        stage="adaptive_strategy", iteration=2,
        prompt="hello", response="world",
    )
    ctx = TapeCallContext(tape=tape)
    ctx.set_iteration(2)

    text, resp = ctx.caller().call(
        w=None,
        system_msg="sys",
        prompt="hello",
        span_name="adaptive_strategy",
        max_retries=3,
        temperature=0.0,
        max_tokens=None,
        response_validator=None,
        response_format=None,
        response_model=None,
    )

    assert text == "world"
    assert resp == {"tape_metadata": {"model": "claude-sonnet-4-6"}}


def test_caller_raises_on_miss_under_default_policy():
    tape = _tape_with_one_entry(
        stage="adaptive_strategy", iteration=2,
        prompt="hello", response="world",
    )
    ctx = TapeCallContext(tape=tape)
    ctx.set_iteration(2)

    with pytest.raises(TapeMissError):
        ctx.caller().call(
            w=None,
            system_msg="sys",
            prompt="MISMATCHED PROMPT",
            span_name="adaptive_strategy",
            max_retries=3,
            temperature=0.0,
            max_tokens=None,
            response_validator=None,
            response_format=None,
            response_model=None,
        )


def test_caller_routes_ag_and_cluster_ids_via_context():
    """When the harness binds ag_id/cluster_id to the context, those
    become part of the lookup key."""
    tape = LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage="cluster_driven_synthesis", iteration=3,
                    ag_id="AG_007", cluster_id="cluster_airline_gs_009",
                    prompt_sha256=prompt_sha256("synth prompt"),
                ),
                prompt="synth prompt",
                response_text="synth response",
                response_metadata={},
            )
        ],
    )
    ctx = TapeCallContext(tape=tape)
    ctx.set_iteration(3)
    ctx.bind_ag("AG_007", cluster_id="cluster_airline_gs_009")

    text, _ = ctx.caller().call(
        w=None,
        system_msg="",
        prompt="synth prompt",
        span_name="cluster_driven_synthesis",
        max_retries=3,
        temperature=0.0,
        max_tokens=None,
        response_validator=None,
        response_format=None,
        response_model=None,
    )
    assert text == "synth response"


def test_pre_loop_helper_miss_returns_empty_instead_of_raising():
    """Phase 3.6 (2026-05-18) — pre-loop helper stages whose
    ``_traced_llm_call`` sites were added AFTER the historic tapes
    were captured return an empty payload instead of raising
    TapeMissError. The captured tape literally cannot serve these
    calls; the lever loop's call sites wrap them in try/except and
    treat empty as a benign no-op."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
    )
    ctx = TapeCallContext(tape=tape)
    # The default allowlist must include the canonical names.
    assert "generate_sample_questions" in PRE_LOOP_HELPER_STAGES_ALLOWLIST
    assert "generate_proactive_instructions" in PRE_LOOP_HELPER_STAGES_ALLOWLIST

    text, resp = ctx.caller().call(
        w=None,
        system_msg="",
        prompt="anything",
        span_name="generate_proactive_instructions",
        max_retries=3,
        temperature=0.0,
        max_tokens=None,
        response_validator=None,
        response_format=None,
        response_model=None,
    )
    # Phase 3.6.1 D1 (2026-05-18) — "{}" instead of "" so JSON-path
    # callers parse cleanly to {} and don't NoneType-crash.
    assert text == "{}"
    assert resp == {"tape_metadata": {"replay_no_op": True}}


def test_pre_loop_helper_miss_payload_is_parseable_json():
    """Phase 3.6.1 D1 — the empty-payload return must be valid JSON
    so JSON-parsing callers (e.g. ``_generate_sample_questions`` at
    optimizer.py:4569) parse it cleanly to ``{}`` rather than
    receiving ``None`` from a JSON parse of ``""`` and then
    NoneType-crashing on ``.get(...)``."""
    import json as _json

    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
    )
    ctx = TapeCallContext(tape=tape)
    text, _ = ctx.caller().call(
        w=None,
        system_msg="",
        prompt="anything",
        span_name="generate_sample_questions",
        max_retries=3,
        temperature=0.0,
        max_tokens=None,
        response_validator=None,
        response_format=None,
        response_model=None,
    )
    parsed = _json.loads(text)
    assert parsed == {}
    # The .get(...)-on-empty-dict pattern callers use must work:
    assert parsed.get("questions", []) == []
    assert parsed.get("instructions", []) == []


def test_non_allowlisted_miss_still_raises():
    """Allowlist is surgical: only the listed pre-loop helpers no-op
    on miss. Every other unmatched call still raises so genuine
    tape coverage gaps are caught."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
    )
    ctx = TapeCallContext(tape=tape)
    with pytest.raises(TapeMissError):
        ctx.caller().call(
            w=None,
            system_msg="",
            prompt="anything",
            span_name="stage_1_discovery",  # NOT in the allowlist
            max_retries=3,
            temperature=0.0,
            max_tokens=None,
            response_validator=None,
            response_format=None,
            response_model=None,
        )


def test_caller_passes_through_response_validator():
    """The validator must be applied to the recorded text so prompt-
    output drift surfaces as a tape-replay failure, not a silent pass."""
    tape = _tape_with_one_entry(
        stage="adaptive_strategy", iteration=1,
        prompt="p", response="not-json",
    )
    ctx = TapeCallContext(tape=tape)
    ctx.set_iteration(1)

    def _validator(text: str) -> None:
        if text != "expected-json":
            raise ValueError(f"validator reject: {text!r}")

    with pytest.raises(ValueError, match="validator reject"):
        ctx.caller().call(
            w=None,
            system_msg="",
            prompt="p",
            span_name="adaptive_strategy",
            max_retries=3,
            temperature=0.0,
            max_tokens=None,
            response_validator=_validator,
            response_format=None,
            response_model=None,
        )
