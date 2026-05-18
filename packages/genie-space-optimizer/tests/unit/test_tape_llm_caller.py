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
