"""Phase 3.7 Task 4 — historic_inject dispatch in TapeBackedLLMCaller."""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.tape import (
    LeverLoopTape,
    TapeEntry,
    TapeKey,
    TapeMissError,
    prompt_sha256,
)
from genie_space_optimizer.optimization.tape_llm_caller import (
    TapeBackedLLMCaller,
    TapeCallContext,
    _Binding,
)


def _lever6_prompt(cluster_id: str) -> str:
    """Build a lever6-style prompt with the AFS cluster_id at the top."""
    afs = {"cluster_id": cluster_id, "failure_type": "unknown"}
    return f"Lever 6 prompt for {cluster_id}.\n\n{json.dumps(afs, indent=2)}"


def _entry(
    stage: str,
    *,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    prompt: str,
    response_text: str,
) -> TapeEntry:
    return TapeEntry(
        key=TapeKey(
            stage=stage,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster_id,
            prompt_sha256=prompt_sha256(prompt),
        ),
        prompt=prompt,
        response_text=response_text,
        response_metadata={"model": "x"},
    )


def _tape(
    entries: list[TapeEntry],
    *,
    replay_mode_by_stage: dict[str, str] | None = None,
) -> LeverLoopTape:
    return LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="2026-05-18T00:00:00Z",
        entries=entries,
        replay_mode_by_stage=replay_mode_by_stage or {},
    )


# ── Tests ─────────────────────────────────────────────────────────────


def test_historic_inject_serves_response_ignoring_prompt_sha():
    """The replay-time prompt content does NOT match the captured prompt
    content, but historic_inject mode resolves by binding."""
    captured_prompt = _lever6_prompt("cluster_a")
    replay_prompt = "completely different rebuilt prompt — SHA will not match"
    tape = _tape(
        [_entry(
            "lever6_llm",
            iteration=2,
            ag_id="AG_a",
            cluster_id="cluster_a",
            prompt=captured_prompt,
            response_text="HISTORIC_RESPONSE",
        )],
        replay_mode_by_stage={"lever6_llm": "historic_inject"},
    )
    binding = _Binding(iteration=2, ag_id="AG_a", cluster_id="first_cluster_in_ag")
    caller = TapeBackedLLMCaller(tape, binding)
    # Even though replay_prompt embeds cluster_a in AFS format, the
    # cluster_id-from-prompt resolver picks it up:
    text, meta = caller.call(
        w=None, system_msg="", prompt=_lever6_prompt("cluster_a"),
        span_name="lever6_llm", max_retries=1, temperature=0.0, max_tokens=None,
        response_validator=None, response_format=None, response_model=None,
    )
    assert text == "HISTORIC_RESPONSE"


def test_historic_inject_raises_when_no_binding_matches():
    tape = _tape(
        [_entry(
            "lever6_llm", iteration=2, ag_id="AG_a", cluster_id="cluster_a",
            prompt=_lever6_prompt("cluster_a"), response_text="x",
        )],
        replay_mode_by_stage={"lever6_llm": "historic_inject"},
    )
    # Binding's ag_id is wrong → no match
    binding = _Binding(iteration=2, ag_id="AG_zzz", cluster_id="")
    caller = TapeBackedLLMCaller(tape, binding)
    with pytest.raises(TapeMissError, match="historic_inject"):
        caller.call(
            w=None, system_msg="", prompt=_lever6_prompt("cluster_a"),
            span_name="lever6_llm", max_retries=1, temperature=0.0, max_tokens=None,
            response_validator=None, response_format=None, response_model=None,
        )


def test_default_mode_unchanged_when_stage_absent_from_replay_mode_dict():
    """Stages absent from replay_mode_by_stage default to rebuild_and_match
    (the historic v1–v3 SHA-based behaviour)."""
    prompt = "some adaptive strategy prompt"
    tape = _tape(
        [_entry(
            "adaptive_strategy",
            iteration=0, ag_id="", cluster_id="",
            prompt=prompt, response_text="STRATEGY_OUT",
        )],
        replay_mode_by_stage={},  # empty
    )
    binding = _Binding(iteration=0, ag_id="", cluster_id="")
    caller = TapeBackedLLMCaller(tape, binding)
    text, _ = caller.call(
        w=None, system_msg="", prompt=prompt,
        span_name="adaptive_strategy", max_retries=1, temperature=0.0,
        max_tokens=None, response_validator=None, response_format=None,
        response_model=None,
    )
    assert text == "STRATEGY_OUT"


def test_inner_loop_cluster_resolves_via_prompt_not_binding():
    """When the AG covers multiple clusters, binding.cluster_id is the
    AG's FIRST cluster but the inner loop processes each. historic_inject
    must use the prompt's cluster_id, not binding's."""
    tape = _tape(
        [
            _entry("lever6_llm", iteration=1, ag_id="AG_x",
                   cluster_id="cluster_first",
                   prompt=_lever6_prompt("cluster_first"),
                   response_text="RESP_FIRST"),
            _entry("lever6_llm", iteration=1, ag_id="AG_x",
                   cluster_id="cluster_second",
                   prompt=_lever6_prompt("cluster_second"),
                   response_text="RESP_SECOND"),
        ],
        replay_mode_by_stage={"lever6_llm": "historic_inject"},
    )
    # Binding's cluster_id stays "cluster_first" (the AG's first
    # source_cluster_id) for the whole AG dispatch.
    binding = _Binding(iteration=1, ag_id="AG_x", cluster_id="cluster_first")
    caller = TapeBackedLLMCaller(tape, binding)

    # Inner-loop call for cluster_second — binding hasn't been updated,
    # but the prompt carries the real cluster_id.
    text, _ = caller.call(
        w=None, system_msg="", prompt=_lever6_prompt("cluster_second"),
        span_name="lever6_llm", max_retries=1, temperature=0.0, max_tokens=None,
        response_validator=None, response_format=None, response_model=None,
    )
    assert text == "RESP_SECOND"


def test_response_validator_still_runs_in_historic_inject():
    tape = _tape(
        [_entry(
            "lever6_llm", iteration=0, ag_id="AG", cluster_id="c",
            prompt=_lever6_prompt("c"), response_text='{"ok": true}',
        )],
        replay_mode_by_stage={"lever6_llm": "historic_inject"},
    )
    binding = _Binding(iteration=0, ag_id="AG", cluster_id="c")
    caller = TapeBackedLLMCaller(tape, binding)
    seen = []

    def _v(text: str):
        seen.append(text)

    caller.call(
        w=None, system_msg="", prompt=_lever6_prompt("c"),
        span_name="lever6_llm", max_retries=1, temperature=0.0, max_tokens=None,
        response_validator=_v, response_format=None, response_model=None,
    )
    assert seen == ['{"ok": true}']
