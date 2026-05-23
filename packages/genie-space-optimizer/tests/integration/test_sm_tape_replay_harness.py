"""PR-B — unit tests for the SM tape replay harness primitives.

Distinct from ``test_sm_tape_replay_dc89d1a9.py`` (which is the
end-to-end SM-driven acceptance test). This file pins the harness
itself: tape loading, cursor model, exhaustion error, response /
exception construction. These are the contract every future tape
relies on.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)
from tests.integration.sm_tape_replay import (
    TapeEntry,
    TapeExhaustedError,
    TapeReplayHarness,
    load_tape,
)


_DUMMY_RESULT_CLS = type("X", (), {})


def _req(skill_id: str, call_id: str = "c") -> LlmReasoningRequest:
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=skill_id,
        system_msg="sys",
        user_prompt="user",
        result_cls=_DUMMY_RESULT_CLS,
        max_tokens=100,
    )


# ── load_tape ────────────────────────────────────────────────────────


def test_load_tape_returns_empty_for_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "empty.jsonl"
    p.write_text("")
    assert load_tape(p) == []


def test_load_tape_parses_jsonl_lines_in_order(tmp_path: Path) -> None:
    p = tmp_path / "two.jsonl"
    p.write_text(
        json.dumps({
            "kind": "exception",
            "skill_id": "plan11_diagnose",
            "exception_class": "BadRequestError",
            "exception_message": "first",
        })
        + "\n"
        + json.dumps({
            "kind": "exception",
            "skill_id": "plan11_diagnose",
            "exception_class": "BadRequestError",
            "exception_message": "second",
        })
        + "\n"
    )
    entries = load_tape(p)
    assert [e.exception_message for e in entries] == ["first", "second"]


def test_dc89d1a9_fixture_loads_without_error() -> None:
    """The committed fixture must always parse — it is the canonical
    seed tape for PR-B and breaking it silently strands the replay
    workflow."""
    tape_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures" / "sm_tapes" / "dc89d1a9.jsonl"
    )
    entries = load_tape(tape_path)
    assert all(e.skill_id == "plan11_diagnose" for e in entries)
    assert all(e.kind == "exception" for e in entries)
    assert all(e.exception_class == "BadRequestError" for e in entries)
    assert len(entries) >= 1


# ── cursor model ─────────────────────────────────────────────────────


def test_harness_consumes_entries_in_capture_order() -> None:
    tape = [
        TapeEntry(kind="exception", skill_id="plan11_diagnose", call_id="c1",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="A"),
        TapeEntry(kind="exception", skill_id="plan11_diagnose", call_id="c2",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="B"),
    ]
    h = TapeReplayHarness(tape=tape)
    r1 = h._invoke(w=None, request=_req("plan11_diagnose"))
    r2 = h._invoke(w=None, request=_req("plan11_diagnose"))
    assert r1.error.endswith("A")
    assert r2.error.endswith("B")
    assert h.consumed_count == 2
    assert h.unconsumed() == []


def test_harness_routes_by_skill_id() -> None:
    """A tape carrying mixed skills must hand each request the next
    entry for its own skill, not the next entry overall."""
    tape = [
        TapeEntry(kind="exception", skill_id="plan11_diagnose",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="DIAGNOSE-A"),
        TapeEntry(kind="exception", skill_id="plan11_synthesize",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="SYNTH-A"),
        TapeEntry(kind="exception", skill_id="plan11_diagnose",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="DIAGNOSE-B"),
    ]
    h = TapeReplayHarness(tape=tape)
    r1 = h._invoke(w=None, request=_req("plan11_diagnose"))
    r2 = h._invoke(w=None, request=_req("plan11_diagnose"))
    r3 = h._invoke(w=None, request=_req("plan11_synthesize"))
    assert r1.error.endswith("DIAGNOSE-A")
    assert r2.error.endswith("DIAGNOSE-B")
    assert r3.error.endswith("SYNTH-A")


def test_harness_raises_tape_exhausted_when_skill_runs_out() -> None:
    """Drifting from the recorded trajectory must surface as a typed
    error, not a silent fallthrough — that's the whole point of the
    cursor model."""
    h = TapeReplayHarness(tape=[
        TapeEntry(kind="exception", skill_id="plan11_diagnose",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="only"),
    ])
    h._invoke(w=None, request=_req("plan11_diagnose"))
    with pytest.raises(TapeExhaustedError, match="plan11_diagnose"):
        h._invoke(w=None, request=_req("plan11_diagnose"))


# ── response / exception construction ────────────────────────────────


def test_exception_entry_returns_typed_failure_response() -> None:
    h = TapeReplayHarness(tape=[
        TapeEntry(
            kind="exception", skill_id="plan11_diagnose", iteration=1,
            duration_ms=4904,
            exception_class="BadRequestError",
            exception_message="Error code: 400 - body",
        ),
    ])
    resp = h._invoke(w=None, request=_req("plan11_diagnose"))
    assert resp.succeeded is False
    assert resp.parsed_output is None
    assert resp.declined is None
    assert resp.tokens_input == 0
    assert resp.duration_ms == 4904
    # Error string matches what LlmReasoningCall.invoke emits — the
    # classifier in stages.diagnose splits on the first colon to peel
    # off the class name, so the harness must mirror that exact format.
    assert resp.error == "BadRequestError: Error code: 400 - body"


def test_exception_entry_does_not_double_prefix_class_name() -> None:
    """PR-C: ``_format_provider_error`` produces strings already
    prefixed with the class name. Tapes captured after PR-C carry that
    full prefixed form in ``exception_message``, so the harness must
    not re-prefix — otherwise we'd emit ``"BadRequestError:
    BadRequestError: body=..."``."""
    h = TapeReplayHarness(tape=[
        TapeEntry(
            kind="exception", skill_id="plan11_diagnose", iteration=1,
            duration_ms=4904,
            exception_class="BadRequestError",
            exception_message=(
                "BadRequestError: body={'error': {'message': "
                "'Invalid response_format'}} | str=Error code: 400"
            ),
        ),
    ])
    resp = h._invoke(w=None, request=_req("plan11_diagnose"))
    assert resp.error.startswith("BadRequestError: body="), resp.error
    assert resp.error.count("BadRequestError:") == 1, (
        f"Class name double-prefixed: {resp.error!r}"
    )


def test_response_entry_returns_succeeded_when_parsed_output_present() -> None:
    h = TapeReplayHarness(tape=[
        TapeEntry(
            kind="response", skill_id="plan11_diagnose", iteration=1,
            parsed_output={"diagnoses": [{"qid": "x"}]},
            raw_text="{...}",
            tokens_input=200, tokens_output=80, duration_ms=900,
        ),
    ])
    resp = h._invoke(w=None, request=_req("plan11_diagnose"))
    assert resp.succeeded is True
    assert resp.parsed_output == {"diagnoses": [{"qid": "x"}]}
    assert resp.tokens_input == 200
    assert resp.tokens_output == 80


def test_invocations_records_every_call() -> None:
    h = TapeReplayHarness(tape=[
        TapeEntry(kind="exception", skill_id="plan11_diagnose",
                  iteration=1, exception_class="BadRequestError",
                  exception_message="x"),
        TapeEntry(kind="response", skill_id="plan11_diagnose",
                  iteration=1, parsed_output={"diagnoses": []}),
    ])
    h._invoke(w=None, request=_req("plan11_diagnose", call_id="first"))
    h._invoke(w=None, request=_req("plan11_diagnose", call_id="second"))
    assert h.invocations == [
        {"skill_id": "plan11_diagnose", "call_id": "first",
         "kind": "exception"},
        {"skill_id": "plan11_diagnose", "call_id": "second",
         "kind": "response"},
    ]


# ── unknown kind ─────────────────────────────────────────────────────


def test_unknown_kind_raises_value_error() -> None:
    h = TapeReplayHarness(tape=[
        TapeEntry(kind="garbled", skill_id="plan11_diagnose", iteration=1),
    ])
    with pytest.raises(ValueError, match="Unknown tape entry kind"):
        h._invoke(w=None, request=_req("plan11_diagnose"))
