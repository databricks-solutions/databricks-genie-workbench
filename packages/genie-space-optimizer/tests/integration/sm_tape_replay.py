"""PR-B — SM tape-replay harness.

Collapses the diagnose loop for Stage 1 LLM failures from a 45-minute
lever-loop trial to a sub-second unit test. Built per the
``stage1-badrequest-diagnostic-instrumentation`` plan after the
2026-05-23 trial postmortems showed three consecutive behavioral fixes
(canonical-row-shape adapter, baseline_eval_rows wiring, SM Cutover
deletion-first) that all left the underlying LLM endpoint failure
untouched.

Architecture
------------
The harness intercepts ``LlmReasoningCall.invoke`` — the single chokepoint
every Plan 11 reasoning stage (``stages.diagnose``, ``stages.synthesize``,
``stages.cluster_plan11``, ``stages.narrow_replacement``,
``stages.candidate_critique``, ``stages.rca_evidence``) routes through.
By replacing ``invoke`` with a tape-replay shim, the SM transformer
graph runs end to end against pre-recorded responses (or exceptions)
without any Databricks network round-trip.

Tape format
-----------
A JSONL file at ``tests/fixtures/sm_tapes/<tape_id>.jsonl`` where each
line is one captured LLM call::

    {
      "kind": "response" | "exception",
      "skill_id": "plan11_diagnose",
      "call_id": "plan11_stage1_diagnose.iter_1",
      "iteration": 1,
      // for kind=response:
      "parsed_output": {...},
      "raw_text": "...",
      "tokens_input": 1234,
      "tokens_output": 567,
      "duration_ms": 4904,
      // for kind=exception:
      "exception_class": "BadRequestError",
      "exception_message": "Error code: 400 - {...}",
      "duration_ms": 4904
    }

The replay harness consumes entries strictly in capture order, filtered
by ``skill_id``. This way a tape recorded from a real run plays back
deterministically through the same transformer chain.

Why ``LlmReasoningCall.invoke`` and not ``_traced_llm_call``
-----------------------------------------------------------
``_traced_llm_call`` returns the raw OpenAI completion object, which
forces the replay tape to carry full HTTP-level fixtures. ``invoke``
returns a typed ``LlmReasoningResponse`` — the shape the SM transformers
actually consume — so the tape stays small and the replay is one-hop.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
    LlmReasoningResponse,
)


@dataclass(frozen=True)
class TapeEntry:
    """One captured LLM call from a real production run.

    ``qid`` is the optional per-entry QID tag that lets the replay
    harness route a request to the correct response even when the SM
    dispatches QIDs in a different order than the tape was authored
    in. When ``qid`` is empty the harness falls back to capture order
    (the legacy behaviour every existing tape relies on). Stock
    factories populate ``qid`` so any test built on them is
    automatically order-resilient — that means an upstream Stage 1
    abstain on a single QID cannot misalign every downstream call.
    """

    kind: str  # "response" or "exception"
    skill_id: str
    call_id: str = ""
    iteration: int = 0
    qid: str = ""
    # Response branch
    parsed_output: dict | None = None
    raw_text: str = ""
    tokens_input: int = 0
    tokens_output: int = 0
    duration_ms: int = 0
    # Exception branch
    exception_class: str = ""
    exception_message: str = ""

    @classmethod
    def from_json(cls, payload: dict) -> "TapeEntry":
        return cls(
            kind=str(payload["kind"]),
            skill_id=str(payload["skill_id"]),
            call_id=str(payload.get("call_id", "")),
            iteration=int(payload.get("iteration", 0)),
            qid=str(payload.get("qid", "") or ""),
            parsed_output=(
                dict(payload["parsed_output"])
                if isinstance(payload.get("parsed_output"), dict)
                else None
            ),
            raw_text=str(payload.get("raw_text") or ""),
            tokens_input=int(payload.get("tokens_input") or 0),
            tokens_output=int(payload.get("tokens_output") or 0),
            duration_ms=int(payload.get("duration_ms") or 0),
            exception_class=str(payload.get("exception_class") or ""),
            exception_message=str(payload.get("exception_message") or ""),
        )


def load_tape(path: Path) -> list[TapeEntry]:
    """Parse a JSONL tape file into ``TapeEntry`` objects in capture order."""
    raw = Path(path).read_text().strip()
    if not raw:
        return []
    return [TapeEntry.from_json(json.loads(line)) for line in raw.splitlines()]


@dataclass
class TapeReplayHarness:
    """Replay a tape against the SM by patching ``LlmReasoningCall.invoke``.

    Usage::

        harness = TapeReplayHarness(tape=load_tape(path))
        with harness.patch():
            final_states = run_state_machine_iteration_and_persist(...)
        assert harness.consumed_count == len(harness.tape)
        assert harness.unconsumed() == []

    Two routing modes share one harness:

    * **Arrival order** (legacy) — when tape entries have no ``qid``,
      the harness consumes entries strictly in capture order, filtered
      by ``skill_id``. Every existing tape uses this mode.
    * **QID-keyed** — when a tape entry has ``qid`` populated, the
      harness only consumes it for a request whose ``call_id``
      mentions that QID (the request's ``call_id`` always ends with
      ``.{qid}`` because every Stage 1/2/3 factory and production
      caller builds call_ids that way). This is order-resilient: an
      upstream Stage 1 abstain on one QID does not cause the rest of
      the SM to consume the wrong response. Fixes the
      ``diagnose_returned_no_matching_qid`` aliasing the workbench
      surfaced when an early QID abort skewed the tape cursor.

    If the SM requests a skill that has no remaining matching tape
    entries, the harness raises ``TapeExhaustedError`` — that is itself
    a diagnostic signal (the replay drifted from the recorded
    trajectory).
    """

    tape: list[TapeEntry]
    _consumed: list[bool] = field(default_factory=list)
    _invocations: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._consumed = [False] * len(self.tape)

    @property
    def consumed_count(self) -> int:
        return sum(1 for c in self._consumed if c)

    def unconsumed(self) -> list[TapeEntry]:
        return [
            entry
            for entry, consumed in zip(self.tape, self._consumed)
            if not consumed
        ]

    @property
    def invocations(self) -> list[dict[str, Any]]:
        """Return the recorded ``LlmReasoningCall.invoke`` calls."""
        return list(self._invocations)

    @staticmethod
    def _request_mentions_qid(request: LlmReasoningRequest, qid: str) -> bool:
        """Return True iff ``request`` is keyed to ``qid``.

        Two routing channels are recognized:

        1. ``call_id`` ends with the QID after a ``.`` separator, e.g.
           ``..diagnose.iter_1.gs_009``. Used by tests that synthesize
           per-QID call_ids.

        2. ``user_prompt`` JSON carries the QID as a value of a top-level
           ``qid``-shaped field (``qid``, ``question_id``, ``failing_qid``,
           or in ``failing_qids[*].qid`` / ``failing_qids[*].question_id``).
           Used by production stages whose ``call_id`` is per-iteration
           (e.g. ``plan11_stage1_diagnose.iter_1``) and whose per-QID
           identity lives only in the payload body. Without this channel
           an upstream abstain on a single QID would silently misalign
           every downstream tape entry — a workbench-only fragility that
           does not exist at the wire boundary.
        """
        if not qid:
            return False
        call_id = getattr(request, "call_id", "") or ""
        if call_id and (call_id.endswith(f".{qid}") or call_id == qid):
            return True
        user_prompt = getattr(request, "user_prompt", "") or ""
        if not user_prompt:
            return False
        # Cheap pre-check: bail out before paying for json.loads on every
        # tape lookup if the qid token is not even a substring.
        if qid not in user_prompt:
            return False
        try:
            payload = json.loads(user_prompt)
        except (ValueError, TypeError):
            return False
        return TapeReplayHarness._payload_mentions_qid(payload, qid)

    @staticmethod
    def _payload_mentions_qid(payload: Any, qid: str) -> bool:
        """Recognise the QID-bearing fields used by Plan 11 user prompts.

        Recognised shapes:
          * ``{"qid": "<qid>"}`` / ``{"question_id": "<qid>"}`` /
            ``{"failing_qid": "<qid>"}`` (top-level scalar)
          * ``{"failing_qids": [{"qid": "<qid>"}, ...]}`` (Stage 1 batched
            shape — production today emits a 1-element list per call).
          * ``{"cluster": {"qids": [...]}}`` and similar grouped shapes —
            handled by recursion through dict values.
        """
        if not isinstance(payload, dict):
            return False
        for key in ("qid", "question_id", "failing_qid"):
            if payload.get(key) == qid:
                return True
        failing = payload.get("failing_qids")
        if isinstance(failing, list):
            for item in failing:
                if isinstance(item, dict) and (
                    item.get("qid") == qid
                    or item.get("question_id") == qid
                ):
                    return True
        return False

    def _next_entry(self, request: LlmReasoningRequest) -> TapeEntry:
        skill_id = request.skill_id
        # First pass: prefer a QID-keyed match if any unconsumed entry
        # for this skill carries a qid that matches the request.
        for idx, entry in enumerate(self.tape):
            if self._consumed[idx] or entry.skill_id != skill_id:
                continue
            if entry.qid and self._request_mentions_qid(request, entry.qid):
                self._consumed[idx] = True
                return entry
        # Second pass: legacy arrival order over remaining qid-less
        # entries for this skill.
        for idx, entry in enumerate(self.tape):
            if self._consumed[idx] or entry.skill_id != skill_id:
                continue
            if not entry.qid:
                self._consumed[idx] = True
                return entry
        # Third pass: if every remaining entry for this skill is
        # QID-keyed but none matches the request, fall back to the
        # first remaining qid-keyed entry. This preserves backward
        # compatibility for tests that authored QID tapes whose
        # call_id conventions diverge from production.
        for idx, entry in enumerate(self.tape):
            if self._consumed[idx] or entry.skill_id != skill_id:
                continue
            self._consumed[idx] = True
            return entry
        raise TapeExhaustedError(
            f"Tape exhausted for skill_id={skill_id!r} "
            f"call_id={getattr(request, 'call_id', '')!r}: every entry "
            f"for this skill is already consumed but the SM requested "
            f"another call. Either the tape under-represents the "
            f"production run (re-capture) or the SM is drifting from "
            f"the recorded trajectory."
        )

    def _invoke(self, *, w: Any, request: LlmReasoningRequest) -> LlmReasoningResponse:
        entry = self._next_entry(request)
        self._invocations.append(
            {
                "skill_id": request.skill_id,
                "call_id": request.call_id,
                "kind": entry.kind,
            }
        )
        if entry.kind == "exception":
            # Mirror what ``LlmReasoningCall.invoke`` writes into
            # ``LlmReasoningResponse.error`` when ``_traced_llm_call``
            # raises. Pre-PR-C that was ``"ClassName: str(exc)"``; PR-C
            # ``_format_provider_error`` produces a richer ``"ClassName:
            # body=... | response_text=... | str=..."`` shape so the
            # diagnose classifier can match the real provider body.
            #
            # Tapes captured before PR-C carry the bare body in
            # ``exception_message`` and need the class prefix added.
            # Tapes captured after PR-C (via the post-PR-A llm_errors
            # dump) already carry the full prefixed form. Detect both.
            err_message = entry.exception_message or "(no message)"
            class_prefix = (
                f"{entry.exception_class}:"
                if entry.exception_class else ""
            )
            already_prefixed = (
                bool(class_prefix)
                and err_message.startswith(class_prefix)
            )
            error_str = (
                err_message if already_prefixed
                else f"{entry.exception_class}: {err_message}"
            )
            return LlmReasoningResponse(
                call_id=request.call_id,
                skill_id=request.skill_id,
                succeeded=False,
                parsed_output=None,
                declined=None,
                raw_text="",
                tokens_input=0,
                tokens_output=0,
                duration_ms=entry.duration_ms,
                error=error_str,
            )
        if entry.kind == "response":
            return LlmReasoningResponse(
                call_id=request.call_id,
                skill_id=request.skill_id,
                succeeded=entry.parsed_output is not None,
                parsed_output=entry.parsed_output,
                declined=None,
                raw_text=entry.raw_text,
                tokens_input=entry.tokens_input,
                tokens_output=entry.tokens_output,
                duration_ms=entry.duration_ms,
                error=None,
            )
        raise ValueError(f"Unknown tape entry kind: {entry.kind!r}")

    def patch(self):
        """Return a context manager that installs the replay invoke shim.

        The patch points at every call site in ``optimization.stages.*``
        that imports ``LlmReasoningCall`` from ``llm_reasoning_call``.
        Each stage module did ``from ... import LlmReasoningCall``, so
        patching the symbol on the source module is not enough — we
        patch the class's ``invoke`` method directly so every call site
        routes through the harness regardless of import order.
        """
        from unittest.mock import patch as _patch

        from genie_space_optimizer.optimization import (
            llm_reasoning_call as lrc_mod,
        )

        return _patch.object(
            lrc_mod.LlmReasoningCall, "invoke", autospec=False,
            new=lambda _self, *, w, request: self._invoke(
                w=w, request=request,
            ),
        )


class TapeExhaustedError(RuntimeError):
    """Raised when the SM requests more LLM calls than the tape recorded.

    A run that exhausts the tape is either (a) under-captured (the tape
    only covers part of the run), or (b) drifting from the recorded
    trajectory (a behavioral change made the SM ask more questions than
    the original run did). Both are diagnostic signals worth surfacing
    rather than papering over with a generic mock.
    """


__all__ = [
    "TapeEntry",
    "TapeReplayHarness",
    "TapeExhaustedError",
    "load_tape",
]
