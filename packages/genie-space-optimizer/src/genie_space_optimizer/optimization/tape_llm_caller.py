"""Tape-backed implementation of the LLMCallerOverride Protocol.

``TapeCallContext`` holds the tape plus mutable per-call binding state
(iteration index, optional ag_id / cluster_id) that the lever-loop
driver updates as it advances. ``TapeBackedLLMCaller`` is the
Protocol-conforming object the ContextVar receives.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from genie_space_optimizer.optimization.tape import (
    LeverLoopTape,
    TapeMissError,
)


# Phase 3.6 (2026-05-18) — pre-loop space-setup helpers that may fire
# under replay but are NEVER present in historic tapes (they are
# ``_traced_llm_call`` sites added AFTER the captured runs ran).
# Replaying them as no-ops keeps the lever loop on its postmortem
# trajectory; the lever-loop body either ignores their failure
# (already wrapped in try/except in optimizer.py) or proceeds as if
# they returned an empty payload. The allowlist is short, explicit,
# and documented in ``docs/architecture/tape-replay-protocol.md``.
PRE_LOOP_HELPER_STAGES_ALLOWLIST: frozenset[str] = frozenset({
    "generate_sample_questions",
    "generate_space_description",
    "generate_proactive_instructions",
})


@dataclass
class _Binding:
    """Mutable per-call context tracked across nested LLM invocations."""

    iteration: int = 0
    ag_id: str = ""
    cluster_id: str = ""


class TapeBackedLLMCaller:
    """LLMCallerOverride implementation that resolves calls against a tape.

    Lookups consult ``ctx.binding`` for ``iteration``, ``ag_id``, and
    ``cluster_id``. The harness rebinds these as it advances through
    iterations and AGs (see ``LeverLoopReplayHarness``).
    """

    def __init__(
        self,
        tape: LeverLoopTape,
        binding: _Binding,
        *,
        miss_allowlist: frozenset[str] = frozenset(),
    ):
        self._tape = tape
        self._binding = binding
        self._miss_allowlist = miss_allowlist

    def call(
        self,
        *,
        w: Any,
        system_msg: str,
        prompt: str,
        span_name: str,
        max_retries: int,
        temperature: float,
        max_tokens: int | None,
        response_validator: Callable[[str], Any] | None,
        response_format: dict[str, Any] | None,
        response_model: type | None,
    ) -> tuple[str, Any]:
        try:
            entry = self._tape.lookup(
                stage=span_name,
                iteration=self._binding.iteration,
                ag_id=self._binding.ag_id,
                cluster_id=self._binding.cluster_id,
                prompt=prompt,
            )
        except TapeMissError:
            # Phase 3.6 (2026-05-18) — allowlist of pre-loop helpers
            # whose ``_traced_llm_call`` sites postdate the captured
            # tape. Return an empty response so the lever loop keeps
            # its postmortem trajectory (callers already wrap these
            # in try/except — see ``_generate_proactive_instructions``
            # at ``optimizer.py:4253``).
            if span_name in self._miss_allowlist:
                return "", {"tape_metadata": {"replay_no_op": True}}
            raise

        text = entry.response_text

        if response_validator is not None:
            response_validator(text)

        return text, {"tape_metadata": dict(entry.response_metadata)}


@dataclass
class TapeCallContext:
    """Aggregates a tape with mutable per-call binding state.

    The replay harness updates binding fields as the lever loop advances
    iterations and AGs. ``caller()`` returns a Protocol-conforming
    object that the ContextVar receives.
    """

    tape: LeverLoopTape
    binding: _Binding = field(default_factory=_Binding)
    miss_allowlist: frozenset[str] = field(
        default_factory=lambda: PRE_LOOP_HELPER_STAGES_ALLOWLIST,
    )

    def set_iteration(self, iteration: int) -> None:
        self.binding.iteration = int(iteration)
        self.binding.ag_id = ""
        self.binding.cluster_id = ""

    def bind_ag(self, ag_id: str, *, cluster_id: str = "") -> None:
        self.binding.ag_id = str(ag_id or "")
        self.binding.cluster_id = str(cluster_id or "")

    def clear_ag(self) -> None:
        self.binding.ag_id = ""
        self.binding.cluster_id = ""

    def caller(self) -> TapeBackedLLMCaller:
        return TapeBackedLLMCaller(
            self.tape,
            self.binding,
            miss_allowlist=self.miss_allowlist,
        )
