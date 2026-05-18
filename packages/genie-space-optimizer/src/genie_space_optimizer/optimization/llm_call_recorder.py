"""Phase 3.5 (2026-05-17) — production-side LLM call recorder.

The recorder is the capture-time counterpart to Phase 3's
``_LLM_CALLER_OVERRIDE``. Production runs install an
``InMemoryLLMCallRecorder`` at the lever-loop boundary; every LLM call
routed through ``optimizer._traced_llm_call`` appends to it. The
recorder is best-effort: any exception during capture is swallowed at
debug level by the calling site, never crashing the real LLM path.

The ``_RECORDER_BINDING`` ContextVar carries the current
(iteration, ag_id, cluster_id) so the recorder can key each call
without threading those through ``_traced_llm_call``'s signature.
``harness._tape_binding_set_iteration`` and
``harness._tape_binding_set_ag`` are extended (Task 3) to update both
Phase 3's hook *and* this binding in lockstep.
"""
from __future__ import annotations

import hashlib
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any, Protocol


@dataclass(frozen=True)
class RecorderBinding:
    """Per-LLM-call identity for the recorder.

    ``iteration``: 0-indexed lever-loop iteration ordinal. ``-1`` means
    "no iteration bound" (calls outside the lever loop).

    ``ag_id``: dispatch-site AG identifier (empty when the call is not
    AG-scoped, e.g. the Stage-1 discovery call which fires once per
    iteration before AG dispatch).

    ``cluster_id``: optional primary cluster id for the AG (the first
    ``source_cluster_ids`` entry). Empty when not bound.
    """

    iteration: int
    ag_id: str
    cluster_id: str


_RECORDER_BINDING: ContextVar[RecorderBinding] = ContextVar(
    "_RECORDER_BINDING",
    default=RecorderBinding(iteration=-1, ag_id="", cluster_id=""),
)


def set_iteration_binding(iteration: int) -> None:
    """Update only the iteration component of the recorder binding."""
    current = _RECORDER_BINDING.get()
    _RECORDER_BINDING.set(replace(current, iteration=int(iteration)))


def set_ag_binding(ag_id: str, *, cluster_id: str = "") -> None:
    """Update the AG + cluster components of the recorder binding.

    Iteration is preserved — Stage-1 fires before AG binding, Stage-2
    fires after, and both must land under the same iteration.
    """
    current = _RECORDER_BINDING.get()
    _RECORDER_BINDING.set(
        replace(
            current,
            ag_id=str(ag_id or ""),
            cluster_id=str(cluster_id or ""),
        ),
    )


def reset_ag_binding() -> None:
    """Clear ag_id + cluster_id (used at end of AG dispatch)."""
    current = _RECORDER_BINDING.get()
    _RECORDER_BINDING.set(replace(current, ag_id="", cluster_id=""))


class LLMCallRecorder(Protocol):
    """Protocol for a recorder that observes every successful LLM call."""

    def record(
        self,
        *,
        span_name: str,
        system_msg: str,
        prompt: str,
        response_text: str,
        response_metadata: dict[str, Any],
    ) -> None: ...


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class InMemoryLLMCallRecorder:
    """Default recorder. Buffers calls in-process for end-of-iteration drain."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def record(
        self,
        *,
        span_name: str,
        system_msg: str,
        prompt: str,
        response_text: str,
        response_metadata: dict[str, Any],
    ) -> None:
        binding = _RECORDER_BINDING.get()
        self.calls.append({
            "span_name": str(span_name),
            "iteration": int(binding.iteration),
            "ag_id": str(binding.ag_id),
            "cluster_id": str(binding.cluster_id),
            "prompt_sha256": _sha256(prompt or ""),
            "system_msg": str(system_msg or ""),
            "prompt": str(prompt or ""),
            "response_text": str(response_text or ""),
            "response_metadata": dict(response_metadata or {}),
        })

    def drain(self) -> list[dict[str, Any]]:
        """Return and clear all calls accumulated since last drain.

        The harness drains once per iteration into
        ``_current_iter_inputs["llm_call_log"]``.
        """
        out = self.calls
        self.calls = []
        return out
