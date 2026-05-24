"""Workbench-internal recording applier.

Two surfaces are exposed:

* :class:`PatchRecorder` — accumulates :class:`RecordedPatch` entries
  for the workbench report.
* :func:`make_recording_applier_stub` — returns a callable suitable for
  ``ctx.extras["applier"]``. The applier gate routes the proposal
  through this stub, which records the would-be PATCH and returns
  ``(apply_call_id, succeeded=True, "")`` so the state machine can
  advance to ``APPLIED`` without touching a Genie space.

The applier-gate stub never imports from ``tests/``. It is a fresh
copy of the minimal surface the workbench needs, so production code
never has to think about whether a fake or a real Workspace client
is on the other side.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from local_lever_workbench.models import RecordedPatch


@dataclass
class PatchRecorder:
    """Collects PATCH payloads the recording applier observes.

    The recorder is intentionally chatty: it captures everything the
    apply path could have shipped (intent id, patch type, the typed
    proposal body) so the workbench report can answer "what would the
    optimizer have shipped if we deployed?" without re-running the
    pipeline.
    """

    patches: list[RecordedPatch] = field(default_factory=list)

    def record(
        self,
        *,
        qid: str,
        intent_id: str,
        patch_type: str,
        serialized_space: dict | None,
        raw_body: dict | None,
    ) -> None:
        self.patches.append(
            RecordedPatch(
                qid=qid,
                intent_id=intent_id,
                patch_type=patch_type,
                serialized_space=serialized_space,
                raw_body=raw_body,
            )
        )

    def as_tuple(self) -> tuple[RecordedPatch, ...]:
        return tuple(self.patches)


def _coerce_patch_body(proposal: Any) -> dict | None:
    """Best-effort projection of ``RepairProposal.patch_body`` to a
    JSON-friendly dict.

    The workbench is intentionally permissive here: a brand-new patch
    type that does not yet have a dict-shaped body should still be
    recorded so the report can warn about it, rather than swallowing
    the new shape.
    """
    body = getattr(proposal, "patch_body", None)
    if body is None:
        return None
    if isinstance(body, dict):
        return dict(body)
    if hasattr(body, "model_dump") and callable(body.model_dump):
        try:
            return dict(body.model_dump())
        except (TypeError, ValueError):
            return None
    if hasattr(body, "__dict__"):
        return {k: v for k, v in vars(body).items() if not k.startswith("_")}
    return None


def make_recording_applier_stub(
    recorder: PatchRecorder,
) -> Callable[..., tuple[str, bool, str]]:
    """Build a callable suitable for ``ctx.extras["applier"]``.

    The applier gate calls ``stub(state=..., ctx=..., proposal=...)``
    and expects a ``(apply_call_id, succeeded, error_reason)`` tuple.
    The stub always returns success because the workbench's goal is
    to observe the deepest funnel stage the optimizer reaches, not to
    simulate apply failures (those have dedicated unit coverage under
    ``tests/integration/test_sm_forward_pipeline_to_applied.py``).
    """

    def _stub(*, state, ctx, proposal) -> tuple[str, bool, str]:
        intent_id = ""
        if state.proposals:
            intent_id = str(state.proposals[-1].intent_id or "")
        patch_type = ""
        pt = getattr(proposal, "patch_type", None)
        if pt is not None:
            patch_type = pt.value if hasattr(pt, "value") else str(pt)
        recorder.record(
            qid=str(state.qid),
            intent_id=intent_id,
            patch_type=patch_type,
            serialized_space=None,
            raw_body=_coerce_patch_body(proposal),
        )
        apply_call_id = f"apply_{ctx.iteration}_{intent_id}"
        return apply_call_id, True, ""

    return _stub


__all__ = ["PatchRecorder", "make_recording_applier_stub"]
