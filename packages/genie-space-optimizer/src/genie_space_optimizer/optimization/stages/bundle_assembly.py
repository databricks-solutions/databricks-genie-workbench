"""Stage 10 (NEW for C15): Bundle assembly.

Wraps the existing run-output-bundle assembly under a typed I/O
contract so D-4 (``AttributeError: 'list' object has no attribute
'get'``) cannot recur. Every stage capture passed through this
stage is normalized to a dict-shaped ``StageCaptureNormalized``
before any downstream code calls ``.get()`` on it.

The actual bundle write is delegated to the legacy
``run_output_bundle.assemble_bundle_for_replay`` (still authoritative
for the schema). This stage only owns the *input contract* and the
*list-shape normalization* — the two surfaces D-4 lives on.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


STAGE_KEY: str = "bundle_assembly"


@dataclass(frozen=True, slots=True)
class StageCaptureNormalized(JsonRoundTrip):
    """A stage capture coerced to dict shape.

    ``payload`` is always a dict.
    ``was_list`` records whether the original was a list, so the
    contract-health summary can detect drift.
    """

    payload: dict[str, Any]
    was_list: bool


def normalize_stage_capture(capture: Any) -> StageCaptureNormalized:
    """D-4 closure: coerce list-valued captures to dict.

    Rules:
      * dict → wrapped as-is.
      * [single_dict] → unwrapped to the dict.
      * [multi_dicts] → wrapped as ``{"_items": [...]}``.
      * [] → empty dict.
      * None → empty dict.
      * anything else → empty dict + ``was_list=False`` (defensive).
    """
    if isinstance(capture, dict):
        return StageCaptureNormalized(payload=dict(capture), was_list=False)
    if isinstance(capture, list):
        if len(capture) == 0:
            return StageCaptureNormalized(payload={}, was_list=True)
        if len(capture) == 1 and isinstance(capture[0], dict):
            return StageCaptureNormalized(payload=dict(capture[0]), was_list=True)
        return StageCaptureNormalized(payload={"_items": list(capture)}, was_list=True)
    return StageCaptureNormalized(payload={}, was_list=False)


@dataclass(frozen=True, slots=True)
class BundleAssemblyInput(JsonRoundTrip):
    """Input to stages.bundle_assembly.execute."""

    run_id: str
    iteration: int
    stage_captures: dict[str, Any] = field(default_factory=dict)
    learning_output: dict[str, Any] = field(default_factory=dict)
    acceptance_output: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BundleAssemblyOutput(JsonRoundTrip):
    """Output of stages.bundle_assembly.execute."""

    normalized_captures: dict[str, dict[str, Any]] = field(default_factory=dict)
    list_normalizations: tuple[str, ...] = ()  # stage keys whose capture was a list
    exception: str | None = None  # str repr of any caught error; None on success


def assemble(ctx, inp: BundleAssemblyInput) -> BundleAssemblyOutput:
    """Normalize every stage capture before any downstream consumer
    calls ``.get()`` on it. Emits bundle_assembly_list_normalized
    journey event for every list-shape coercion."""
    normalized: dict[str, dict[str, Any]] = {}
    list_norm: list[str] = []
    try:
        for stage_key, capture in (inp.stage_captures or {}).items():
            n = normalize_stage_capture(capture)
            normalized[stage_key] = n.payload
            if n.was_list:
                list_norm.append(stage_key)
                if ctx is not None and getattr(ctx, "journey_emit", None) is not None:
                    try:
                        ctx.journey_emit(
                            event="bundle_assembly_list_normalized",
                            stage_key=stage_key,
                            iteration=inp.iteration,
                        )
                    except Exception:
                        pass
        return BundleAssemblyOutput(
            normalized_captures=normalized,
            list_normalizations=tuple(list_norm),
            exception=None,
        )
    except Exception as exc:  # noqa: BLE001 — defensive at the outer perimeter only
        return BundleAssemblyOutput(
            normalized_captures=normalized,
            list_normalizations=tuple(list_norm),
            exception=f"{type(exc).__name__}: {exc}",
        )


INPUT_CLASS = BundleAssemblyInput
OUTPUT_CLASS = BundleAssemblyOutput
execute = assemble
