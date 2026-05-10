import pytest
from dataclasses import FrozenInstanceError

from genie_space_optimizer.optimization.stages.bundle_assembly import (
    BundleAssemblyInput,
    BundleAssemblyOutput,
    StageCaptureNormalized,
    normalize_stage_capture,
    assemble,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_normalize_stage_capture_handles_dict() -> None:
    cap = {"phase": "x", "ok": True}
    out = normalize_stage_capture(cap)
    assert isinstance(out, StageCaptureNormalized)
    assert out.payload == {"phase": "x", "ok": True}
    assert out.was_list is False


def test_normalize_stage_capture_handles_list_of_one_dict() -> None:
    """Anchor for D-4. The bundle assembler crashed on
    ``AttributeError: 'list' object has no attribute 'get'`` when a
    stage capture arrived as ``[dict]`` instead of ``dict``."""
    cap = [{"phase": "x", "ok": True}]
    out = normalize_stage_capture(cap)
    assert out.payload == {"phase": "x", "ok": True}
    assert out.was_list is True


def test_normalize_stage_capture_handles_list_of_many_dicts() -> None:
    cap = [{"a": 1}, {"b": 2}]
    out = normalize_stage_capture(cap)
    assert out.payload == {"_items": [{"a": 1}, {"b": 2}]}
    assert out.was_list is True


def test_normalize_stage_capture_handles_empty_list() -> None:
    out = normalize_stage_capture([])
    assert out.payload == {}
    assert out.was_list is True


def test_normalize_stage_capture_handles_none() -> None:
    out = normalize_stage_capture(None)
    assert out.payload == {}
    assert out.was_list is False


def test_input_is_frozen() -> None:
    inp = BundleAssemblyInput(
        run_id="run-1",
        iteration=1,
        stage_captures={"evaluation_state": {"records": 24}},
    )
    with pytest.raises(FrozenInstanceError):
        inp.iteration = 2  # type: ignore[misc]


def test_input_round_trips_with_list_capture() -> None:
    inp = BundleAssemblyInput(
        run_id="run-1",
        iteration=1,
        stage_captures={"evaluation_state": [{"records": 24}]},
    )
    payload = inp.to_json()
    restored = BundleAssemblyInput.from_json(payload)
    assert restored.stage_captures["evaluation_state"] == [{"records": 24}]


def test_assemble_does_not_crash_on_list_capture_anchor() -> None:
    """Replays the airline iter 1 anchor that crashed C14-V T5."""
    inp = BundleAssemblyInput(
        run_id="run-1",
        iteration=1,
        stage_captures={"evaluation_state": [{"records": 24, "accuracy": 0.833}]},
    )
    out = assemble(ctx=None, inp=inp)
    assert isinstance(out, BundleAssemblyOutput)
    assert out.exception is None
    assert "evaluation_state" in out.normalized_captures


def test_input_and_output_mix_jsonroundtrip() -> None:
    assert issubclass(BundleAssemblyInput, JsonRoundTrip)
    assert issubclass(BundleAssemblyOutput, JsonRoundTrip)
