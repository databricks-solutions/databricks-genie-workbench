"""Tests for assert_lever_loop_inputs_sane()."""
import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    HandoffValue,
    assert_lever_loop_inputs_sane,
)


def _hv(key, value, source=HandoffSource.TASK_VALUES):
    return HandoffValue(key=key, value=value, source=source)


def test_sane_inputs_pass():
    """Real baseline state must pass."""
    state = {
        "overall_accuracy": _hv("overall_accuracy", 85.7),
        "scores": _hv("scores", {"syntax_validity": 95.0}),
        "model_id": _hv("model_id", "m-abc"),
    }
    assert_lever_loop_inputs_sane(state)  # must not raise


def test_zero_accuracy_with_empty_scores_raises():
    """Degenerate state (the Repair Run failure mode) must raise."""
    state = {
        "overall_accuracy": _hv(
            "overall_accuracy", 0.0, HandoffSource.MISSING,
        ),
        "scores": _hv("scores", {}, HandoffSource.MISSING),
        "model_id": _hv("model_id", "", HandoffSource.MISSING),
    }
    with pytest.raises(RuntimeError, match="degenerate"):
        assert_lever_loop_inputs_sane(state)


def test_zero_accuracy_with_real_scores_passes():
    """A real run with 0% accuracy is unusual but legal — only fail when
    BOTH accuracy=0 AND scores={}."""
    state = {
        "overall_accuracy": _hv(
            "overall_accuracy", 0.0, HandoffSource.TASK_VALUES,
        ),
        "scores": _hv(
            "scores", {"syntax_validity": 0.0}, HandoffSource.TASK_VALUES,
        ),
        "model_id": _hv("model_id", "m-abc", HandoffSource.TASK_VALUES),
    }
    assert_lever_loop_inputs_sane(state)  # must not raise


def test_message_includes_diagnostic_keys():
    state = {
        "overall_accuracy": _hv(
            "overall_accuracy", 0.0, HandoffSource.MISSING,
        ),
        "scores": _hv("scores", {}, HandoffSource.MISSING),
        "model_id": _hv("model_id", "", HandoffSource.MISSING),
    }
    with pytest.raises(RuntimeError) as exc_info:
        assert_lever_loop_inputs_sane(state)
    msg = str(exc_info.value)
    assert "Repair Run" in msg
    assert "overall_accuracy" in msg or "scores" in msg
