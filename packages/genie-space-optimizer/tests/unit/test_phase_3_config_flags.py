"""Phase 3 config-flag tests. Operational flags are default-ON so
operators don't need env vars; an explicit "0" disables."""

from __future__ import annotations

import os
from unittest.mock import patch

from genie_space_optimizer.common.config import (
    iteration_feedback_enabled,
    near_miss_reflection_enabled,
    near_miss_reflection_strict_drop_enabled,
    soft_signal_trend_report_enabled,
)


def test_iteration_feedback_default_on() -> None:
    with patch.dict(os.environ, {}, clear=True):
        assert iteration_feedback_enabled() is True


def test_iteration_feedback_off_when_explicit_false() -> None:
    with patch.dict(os.environ, {"GSO_ITERATION_FEEDBACK": "0"}, clear=True):
        assert iteration_feedback_enabled() is False


def test_near_miss_reflection_default_on() -> None:
    with patch.dict(os.environ, {}, clear=True):
        assert near_miss_reflection_enabled() is True


def test_near_miss_reflection_off_when_explicit_false() -> None:
    with patch.dict(os.environ, {"GSO_NEAR_MISS_REFLECTION": "0"}, clear=True):
        assert near_miss_reflection_enabled() is False


def test_near_miss_strict_drop_default_off() -> None:
    """Observability-first: the strict-drop gate is OFF by default so
    operators can read NEAR_MISS_AG_SHAPE_REPEATED telemetry before
    enabling rejection."""
    with patch.dict(os.environ, {}, clear=True):
        assert near_miss_reflection_strict_drop_enabled() is False


def test_near_miss_strict_drop_on_when_explicit_true() -> None:
    with patch.dict(
        os.environ, {"GSO_NEAR_MISS_REFLECTION_STRICT_DROP": "1"}, clear=True,
    ):
        assert near_miss_reflection_strict_drop_enabled() is True


def test_soft_signal_trend_report_default_on() -> None:
    with patch.dict(os.environ, {}, clear=True):
        assert soft_signal_trend_report_enabled() is True


def test_soft_signal_trend_report_off_when_explicit_false() -> None:
    with patch.dict(os.environ, {"GSO_SOFT_SIGNAL_TREND_REPORT": "0"}, clear=True):
        assert soft_signal_trend_report_enabled() is False
