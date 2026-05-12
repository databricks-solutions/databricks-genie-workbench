"""Risk 1 — pure-helper unit test for the consecutive-collision counter."""
from __future__ import annotations


def test_should_terminate_on_collision_saturation_below_threshold() -> None:
    from genie_space_optimizer.optimization.harness import (
        _should_terminate_on_collision_saturation,
    )
    assert _should_terminate_on_collision_saturation(
        consecutive_skips=1, threshold=2
    ) is False


def test_should_terminate_on_collision_saturation_at_threshold() -> None:
    from genie_space_optimizer.optimization.harness import (
        _should_terminate_on_collision_saturation,
    )
    assert _should_terminate_on_collision_saturation(
        consecutive_skips=2, threshold=2
    ) is True


def test_should_terminate_on_collision_saturation_above_threshold() -> None:
    from genie_space_optimizer.optimization.harness import (
        _should_terminate_on_collision_saturation,
    )
    assert _should_terminate_on_collision_saturation(
        consecutive_skips=5, threshold=2
    ) is True


def test_should_terminate_on_collision_saturation_threshold_zero_disabled() -> None:
    """Threshold=0 disables the guard so legacy fixtures replay byte-stable."""
    from genie_space_optimizer.optimization.harness import (
        _should_terminate_on_collision_saturation,
    )
    assert _should_terminate_on_collision_saturation(
        consecutive_skips=99, threshold=0
    ) is False
