"""Phase 0 P0.2 — synchronous token-bucket pacer tests."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.llm_pacer import (
    TokenBucket,
    get_pacer_for_endpoint,
    reset_pacer_registry,
    set_pacer_for_endpoint,
)


def test_bucket_starts_full() -> None:
    b = TokenBucket(capacity=1000, refill_rate=10)
    assert b.available() == pytest.approx(1000, abs=1)


def test_acquire_within_capacity_does_not_sleep() -> None:
    """A first call that fits in the initial credit should consume
    immediately without sleeping."""
    sleeps: list[float] = []
    b = TokenBucket(capacity=1000, refill_rate=10)
    waited = b.acquire(500, sleep=sleeps.append)
    assert waited == 0.0
    assert sleeps == []
    assert b.available() == pytest.approx(500, abs=1)


def test_acquire_beyond_credit_blocks_for_refill() -> None:
    """Draining the bucket then acquiring more must wait
    ``deficit / refill_rate`` seconds (via the supplied sleep
    closure)."""
    sleeps: list[float] = []
    b = TokenBucket(capacity=100, refill_rate=10)
    b.acquire(100, sleep=sleeps.append)
    assert sleeps == [] or sleeps == [0.0]
    sleeps.clear()
    # Use a fake sleep that advances the bucket's internal clock so
    # the refill arithmetic can satisfy the next acquire.
    import time as time_mod
    real_monotonic = time_mod.monotonic
    advanced = [real_monotonic()]

    def _fake_sleep(s: float) -> None:
        sleeps.append(s)
        advanced[0] += s

    def _fake_monotonic() -> float:
        return advanced[0]

    time_mod.monotonic = _fake_monotonic  # type: ignore[assignment]
    try:
        b.acquire(50, sleep=_fake_sleep)
    finally:
        time_mod.monotonic = real_monotonic  # type: ignore[assignment]
    # Need 50 / 10 = 5.0 seconds of refill.
    assert sum(sleeps) >= 4.9


def test_acquire_exceeding_capacity_raises() -> None:
    """The pacer must refuse a request larger than the bucket can
    ever hold; this would indicate the prompt-size gate was bypassed."""
    b = TokenBucket(capacity=100, refill_rate=10)
    with pytest.raises(RuntimeError, match="exceeds capacity"):
        b.acquire(101)


def test_acquire_respects_max_wait_seconds() -> None:
    """Long deficits must raise instead of blocking forever."""
    b = TokenBucket(capacity=100, refill_rate=0.01)
    b.acquire(100)
    sleeps: list[float] = []

    def _fake_sleep(s: float) -> None:
        sleeps.append(s)

    import time as time_mod
    real_monotonic = time_mod.monotonic
    advanced = [real_monotonic()]

    def _fake_sleep_adv(s: float) -> None:
        sleeps.append(s)
        advanced[0] += s

    def _fake_monotonic() -> float:
        return advanced[0]

    time_mod.monotonic = _fake_monotonic  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="timed out"):
            b.acquire(50, max_wait_seconds=0.5, sleep=_fake_sleep_adv)
    finally:
        time_mod.monotonic = real_monotonic  # type: ignore[assignment]


def test_registry_returns_default_opus_bucket() -> None:
    """First call for an endpoint constructs a default Opus-sized
    bucket; subsequent calls return the same instance."""
    reset_pacer_registry()
    try:
        b1 = get_pacer_for_endpoint("databricks-claude-opus-4-6")
        b2 = get_pacer_for_endpoint("databricks-claude-opus-4-6")
        assert b1 is b2
        # 120k capacity / 2000 refill is the Phase 0 sizing.
        assert b1.capacity == pytest.approx(120_000, abs=1)
        assert b1.refill_rate == pytest.approx(2_000, abs=1)
    finally:
        reset_pacer_registry()


def test_set_pacer_for_endpoint_overrides_default() -> None:
    """Tests inject fast/no-op pacers so they don't wait on the
    real refill schedule."""
    reset_pacer_registry()
    try:
        fast = TokenBucket(capacity=10**9, refill_rate=10**9)
        set_pacer_for_endpoint("databricks-claude-opus-4-6", fast)
        got = get_pacer_for_endpoint("databricks-claude-opus-4-6")
        assert got is fast
    finally:
        reset_pacer_registry()


def test_capacity_clamp_on_refill() -> None:
    """Refill must not let the bucket grow above ``capacity`` even
    after a long idle window."""
    b = TokenBucket(capacity=100, refill_rate=1000)
    # Drain.
    b.acquire(100)
    # Force a refill that would overshoot.
    import time as time_mod
    real_monotonic = time_mod.monotonic
    base = real_monotonic()
    time_mod.monotonic = lambda: base + 10**6  # type: ignore[assignment]
    try:
        assert b.available() == pytest.approx(100, abs=1)
    finally:
        time_mod.monotonic = real_monotonic  # type: ignore[assignment]
