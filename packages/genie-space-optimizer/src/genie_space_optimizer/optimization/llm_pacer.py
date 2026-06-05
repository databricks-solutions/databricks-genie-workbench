"""Phase 0 P0.2 — synchronous token-bucket pacer for the Opus FMAPI quota.

The Foundation Model API rate-limits Claude Opus 4.6 on a sliding
60-second window: 200,000 input tokens per minute (ITPM) and 20,000
output tokens per minute (OTPM). The Plan-2 ``IterationTokenBudget``
in :mod:`llm_token_budget` enforces a per-iteration aggregate cap on
top of those numbers but does NOT pace the rate at which tokens are
consumed inside an iteration. A burst of three Stage 3 calls inside a
20-second window can therefore drain a budget that is well-shaped on
paper yet still 429s in practice — the budget says "you have 120k
ITPM left this iteration" while the workspace clock says "you spent
60k of that in the past 18 seconds".

The pacer closes that gap with a simple synchronous token bucket:

* ``capacity`` — the maximum credit the bucket can hold. Set to one
  iteration's worth of input budget so a long-idle pacer cannot
  release more than one iteration's worth of bursts.
* ``refill_rate`` — tokens credited back per wall-clock second. For
  Opus, the production setting is ``120_000 / 60 = 2000`` input
  tokens per second (i.e. 60% of the 200k ITPM workspace quota,
  matching the headroom the iteration budget reserves).
* :meth:`acquire(tokens)` — block (via ``time.sleep``) until the
  requested credit is available, then deduct it.

The bucket is intentionally synchronous because the lever loop is
single-threaded: every reasoning call is dispatched sequentially
behind :class:`LlmReasoningCall.invoke`, so there is no async or
threaded contention to design around. A single global instance per
endpoint key is exposed through :func:`get_opus_pacer`. Callers that
want to disable pacing (unit tests, workbench fixture replay) install
a no-op pacer via :func:`set_pacer_for_endpoint`.

Why this is not just sleep-on-429:

  Pre-call pacing pays the latency cost EVENLY across calls; sleep-
  on-429 stacks the cost onto the calls that trip the limit AND
  loses the prompt's ``max_tokens`` reservation (Databricks credits
  it back only on success). The pacer is the architectural defense
  the plan calls for; the retry-with-Retry-After path in
  :mod:`llm_client` is a backstop for the irreducible
  shared-workspace bursts the pacer cannot model.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class TokenBucket:
    """Classic token-bucket rate limiter; synchronous, thread-safe.

    Stores credit (tokens) up to ``capacity``; refills at
    ``refill_rate`` per second. :meth:`acquire(n)` blocks via
    ``time.sleep`` until ``available >= n``, then deducts ``n``.

    Thread-safety: a single :class:`threading.Lock` guards the
    refill+deduct critical section. The lever loop is single-threaded
    today, but the lock makes the bucket safe for any future caller
    that fans out reasoning calls behind a pool.
    """

    capacity: float
    refill_rate: float
    _available: float = field(init=False)
    _last_refill_ts: float = field(init=False)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError(f"capacity must be > 0, got {self.capacity}")
        if self.refill_rate <= 0:
            raise ValueError(
                f"refill_rate must be > 0, got {self.refill_rate}"
            )
        self._available = float(self.capacity)
        self._last_refill_ts = time.monotonic()

    def _refill_locked(self) -> None:
        """Refill the bucket based on time elapsed since the last
        refill. Caller must hold ``self._lock``."""
        now = time.monotonic()
        elapsed = now - self._last_refill_ts
        if elapsed > 0:
            self._available = min(
                self.capacity, self._available + elapsed * self.refill_rate,
            )
            self._last_refill_ts = now

    def available(self) -> float:
        """Return current credit (refills first). Mostly for tests
        and postmortem markers."""
        with self._lock:
            self._refill_locked()
            return self._available

    def acquire(
        self,
        tokens: float,
        *,
        max_wait_seconds: float = 120.0,
        sleep: callable = time.sleep,
    ) -> float:
        """Block until ``tokens`` credit is available, then deduct.

        Returns the number of seconds slept (0.0 if no wait was
        required). Raises ``RuntimeError`` if the wait would exceed
        ``max_wait_seconds`` — a deliberate ceiling so a misconfigured
        bucket cannot stall the lever loop indefinitely.

        ``sleep`` is overridable so tests can run deterministically
        without real wall-clock sleeps.
        """
        if tokens <= 0:
            return 0.0
        # A request larger than capacity can never be satisfied.
        # Clamp to capacity instead of looping forever — the caller's
        # iteration budget already rejects oversized prompts via
        # ``PROMPT_TOO_LARGE`` (Phase 0 P0.4); reaching this branch
        # would indicate a bypass we want to fail loudly.
        if tokens > self.capacity:
            raise RuntimeError(
                f"TokenBucket.acquire({tokens}) exceeds capacity "
                f"{self.capacity}; the prompt-size gate should have "
                "rejected this call before the pacer ever saw it."
            )
        total_slept = 0.0
        deadline = time.monotonic() + max_wait_seconds
        while True:
            with self._lock:
                self._refill_locked()
                if self._available >= tokens:
                    self._available -= tokens
                    return total_slept
                deficit = tokens - self._available
                wait_seconds = deficit / self.refill_rate
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    f"TokenBucket.acquire({tokens}) timed out after "
                    f"{max_wait_seconds:.1f}s — bucket cannot satisfy "
                    "the request even with full refill."
                )
            sleep_for = min(wait_seconds, remaining)
            sleep(sleep_for)
            total_slept += sleep_for


# ── Per-endpoint bucket registry ──────────────────────────────────────
#
# The lever loop uses a single Opus endpoint, but the registry keeps
# the door open for a future multi-endpoint configuration without
# touching call sites. Callers acquire pacing credit by endpoint name
# (the same string the OpenAI client uses as ``model=``).

# Phase 0 production sizing: 60% of the 200k Opus ITPM workspace quota,
# refilled per second. Capacity equals one iteration budget so the
# bucket can release an entire iteration's worth of tokens in a single
# burst (matching what ``IterationTokenBudget`` already permits), then
# refills at the per-second rate while the iteration body runs.
_DEFAULT_OPUS_CAPACITY = 120_000.0
_DEFAULT_OPUS_REFILL_PER_SEC = 2_000.0

_pacer_registry: dict[str, TokenBucket] = {}
_pacer_registry_lock = threading.Lock()


def _make_default_opus_bucket() -> TokenBucket:
    return TokenBucket(
        capacity=_DEFAULT_OPUS_CAPACITY,
        refill_rate=_DEFAULT_OPUS_REFILL_PER_SEC,
    )


def get_pacer_for_endpoint(endpoint: str) -> TokenBucket:
    """Return the active pacer for ``endpoint``, creating a default
    Opus-sized bucket if one is not yet registered.

    Test code that wants a fast deterministic pacer should install
    one via :func:`set_pacer_for_endpoint`.
    """
    with _pacer_registry_lock:
        bucket = _pacer_registry.get(endpoint)
        if bucket is None:
            bucket = _make_default_opus_bucket()
            _pacer_registry[endpoint] = bucket
        return bucket


def set_pacer_for_endpoint(endpoint: str, bucket: TokenBucket) -> None:
    """Replace the active pacer for ``endpoint``. Used by tests and
    workbench fixtures to swap in a no-op or fast-refill bucket."""
    with _pacer_registry_lock:
        _pacer_registry[endpoint] = bucket


def reset_pacer_registry() -> None:
    """Clear the registry. Used by tests to start fresh."""
    with _pacer_registry_lock:
        _pacer_registry.clear()
