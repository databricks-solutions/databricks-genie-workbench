"""Phase 0 P0.3 — rate-limit detection + backoff math tests."""
from __future__ import annotations

import random

import pytest

from genie_space_optimizer.optimization.llm_rate_limit import (
    compute_rate_limit_backoff_seconds,
    is_rate_limit_error,
    parse_retry_after_seconds,
)


class _FakeRateLimitError(Exception):
    """Mimics the openai SDK's ``RateLimitError`` by class name."""


class _FakeTooManyRequestsError(Exception):
    pass


class _FakeResponse:
    def __init__(self, headers: dict[str, str] | None = None,
                 status_code: int | None = None) -> None:
        self.headers = headers or {}
        self.status_code = status_code


def test_detect_by_class_name_rate_limit_error() -> None:
    """OpenAI SDK class name is the primary discriminator."""
    exc = _FakeRateLimitError("Error code: 429")
    exc.__class__.__name__ = "RateLimitError"
    assert is_rate_limit_error(exc) is True


def test_detect_by_class_name_too_many_requests() -> None:
    exc = _FakeTooManyRequestsError("server busy")
    exc.__class__.__name__ = "TooManyRequestsError"
    assert is_rate_limit_error(exc) is True


def test_detect_by_message_request_limit_exceeded() -> None:
    """Databricks embeds ``REQUEST_LIMIT_EXCEEDED`` in the response body
    text — must be detected even when the exception class is the
    generic OpenAI ``APIStatusError``."""
    exc = RuntimeError(
        "Error code: 429 - REQUEST_LIMIT_EXCEEDED: input token rate "
        "limit reached for endpoint databricks-claude-opus-4-6"
    )
    assert is_rate_limit_error(exc) is True


def test_detect_by_message_tokens_per_minute() -> None:
    exc = RuntimeError("input tokens per minute exceeded; please retry")
    assert is_rate_limit_error(exc) is True


def test_detect_by_response_status_code() -> None:
    exc = RuntimeError("opaque")
    exc.response = _FakeResponse(status_code=429)  # type: ignore[attr-defined]
    assert is_rate_limit_error(exc) is True


def test_non_rate_limit_error_returns_false() -> None:
    """A vanilla ValueError must not trip the detector."""
    exc = ValueError("schema mismatch")
    assert is_rate_limit_error(exc) is False


def test_parse_retry_after_header_integer_seconds() -> None:
    exc = RuntimeError("429")
    exc.response = _FakeResponse(headers={"Retry-After": "12"})  # type: ignore[attr-defined]
    assert parse_retry_after_seconds(exc) == 12.0


def test_parse_retry_after_lowercase_header() -> None:
    exc = RuntimeError("429")
    exc.response = _FakeResponse(headers={"retry-after": "7"})  # type: ignore[attr-defined]
    assert parse_retry_after_seconds(exc) == 7.0


def test_parse_retry_after_from_body_text() -> None:
    """When the server doesn't send a header, scan the message body
    for a ``retry after N`` phrase."""
    exc = RuntimeError(
        "REQUEST_LIMIT_EXCEEDED: please retry-after 15 seconds"
    )
    assert parse_retry_after_seconds(exc) == 15.0


def test_parse_retry_after_returns_zero_when_absent() -> None:
    exc = RuntimeError("generic 429")
    assert parse_retry_after_seconds(exc) == 0.0


def test_parse_retry_after_caps_at_120s() -> None:
    """Defensive ceiling: a misconfigured server must not pin the
    optimizer indefinitely."""
    exc = RuntimeError("429")
    exc.response = _FakeResponse(headers={"Retry-After": "9999"})  # type: ignore[attr-defined]
    assert parse_retry_after_seconds(exc) == 120.0


def test_compute_backoff_uses_max_of_retry_after_and_exponential() -> None:
    """If Retry-After is bigger than the exponential floor, honor it."""
    exc = RuntimeError("429")
    exc.response = _FakeResponse(headers={"Retry-After": "30"})  # type: ignore[attr-defined]
    rng = random.Random(0)
    seconds = compute_rate_limit_backoff_seconds(
        exc, attempt=1, jitter_seconds=0.0, rng=rng,
    )
    # 2**1 = 2; Retry-After 30 wins; jitter 0 → exactly 30.
    assert seconds == pytest.approx(30.0)


def test_compute_backoff_uses_exponential_when_no_header() -> None:
    exc = RuntimeError("429 no header")
    rng = random.Random(0)
    seconds = compute_rate_limit_backoff_seconds(
        exc, attempt=2, jitter_seconds=0.0, rng=rng,
    )
    # 2**2 = 4.0; jitter 0.
    assert seconds == pytest.approx(4.0)


def test_compute_backoff_adds_jitter() -> None:
    """Jitter must add 0..jitter_seconds on top of the floor."""
    exc = RuntimeError("429 no header")
    rng = random.Random(42)
    seconds = compute_rate_limit_backoff_seconds(
        exc, attempt=0, jitter_seconds=2.0, rng=rng,
    )
    # 2**0 = 1; jitter in [0, 2) → seconds in [1, 3).
    assert 1.0 <= seconds < 3.0


def test_diagnose_classify_llm_error_rate_limited_by_class() -> None:
    """The diagnose classifier must map ``RateLimitError`` to
    ``rate_limited`` and not fall through to ``client_construction``."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        _classify_llm_error,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )
    from pydantic import BaseModel

    class _Dummy(BaseModel):
        pass

    req = LlmReasoningRequest(
        call_id="test", skill_id="test",
        system_msg="sys", user_prompt="usr",
        result_cls=_Dummy, max_tokens=100,
    )
    kind = _classify_llm_error(
        exception_class="RateLimitError",
        error_message="RateLimitError: Error code: 429",
        tokens_input=0,
        request=req,
    )
    assert kind == "rate_limited"


def test_diagnose_classify_llm_error_rate_limited_by_message() -> None:
    """REQUEST_LIMIT_EXCEEDED in the body must classify as
    ``rate_limited`` even when the exception class is generic."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        _classify_llm_error,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )
    from pydantic import BaseModel

    class _Dummy(BaseModel):
        pass

    req = LlmReasoningRequest(
        call_id="test", skill_id="test",
        system_msg="sys", user_prompt="usr",
        result_cls=_Dummy, max_tokens=100,
    )
    kind = _classify_llm_error(
        exception_class="APIStatusError",
        error_message=(
            "APIStatusError: Error code: 429 - REQUEST_LIMIT_EXCEEDED: "
            "input token rate limit reached"
        ),
        tokens_input=0,
        request=req,
    )
    assert kind == "rate_limited"


def test_optimizer_capacity_starved_in_abstain_enum() -> None:
    """The closed vocabulary now includes the new framework reason."""
    from genie_space_optimizer.optimization.llm_abstain import AbstainReason
    assert AbstainReason.OPTIMIZER_CAPACITY_STARVED.value == (
        "optimizer_capacity_starved"
    )
