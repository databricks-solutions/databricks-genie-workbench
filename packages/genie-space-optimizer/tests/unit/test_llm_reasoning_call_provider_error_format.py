"""PR-C reviewer P0 #2 — surface raw provider body in
``LlmReasoningResponse.error``.

The pre-PR-C ``LlmReasoningCall.invoke`` exception handler did::

    error=f"{type(exc).__name__}: {exc}"

which collapsed every Databricks model-serving 400 to the same opaque
``BadRequestError: Error code: 400 - {...}`` line and stripped the
structured error body. The 2026-05-22 dc89d1a9 trial postmortem showed
this directly: ``error_kind=unknown`` everywhere, no way to
distinguish a schema rejection (``response_format_invalid``) from a
token overflow (``token_limit_exceeded``) without a re-run.

``_format_provider_error`` is the fix. It walks the exception for the
most structured form first (``exc.body`` → ``exc.response.text`` →
``str(exc)``) and concatenates them. The diagnose-stage classifier's
substring matches (``"response_format"`` in message,
``"maximum tokens"`` in message, etc.) keep working — the difference
is that those matches now actually find their substrings.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.llm_reasoning_call import (
    _format_provider_error,
)


class _StubResponse:
    def __init__(self, text: str = ""):
        self.text = text


class _BadRequestLike(Exception):
    """Looks like an OpenAI BadRequestError — has body and response."""

    def __init__(self, message: str, body: dict | None = None,
                 response: _StubResponse | None = None):
        super().__init__(message)
        self.body = body
        self.response = response


def test_format_preserves_body_when_present() -> None:
    """``exc.body`` is the highest-fidelity source: a dict the SDK
    parsed from the 400 response. It MUST appear verbatim in the
    formatted string so the diagnose classifier can detect the
    ``response_format`` substring."""
    exc = _BadRequestLike(
        "Error code: 400",
        body={"error": {
            "message": "Invalid response_format: schema is malformed",
            "type": "invalid_request_error",
        }},
    )
    formatted = _format_provider_error(exc)
    assert formatted.startswith("_BadRequestLike: "), formatted
    assert "response_format" in formatted, (
        f"raw body lost in formatting: {formatted!r}"
    )
    assert "Invalid response_format" in formatted, (
        f"the specific message did not survive: {formatted!r}"
    )


def test_format_falls_back_to_response_text_when_body_missing() -> None:
    """If the SDK could not parse the JSON body but captured the raw
    HTTP response text, surface that instead. Real failure modes:
    proxy 502s, malformed JSON from upstream."""
    exc = _BadRequestLike(
        "Error code: 503",
        body=None,
        response=_StubResponse(text="upstream gateway timeout"),
    )
    formatted = _format_provider_error(exc)
    assert "upstream gateway timeout" in formatted, formatted


def test_format_includes_str_fallback_for_plain_exceptions() -> None:
    """For exceptions that have no body / response attributes (most
    non-OpenAI errors), ``str(exc)`` is the only available signal.
    The formatter must NOT crash on missing attributes."""
    exc = ValueError("something went wrong")
    formatted = _format_provider_error(exc)
    assert formatted.startswith("ValueError: "), formatted
    assert "something went wrong" in formatted, formatted


def test_format_includes_exception_class_prefix() -> None:
    """The diagnose-stage classifier dispatches on exception class
    name (``"BadRequestError" in exc_name``). The prefix MUST be the
    real class name, not a generic ``Exception``."""

    class CustomBadRequestError(Exception):
        pass

    exc = CustomBadRequestError("kaboom")
    formatted = _format_provider_error(exc)
    assert formatted.startswith("CustomBadRequestError: "), formatted


def test_format_does_not_duplicate_body_in_response_text() -> None:
    """When the SDK populates both ``body`` and ``response.text`` with
    the same content (common case — SDK parses then re-emits), we
    avoid emitting the same payload twice."""
    body = {"error": {"message": "schema invalid"}}
    response_text = repr(body)  # same content
    exc = _BadRequestLike("400", body=body,
                          response=_StubResponse(text=response_text))
    formatted = _format_provider_error(exc)
    assert formatted.count("schema invalid") == 1, (
        f"body was duplicated: {formatted!r}"
    )


def test_format_truncates_excessively_long_response_text() -> None:
    """Some Databricks proxy errors echo a megabyte of HTML. Cap the
    response_text fragment so the marker stays under the 500-char
    truncation budget downstream."""
    exc = _BadRequestLike(
        "400",
        body=None,
        response=_StubResponse(text="x" * 100_000),
    )
    formatted = _format_provider_error(exc)
    # Response text fragment alone is capped at 1000 chars.
    assert formatted.count("x") <= 1_005, (
        f"response_text not truncated: len={len(formatted)}"
    )


def test_format_survives_non_json_serializable_body() -> None:
    """If ``body`` contains a non-JSON-serializable object, fall back
    to repr() rather than crashing the error path."""

    class Weird:
        def __repr__(self) -> str:
            return "<weird-object>"

    exc = _BadRequestLike("400", body={"data": Weird()})
    formatted = _format_provider_error(exc)
    assert "weird-object" in formatted or "Weird" in formatted, formatted
