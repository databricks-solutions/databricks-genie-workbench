"""RCO-4b Phase A Task 5/6 — unit tests for the pure
``run_propagation_wait_gate`` helper.

The helper is pure: it takes already-resolved inputs (no apply_log dict,
no WorkspaceClient, no spark), accepts injected ``sleep_fn`` and
``fetch_text_fn`` dependencies so the polling loop is testable without
real I/O, and returns a typed ``PropagationWaitOutcome``.

Outcome shape matches the real harness ``_audit_emit`` rows at
``harness._run_gate_checks:12915-12946`` — ``audit_decision`` is
``"confirmed"`` or ``"waited_full_budget"`` (not ``"completed"``), and
``reason_code`` differentiates ``"no_verifiable_snippet"`` vs
``"snippet_not_observed"`` for the full-budget branch.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.eval_gates import (
    run_propagation_wait_gate,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    PropagationWaitInput,
    PropagationWaitOutcome,
)


class _FakeClock:
    def __init__(self) -> None:
        self.elapsed = 0.0

    def sleep(self, seconds: float) -> None:
        self.elapsed += float(seconds)


def test_confirmed_fast_when_snippet_visible_on_first_poll() -> None:
    clock = _FakeClock()
    inp = PropagationWaitInput(
        ag_id="AG_alpha",
        max_wait_seconds=30,
        poll_interval_seconds=2.0,
        applied_patches_count=3,
        patched_objects=("table_a",),
        expected_instruction_snippets=("foo bar",),
        has_dictionary_changes=False,
    )
    out = run_propagation_wait_gate(
        inp,
        sleep_fn=clock.sleep,
        fetch_text_fn=lambda: "lorem ipsum foo bar dolor sit",
    )
    assert isinstance(out, PropagationWaitOutcome)
    assert out.propagated is True
    assert out.audit_decision == "confirmed"
    assert out.reason_code is None
    assert out.elapsed_seconds == 2.0
    assert out.max_wait_seconds == 30
    assert out.applied_patches_count == 3


def test_full_budget_with_snippet_not_observed() -> None:
    """Snippets were declared but never appeared in the fetched text →
    ``reason_code="snippet_not_observed"``."""
    clock = _FakeClock()
    inp = PropagationWaitInput(
        ag_id="AG_alpha",
        max_wait_seconds=10,
        poll_interval_seconds=2.0,
        applied_patches_count=1,
        patched_objects=("table_a",),
        expected_instruction_snippets=("never_seen_snippet",),
        has_dictionary_changes=False,
    )
    out = run_propagation_wait_gate(
        inp,
        sleep_fn=clock.sleep,
        fetch_text_fn=lambda: "unrelated content",
    )
    assert out.propagated is False
    assert out.audit_decision == "waited_full_budget"
    assert out.reason_code == "snippet_not_observed"
    assert out.elapsed_seconds == 10.0


def test_no_snippet_falls_through_to_full_budget() -> None:
    """When ``expected_instruction_snippets`` is empty (snippet-only
    patches like a join_spec change), the helper waits the full budget
    without calling ``fetch_text_fn`` → ``reason_code="no_verifiable_snippet"``."""
    clock = _FakeClock()
    inp = PropagationWaitInput(
        ag_id="AG_alpha",
        max_wait_seconds=10,
        poll_interval_seconds=2.0,
        applied_patches_count=1,
        patched_objects=("table_a",),
        expected_instruction_snippets=(),
        has_dictionary_changes=False,
    )
    fetch_calls = [0]
    def _never_call() -> str:
        fetch_calls[0] += 1
        return ""
    out = run_propagation_wait_gate(
        inp,
        sleep_fn=clock.sleep,
        fetch_text_fn=_never_call,
    )
    assert out.propagated is False
    assert out.audit_decision == "waited_full_budget"
    assert out.reason_code == "no_verifiable_snippet"
    # When there is no snippet, the helper still ticks through the
    # polling interval but does not call fetch_text_fn.
    assert fetch_calls[0] == 0


def test_fetch_text_fn_raising_does_not_break_polling() -> None:
    """fetch errors during polling are swallowed; the next interval
    retries. Mirrors the inline ``except Exception: continue``."""
    clock = _FakeClock()
    calls = [0]

    def _flaky() -> str:
        calls[0] += 1
        if calls[0] < 3:
            raise RuntimeError("transient")
        return "lorem foo bar ipsum"

    inp = PropagationWaitInput(
        ag_id="AG_alpha",
        max_wait_seconds=30,
        poll_interval_seconds=2.0,
        applied_patches_count=1,
        patched_objects=("table_a",),
        expected_instruction_snippets=("foo bar",),
        has_dictionary_changes=False,
    )
    out = run_propagation_wait_gate(
        inp,
        sleep_fn=clock.sleep,
        fetch_text_fn=_flaky,
    )
    assert out.propagated is True
    assert out.audit_decision == "confirmed"
    assert out.reason_code is None
    assert out.elapsed_seconds == 6.0  # three intervals: 2s + 2s + 2s
    assert calls[0] == 3


def test_helper_is_pure_no_globals() -> None:
    """A second invocation with identical inputs produces an identical
    outcome. This is the basic purity check."""
    clock_a = _FakeClock()
    clock_b = _FakeClock()
    inp = PropagationWaitInput(
        ag_id="AG_alpha",
        max_wait_seconds=30,
        poll_interval_seconds=2.0,
        applied_patches_count=3,
        patched_objects=("table_a",),
        expected_instruction_snippets=("foo bar",),
        has_dictionary_changes=False,
    )
    fetcher = lambda: "lorem foo bar ipsum"
    a = run_propagation_wait_gate(inp, sleep_fn=clock_a.sleep, fetch_text_fn=fetcher)
    b = run_propagation_wait_gate(inp, sleep_fn=clock_b.sleep, fetch_text_fn=fetcher)
    assert a == b
