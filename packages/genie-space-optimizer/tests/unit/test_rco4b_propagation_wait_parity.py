"""RCO-4b Phase A Task 7 — production-shape parity test for
``run_propagation_wait_gate``.

Loads fixture pairs under ``tests/unit/fixtures/rco4b/propagation_wait/``
and asserts the helper's output equals the recorded expected output.

The ``fetch_text_sequence`` field in each input.json drives the
injected ``fetch_text_fn`` deterministically: poll N returns the
N-th string. When the sequence is exhausted, subsequent fetches
return the empty string (matching the legacy "fetch returned no text"
branch which falls through to the next interval).
"""

from __future__ import annotations

import json
import pathlib

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    run_propagation_wait_gate,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    PropagationWaitInput,
    PropagationWaitOutcome,
)

_FIXTURE_ROOT = (
    pathlib.Path(__file__).resolve().parent
    / "fixtures"
    / "rco4b"
    / "propagation_wait"
)


def _discover_cases() -> list[pathlib.Path]:
    return sorted(
        p for p in _FIXTURE_ROOT.iterdir() if p.is_dir() and not p.name.startswith("_")
    )


@pytest.mark.parametrize("case_dir", _discover_cases(), ids=lambda p: p.name)
def test_propagation_wait_parity(case_dir: pathlib.Path) -> None:
    inp_data = json.loads((case_dir / "input.json").read_text())
    expected_data = json.loads((case_dir / "expected_output.json").read_text())

    fetch_seq = list(inp_data.pop("fetch_text_sequence", []))

    inp = PropagationWaitInput(
        ag_id=inp_data["ag_id"],
        max_wait_seconds=int(inp_data["max_wait_seconds"]),
        poll_interval_seconds=float(inp_data["poll_interval_seconds"]),
        applied_patches_count=int(inp_data["applied_patches_count"]),
        patched_objects=tuple(inp_data.get("patched_objects") or ()),
        expected_instruction_snippets=tuple(
            inp_data.get("expected_instruction_snippets") or ()
        ),
        has_dictionary_changes=bool(inp_data["has_dictionary_changes"]),
    )

    elapsed_box = [0.0]
    fetch_idx = [0]

    def _sleep(s: float) -> None:
        elapsed_box[0] += float(s)

    def _fetch() -> str:
        idx = fetch_idx[0]
        fetch_idx[0] += 1
        if idx < len(fetch_seq):
            return str(fetch_seq[idx])
        return ""

    out = run_propagation_wait_gate(inp, sleep_fn=_sleep, fetch_text_fn=_fetch)

    expected = PropagationWaitOutcome(
        propagated=bool(expected_data["propagated"]),
        elapsed_seconds=float(expected_data["elapsed_seconds"]),
        max_wait_seconds=int(expected_data["max_wait_seconds"]),
        applied_patches_count=int(expected_data["applied_patches_count"]),
        audit_decision=str(expected_data["audit_decision"]),
        reason_code=expected_data.get("reason_code"),
    )
    assert out == expected, (
        f"case {case_dir.name}: helper output {out} != expected {expected}"
    )


def test_at_least_three_cases_exist() -> None:
    cases = _discover_cases()
    case_names = {p.name for p in cases}
    assert {
        "case_propagated_fast",
        "case_full_budget_timeout",
        "case_no_verifiable_snippet",
    }.issubset(case_names), (
        f"RCO-4b Phase A requires at least three propagation_wait fixture "
        f"cases. Found: {sorted(case_names)}"
    )


# ---------------------------------------------------------------------------
# Task 9 — flag-on vs flag-off parity (helper-equivalence is enforced by
# the existing parametrized tests above; this section asserts that the
# *audit-row* sequence the harness produces is structurally identical
# under either flag setting on the same input).
# ---------------------------------------------------------------------------


def _simulate_harness_audit_rows(
    *,
    flag_on: bool,
    fetch_seq: list[str],
    max_wait: int,
    poll_interval: float,
    snippets: tuple[str, ...],
    applied_count: int,
) -> list[dict]:
    """Replays just the audit-row emission side of the propagation
    block. Both branches of the harness wiring should emit the same
    audit rows for the same input."""
    audit_rows: list[dict] = []
    elapsed = 0.0
    fetch_idx = [0]

    def _fetch() -> str:
        idx = fetch_idx[0]
        fetch_idx[0] += 1
        if idx < len(fetch_seq):
            return str(fetch_seq[idx])
        return ""

    if flag_on:
        inp = PropagationWaitInput(
            ag_id="AG_X",
            max_wait_seconds=max_wait,
            poll_interval_seconds=poll_interval,
            applied_patches_count=applied_count,
            patched_objects=(),
            expected_instruction_snippets=snippets,
            has_dictionary_changes=False,
        )
        out = run_propagation_wait_gate(
            inp,
            sleep_fn=lambda s: None,
            fetch_text_fn=_fetch,
        )
        elapsed = float(out.elapsed_seconds)
        decision = out.audit_decision
        reason_code = out.reason_code
    else:
        propagated = False
        while elapsed < float(max_wait):
            elapsed += poll_interval
            if not snippets:
                continue
            try:
                text = _fetch()
            except Exception:
                continue
            if text and any(s in text for s in snippets):
                propagated = True
                break
        if snippets and propagated:
            decision = "confirmed"
            reason_code = None
        else:
            decision = "waited_full_budget"
            reason_code = (
                "no_verifiable_snippet" if not snippets else "snippet_not_observed"
            )

    row: dict = {
        "gate_name": "propagation_wait",
        "decision": decision,
        "metrics": {
            "elapsed_seconds": round(elapsed, 1),
            "max_wait_seconds": max_wait,
            "patches_applied": applied_count,
        },
    }
    if reason_code is not None:
        row["reason_code"] = reason_code
    audit_rows.append(row)
    return audit_rows


def test_harness_audit_row_parity_on_propagated_fast() -> None:
    on = _simulate_harness_audit_rows(
        flag_on=True,
        fetch_seq=["stale", "lorem foo bar ipsum"],
        max_wait=30,
        poll_interval=2.0,
        snippets=("foo bar",),
        applied_count=2,
    )
    off = _simulate_harness_audit_rows(
        flag_on=False,
        fetch_seq=["stale", "lorem foo bar ipsum"],
        max_wait=30,
        poll_interval=2.0,
        snippets=("foo bar",),
        applied_count=2,
    )
    assert on == off


def test_harness_audit_row_parity_on_full_budget_timeout() -> None:
    on = _simulate_harness_audit_rows(
        flag_on=True,
        fetch_seq=["no match", "still no match"],
        max_wait=10,
        poll_interval=2.0,
        snippets=("never_seen",),
        applied_count=1,
    )
    off = _simulate_harness_audit_rows(
        flag_on=False,
        fetch_seq=["no match", "still no match"],
        max_wait=10,
        poll_interval=2.0,
        snippets=("never_seen",),
        applied_count=1,
    )
    assert on == off


def test_harness_audit_row_parity_on_no_snippet() -> None:
    on = _simulate_harness_audit_rows(
        flag_on=True,
        fetch_seq=[],
        max_wait=6,
        poll_interval=2.0,
        snippets=(),
        applied_count=1,
    )
    off = _simulate_harness_audit_rows(
        flag_on=False,
        fetch_seq=[],
        max_wait=6,
        poll_interval=2.0,
        snippets=(),
        applied_count=1,
    )
    assert on == off
