"""Phase 2 (2026-05-16) — ``_build_reflection_entry`` must accept a
``terminal_signature`` kwarg and persist it into the returned dict
so ``compute_retired_signatures`` (which reads
``entry["terminal_signature"]``) can retire AG signatures.

Today the helper has no such kwarg — callers would have to use the
``extra={"terminal_signature": ...}`` back-channel, which no call
site does. We promote it to a typed kwarg with a strict
``isinstance`` check so misuse is loud.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
    build_terminal_signature,
)


def _build_signature() -> TerminalSignature:
    return build_terminal_signature(
        root_cause="missing_or_misordered_join",
        blame_set=("catalog.airline.fact_bookings",),
        lever_set=frozenset({5}),
        target_qids=frozenset({"airline_gs_017"}),
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )


def _build_kwargs() -> dict:
    """Minimum-viable kwargs for ``_build_reflection_entry``."""
    return dict(
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        accepted=False,
        levers=[5],
        target_objects=["airline_gs_017"],
        prev_scores={"q1": 0.0},
        new_scores={"q1": 0.0},
        rollback_reason="no_applied_patches",
        patches=[],
    )


def test_entry_has_terminal_signature_key_when_passed():
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    sig = _build_signature()
    entry = _build_reflection_entry(
        **_build_kwargs(),
        terminal_signature=sig,
    )
    assert "terminal_signature" in entry, (
        "_build_reflection_entry must surface 'terminal_signature' "
        "key when caller passes the kwarg. Got keys: "
        f"{sorted(entry.keys())}"
    )


def test_entry_terminal_signature_value_is_the_passed_sig():
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    sig = _build_signature()
    entry = _build_reflection_entry(
        **_build_kwargs(),
        terminal_signature=sig,
    )
    assert entry["terminal_signature"] is sig, (
        "Helper must pass the signature through without mutation/wrap."
    )


def test_entry_omits_terminal_signature_when_none():
    """Default ``None`` ⇒ key not present, so accepted entries (which
    don't carry signatures by contract) stay byte-stable."""
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    entry = _build_reflection_entry(**_build_kwargs())
    assert "terminal_signature" not in entry


def test_entry_rejects_non_terminal_signature():
    """Defensive: a dict / tuple / None-ish smuggled in via the
    typed kwarg must fail loud, not silently mis-shape the entry."""
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    with pytest.raises(TypeError) as exc:
        _build_reflection_entry(
            **_build_kwargs(),
            terminal_signature={"root_cause": "x"},  # type: ignore[arg-type]
        )
    assert "TerminalSignature" in str(exc.value)


def test_existing_entry_keys_unchanged_when_signature_added():
    """Every baseline key in the entry dict must still be present and
    unchanged when ``terminal_signature`` is added."""
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    sig = _build_signature()
    baseline = _build_reflection_entry(**_build_kwargs())
    with_sig = _build_reflection_entry(
        **_build_kwargs(),
        terminal_signature=sig,
    )
    for key in baseline:
        assert key in with_sig, f"key '{key}' dropped when signature added"
        assert with_sig[key] == baseline[key], (
            f"key '{key}' value changed: baseline={baseline[key]!r} "
            f"with_sig={with_sig[key]!r}"
        )


def test_signature_round_trips_through_compute_retired_signatures():
    """End-to-end check: an entry with a signature is admitted by
    ``compute_retired_signatures``; an entry without is not."""
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    from genie_space_optimizer.optimization.harness import (
        _build_reflection_entry,
    )
    sig = _build_signature()
    entry_with = _build_reflection_entry(
        **_build_kwargs(),
        terminal_signature=sig,
    )
    entry_without = _build_reflection_entry(**_build_kwargs())
    retired = compute_retired_signatures(
        reflection_buffer=[entry_with, entry_without],
    )
    assert sig in retired
    assert len(retired) == 1
