"""Trial 20 Workstream F2 — canonical-target resolution.

Pins:

* Exact identifier match is returned unchanged.
* Partial tail-match resolves to the fully-qualified name when the
  tail is unique.
* Ambiguous tails return ``""`` so the caller drops the proposal.
* Unknown tails return ``""``.
* Empty / whitespace input returns ``""``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stages.synthesize import (
    _trial20_resolve_canonical_target_table,
)


def test_f2_exact_match_passes_through() -> None:
    out = _trial20_resolve_canonical_target_table(
        raw_target="main.7now.tkt_payment",
        resolved_tables=frozenset({"main.7now.tkt_payment"}),
    )
    assert out == "main.7now.tkt_payment"


def test_f2_unique_tail_match_returns_canonical() -> None:
    """Postmortem 7now case — patch named ``tkt_payment`` should
    resolve to ``main.7now.tkt_payment`` rather than be dropped."""
    out = _trial20_resolve_canonical_target_table(
        raw_target="tkt_payment",
        resolved_tables=frozenset({"main.7now.tkt_payment"}),
    )
    assert out == "main.7now.tkt_payment"


def test_f2_ambiguous_tail_drops() -> None:
    """A bare ``users`` matching two catalogs is ambiguous; F2
    conservatively drops rather than guess."""
    out = _trial20_resolve_canonical_target_table(
        raw_target="users",
        resolved_tables=frozenset(
            {"main.a.users", "main.b.users"}
        ),
    )
    assert out == ""


def test_f2_unknown_tail_drops() -> None:
    out = _trial20_resolve_canonical_target_table(
        raw_target="not_a_table",
        resolved_tables=frozenset({"main.7now.tkt_payment"}),
    )
    assert out == ""


def test_f2_empty_returns_empty() -> None:
    assert (
        _trial20_resolve_canonical_target_table(
            raw_target="",
            resolved_tables=frozenset({"main.7now.tkt_payment"}),
        )
        == ""
    )


def test_f2_whitespace_returns_empty() -> None:
    assert (
        _trial20_resolve_canonical_target_table(
            raw_target="   ",
            resolved_tables=frozenset({"main.7now.tkt_payment"}),
        )
        == ""
    )


def test_f2_dotted_partial_tail_matches() -> None:
    """A schema-qualified-but-not-fully-qualified ``7now.tkt_payment``
    must still resolve when its tail is unique."""
    out = _trial20_resolve_canonical_target_table(
        raw_target="7now.tkt_payment",
        resolved_tables=frozenset({"main.7now.tkt_payment"}),
    )
    assert out == "main.7now.tkt_payment"
