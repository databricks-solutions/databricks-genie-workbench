"""Trial 16.3 — harvest helper for SM-lane TerminalRecord forbidden
signatures (Gap A + Gap C from the analyst's review).

Production reality before Trial 16.3:
    * The producer side of the typed-feedback channel is wired
      (applier_gate, evaluated_gate, acceptance_gate, synthesize_llm
      all set ``TerminalRecord.forbidden_signature`` to typed strings).
    * The harness loop reads ``_sm_final_states`` only for
      ``s.terminal.terminal_kind`` (the SM-vs-legacy equivalence
      marker) — ``s.terminal.forbidden_signature`` is dropped on the
      floor.
    * The legacy lane's ``_forbidden_set`` only grows via
      ``decide_iteration_terminal_action.add_to_forbidden_set`` at
      harness.py:33031, and the values are
      ``TerminalSignature`` NamedTuples (5 hashable fields).
    * ``TransformerContext.forbidden_signatures`` is typed
      ``tuple[str, ...]`` — the harness passes ``TerminalSignature``
      tuples there silently (Gap C: Python type hints aren't enforced).

This module owns the harvest contract so the harness loop can call a
single, well-tested helper instead of inlining the harvest logic
inside the 37k-line ``harness.py``. The helper is a pure function
over the SM final-state objects' ``terminal.forbidden_signature``
fields. Empty strings, whitespace-only strings, and missing terminals
are skipped; dedup is performed; output is a deterministic sorted
tuple suitable for cross-iteration carryover.

The helper does NOT touch the legacy ``_forbidden_set`` (which is a
``set[TerminalSignature]`` consumed by the legacy router) — it lives
in a separate ``set[str]`` dedicated to the SM lane.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.forbidden_signatures import (
    extend_sm_forbidden_signatures,
    harvest_sm_forbidden_signatures,
)


class _FakeTerminal:
    """Duck-typed stand-in for ``TerminalRecord`` with only the field
    the harvest helper looks at."""

    def __init__(self, forbidden_signature: str):
        self.forbidden_signature = forbidden_signature


class _FakeFinalState:
    """Duck-typed stand-in for ``QuestionStateInIteration`` with only
    the ``.terminal`` field the harvest helper looks at."""

    def __init__(self, terminal=None):
        self.terminal = terminal


def test_harvest_collects_terminal_forbidden_signature_strings():
    """The harvest must collect every non-empty
    ``terminal.forbidden_signature`` string from the SM final states."""
    states = (
        _FakeFinalState(_FakeTerminal(
            "add_column_description:dropped_no_op:missing_table"
        )),
        _FakeFinalState(_FakeTerminal(
            "update_column_description:dropped_no_op:missing_table"
        )),
    )
    harvested = harvest_sm_forbidden_signatures(states)
    assert harvested == (
        "add_column_description:dropped_no_op:missing_table",
        "update_column_description:dropped_no_op:missing_table",
    ), (
        "harvest must produce a deterministic sorted tuple of the "
        f"non-empty signature strings — got {harvested!r}"
    )


def test_harvest_skips_states_without_terminal():
    """States with ``terminal is None`` are still in-progress or
    accepted; they carry no signature to harvest."""
    states = (
        _FakeFinalState(terminal=None),
        _FakeFinalState(_FakeTerminal("X:y")),
        _FakeFinalState(terminal=None),
    )
    assert harvest_sm_forbidden_signatures(states) == ("X:y",)


def test_harvest_skips_empty_and_whitespace_only_signatures():
    """Empty / whitespace-only signature strings carry no information;
    they must be dropped so the consumer prompt isn't polluted with
    blanks."""
    states = (
        _FakeFinalState(_FakeTerminal("")),
        _FakeFinalState(_FakeTerminal("   ")),
        _FakeFinalState(_FakeTerminal("X:y")),
    )
    assert harvest_sm_forbidden_signatures(states) == ("X:y",)


def test_harvest_dedupes_identical_signatures_within_iteration():
    """Two states terminating with the same typed signature (e.g.
    cluster mates rejected by the same applier-gate reason) must
    produce a single entry — dedup is a property of the channel."""
    states = (
        _FakeFinalState(_FakeTerminal("A:b")),
        _FakeFinalState(_FakeTerminal("A:b")),
        _FakeFinalState(_FakeTerminal("C:d")),
    )
    assert harvest_sm_forbidden_signatures(states) == ("A:b", "C:d")


def test_harvest_handles_empty_state_list():
    """No states → no signatures."""
    assert harvest_sm_forbidden_signatures(()) == ()
    assert harvest_sm_forbidden_signatures(None) == ()


def test_extend_grows_the_running_set_across_iterations():
    """The cross-iteration accumulator must monotonically grow as more
    typed terminals fire. Order of insertion is irrelevant; the
    output is canonical (sorted)."""
    running: set[str] = set()
    extend_sm_forbidden_signatures(
        running,
        harvest_sm_forbidden_signatures(
            (_FakeFinalState(_FakeTerminal("A:b")),),
        ),
    )
    extend_sm_forbidden_signatures(
        running,
        harvest_sm_forbidden_signatures(
            (_FakeFinalState(_FakeTerminal("C:d")),),
        ),
    )
    # Re-harvest the same signature from a third iteration — must dedup.
    extend_sm_forbidden_signatures(
        running,
        harvest_sm_forbidden_signatures(
            (_FakeFinalState(_FakeTerminal("A:b")),),
        ),
    )
    assert sorted(running) == ["A:b", "C:d"]


def test_harvest_output_is_a_tuple_of_str_not_namedtuple():
    """Type contract: the harvest output is ``tuple[str, ...]``, which
    matches ``TransformerContext.forbidden_signatures: tuple[str, ...]``
    end-to-end. This pins Gap C of the analyst's review (legacy lane
    uses ``set[TerminalSignature]`` which silently violates the typed
    contract)."""
    states = (_FakeFinalState(_FakeTerminal("X:y")),)
    harvested = harvest_sm_forbidden_signatures(states)
    assert isinstance(harvested, tuple)
    assert all(isinstance(s, str) for s in harvested), (
        f"every element must be ``str`` — got {[type(s) for s in harvested]!r}"
    )
