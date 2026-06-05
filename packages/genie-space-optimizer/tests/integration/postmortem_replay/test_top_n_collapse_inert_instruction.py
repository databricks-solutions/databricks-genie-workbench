"""Track B / B0 — the inert-instruction false-accept probe.

The d139 / e943 postmortems showed the optimizer "fixing" a SQL-shape
RCA (``top_n_cardinality_collapse``) with a lone ``add_instruction``
(natural-language) patch. A free-text instruction does not change the
LLM's generated SQL *shape* for a ranking/top-N collapse — the planner
still aggregates at the wrong grain — so the applied patch yields
``accuracy_delta_pp == 0``. Under ``live-llm-only`` the post-apply eval
is stubbed, so the run reports a **false accept** and the inertness is
hidden.

This probe distills that false-accept deterministically (no live LLM):

  * the routing brain must recognise a lone ``add_instruction`` as
    behaviorally insufficient for a SQL-shape RCA (B1), and
  * a post-apply inertness detector must flag an applied instruction
    whose generated SQL shape is unchanged (B2).

It also pins the *negative* cases so the new machinery cannot over-fire:
an instruction paired with a SQL-shaping mechanism, or an instruction
that actually changed the SQL shape, must NOT be flagged.

Fails today (the predicate, the alias, and the detector do not exist);
goes green once B1 (routing) and B2 (inertness detector) land.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    instruction_text_is_insufficient_for,
    rca_instruction_default_reason,
)
from genie_space_optimizer.optimization.sql_shape_inertness import (
    detect_applied_but_inert,
    sql_shape_signature,
)


# The plan's shorthand RCA label and its canonical routing-brain key.
_TOP_N_SHORTHAND = "top_n_collapse"
_TOP_N_CANONICAL = "top_n_cardinality_collapse"

# A representative top-N / ranking query and its (literal-only) variant —
# same SQL *shape*, only a literal changed. Inert for the RCA.
_SQL_BEFORE = (
    "SELECT carrier, COUNT(*) AS n FROM flights "
    "GROUP BY carrier ORDER BY n DESC LIMIT 5"
)
_SQL_LITERAL_ONLY_CHANGE = (
    "SELECT carrier, COUNT(*) AS n FROM flights "
    "GROUP BY carrier ORDER BY n DESC LIMIT 10"
)
# A genuinely re-shaped query: the grain changed (window function instead
# of a flat GROUP BY + LIMIT). NOT inert.
_SQL_RESHAPED = (
    "SELECT carrier, n FROM (SELECT carrier, COUNT(*) AS n, "
    "ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rk FROM flights "
    "GROUP BY carrier) t WHERE rk <= 5"
)


def test_top_n_collapse_shorthand_resolves_to_instruction_insufficient():
    """B1: the plan shorthand and the canonical key are both SQL-shape
    RCAs where a lone natural-language instruction is inert."""
    assert instruction_text_is_insufficient_for(_TOP_N_SHORTHAND) is True
    assert instruction_text_is_insufficient_for(_TOP_N_CANONICAL) is True


def test_lone_add_instruction_for_top_n_collapse_fires_inert_reason():
    """B1: a slate that reached for INSTRUCTION_TEXT only (the false-fix)
    must yield a typed inert-default reason naming the RCA."""
    reason = rca_instruction_default_reason(
        _TOP_N_SHORTHAND,
        {PatchMechanism.INSTRUCTION_TEXT},
    )
    assert reason == (
        "rca_mechanism_defaulted_to_instruction_text:"
        "rca=top_n_cardinality_collapse"
    )


def test_instruction_paired_with_sql_shaping_mechanism_is_admissible():
    """B1 negative: instruction + a fixing mechanism (sql_snippet) is a
    legitimate multi-lever bundle, not a lone-instruction default."""
    assert (
        rca_instruction_default_reason(
            _TOP_N_SHORTHAND,
            {PatchMechanism.INSTRUCTION_TEXT, PatchMechanism.SQL_SNIPPET},
        )
        == ""
    )


def test_lone_instruction_for_defensive_filter_fires_inert_reason():
    """B1: ``extra_defensive_filter`` is a SQL-shape RCA — the e943
    ``live-llm-only`` run proved a lone ``add_instruction`` left the SQL
    shape unchanged (``behavioral_diff=unchanged``) and phantom-accepted.
    A lone instruction must therefore fire the inert reason; it is only
    admissible when paired with the structural companion (sql_snippet)."""
    assert instruction_text_is_insufficient_for("extra_defensive_filter") is (
        True
    )
    assert (
        rca_instruction_default_reason(
            "defensive_filter",
            {PatchMechanism.INSTRUCTION_TEXT},
        )
        == (
            "rca_mechanism_defaulted_to_instruction_text:"
            "rca=extra_defensive_filter"
        )
    )
    # Paired with the structural companion → admissible (no reason).
    assert (
        rca_instruction_default_reason(
            "defensive_filter",
            {PatchMechanism.INSTRUCTION_TEXT, PatchMechanism.SQL_SNIPPET},
        )
        == ""
    )


def test_applied_instruction_with_unchanged_sql_shape_is_inert():
    """B2: an applied lone instruction for a SQL-shape RCA whose
    generated SQL shape is unchanged (only a literal differs) is the
    phantom accept — flag it ``applied_but_inert``."""
    assert (
        detect_applied_but_inert(
            rca_kind=_TOP_N_SHORTHAND,
            mechanisms={PatchMechanism.INSTRUCTION_TEXT},
            sql_before=_SQL_BEFORE,
            sql_after=_SQL_LITERAL_ONLY_CHANGE,
        )
        is True
    )


def test_inertness_silent_when_sql_shape_actually_changed():
    """B2 negative: a real re-shape (window-function rewrite) is not
    inert even under a lone instruction."""
    assert (
        detect_applied_but_inert(
            rca_kind=_TOP_N_SHORTHAND,
            mechanisms={PatchMechanism.INSTRUCTION_TEXT},
            sql_before=_SQL_BEFORE,
            sql_after=_SQL_RESHAPED,
        )
        is False
    )


def test_inertness_silent_when_sql_shaping_mechanism_present():
    """B2 negative: when a SQL-shaping mechanism is in the slate, an
    unchanged shape is a different (coverage) concern, not the
    lone-instruction phantom — do not flag it here."""
    assert (
        detect_applied_but_inert(
            rca_kind=_TOP_N_SHORTHAND,
            mechanisms={
                PatchMechanism.INSTRUCTION_TEXT,
                PatchMechanism.SQL_SNIPPET,
            },
            sql_before=_SQL_BEFORE,
            sql_after=_SQL_BEFORE,
        )
        is False
    )


def test_inertness_fires_for_defensive_filter_lone_instruction():
    """B2: a lone instruction for ``extra_defensive_filter`` with an
    unchanged SQL shape is the e943 phantom accept — flag it. (Paired
    with a structural companion it would be a coverage concern, covered
    by ``test_inertness_silent_when_sql_shaping_mechanism_present``.)"""
    assert (
        detect_applied_but_inert(
            rca_kind="extra_defensive_filter",
            mechanisms={PatchMechanism.INSTRUCTION_TEXT},
            sql_before=_SQL_BEFORE,
            sql_after=_SQL_BEFORE,
        )
        is True
    )


def test_inertness_silent_for_unmapped_rca():
    """B2 negative: an instruction for an RCA outside the SQL-shape
    contract has no inertness obligation, regardless of shape stability."""
    assert (
        detect_applied_but_inert(
            rca_kind="some_unmapped_rca",
            mechanisms={PatchMechanism.INSTRUCTION_TEXT},
            sql_before=_SQL_BEFORE,
            sql_after=_SQL_BEFORE,
        )
        is False
    )


def test_sql_shape_signature_ignores_literals_and_whitespace():
    """The shape signature must treat literal-only / whitespace-only
    edits as the same shape, but a structural rewrite as different."""
    assert sql_shape_signature(_SQL_BEFORE) == sql_shape_signature(
        _SQL_LITERAL_ONLY_CHANGE
    )
    assert sql_shape_signature(_SQL_BEFORE) != sql_shape_signature(
        _SQL_RESHAPED
    )
