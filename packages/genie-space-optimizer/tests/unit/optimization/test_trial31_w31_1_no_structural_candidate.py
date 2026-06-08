"""Trial 31 W31.1 — ``slate_lacks_structural_candidate`` predicate.

The airline W30.5 failure: the enforced switch (W30.1) correctly dropped
the re-emitted inert mechanism and the anti-success detector recommended a
structural ``sql_snippet`` (×2), but the forced-L6 / plan11 structural
synthesis DECLINED (`lever6_force_llm_declined`). With no structural patch
synthesised, an inert (`add_instruction` / `add_example_sql`) proposal
survived to application and tripped
``rca_mechanism_defaulted_to_instruction_text`` ->
``OPTIMIZER_INVARIANT_VIOLATION`` — which W31.3 now fails the whole task on.

This predicate is the apply-stage complement of the Track B / W4 routing
binders: it recognises an inert-only slate for a structural-mandate RCA so
the SM finalizer can emit a typed ``no_structural_candidate`` no-op instead
of applying the inert patch. It generalises across the entire
structural-mandate RCA family (keyed on the fixing map) and is a no-op for
RCAs that carry no structural mandate.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    slate_lacks_structural_candidate,
)


@pytest.mark.parametrize(
    "rca_kind,observed,expected",
    [
        # top_n mandates {SQL_SNIPPET, METADATA_DESCRIPTION}; an inert-only
        # slate (lone instruction) lacks a structural candidate.
        ("top_n_cardinality_collapse", [PatchMechanism.INSTRUCTION_TEXT], True),
        # example_sql is NOT structural for top_n either.
        ("top_n_cardinality_collapse", [PatchMechanism.EXAMPLE_SQL], True),
        # a structural companion present -> not lacking.
        ("top_n_cardinality_collapse", [PatchMechanism.SQL_SNIPPET], False),
        (
            "top_n_cardinality_collapse",
            [PatchMechanism.INSTRUCTION_TEXT, PatchMechanism.METADATA_DESCRIPTION],
            False,
        ),
        # wrong_column mandates {METADATA_DESCRIPTION} (INSTRUCTION_TEXT is
        # excluded from the structural set) — lone instruction lacks it.
        ("wrong_column", [PatchMechanism.INSTRUCTION_TEXT], True),
        ("wrong_column", [PatchMechanism.METADATA_DESCRIPTION], False),
        # An RCA kind OUTSIDE the fixing map carries no structural mandate;
        # the predicate must never force it to a no-op.
        ("some_unmapped_rca_kind", [PatchMechanism.INSTRUCTION_TEXT], False),
        ("some_unmapped_rca_kind", [], False),
        # None / empty mechanisms on a mandated RCA -> lacking.
        ("top_n_cardinality_collapse", [], True),
        ("top_n_cardinality_collapse", [None], True),
    ],
)
def test_trial31_w311_slate_lacks_structural_candidate(
    rca_kind, observed, expected,
) -> None:
    assert slate_lacks_structural_candidate(rca_kind, observed) is expected


def test_trial31_w311_none_rca_kind_is_never_lacking() -> None:
    # Defensive: a missing RCA kind carries no mandate.
    assert (
        slate_lacks_structural_candidate(None, [PatchMechanism.INSTRUCTION_TEXT])
        is False
    )
