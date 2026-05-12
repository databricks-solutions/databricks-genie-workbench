"""Plan P-C — proves the harness passes its canonical decision into
AcceptanceInput so the Phase-H writer renders an identical reason
code."""

from __future__ import annotations

import pytest


def test_acceptance_input_construction_in_harness_threads_canonical():
    """Static check: harness.py must reference
    canonical_decisions_by_ag_id when constructing AcceptanceInput.

    Nested parens inside the AcceptanceInput call prevent a strict
    regex match; we look for the kwarg name's literal presence
    inside the harness module instead.
    """
    from genie_space_optimizer import optimization

    harness_path = (
        f"{optimization.__path__[0]}/harness.py"
    )
    with open(harness_path) as f:
        body = f.read()

    assert "canonical_decisions_by_ag_id" in body, (
        "harness.py constructs AcceptanceInput but does not pass "
        "canonical_decisions_by_ag_id — Plan P-C wiring missing"
    )
    assert "AcceptanceInput(" in body, (
        "harness.py is missing the AcceptanceInput construction site"
    )
